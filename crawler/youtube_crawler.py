"""
Selenium 기반 유튜브 멤버십 게시글 크롤러.
CRAWL_INTERVAL_SECONDS 간격으로 폴링 → Redis 중복 필터 → DB 저장 → Kafka post.new 발행.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

CHANNEL_ID     = os.environ["YOUTUBE_CHANNEL_ID"]
SESSION_COOKIE = os.environ["YOUTUBE_SESSION_COOKIE"]
CRAWL_INTERVAL = int(os.environ.get("CRAWL_INTERVAL_SECONDS", "60"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC_POST_NEW", "post.new")
REDIS_URL      = os.environ.get("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL   = os.environ["DATABASE_URL"]
SELENIUM_URL   = os.environ.get("SELENIUM_URL", "http://selenium:4444/wd/hub")


# ── 드라이버 ─────────────────────────────────────────────────

def _build_driver() -> webdriver.Remote:
    """Selenium 컨테이너에 Remote WebDriver로 연결한다."""
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return webdriver.Remote(command_executor=SELENIUM_URL, options=opts)


# youtube.com 전용 쿠키 (LOGIN_INFO, YSC, VISITOR_INFO1_LIVE 등)
_YOUTUBE_ONLY_COOKIES = {"LOGIN_INFO", "YSC", "VISITOR_INFO1_LIVE", "PREF", "GPS"}


def _inject_cookies(driver: webdriver.Remote) -> None:
    """
    환경 변수의 쿠키를 도메인에 맞게 나눠서 주입한다.
    __Secure- 접두사 쿠키는 secure=True 플래그가 필수이며, 없으면 브라우저가 무시한다.
    """
    pairs = []
    for pair in SESSION_COOKIE.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        name, _, value = pair.partition("=")
        pairs.append((name.strip(), value.strip()))

    def _add_cookies(domain: str) -> None:
        for name, value in pairs:
            cookie: dict = {"name": name, "value": value, "domain": domain}
            if name.startswith("__Secure-") or name.startswith("__Host-"):
                cookie["secure"] = True
            try:
                driver.add_cookie(cookie)
            except Exception:
                pass

    # google.com 쿠키 주입
    driver.get("https://www.google.com")
    _add_cookies(".google.com")

    # youtube.com 쿠키 주입
    driver.get("https://www.youtube.com")
    _add_cookies(".youtube.com")

    driver.refresh()


# ── 스크래핑 ─────────────────────────────────────────────────

def _scrape_posts(driver: webdriver.Remote) -> list[dict]:
    """채널 커뮤니티 탭에서 최신 게시글 최대 10개를 추출한다."""
    driver.get(f"https://www.youtube.com/{CHANNEL_ID}/posts")
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ytd-backstage-post-thread-renderer")
            )
        )
    except Exception:
        return []

    posts = []
    elements = driver.find_elements(By.CSS_SELECTOR, "ytd-backstage-post-thread-renderer")
    for el in elements[:10]:
        try:
            # 멤버십 전용 뱃지(span#sponsors-only-badge)가 없는 게시물은 건너뜀
            if not el.find_elements(By.CSS_SELECTOR, "span#sponsors-only-badge"):
                continue

            content  = el.find_element(By.CSS_SELECTOR, "#content-text").text.strip()
            time_el  = el.find_element(By.CSS_SELECTOR, "#published-time-text a")
            post_url = time_el.get_attribute("href") or ""
            post_id  = post_url.split("=")[-1] if "=" in post_url else post_url.split("/")[-1]
            if content and post_id:
                posts.append({
                    "post_id":    post_id,
                    "content":    content,
                    "channel_id": CHANNEL_ID,
                })
        except Exception:
            continue
    return posts


# ── DB 저장 ──────────────────────────────────────────────────

def _db_save_sync(posts: list[dict]) -> list[tuple[int, str]]:
    """
    psycopg2(동기)로 posts 테이블에 저장하고 새로 삽입된 (db_id, channel_id) 목록을 반환한다.
    asyncpg / psycopg3 Windows 버그를 우회하기 위해 동기 드라이버를 사용한다.
    """
    import psycopg2
    from urllib.parse import urlparse

    # postgresql+asyncpg:// 또는 postgresql+psycopg:// → psycopg2 표준 URL로 변환
    url = (DATABASE_URL
           .replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgresql+psycopg://",  "postgresql://"))
    p = urlparse(url)

    conn = psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        dbname=p.path.lstrip("/"),
        options="-c client_encoding=UTF8",
    )
    saved: list[tuple[int, str]] = []
    try:
        with conn.cursor() as cur:
            for post in posts:
                cur.execute(
                    "INSERT INTO posts (channel_id, post_id, content, published_at) "
                    "VALUES (%s, %s, %s, NOW()) "
                    "ON CONFLICT (post_id) DO NOTHING RETURNING id",
                    (post["channel_id"], post["post_id"], post["content"]),
                )
                row = cur.fetchone()
                if row:
                    saved.append((row[0], post["channel_id"]))
                    print(f"[crawler] 저장: {post['post_id']}")
        conn.commit()
    finally:
        conn.close()
    return saved


# ── 파이프라인 ────────────────────────────────────────────────

async def _pipeline(posts: list[dict]) -> None:
    """Redis 중복 필터 → DB 저장 → Kafka 발행."""
    import redis.asyncio as aioredis
    from aiokafka import AIOKafkaProducer

    r = aioredis.from_url(REDIS_URL)

    # 이미 처리된 게시글 제외
    new_posts = []
    for p in posts:
        if not await r.exists(f"post:{p['post_id']}"):
            new_posts.append(p)

    if not new_posts:
        print("[crawler] 새 게시글 없음 (전부 중복)")
        await r.aclose()
        return

    # psycopg2 동기 저장 (executor에서 실행)
    loop = asyncio.get_event_loop()
    saved = await loop.run_in_executor(None, _db_save_sync, new_posts)

    # Kafka 발행 + Redis 중복 마킹
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        for db_id, channel_id in saved:
            await producer.send_and_wait(
                KAFKA_TOPIC,
                json.dumps({"post_id": db_id, "channel_id": channel_id}).encode(),
            )
        for p in new_posts:
            await r.set(f"post:{p['post_id']}", "1", ex=60 * 60 * 24 * 7)
    finally:
        await producer.stop()
        await r.aclose()


# ── 실행 루프 ─────────────────────────────────────────────────

async def run_once() -> None:
    """크롤링을 한 번 실행한다."""
    driver = _build_driver()
    try:
        _inject_cookies(driver)
        posts = _scrape_posts(driver)
        print(f"[crawler] 수집: {len(posts)}개")
        if posts:
            await _pipeline(posts)
    finally:
        driver.quit()


async def run_loop() -> None:
    """크롤러를 CRAWL_INTERVAL_SECONDS 간격으로 반복 실행한다."""
    print(f"[crawler] 시작 — {CRAWL_INTERVAL}초 간격")
    while True:
        try:
            await run_once()
        except Exception as e:
            print(f"[crawler] 에러: {e}")
        await asyncio.sleep(CRAWL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run_loop())
