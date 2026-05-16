"""
Selenium 기반 유튜브 멤버십 게시글 크롤러.

전체 흐름:
  1. Docker Selenium 컨테이너에 Remote WebDriver로 연결
  2. .env의 유튜브 쿠키를 주입해서 로그인 상태 만들기
  3. 채널 커뮤니티 탭에서 멤버십 전용 게시글만 수집
  4. Redis로 이미 처리한 게시글 중복 제거
  5. PostgreSQL에 저장 (이미지 URL, 링크도 함께)
  6. Kafka post.new 토픽으로 분석기에 알림 발행
  7. CRAWL_INTERVAL_SECONDS 간격으로 반복
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys

# Windows에서 asyncio 이벤트 루프 정책 문제 방지
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

# ── 환경 변수 ─────────────────────────────────────────────────
CHANNEL_ID      = os.environ["YOUTUBE_CHANNEL_ID"]        # 크롤링할 유튜브 채널 ID (예: @Mr_anything)
SESSION_COOKIE  = os.environ["YOUTUBE_SESSION_COOKIE"]    # 유튜브 로그인 쿠키 문자열
CRAWL_INTERVAL  = int(os.environ.get("CRAWL_INTERVAL_SECONDS", "60"))  # 크롤링 주기(초)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC_POST_NEW", "post.new")   # 새 게시글 알림 토픽
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL    = os.environ["DATABASE_URL"]
SELENIUM_URL    = os.environ.get("SELENIUM_URL", "http://selenium:4444/wd/hub")  # Docker Selenium 주소

# 게시글 본문에서 URL을 추출하는 정규식
_URL_PATTERN = re.compile(r'https?://[^\s\]\)>\"\']+')


def _extract_links(text: str) -> list[dict]:
    """
    게시글 본문에서 URL을 추출하고 종류를 분류한다.

    반환 예시:
      [{"url": "https://tradingview.com/...", "link_type": "tradingview"}, ...]
    """
    links = []
    for url in _URL_PATTERN.findall(text):
        if "tradingview.com" in url:
            link_type = "tradingview"   # 차트 분석 링크
        elif "youtube.com" in url or "youtu.be" in url:
            link_type = "youtube"       # 유튜브 영상 링크
        else:
            link_type = "other"
        links.append({"url": url, "link_type": link_type})
    return links


# ── 드라이버 ─────────────────────────────────────────────────

def _build_driver() -> webdriver.Remote:
    """
    Docker Selenium 컨테이너에 Remote WebDriver로 연결한다.
    로컬에 Chrome이 없어도 Docker 컨테이너의 Chrome을 원격으로 제어한다.
    """
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--disable-blink-features=AutomationControlled")   # 봇 감지 우회
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return webdriver.Remote(command_executor=SELENIUM_URL, options=opts)


# youtube.com에서만 유효한 쿠키 목록 (google.com에 주입하면 오류 발생)
_YOUTUBE_ONLY_COOKIES = {"LOGIN_INFO", "YSC", "VISITOR_INFO1_LIVE", "PREF", "GPS"}


def _inject_cookies(driver: webdriver.Remote) -> None:
    """
    .env의 SESSION_COOKIE를 브라우저에 주입해서 로그인 상태를 만든다.

    쿠키는 도메인별로 나눠서 주입해야 한다:
      - google.com 계정 쿠키 → .google.com 도메인에 주입
      - youtube.com 전용 쿠키 → .youtube.com 도메인에 주입

    __Secure- 또는 __Host- 접두사 쿠키는 secure=True 플래그가 없으면
    브라우저가 무시하므로 반드시 설정해야 한다.
    """
    # 쿠키 문자열("name=value; name2=value2; ...") → (name, value) 리스트로 파싱
    pairs = []
    for pair in SESSION_COOKIE.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        name, _, value = pair.partition("=")
        pairs.append((name.strip(), value.strip()))

    def _add_cookies(domain: str) -> None:
        """해당 도메인에 쿠키를 주입한다. 실패한 쿠키는 조용히 건너뜀."""
        for name, value in pairs:
            cookie: dict = {"name": name, "value": value, "domain": domain}
            if name.startswith("__Secure-") or name.startswith("__Host-"):
                cookie["secure"] = True  # HTTPS 전용 쿠키에 필수 플래그
            try:
                driver.add_cookie(cookie)
            except Exception:
                pass

    # 1단계: google.com 계정 쿠키 주입 (SID, HSID, SSID 등)
    driver.get("https://www.google.com")
    _add_cookies(".google.com")

    # 2단계: youtube.com 전용 쿠키 추가 주입 (LOGIN_INFO 등)
    driver.get("https://www.youtube.com")
    _add_cookies(".youtube.com")

    # 쿠키 적용 후 새로고침해야 로그인 상태가 반영됨
    driver.refresh()


# ── 스크래핑 ─────────────────────────────────────────────────

def _scrape_posts(driver: webdriver.Remote) -> list[dict]:
    """
    채널 커뮤니티 탭에서 최신 게시글 최대 10개를 수집한다.

    수집 내용:
      - 멤버십 전용 게시글만 (span#sponsors-only-badge 뱃지 확인)
      - 본문 텍스트
      - 이미지 URL 목록
      - 영상 첨부 여부 → post_type 결정
      - 본문 내 링크 (트레이딩뷰, 유튜브, 기타)
    """
    driver.get(f"https://www.youtube.com/{CHANNEL_ID}/posts")

    # 게시글 목록이 로드될 때까지 최대 15초 대기
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ytd-backstage-post-thread-renderer")
            )
        )
    except Exception:
        return []  # 타임아웃 시 빈 리스트 반환

    posts = []
    elements = driver.find_elements(By.CSS_SELECTOR, "ytd-backstage-post-thread-renderer")
    for el in elements[:10]:  # 최신 10개만 처리
        try:
            # 멤버십 전용 뱃지가 없으면 일반 게시글이므로 건너뜀
            if not el.find_elements(By.CSS_SELECTOR, "span#sponsors-only-badge"):
                continue

            # 본문 텍스트와 게시글 고유 ID 추출
            content  = el.find_element(By.CSS_SELECTOR, "#content-text").text.strip()
            time_el  = el.find_element(By.CSS_SELECTOR, "#published-time-text a")
            post_url = time_el.get_attribute("href") or ""
            # URL 끝부분이 게시글 ID (예: ?lb=abc123 → abc123)
            post_id  = post_url.split("=")[-1] if "=" in post_url else post_url.split("/")[-1]
            if not content or not post_id:
                continue

            # 게시글에 첨부된 이미지 URL 수집
            image_urls = []
            for img in el.find_elements(By.CSS_SELECTOR, "ytd-backstage-image-renderer img, #backstage-image img"):
                src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                if src.startswith("http"):
                    image_urls.append(src)

            # 영상 첨부 여부로 게시글 타입 결정
            # video: 유튜브 영상 링크 첨부 / mixed: 이미지 첨부 / text: 텍스트만
            is_video = bool(el.find_elements(
                By.CSS_SELECTOR,
                "ytd-video-renderer, ytd-backstage-video-renderer, ytd-playlist-renderer",
            ))
            post_type = "video" if is_video else ("mixed" if image_urls else "text")

            # 본문에서 트레이딩뷰·유튜브 링크 등 추출
            links = _extract_links(content)

            posts.append({
                "post_id":    post_id,
                "content":    content,
                "channel_id": CHANNEL_ID,
                "image_urls": image_urls,
                "post_type":  post_type,
                "links":      links,
            })
        except Exception:
            continue  # 개별 게시글 파싱 실패 시 나머지 계속 진행
    return posts


# ── DB 저장 ──────────────────────────────────────────────────

def _db_save_sync(posts: list[dict]) -> list[tuple[int, dict]]:
    """
    psycopg2(동기)로 posts, post_links 테이블에 저장한다.
    새로 삽입된 게시글의 (db_id, post_dict) 목록을 반환한다.

    asyncpg / psycopg3 async 드라이버는 Windows에서 이벤트 루프 충돌이 있어
    동기 psycopg2를 run_in_executor로 실행한다.
    """
    import psycopg2
    from urllib.parse import urlparse

    # SQLAlchemy URL 형식 → psycopg2 표준 URL로 변환
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
    saved: list[tuple[int, dict]] = []
    try:
        with conn.cursor() as cur:
            for post in posts:
                # 동일 post_id가 이미 있으면 무시 (ON CONFLICT DO NOTHING)
                cur.execute(
                    "INSERT INTO posts (channel_id, post_id, content, post_type, image_urls, published_at) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb, NOW()) "
                    "ON CONFLICT (post_id) DO NOTHING RETURNING id",
                    (
                        post["channel_id"],
                        post["post_id"],
                        post["content"],
                        post["post_type"],
                        json.dumps(post["image_urls"]),  # 리스트 → JSON 문자열
                    ),
                )
                row = cur.fetchone()
                if row:
                    db_id = row[0]
                    # 추출된 링크를 post_links 테이블에 저장
                    for link in post.get("links", []):
                        cur.execute(
                            "INSERT INTO post_links (post_id, url, link_type) VALUES (%s, %s, %s)",
                            (db_id, link["url"], link["link_type"]),
                        )
                    saved.append((db_id, post))
                    print(f"[crawler] 저장: {post['post_id']} ({post['post_type']}, 이미지 {len(post['image_urls'])}개, 링크 {len(post['links'])}개)")
        conn.commit()
    finally:
        conn.close()
    return saved


# ── 파이프라인 ────────────────────────────────────────────────

async def _pipeline(posts: list[dict]) -> None:
    """
    Redis 중복 필터 → DB 저장 → Kafka 발행 순서로 처리한다.

    Redis 키 형식: "post:{post_id}" (7일 TTL)
    Kafka 메시지: {"post_id": db_id, "channel_id": ..., "post_type": ..., "image_urls": [...]}
    """
    import redis.asyncio as aioredis
    from aiokafka import AIOKafkaProducer

    r = aioredis.from_url(REDIS_URL)

    # Redis에 이미 키가 있는 게시글은 제외 (중복 처리 방지)
    new_posts = []
    for p in posts:
        if not await r.exists(f"post:{p['post_id']}"):
            new_posts.append(p)

    if not new_posts:
        print("[crawler] 새 게시글 없음 (전부 중복)")
        await r.aclose()
        return

    # DB 저장은 동기 함수이므로 executor에서 실행 (이벤트 루프 블로킹 방지)
    loop = asyncio.get_event_loop()
    saved = await loop.run_in_executor(None, _db_save_sync, new_posts)

    # 저장된 게시글을 Kafka로 발행해서 분석기(gpt_analyzer)에 알림
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        for db_id, post in saved:
            # 분석기가 이미지 URL과 post_type을 바로 활용할 수 있도록 함께 전송
            await producer.send_and_wait(
                KAFKA_TOPIC,
                json.dumps({
                    "post_id":    db_id,
                    "channel_id": post["channel_id"],
                    "post_type":  post["post_type"],
                    "image_urls": post["image_urls"],
                }).encode(),
            )
        # 처리 완료된 게시글을 Redis에 마킹 (7일간 중복 방지)
        for p in new_posts:
            await r.set(f"post:{p['post_id']}", "1", ex=60 * 60 * 24 * 7)
    finally:
        await producer.stop()
        await r.aclose()


# ── 실행 루프 ─────────────────────────────────────────────────

async def run_once() -> None:
    """드라이버 생성 → 쿠키 주입 → 스크래핑 → 파이프라인을 한 번 실행한다."""
    driver = _build_driver()
    try:
        _inject_cookies(driver)
        posts = _scrape_posts(driver)
        print(f"[crawler] 수집: {len(posts)}개")
        if posts:
            await _pipeline(posts)
    finally:
        driver.quit()  # 항상 드라이버 종료 (메모리 누수 방지)


async def run_loop() -> None:
    """크롤러를 CRAWL_INTERVAL_SECONDS 간격으로 무한 반복 실행한다."""
    print(f"[crawler] 시작 — {CRAWL_INTERVAL}초 간격")
    while True:
        try:
            await run_once()
        except Exception as e:
            print(f"[crawler] 에러: {e}")
        await asyncio.sleep(CRAWL_INTERVAL)  # 다음 크롤링까지 대기


if __name__ == "__main__":
    asyncio.run(run_loop())
