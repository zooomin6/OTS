"""
1개월 백필 크롤러 — 최초 1회 실행용.

채널 커뮤니티 탭을 스크롤하며 최근 1개월 멤버십 게시글을 수집한다.
수집된 게시글은 DB + Kafka에 적재되어 GPT 분석기가 순서대로 처리한다.

실행:
  docker compose exec crawler python crawler/backfill.py
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv

load_dotenv()

CHANNEL_ID      = os.environ["YOUTUBE_CHANNEL_ID"]
SESSION_COOKIE  = os.environ["YOUTUBE_SESSION_COOKIE"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC_POST_NEW", "post.new")
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL    = os.environ["DATABASE_URL"]
SELENIUM_URL    = os.environ.get("SELENIUM_URL", "http://selenium:4444/wd/hub")

BACKFILL_DAYS   = 180  # 수집 기간 (일)
SCROLL_PAUSE    = 2.0  # 스크롤 후 대기 (초)
MAX_SCROLLS     = 600  # 무한루프 방지 상한

_URL_PATTERN = re.compile(r'https?://[^\s\]\)>\"\']+')


def _parse_relative_date(text: str) -> datetime | None:
    """
    유튜브 상대 시간 텍스트 → datetime 변환.
    예: "3주 전" → 21일 전, "1개월 전" → 30일 전
    """
    now = datetime.now()
    text = text.strip()

    patterns = [
        (r"(\d+)\s*분\s*전",   lambda n: now - timedelta(minutes=n)),
        (r"(\d+)\s*시간\s*전", lambda n: now - timedelta(hours=n)),
        (r"(\d+)\s*일\s*전",   lambda n: now - timedelta(days=n)),
        (r"(\d+)\s*주\s*전",   lambda n: now - timedelta(weeks=n)),
        (r"(\d+)\s*개월\s*전", lambda n: now - timedelta(days=n * 30)),
        (r"(\d+)\s*년\s*전",   lambda n: now - timedelta(days=n * 365)),
        # 영어 fallback
        (r"(\d+)\s*minute",  lambda n: now - timedelta(minutes=n)),
        (r"(\d+)\s*hour",    lambda n: now - timedelta(hours=n)),
        (r"(\d+)\s*day",     lambda n: now - timedelta(days=n)),
        (r"(\d+)\s*week",    lambda n: now - timedelta(weeks=n)),
        (r"(\d+)\s*month",   lambda n: now - timedelta(days=n * 30)),
        (r"(\d+)\s*year",    lambda n: now - timedelta(days=n * 365)),
    ]
    for pattern, calc in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return calc(int(m.group(1)))

    if "방금" in text or "just now" in text.lower():
        return now

    return None


def _extract_links(text: str) -> list[dict]:
    links = []
    for url in _URL_PATTERN.findall(text):
        if "tradingview.com" in url:
            link_type = "tradingview"
        elif "youtube.com" in url or "youtu.be" in url:
            link_type = "youtube"
        else:
            link_type = "other"
        links.append({"url": url, "link_type": link_type})
    return links


def _build_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return webdriver.Remote(command_executor=SELENIUM_URL, options=opts)


def _inject_cookies(driver) -> None:
    pairs = []
    for pair in SESSION_COOKIE.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        name, _, value = pair.partition("=")
        pairs.append((name.strip(), value.strip()))

    def _add(domain):
        for name, value in pairs:
            cookie: dict = {"name": name, "value": value, "domain": domain}
            if name.startswith("__Secure-") or name.startswith("__Host-"):
                cookie["secure"] = True
            try:
                driver.add_cookie(cookie)
            except Exception:
                pass

    driver.get("https://www.google.com")
    _add(".google.com")
    driver.get("https://www.youtube.com")
    _add(".youtube.com")
    driver.refresh()


def _check_login(driver) -> bool:
    """
    YouTube 로그인 상태 확인.
    로그인 안 되어 있으면 NoVNC 수동 로그인 대기.
    """
    from selenium.webdriver.common.by import By

    driver.get("https://www.youtube.com")
    time.sleep(3)

    try:
        driver.find_element(By.CSS_SELECTOR, "button#avatar-btn, yt-icon-button#avatar-btn")
        print("[backfill] 로그인 확인됨")
        return True
    except Exception:
        pass

    print()
    print("=" * 60)
    print("[backfill] 로그인이 필요합니다.")
    print("  1. 브라우저에서 http://localhost:7900 으로 접속하세요")
    print("  2. 열린 Chrome에서 YouTube에 로그인하세요 (구글 계정)")
    print("  3. 로그인 완료 후 여기서 Enter를 눌러주세요")
    print("=" * 60)
    try:
        input("Enter 키를 눌러 계속... ")
    except EOFError:
        pass

    # 재확인
    try:
        driver.find_element(By.CSS_SELECTOR, "button#avatar-btn, yt-icon-button#avatar-btn")
        print("[backfill] 로그인 확인됨")
    except Exception:
        print("[backfill] 아바타 확인 못 했지만 계속 진행합니다")
    return True


def _extract_cookies(driver) -> None:
    """로그인 성공 후 쿠키를 추출해 출력 — 다음 실행 때 .env에 붙여넣기용."""
    important = {
        "SID", "SSID", "HSID", "APISID", "SAPISID",
        "__Secure-3PSID", "__Secure-3PAPISID",
        "__Secure-1PSID", "__Secure-1PAPISID",
        "LOGIN_INFO", "YSC", "VISITOR_INFO1_LIVE",
    }
    pairs = [
        f"{c['name']}={c['value']}"
        for c in driver.get_cookies()
        if c["name"] in important
    ]
    if pairs:
        print("\n[backfill] ── 다음 세션을 위해 .env에 저장하세요 ──")
        print(f"YOUTUBE_SESSION_COOKIE={'; '.join(pairs)}")
        print("─" * 60)


def _scrape_all(driver) -> list[dict]:
    """
    커뮤니티 탭을 스크롤하며 BACKFILL_DAYS 이내 게시글을 모두 수집한다.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    cutoff = datetime.now() - timedelta(days=BACKFILL_DAYS)

    driver.get(f"https://www.youtube.com/{CHANNEL_ID}/posts")
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ytd-backstage-post-thread-renderer")
            )
        )
    except Exception:
        print("[backfill] 커뮤니티 탭 로드 실패")
        return []

    seen_ids: set[str] = set()
    posts: list[dict] = []
    stop_scrolling = False

    for scroll_n in range(MAX_SCROLLS):
        elements = driver.find_elements(By.CSS_SELECTOR, "ytd-backstage-post-thread-renderer")

        if scroll_n == 0:
            print(f"[backfill] 게시글 요소 발견: {len(elements)}개")

        for el in elements:
            try:

                time_el  = el.find_element(By.CSS_SELECTOR, "#published-time-text a")
                post_url = time_el.get_attribute("href") or ""
                post_id  = post_url.split("=")[-1] if "=" in post_url else post_url.split("/")[-1]

                if not post_id or post_id in seen_ids:
                    continue

                # 날짜 파싱 — 컷오프보다 오래된 게시글은 건너뜀
                date_text = time_el.text.strip()
                post_date = _parse_relative_date(date_text)
                print(f"[backfill] 날짜: '{date_text}' → {post_date}")
                if post_date and post_date < cutoff:
                    continue  # 핀 고정 등 오래된 게시글 건너뜀

                content = el.find_element(By.CSS_SELECTOR, "#content-text").text.strip()
                if not content:
                    continue

                image_urls = []
                for img in el.find_elements(By.CSS_SELECTOR, "ytd-backstage-image-renderer img, #backstage-image img"):
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if src.startswith("http"):
                        image_urls.append(src)

                is_video = bool(el.find_elements(
                    By.CSS_SELECTOR,
                    "ytd-video-renderer, ytd-backstage-video-renderer, ytd-playlist-renderer",
                ))
                post_type = "video" if is_video else ("mixed" if image_urls else "text")

                seen_ids.add(post_id)
                posts.append({
                    "post_id":    post_id,
                    "content":    content,
                    "channel_id": CHANNEL_ID,
                    "image_urls": image_urls,
                    "post_type":  post_type,
                    "links":      _extract_links(content),
                    "published_at": post_date or datetime.now(),
                })
            except Exception as e:
                print(f"[backfill] 파싱 에러: {e}")
                continue

        if stop_scrolling:
            break

        # 페이지 끝까지 스크롤
        prev_height = driver.execute_script("return document.documentElement.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        new_height = driver.execute_script("return document.documentElement.scrollHeight")

        if new_height == prev_height:
            print("[backfill] 더 이상 로드할 게시글 없음")
            break

        print(f"[backfill] 스크롤 {scroll_n + 1} — 현재 {len(posts)}개 수집")

    return posts


def _db_save(posts: list[dict]) -> list[tuple[int, dict]]:
    import psycopg2
    from urllib.parse import urlparse

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
                cur.execute(
                    "INSERT INTO posts (channel_id, post_id, content, post_type, image_urls, published_at) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb, %s) "
                    "ON CONFLICT (post_id) DO NOTHING RETURNING id",
                    (
                        post["channel_id"],
                        post["post_id"],
                        post["content"],
                        post["post_type"],
                        json.dumps(post["image_urls"]),
                        post["published_at"],
                    ),
                )
                row = cur.fetchone()
                if row:
                    db_id = row[0]
                    for link in post.get("links", []):
                        cur.execute(
                            "INSERT INTO post_links (post_id, url, link_type) VALUES (%s, %s, %s)",
                            (db_id, link["url"], link["link_type"]),
                        )
                    saved.append((db_id, post))
                    print(f"[backfill] DB 저장: {post['post_id']} ({post['post_type']})")
        conn.commit()
    finally:
        conn.close()
    return saved


async def _publish_kafka(saved: list[tuple[int, dict]]) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        for db_id, post in saved:
            await producer.send_and_wait(
                KAFKA_TOPIC,
                json.dumps({
                    "post_id":    db_id,
                    "channel_id": post["channel_id"],
                    "post_type":  post["post_type"],
                    "image_urls": post["image_urls"],
                }).encode(),
            )
        print(f"[backfill] Kafka 발행 완료: {len(saved)}건")
    finally:
        await producer.stop()


async def main() -> None:
    print(f"[backfill] 시작 — 최근 {BACKFILL_DAYS}일 게시글 수집")

    driver = _build_driver()
    try:
        _inject_cookies(driver)
        _check_login(driver)
        posts = _scrape_all(driver)
        _extract_cookies(driver)
    finally:
        driver.quit()

    print(f"[backfill] 수집 완료: {len(posts)}개")
    if not posts:
        print("[backfill] 수집된 게시글 없음.")
        return

    # 날짜 오름차순 정렬 (오래된 것부터 분석하여 시나리오 흐름 파악)
    posts.sort(key=lambda p: p["published_at"])

    saved = _db_save(posts)
    print(f"[backfill] DB 저장 완료: {len(saved)}개 (중복 제외)")

    if saved:
        await _publish_kafka(saved)

    print("[backfill] 완료. GPT 분석기가 순서대로 처리합니다.")


if __name__ == "__main__":
    asyncio.run(main())
