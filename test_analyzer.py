"""분석기 단독 테스트 — Kafka 없이 post_id를 직접 넣어서 실행."""
import asyncio
import json
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from analysis.gpt_analyzer import _process

async def main(post_id: int):
    # Kafka 메시지 형식과 동일하게 구성
    fake_msg = json.dumps({
        "post_id":    post_id,
        "channel_id": "test",
        "post_type":  "text",
        "image_urls": [],
    }).encode()

    print(f"[test] post_id={post_id} 분석 시작")
    await _process(fake_msg)
    print("[test] 완료")

if __name__ == "__main__":
    post_id = int(sys.argv[1]) if len(sys.argv) > 1 else 21
    asyncio.run(main(post_id))
