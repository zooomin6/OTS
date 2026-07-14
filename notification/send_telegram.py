import os

import httpx

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")


async def send_telegram(text: str, log_prefix: str = "telegram", reply_markup: dict | None = None) -> bool:
    """텔레그램 메시지를 보낸다. 성공하면 True, 실패하거나 토큰이 없으면 False.

    reply_markup을 주면 인라인 버튼을 함께 보낸다 (예: SEMI_AUTO 승인/거절 버튼).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload,
                timeout=10,
            )
            return True
        except Exception as e:
            print(f"[{log_prefix}] Telegram 발송 실패: {e}")
            return False
