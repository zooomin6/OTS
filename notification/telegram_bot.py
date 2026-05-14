"""
Telegram 알림 발송 모듈.
분석 결과를 Telegram 봇으로 전송한다.
"""
from __future__ import annotations

import os

import httpx
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

SIGNAL_EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}


async def send_analysis(
    analysis_id: int,
    signal_type: str,
    summary: str,
    content_preview: str,
) -> None:
    """GPT 분석 결과를 Telegram으로 발송한다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    emoji = SIGNAL_EMOJI.get(signal_type, "⚪")
    text = (
        f"{emoji} *새 투자 신호 — {signal_type}*\n\n"
        f"*요약*\n{summary}\n\n"
        f"*원문 미리보기*\n{content_preview[:120]}\n\n"
        f"분석 ID: \\#{analysis_id}"
    )
    await _send(text)


async def send_text(message: str) -> None:
    """단순 텍스트 메시지를 Telegram으로 발송한다."""
    await _send(message)


async def _send(text: str) -> None:
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       text,
                    "parse_mode": "Markdown",
                },
                timeout=10,
            )
        except Exception as e:
            print(f"[telegram] 발송 실패: {e}")
