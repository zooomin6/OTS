"""
Telegram 알림 발송 + 인터랙티브 봇 모듈.

두 가지 역할:
  1. 알림 발송 — 분석기(gpt_analyzer)가 send_analysis()를 호출해 투자 신호 전송
  2. 명령어 처리 — 사용자가 보내는 /coins, /scenario, /memo 등에 응답
"""
from __future__ import annotations

import json
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
DATABASE_URL       = os.environ.get("DATABASE_URL", "")

SIGNAL_EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}
MODE_LABEL   = {"AUTO": "자동매매", "SEMI_AUTO": "반자동", "MANUAL": "수동"}


# ── DB 헬퍼 ──────────────────────────────────────────────────

def _db_connect():
    """psycopg2 DB 연결을 반환한다."""
    import psycopg2
    from urllib.parse import urlparse
    url = (DATABASE_URL
           .replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgresql+psycopg://",  "postgresql://"))
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        dbname=p.path.lstrip("/"),
        options="-c client_encoding=UTF8",
    )


def _get_active_coins() -> list[dict]:
    """현재 활성(is_active=True) 시나리오가 있는 코인 목록을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT coin_symbol, signal_type, created_at
                FROM analyses
                WHERE is_active = TRUE AND coin_symbol IS NOT NULL
                ORDER BY created_at DESC
            """)
            return [
                {"coin": r[0], "signal": r[1], "created_at": r[2]}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()


def _get_latest_scenario(coin_symbol: str) -> dict | None:
    """특정 코인의 가장 최근 활성 분석 결과를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signal_type, coin_symbol, entry_price_1, entry_price_2,
                       stop_loss_price, take_profit_price, summary, invalidation, created_at
                FROM analyses
                WHERE is_active = TRUE AND coin_symbol = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (coin_symbol.upper(),))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "signal_type":       row[0],
                "coin_symbol":       row[1],
                "entry_price_1":     row[2],
                "entry_price_2":     row[3],
                "stop_loss_price":   row[4],
                "take_profit_price": row[5],
                "summary":           row[6],
                "invalidation":      row[7],
                "created_at":        row[8],
            }
    finally:
        conn.close()


def _save_memo(content: str, post_id: int | None = None) -> None:
    """영상 메모를 video_memos 테이블에 저장한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO video_memos (post_id, content) VALUES (%s, %s)",
                (post_id, content),
            )
        conn.commit()
    finally:
        conn.close()


def _get_recent_memos(limit: int = 5) -> list[dict]:
    """최근 저장된 영상 메모 목록을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, content, created_at
                FROM video_memos
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            return [
                {"id": r[0], "content": r[1], "created_at": r[2]}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()


def _get_settings() -> dict:
    """현재 시스템 설정을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT mode, is_halted, max_trade_amount_krw FROM settings WHERE id = 1")
            row = cur.fetchone()
            return {"mode": row[0], "is_halted": row[1], "max_trade_amount_krw": row[2]}
    finally:
        conn.close()


def _set_mode(mode: str) -> None:
    """매매 모드를 변경한다. (AUTO / SEMI_AUTO / MANUAL)"""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE settings SET mode = %s, updated_at = NOW() WHERE id = 1",
                (mode,),
            )
        conn.commit()
    finally:
        conn.close()


def _get_status_summary() -> dict:
    """시스템 현황 요약을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            # 활성 시나리오 수
            cur.execute("SELECT COUNT(*) FROM analyses WHERE is_active = TRUE")
            active_count = cur.fetchone()[0]

            # 대기 중인 가격 알림 수
            cur.execute("SELECT COUNT(*) FROM price_alerts WHERE status = 'PENDING'")
            alert_count = cur.fetchone()[0]

            # 오늘 수집된 게시글 수
            cur.execute("SELECT COUNT(*) FROM posts WHERE collected_at >= CURRENT_DATE")
            post_count = cur.fetchone()[0]

            return {
                "active_scenarios": active_count,
                "pending_alerts":   alert_count,
                "posts_today":      post_count,
            }
    finally:
        conn.close()


# ── 알림 발송 (분석기에서 호출) ───────────────────────────────

async def send_analysis(
    analysis_id: int,
    signal_type: str,
    summary: str,
    content_preview: str,
    coin_symbol: str | None = None,
    entry_price_1: float | None = None,
    entry_price_2: float | None = None,
    stop_loss_price: float | None = None,
    take_profit_price: float | None = None,
) -> None:
    """GPT 분석 결과를 Telegram으로 발송한다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    emoji = SIGNAL_EMOJI.get(signal_type, "⚪")
    coin_line = f"*코인:* {coin_symbol}\n" if coin_symbol else ""

    price_lines = ""
    if entry_price_1:
        price_lines += f"1차 매수: ${entry_price_1:,.0f}\n"
    if entry_price_2:
        price_lines += f"2차 매수: ${entry_price_2:,.0f}\n"
    if stop_loss_price:
        price_lines += f"손절: ${stop_loss_price:,.0f}\n"
    if take_profit_price:
        price_lines += f"목표가: ${take_profit_price:,.0f}\n"

    text = (
        f"{emoji} *새 투자 신호 — {signal_type}*\n\n"
        f"{coin_line}"
        f"{price_lines}\n"
        f"*요약*\n{summary}\n\n"
        f"*원문 미리보기*\n{content_preview[:120]}\n\n"
        f"분석 ID: \\#{analysis_id}"
    )
    await _send(text)


async def send_text(message: str) -> None:
    """단순 텍스트 메시지를 Telegram으로 발송한다."""
    await _send(message)


async def _send(text: str) -> None:
    """Telegram Bot API로 메시지를 발송한다."""
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


# ── 명령어 핸들러 ─────────────────────────────────────────────

async def _cmd_start(update, context) -> None:
    """/start — 봇 소개 메시지."""
    text = (
        "👋 *OTS 투자 어시스턴트*\n\n"
        "사용 가능한 명령어:\n"
        "/coins — 활성 시나리오 코인 목록\n"
        "/scenario BTC — 코인 시나리오 상세\n"
        "/memo [내용] — 영상 메모 저장\n"
        "/memos — 최근 메모 목록\n"
        "/mode auto|semi|manual — 매매 모드 전환\n"
        "/status — 시스템 현황"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def _cmd_coins(update, context) -> None:
    """/coins — 현재 활성 시나리오가 있는 코인 목록을 버튼으로 표시."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    coins = _get_active_coins()
    if not coins:
        await update.message.reply_text("현재 활성 시나리오가 없습니다.")
        return

    # 코인별 인라인 버튼 생성 — 누르면 /scenario [코인] 과 동일하게 동작
    keyboard = [
        [InlineKeyboardButton(
            f"{SIGNAL_EMOJI.get(c['signal'], '⚪')} {c['coin']}",
            callback_data=f"scenario:{c['coin']}"
        )]
        for c in coins
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("📊 *활성 시나리오 코인*", parse_mode="Markdown", reply_markup=reply_markup)


async def _cmd_scenario(update, context) -> None:
    """/scenario [코인] — 해당 코인의 최신 활성 시나리오를 표시."""
    if not context.args:
        await update.message.reply_text("사용법: /scenario BTC")
        return

    coin = context.args[0].upper()
    s = _get_latest_scenario(coin)
    if not s:
        await update.message.reply_text(f"{coin} 활성 시나리오 없음.")
        return

    emoji = SIGNAL_EMOJI.get(s["signal_type"], "⚪")
    lines = [f"{emoji} *{coin} 시나리오*\n"]
    if s["entry_price_1"]:
        lines.append(f"1차 매수: `${s['entry_price_1']:,.0f}`")
    if s["entry_price_2"]:
        lines.append(f"2차 매수: `${s['entry_price_2']:,.0f}`")
    if s["stop_loss_price"]:
        lines.append(f"손절: `${s['stop_loss_price']:,.0f}`")
    if s["take_profit_price"]:
        lines.append(f"목표가: `${s['take_profit_price']:,.0f}`")
    lines.append(f"\n*요약*\n{s['summary']}")
    if s["invalidation"]:
        lines.append(f"\n*무효 조건*\n{s['invalidation']}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def _cmd_memo(update, context) -> None:
    """/memo [내용] — 영상 내용을 메모로 저장한다."""
    if not context.args:
        await update.message.reply_text("사용법: /memo 오늘 영상 핵심 내용...")
        return

    content = " ".join(context.args)
    _save_memo(content)
    await update.message.reply_text(f"✅ 메모 저장 완료\n\n_{content[:100]}_", parse_mode="Markdown")


async def _cmd_memos(update, context) -> None:
    """/memos — 최근 저장된 영상 메모 5개를 표시한다."""
    memos = _get_recent_memos()
    if not memos:
        await update.message.reply_text("저장된 메모가 없습니다.")
        return

    lines = ["📝 *최근 메모*\n"]
    for m in memos:
        date = m["created_at"].strftime("%m/%d %H:%M")
        lines.append(f"[{date}] {m['content'][:80]}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def _cmd_mode(update, context) -> None:
    """/mode [auto|semi|manual] — 매매 모드를 전환한다."""
    MODE_MAP = {"auto": "AUTO", "semi": "SEMI_AUTO", "manual": "MANUAL"}

    if not context.args or context.args[0].lower() not in MODE_MAP:
        await update.message.reply_text("사용법: /mode auto | semi | manual")
        return

    new_mode = MODE_MAP[context.args[0].lower()]
    _set_mode(new_mode)
    label = MODE_LABEL[new_mode]
    await update.message.reply_text(f"✅ 매매 모드 변경: *{label}*", parse_mode="Markdown")


async def _cmd_status(update, context) -> None:
    """/status — 시스템 현황을 표시한다."""
    settings = _get_settings()
    summary  = _get_status_summary()

    mode_label = MODE_LABEL.get(settings["mode"], settings["mode"])
    halted = "🔴 정지 중" if settings["is_halted"] else "🟢 운영 중"

    text = (
        f"*시스템 현황*\n\n"
        f"상태: {halted}\n"
        f"매매 모드: {mode_label}\n\n"
        f"활성 시나리오: {summary['active_scenarios']}개\n"
        f"대기 중 가격 알림: {summary['pending_alerts']}개\n"
        f"오늘 수집 게시글: {summary['posts_today']}개"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def _callback_scenario(update, context) -> None:
    """인라인 버튼 클릭 처리 — /coins에서 코인 버튼을 눌렀을 때."""
    query = update.callback_query
    await query.answer()

    coin = query.data.split(":")[1]  # "scenario:BTC" → "BTC"
    s = _get_latest_scenario(coin)
    if not s:
        await query.edit_message_text(f"{coin} 활성 시나리오 없음.")
        return

    emoji = SIGNAL_EMOJI.get(s["signal_type"], "⚪")
    lines = [f"{emoji} *{coin} 시나리오*\n"]
    if s["entry_price_1"]:
        lines.append(f"1차 매수: `${s['entry_price_1']:,.0f}`")
    if s["entry_price_2"]:
        lines.append(f"2차 매수: `${s['entry_price_2']:,.0f}`")
    if s["stop_loss_price"]:
        lines.append(f"손절: `${s['stop_loss_price']:,.0f}`")
    if s["take_profit_price"]:
        lines.append(f"목표가: `${s['take_profit_price']:,.0f}`")
    lines.append(f"\n*요약*\n{s['summary']}")

    await query.edit_message_text("\n".join(lines), parse_mode="Markdown")


# ── 봇 실행 ───────────────────────────────────────────────────

def run_bot() -> None:
    """
    Telegram 봇을 실행한다 (Long Polling 방식).
    봇이 텔레그램 서버에 주기적으로 새 메시지를 요청해서 명령어를 처리한다.
    """
    from telegram.ext import Application, CallbackQueryHandler, CommandHandler

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # 명령어 핸들러 등록
    app.add_handler(CommandHandler("start",    _cmd_start))
    app.add_handler(CommandHandler("coins",    _cmd_coins))
    app.add_handler(CommandHandler("scenario", _cmd_scenario))
    app.add_handler(CommandHandler("memo",     _cmd_memo))
    app.add_handler(CommandHandler("memos",    _cmd_memos))
    app.add_handler(CommandHandler("mode",     _cmd_mode))
    app.add_handler(CommandHandler("status",   _cmd_status))

    # 인라인 버튼 클릭 핸들러
    app.add_handler(CallbackQueryHandler(_callback_scenario, pattern="^scenario:"))

    print("[telegram-bot] 시작 — 명령어 대기 중")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
