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
MODE_LABEL = {
    "AUTO":        "자동매매",
    "SEMI_AUTO":   "반자동",
    "MANUAL":      "수동",
    "NOTIFY_ONLY": "알림만",
}
TIMEFRAME_LABEL = {
    "MONTHLY": "월봉",
    "WEEKLY":  "주봉",
    "DAILY":   "일봉",
    "HOURLY":  "시간봉",
}
AVAILABLE_COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX"]

# ConversationHandler 상태
(STEP_RISK, STEP_ASSET, STEP_LEVERAGE, STEP_MODE, STEP_AUTO_RATIO, STEP_COINS) = range(6)


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


def _get_user_profile(telegram_user_id: int) -> dict | None:
    """사용자 투자 프로필을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT risk_tolerance, total_asset_krw, leverage,
                       trading_mode, auto_ratio, preferred_coins, onboarding_completed
                FROM user_profiles
                WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "risk_tolerance":       row[0],
                "total_asset_krw":      row[1],
                "leverage":             row[2],
                "trading_mode":         row[3],
                "auto_ratio":           row[4],
                "preferred_coins":      row[5] or [],
                "onboarding_completed": row[6],
            }
    finally:
        conn.close()


def _save_user_profile(
    telegram_user_id: int,
    telegram_username: str | None,
    risk_tolerance: str,
    total_asset_krw: int,
    leverage: int,
    trading_mode: str,
    auto_ratio: int,
    preferred_coins: list[str],
) -> None:
    """사용자 프로필을 저장한다. 이미 존재하면 전체 갱신한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_profiles
                    (telegram_user_id, telegram_username, risk_tolerance,
                     total_asset_krw, leverage, trading_mode, auto_ratio,
                     preferred_coins, onboarding_completed, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (telegram_user_id) DO UPDATE SET
                    telegram_username    = EXCLUDED.telegram_username,
                    risk_tolerance       = EXCLUDED.risk_tolerance,
                    total_asset_krw      = EXCLUDED.total_asset_krw,
                    leverage             = EXCLUDED.leverage,
                    trading_mode         = EXCLUDED.trading_mode,
                    auto_ratio           = EXCLUDED.auto_ratio,
                    preferred_coins      = EXCLUDED.preferred_coins,
                    onboarding_completed = TRUE,
                    updated_at           = NOW()
            """, (
                telegram_user_id, telegram_username, risk_tolerance,
                total_asset_krw, leverage, trading_mode, auto_ratio,
                json.dumps(preferred_coins),
            ))
        conn.commit()
    finally:
        conn.close()


def _update_user_mode(telegram_user_id: int, mode: str) -> None:
    """사용자 프로필의 매매 모드만 갱신한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE user_profiles
                SET trading_mode = %s, updated_at = NOW()
                WHERE telegram_user_id = %s
            """, (mode, telegram_user_id))
        conn.commit()
    finally:
        conn.close()


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
                SELECT signal_type, coin_symbol,
                       entry_price_1, entry_price_2, entry_price_3, entry_price_4,
                       stop_loss_price, take_profit_price,
                       timeframe, is_reference_only,
                       summary, invalidation, created_at
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
                "entry_price_3":     row[4],
                "entry_price_4":     row[5],
                "stop_loss_price":   row[6],
                "take_profit_price": row[7],
                "timeframe":         row[8],
                "is_reference_only": row[9],
                "summary":           row[10],
                "invalidation":      row[11],
                "created_at":        row[12],
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
    """settings 테이블의 매매 모드를 변경한다."""
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
            cur.execute("SELECT COUNT(*) FROM analyses WHERE is_active = TRUE")
            active_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM price_alerts WHERE status = 'PENDING'")
            alert_count = cur.fetchone()[0]

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
    entry_price_3: float | None = None,
    entry_price_4: float | None = None,
    stop_loss_price: float | None = None,
    take_profit_price: float | None = None,
    timeframe: str | None = None,
    is_reference_only: bool = False,
) -> None:
    """GPT 분석 결과를 Telegram으로 발송한다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    emoji = SIGNAL_EMOJI.get(signal_type, "⚪")
    coin_line = f"*코인:* {coin_symbol}\n" if coin_symbol else ""

    tf_label = TIMEFRAME_LABEL.get(timeframe, timeframe) if timeframe else ""
    ref_suffix = " _(참고용 — 자동매매 없음)_" if is_reference_only else ""
    tf_line = f"*차트:* {tf_label}{ref_suffix}\n" if tf_label else ""

    price_lines = ""
    if entry_price_1:
        price_lines += f"안정형: `${entry_price_1:,.0f}`\n"
    if entry_price_2:
        price_lines += f"중립형: `${entry_price_2:,.0f}`\n"
    if entry_price_3:
        price_lines += f"공격형: `${entry_price_3:,.0f}`\n"
    if entry_price_4:
        price_lines += f"초공격형: `${entry_price_4:,.0f}`\n"
    if stop_loss_price:
        price_lines += f"손절: `${stop_loss_price:,.0f}`\n"
    if take_profit_price:
        price_lines += f"목표가: `${take_profit_price:,.0f}`\n"

    text = (
        f"{emoji} *새 투자 신호 — {signal_type}*\n\n"
        f"{coin_line}"
        f"{tf_line}"
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


# ── 온보딩 ────────────────────────────────────────────────────

async def _cmd_start(update, context) -> int:
    """/start — 최초 사용자는 온보딩, 기존 사용자는 도움말."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import ConversationHandler

    user = update.effective_user
    profile = _get_user_profile(user.id)

    if profile and profile["onboarding_completed"]:
        await update.message.reply_text(
            f"👋 {user.first_name}님, 다시 오셨군요!\n\n"
            "/coins — 활성 코인 목록\n"
            "/scenario BTC — 시나리오 조회\n"
            "/status — 시스템 현황\n"
            "/mode — 매매 모드 변경\n"
            "/memo [내용] — 메모 저장\n"
            "/memos — 메모 목록"
        )
        return ConversationHandler.END

    keyboard = [
        [InlineKeyboardButton("안정형 — 최고점 진입, 낮은 리스크", callback_data="risk:CONSERVATIVE")],
        [InlineKeyboardButton("중립형 — 중간 진입, 균형 리스크",   callback_data="risk:MODERATE")],
        [InlineKeyboardButton("공격형 — 최저점 진입, 높은 리스크", callback_data="risk:AGGRESSIVE")],
    ]
    await update.message.reply_text(
        "👋 *OTS 투자 어시스턴트에 오신 걸 환영합니다!*\n\n"
        "*1/6* 투자 성향을 선택해 주세요:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return STEP_RISK


async def _step_risk(update, context) -> int:
    query = update.callback_query
    await query.answer()

    risk = query.data.split(":")[1]
    context.user_data["risk_tolerance"] = risk

    await query.edit_message_text(
        f"✅ 투자 성향: *{risk}*\n\n"
        "*2/6* 총 투자 가능 자산을 입력해 주세요 (원 단위):\n"
        "_예: 5000000_",
        parse_mode="Markdown",
    )
    return STEP_ASSET


async def _step_asset(update, context) -> int:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    text = update.message.text.strip().replace(",", "")
    if not text.isdigit():
        await update.message.reply_text("숫자만 입력해 주세요. (예: 5000000)")
        return STEP_ASSET

    context.user_data["total_asset_krw"] = int(text)

    keyboard = [
        [
            InlineKeyboardButton("1x",  callback_data="lev:1"),
            InlineKeyboardButton("3x",  callback_data="lev:3"),
            InlineKeyboardButton("5x",  callback_data="lev:5"),
            InlineKeyboardButton("10x", callback_data="lev:10"),
        ],
        [
            InlineKeyboardButton("20x", callback_data="lev:20"),
            InlineKeyboardButton("30x", callback_data="lev:30"),
            InlineKeyboardButton("50x", callback_data="lev:50"),
        ],
    ]
    await update.message.reply_text(
        f"✅ 자산: *{int(text):,}원*\n\n"
        "*3/6* 레버리지 배수를 선택해 주세요:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return STEP_LEVERAGE


async def _step_leverage(update, context) -> int:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    query = update.callback_query
    await query.answer()

    lev = int(query.data.split(":")[1])
    context.user_data["leverage"] = lev

    keyboard = [
        [InlineKeyboardButton("자동매매 — GPT 분석 즉시 실행",    callback_data="tmode:AUTO")],
        [InlineKeyboardButton("반자동 — 버튼 확인 후 실행",        callback_data="tmode:SEMI_AUTO")],
        [InlineKeyboardButton("수동 — 알림만, 직접 매매",          callback_data="tmode:MANUAL")],
        [InlineKeyboardButton("알림만 — 매매 없이 신호만 수신",    callback_data="tmode:NOTIFY_ONLY")],
    ]
    await query.edit_message_text(
        f"✅ 레버리지: *{lev}x*\n\n"
        "*4/6* 매매 방식을 선택해 주세요:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return STEP_MODE


async def _step_mode(update, context) -> int:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    query = update.callback_query
    await query.answer()

    mode = query.data.split(":")[1]
    context.user_data["trading_mode"] = mode

    if mode == "AUTO":
        keyboard = [[
            InlineKeyboardButton("25%",  callback_data="ratio:25"),
            InlineKeyboardButton("50%",  callback_data="ratio:50"),
            InlineKeyboardButton("75%",  callback_data="ratio:75"),
            InlineKeyboardButton("100%", callback_data="ratio:100"),
        ]]
        await query.edit_message_text(
            f"✅ 매매 방식: *자동매매*\n\n"
            "*5/6* 자동매매 비중을 선택해 주세요:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return STEP_AUTO_RATIO

    context.user_data["auto_ratio"] = 0
    return await _show_coins_step(query, context, step_num=5)


async def _step_auto_ratio(update, context) -> int:
    query = update.callback_query
    await query.answer()

    ratio = int(query.data.split(":")[1])
    context.user_data["auto_ratio"] = ratio

    return await _show_coins_step(query, context, step_num=6)


async def _show_coins_step(query, context, step_num: int) -> int:
    from telegram import InlineKeyboardMarkup

    context.user_data.setdefault("selected_coins", [])
    keyboard = _build_coins_keyboard(context.user_data["selected_coins"])
    await query.edit_message_text(
        f"*{step_num}/6* 관심 코인을 선택해 주세요 (다중 선택 후 완료 버튼):",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return STEP_COINS


def _build_coins_keyboard(selected: list[str]) -> list:
    from telegram import InlineKeyboardButton

    rows = []
    row = []
    for coin in AVAILABLE_COINS:
        label = f"✅ {coin}" if coin in selected else coin
        row.append(InlineKeyboardButton(label, callback_data=f"coin:{coin}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("완료 ✔", callback_data="coins:done")])
    return rows


async def _step_coins(update, context) -> int:
    from telegram import InlineKeyboardMarkup

    query = update.callback_query
    await query.answer()

    if query.data == "coins:done":
        return await _finish_onboarding(query, context)

    coin = query.data.split(":")[1]
    selected = context.user_data.setdefault("selected_coins", [])
    if coin in selected:
        selected.remove(coin)
    else:
        selected.append(coin)

    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup(_build_coins_keyboard(selected))
    )
    return STEP_COINS


async def _finish_onboarding(query, context) -> int:
    from telegram.ext import ConversationHandler

    user = query.from_user
    d = context.user_data

    _save_user_profile(
        telegram_user_id=user.id,
        telegram_username=user.username,
        risk_tolerance=d.get("risk_tolerance", "MODERATE"),
        total_asset_krw=d.get("total_asset_krw", 0),
        leverage=d.get("leverage", 1),
        trading_mode=d.get("trading_mode", "SEMI_AUTO"),
        auto_ratio=d.get("auto_ratio", 0),
        preferred_coins=d.get("selected_coins", []),
    )

    mode_label = MODE_LABEL.get(d.get("trading_mode", ""), "반자동")
    coins_str = ", ".join(d.get("selected_coins", [])) or "없음"

    await query.edit_message_text(
        "🎉 *온보딩 완료!*\n\n"
        f"투자 성향: *{d.get('risk_tolerance')}*\n"
        f"자산: *{d.get('total_asset_krw', 0):,}원*\n"
        f"레버리지: *{d.get('leverage')}x*\n"
        f"매매 방식: *{mode_label}*\n"
        f"관심 코인: *{coins_str}*\n\n"
        "이제 투자 신호를 받을 준비가 되었습니다!\n"
        "/status — 시스템 현황 확인",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ── 명령어 핸들러 ─────────────────────────────────────────────

async def _cmd_coins(update, context) -> None:
    """/coins — 현재 활성 시나리오가 있는 코인 목록을 버튼으로 표시."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    coins = _get_active_coins()
    if not coins:
        await update.message.reply_text("현재 활성 시나리오가 없습니다.")
        return

    keyboard = [
        [InlineKeyboardButton(
            f"{SIGNAL_EMOJI.get(c['signal'], '⚪')} {c['coin']}",
            callback_data=f"scenario:{c['coin']}"
        )]
        for c in coins
    ]
    await update.message.reply_text(
        "📊 *활성 시나리오 코인*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


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

    await update.message.reply_text(_format_scenario(s), parse_mode="Markdown")


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


async def _cmd_mode(update, _context) -> None:
    """/mode — 매매 모드를 인라인 버튼으로 선택한다."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    keyboard = [
        [InlineKeyboardButton("자동매매",  callback_data="setmode:AUTO")],
        [InlineKeyboardButton("반자동",    callback_data="setmode:SEMI_AUTO")],
        [InlineKeyboardButton("수동",      callback_data="setmode:MANUAL")],
        [InlineKeyboardButton("알림만",    callback_data="setmode:NOTIFY_ONLY")],
    ]
    await update.message.reply_text(
        "⚙️ *매매 모드를 선택해 주세요:*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


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


# ── 콜백 핸들러 ───────────────────────────────────────────────

async def _callback_scenario(update, context) -> None:
    """인라인 버튼 클릭 처리 — /coins에서 코인 버튼을 눌렀을 때."""
    query = update.callback_query
    await query.answer()

    coin = query.data.split(":")[1]
    s = _get_latest_scenario(coin)
    if not s:
        await query.edit_message_text(f"{coin} 활성 시나리오 없음.")
        return

    await query.edit_message_text(_format_scenario(s), parse_mode="Markdown")


async def _callback_setmode(update, context) -> None:
    """/mode 버튼 클릭 처리 — settings + user_profiles 동시 갱신."""
    query = update.callback_query
    await query.answer()

    mode = query.data.split(":")[1]
    _set_mode(mode)
    _update_user_mode(query.from_user.id, mode)

    label = MODE_LABEL.get(mode, mode)
    await query.edit_message_text(f"✅ 매매 모드 변경: *{label}*", parse_mode="Markdown")


# ── 공통 포맷터 ───────────────────────────────────────────────

def _format_scenario(s: dict) -> str:
    """분석 결과를 Telegram Markdown 텍스트로 포맷한다."""
    emoji = SIGNAL_EMOJI.get(s["signal_type"], "⚪")
    coin = s["coin_symbol"]
    tf_label = TIMEFRAME_LABEL.get(s["timeframe"], s["timeframe"]) if s.get("timeframe") else ""

    lines = [f"{emoji} *{coin} 시나리오*"]
    if tf_label:
        ref = " _(참고용)_" if s.get("is_reference_only") else ""
        lines.append(f"차트: {tf_label}{ref}")
    lines.append("")

    if s["entry_price_1"]:
        lines.append(f"안정형: `${s['entry_price_1']:,.0f}`")
    if s["entry_price_2"]:
        lines.append(f"중립형: `${s['entry_price_2']:,.0f}`")
    if s["entry_price_3"]:
        lines.append(f"공격형: `${s['entry_price_3']:,.0f}`")
    if s["entry_price_4"]:
        lines.append(f"초공격형: `${s['entry_price_4']:,.0f}`")
    if s["stop_loss_price"]:
        lines.append(f"손절: `${s['stop_loss_price']:,.0f}`")
    if s["take_profit_price"]:
        lines.append(f"목표가: `${s['take_profit_price']:,.0f}`")
    if s.get("summary"):
        lines.append(f"\n*요약*\n{s['summary']}")
    if s.get("invalidation"):
        lines.append(f"\n*무효 조건*\n{s['invalidation']}")

    return "\n".join(lines)


# ── 봇 실행 ───────────────────────────────────────────────────

def run_bot() -> None:
    """
    Telegram 봇을 실행한다 (Long Polling 방식).
    봇이 텔레그램 서버에 주기적으로 새 메시지를 요청해서 명령어를 처리한다.
    """
    from telegram.ext import (
        Application, CallbackQueryHandler, CommandHandler,
        ConversationHandler, MessageHandler, filters,
    )

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # 온보딩 ConversationHandler (/start 진입, 6단계 완료 시 END)
    onboarding = ConversationHandler(
        entry_points=[CommandHandler("start", _cmd_start)],
        states={
            STEP_RISK:       [CallbackQueryHandler(_step_risk,       pattern="^risk:")],
            STEP_ASSET:      [MessageHandler(filters.TEXT & ~filters.COMMAND, _step_asset)],
            STEP_LEVERAGE:   [CallbackQueryHandler(_step_leverage,   pattern="^lev:")],
            STEP_MODE:       [CallbackQueryHandler(_step_mode,       pattern="^tmode:")],
            STEP_AUTO_RATIO: [CallbackQueryHandler(_step_auto_ratio, pattern="^ratio:")],
            STEP_COINS:      [CallbackQueryHandler(_step_coins,      pattern="^(coin:|coins:done)")],
        },
        fallbacks=[CommandHandler("start", _cmd_start)],
    )

    app.add_handler(onboarding)
    app.add_handler(CommandHandler("coins",    _cmd_coins))
    app.add_handler(CommandHandler("scenario", _cmd_scenario))
    app.add_handler(CommandHandler("memo",     _cmd_memo))
    app.add_handler(CommandHandler("memos",    _cmd_memos))
    app.add_handler(CommandHandler("mode",     _cmd_mode))
    app.add_handler(CommandHandler("status",   _cmd_status))

    app.add_handler(CallbackQueryHandler(_callback_scenario, pattern="^scenario:"))
    app.add_handler(CallbackQueryHandler(_callback_setmode,  pattern="^setmode:"))

    print("[telegram-bot] 시작 — 명령어 대기 중")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
