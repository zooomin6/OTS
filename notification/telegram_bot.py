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

RISK_TO_ENTRY_KEY = {
    "CONSERVATIVE": "entry_price_1",
    "MODERATE":     "entry_price_2",
    "AGGRESSIVE":   "entry_price_3",
}
RISK_LABEL = {
    "CONSERVATIVE": "안정형",
    "MODERATE":     "중립형",
    "AGGRESSIVE":   "공격형",
}


def _get_user_risk() -> str:
    """TELEGRAM_CHAT_ID 기준으로 사용자 risk_tolerance를 반환한다. 기본값 MODERATE."""
    if not TELEGRAM_CHAT_ID:
        return "MODERATE"
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT risk_tolerance FROM user_profiles WHERE telegram_user_id = %s",
                (int(TELEGRAM_CHAT_ID),)
            )
            row = cur.fetchone()
            return row[0] if row else "MODERATE"
    except Exception:
        return "MODERATE"
    finally:
        conn.close()


def _direction_label(signal_type: str, has_short_entry: bool = False) -> str:
    if signal_type == "BUY":
        return "🟢 롱 진입"
    if signal_type == "SELL":
        return "🔴 숏 진입" if has_short_entry else "🔴 롱 청산"
    return "🟡 관망"
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
(STEP_RISK, STEP_ASSET, STEP_BTC_LEVERAGE, STEP_ETH_LEVERAGE, STEP_MODE, STEP_AUTO_RATIO, STEP_COINS) = range(7)

# 사용자별 대화 이력 (봇 재시작 시 초기화)
_chat_history: dict[int, list[dict]] = {}
_MAX_HISTORY = 8

CHAT_SYSTEM_PROMPT = """\
당신은 OTS(One Trading System) AI 투자 어시스턴트입니다.
사용자의 코인 선물 투자를 돕는 대화형 어시스턴트입니다.

역할:
- 현재 활성 투자 시나리오 설명 및 해석
- 시장 상황 질문 답변 (시나리오 기반)
- 진입/손절/목표가 판단 도움
- 리스크 조언

규칙:
- 한국어로 간결하게 답변
- 투자 판단의 최종 책임은 사용자에게 있음을 인지
- 현재 시나리오에 없는 코인 질문은 "현재 시나리오 없음"으로 안내
- 확실하지 않은 내용은 추측이라고 명시
- 시나리오 가격은 반드시 "유튜버 제시 진입 목표가"로 표현할 것. "현재가"라고 절대 쓰지 말 것.
- 각 시나리오마다 반드시 "X월 X일 게시글 기준" 형식으로 분석 날짜를 명시할 것.
- 최근 HOLD/업데이트 게시글이 있으면 날짜와 함께 "최근 업데이트" 항목으로 포함할 것.
- USDT.D 상황이 있으면 BTC/ETH 시나리오 해석에 반드시 연결해서 설명할 것.
"""


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
                       trading_mode, auto_ratio, preferred_coins,
                       onboarding_completed, leverage_config
                FROM user_profiles
                WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "risk_tolerance":       row[0],
                "total_asset_usdt":     row[1],
                "leverage":             row[2],
                "trading_mode":         row[3],
                "auto_ratio":           row[4],
                "preferred_coins":      row[5] or [],
                "onboarding_completed": row[6],
                "leverage_config":      row[7] or {},
            }
    finally:
        conn.close()


def _save_user_profile(
    telegram_user_id: int,
    telegram_username: str | None,
    risk_tolerance: str,
    total_asset_usdt: int,
    leverage_config: dict,
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
                     total_asset_krw, leverage, leverage_config, trading_mode, auto_ratio,
                     preferred_coins, onboarding_completed, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (telegram_user_id) DO UPDATE SET
                    telegram_username    = EXCLUDED.telegram_username,
                    risk_tolerance       = EXCLUDED.risk_tolerance,
                    total_asset_krw      = EXCLUDED.total_asset_krw,
                    leverage             = EXCLUDED.leverage,
                    leverage_config      = EXCLUDED.leverage_config,
                    trading_mode         = EXCLUDED.trading_mode,
                    auto_ratio           = EXCLUDED.auto_ratio,
                    preferred_coins      = EXCLUDED.preferred_coins,
                    onboarding_completed = TRUE,
                    updated_at           = NOW()
            """, (
                telegram_user_id, telegram_username, risk_tolerance,
                total_asset_usdt,
                leverage_config.get("BTC", 1),
                json.dumps(leverage_config),
                trading_mode, auto_ratio,
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
    """
    특정 코인의 시나리오를 반환한다.
    진입가는 가장 최근 BUY/SELL에서, 현재 상태는 가장 최근 신호에서 가져온다.
    HOLD가 와도 이전 BUY/SELL 진입가는 유지된다.
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            # 1) 가장 최근 BUY/SELL — 진입가·손절가 기준
            cur.execute("""
                SELECT a.signal_type, a.coin_symbol,
                       a.entry_price_1, a.entry_price_2, a.entry_price_3, a.entry_price_4,
                       a.stop_loss_price, a.take_profit_price,
                       a.timeframe, a.is_reference_only,
                       a.summary, a.invalidation, a.created_at, p.published_at,
                       a.short_entry_price
                FROM analyses a
                JOIN posts p ON a.post_id = p.id
                WHERE a.is_active = TRUE AND a.coin_symbol = %s
                  AND a.signal_type IN ('BUY', 'SELL')
                ORDER BY a.created_at DESC
                LIMIT 1
            """, (coin_symbol.upper(),))
            price_row = cur.fetchone()

            # 2) 가장 최근 신호 — 현재 상태 표시용
            cur.execute("""
                SELECT signal_type, summary, created_at
                FROM analyses
                WHERE is_active = TRUE AND coin_symbol = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (coin_symbol.upper(),))
            status_row = cur.fetchone()

    finally:
        conn.close()

    if not price_row and not status_row:
        return None

    # 진입가 있는 BUY/SELL 기준으로 기본값 세팅
    base = price_row or status_row
    result = {
        "signal_type":       base[0],
        "coin_symbol":       base[1] if price_row else coin_symbol.upper(),
        "entry_price_1":     base[2] if price_row else None,
        "entry_price_2":     base[3] if price_row else None,
        "entry_price_3":     base[4] if price_row else None,
        "entry_price_4":     base[5] if price_row else None,
        "stop_loss_price":   base[6] if price_row else None,
        "take_profit_price": base[7] if price_row else None,
        "timeframe":         base[8] if price_row else None,
        "is_reference_only": base[9] if price_row else False,
        "summary":           base[10] if price_row else None,
        "invalidation":      base[11] if price_row else None,
        "created_at":        base[12] if price_row else base[2],
        "published_at":      base[13] if price_row else None,
        "short_entry_price": float(base[14]) if price_row and base[14] else None,
    }

    # 최신 신호가 HOLD이고 이전 BUY/SELL이 있으면 → 상태 업데이트 표시
    if status_row and price_row and status_row[0] == "HOLD":
        hold_date = status_row[2].strftime("%m/%d") if status_row[2] else ""
        result["hold_status"] = f"HOLD ({hold_date}) — {(status_row[1] or '')[:60]}"
    else:
        result["hold_status"] = None

    return result


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


def _get_tradingview_links(coin_symbol: str | None, limit: int = 5) -> list[dict]:
    """최근 분석과 연결된 트레이딩뷰 링크를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            if coin_symbol:
                cur.execute("""
                    SELECT pl.url, a.coin_symbol, a.timeframe, p.published_at
                    FROM post_links pl
                    JOIN posts p ON pl.post_id = p.id
                    LEFT JOIN analyses a ON a.post_id = p.id
                    WHERE pl.link_type = 'tradingview'
                      AND (a.coin_symbol = %s OR a.coin_symbol IS NULL)
                    ORDER BY p.published_at DESC
                    LIMIT %s
                """, (coin_symbol.upper(), limit))
            else:
                cur.execute("""
                    SELECT pl.url, a.coin_symbol, a.timeframe, p.published_at
                    FROM post_links pl
                    JOIN posts p ON pl.post_id = p.id
                    LEFT JOIN analyses a ON a.post_id = p.id
                    WHERE pl.link_type = 'tradingview'
                    ORDER BY p.published_at DESC
                    LIMIT %s
                """, (limit,))
            return [
                {"url": r[0], "coin": r[1], "timeframe": r[2], "published_at": r[3]}
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
    stop_loss_price: float | None = None,
    take_profit_price: float | None = None,
    short_entry_price: float | None = None,
    timeframe: str | None = None,
    is_reference_only: bool = False,
) -> None:
    """GPT 분석 결과를 Telegram으로 발송한다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    direction = _direction_label(signal_type, bool(short_entry_price))
    coin_line = f"*코인:* {coin_symbol}\n" if coin_symbol else ""

    tf_label = TIMEFRAME_LABEL.get(timeframe, timeframe) if timeframe else ""
    ref_suffix = " _(참고용 — 자동매매 없음)_" if is_reference_only else ""
    tf_line = f"*차트:* {tf_label}{ref_suffix}\n" if tf_label else ""

    risk  = _get_user_risk()
    entry_map = {
        "CONSERVATIVE": entry_price_1,
        "MODERATE":     entry_price_2,
        "AGGRESSIVE":   entry_price_3,
    }
    entry_val  = entry_map.get(risk) or entry_price_1 or entry_price_2
    risk_name  = RISK_LABEL.get(risk, "중립형")

    price_lines = ""
    if entry_val:
        price_lines += f"진입가 ({risk_name}): `${entry_val:,.0f}`\n"
    if stop_loss_price:
        price_lines += f"손절: `${stop_loss_price:,.0f}`\n"
    if take_profit_price:
        price_lines += f"목표가: `${take_profit_price:,.0f}`\n"

    text = (
        f"{direction} *새 투자 신호*\n\n"
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

async def _cmd_start(update, _context) -> int:
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
        "*2/6* 총 투자 가능 자산을 입력해 주세요 (USDT):\n"
        "_예: 1000_",
        parse_mode="Markdown",
    )
    return STEP_ASSET


def _leverage_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([
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
    ])


async def _step_asset(update, context) -> int:
    text = update.message.text.strip().replace(",", "").replace(".", "")
    if not text.isdigit() or int(text) <= 0:
        await update.message.reply_text("숫자만 입력해 주세요. (예: 1000)")
        return STEP_ASSET

    context.user_data["total_asset_usdt"] = int(text)

    await update.message.reply_text(
        f"✅ 자산: *${int(text):,} USDT*\n\n"
        "*3/6* BTC 레버리지 배수를 선택해 주세요:",
        parse_mode="Markdown",
        reply_markup=_leverage_keyboard(),
    )
    return STEP_BTC_LEVERAGE


async def _step_btc_leverage(update, context) -> int:
    query = update.callback_query
    await query.answer()

    lev = int(query.data.split(":")[1])
    context.user_data["leverage_btc"] = lev

    await query.edit_message_text(
        f"✅ BTC 레버리지: *{lev}x*\n\n"
        "*4/6* ETH 레버리지 배수를 선택해 주세요:",
        parse_mode="Markdown",
        reply_markup=_leverage_keyboard(),
    )
    return STEP_ETH_LEVERAGE


async def _step_eth_leverage(update, context) -> int:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    query = update.callback_query
    await query.answer()

    lev = int(query.data.split(":")[1])
    context.user_data["leverage_eth"] = lev

    keyboard = [
        [InlineKeyboardButton("자동매매 — GPT 분석 즉시 실행",    callback_data="tmode:AUTO")],
        [InlineKeyboardButton("반자동 — 버튼 확인 후 실행",        callback_data="tmode:SEMI_AUTO")],
        [InlineKeyboardButton("수동 — 알림만, 직접 매매",          callback_data="tmode:MANUAL")],
        [InlineKeyboardButton("알림만 — 매매 없이 신호만 수신",    callback_data="tmode:NOTIFY_ONLY")],
    ]
    await query.edit_message_text(
        f"✅ ETH 레버리지: *{lev}x*\n\n"
        "*5/6* 매매 방식을 선택해 주세요:",
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

    leverage_config = {
        "BTC": d.get("leverage_btc", 1),
        "ETH": d.get("leverage_eth", 1),
    }
    _save_user_profile(
        telegram_user_id=user.id,
        telegram_username=user.username,
        risk_tolerance=d.get("risk_tolerance", "MODERATE"),
        total_asset_usdt=d.get("total_asset_usdt", 0),
        leverage_config=leverage_config,
        trading_mode=d.get("trading_mode", "SEMI_AUTO"),
        auto_ratio=d.get("auto_ratio", 0),
        preferred_coins=d.get("selected_coins", []),
    )

    mode_label = MODE_LABEL.get(d.get("trading_mode", ""), "반자동")
    coins_str = ", ".join(d.get("selected_coins", [])) or "없음"

    await query.edit_message_text(
        "🎉 *온보딩 완료!*\n\n"
        f"투자 성향: *{d.get('risk_tolerance')}*\n"
        f"자산: *${d.get('total_asset_usdt', 0):,} USDT*\n"
        f"BTC 레버리지: *{d.get('leverage_btc', 1)}x* | ETH 레버리지: *{d.get('leverage_eth', 1)}x*\n"
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


async def _cmd_alert(update, context) -> None:
    """/alert [코인] [가격] [up|down] — 가격 알림 등록.
    예) /alert BTC 74200       → 74200 상향 돌파 시 알림
        /alert ETH 2000 down   → 2000 하향 이탈 시 알림
    """
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "사용법:\n"
            "/alert BTC 74200       (상향 돌파 알림)\n"
            "/alert ETH 2000 down   (하향 이탈 알림)"
        )
        return

    coin = args[0].upper()
    try:
        price = float(args[1].replace(",", ""))
    except ValueError:
        await update.message.reply_text("가격 형식이 올바르지 않습니다.")
        return

    direction_raw = args[2].lower() if len(args) > 2 else "up"
    direction = "BELOW" if direction_raw in ("down", "below", "아래") else "ABOVE"
    dir_label = "🔽 하향 이탈" if direction == "BELOW" else "🔼 상향 돌파"

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO custom_alerts (coin_symbol, target_price, direction)
                VALUES (%s, %s, %s) RETURNING id
            """, (coin, price, direction))
            alert_id = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(
        f"✅ 알림 등록 완료 (#{alert_id})\n"
        f"{coin} {price:,.2f} {dir_label} 시 알림"
    )


async def _cmd_alerts(update, context) -> None:
    """/alerts — 등록된 PENDING 알림 목록."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, coin_symbol, target_price, direction, created_at
                FROM custom_alerts WHERE status = 'PENDING'
                ORDER BY created_at DESC LIMIT 10
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        await update.message.reply_text("등록된 알림이 없습니다.")
        return

    lines = ["🔔 *대기 중인 알림*\n"]
    for r in rows:
        aid, coin, price, direction, created = r
        dir_label = "🔼 상향" if direction == "ABOVE" else "🔽 하향"
        lines.append(f"#{aid} {coin} {float(price):,.2f} {dir_label}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def _cmd_cancelalert(update, context) -> None:
    """/cancelalert [id] — 알림 취소."""
    if not context.args:
        await update.message.reply_text("사용법: /cancelalert [알림번호]")
        return
    try:
        alert_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("번호가 올바르지 않습니다.")
        return

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE custom_alerts SET status = 'CANCELLED'
                WHERE id = %s AND status = 'PENDING' RETURNING coin_symbol, target_price
            """, (alert_id,))
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    if row:
        await update.message.reply_text(f"❌ #{alert_id} 알림 취소됨 ({row[0]} {float(row[1]):,.2f})")
    else:
        await update.message.reply_text("해당 알림을 찾을 수 없습니다.")


async def _cmd_links(update, context) -> None:
    """/links [코인] — 최근 트레이딩뷰 차트 링크를 보여준다."""
    coin = context.args[0].upper() if context.args else None
    links = _get_tradingview_links(coin)

    if not links:
        coin_str = f" ({coin})" if coin else ""
        await update.message.reply_text(f"저장된 트레이딩뷰 링크가 없습니다{coin_str}.")
        return

    tf_label = {"MONTHLY": "월봉", "WEEKLY": "주봉", "DAILY": "일봉", "HOURLY": "시간봉"}
    lines = [f"📊 *트레이딩뷰 차트 링크{' — ' + coin if coin else ''}*\n"]
    seen = set()
    for lk in links:
        if lk["url"] in seen:
            continue
        seen.add(lk["url"])
        coin_str = lk["coin"] or "?"
        tf_str   = tf_label.get(lk["timeframe"] or "", lk["timeframe"] or "")
        date_str = lk["published_at"].strftime("%m/%d") if lk["published_at"] else ""
        label = f"{coin_str} {tf_str} ({date_str})".strip()
        lines.append(f"[{label}]({lk['url']})")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )


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


async def _callback_feedback(update, context) -> None:
    """✅ 맞음 / ❌ 틀림 버튼 처리."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")  # fb:ok:123 or fb:bad:123 or fb:fix:signal:123
    action = parts[1]
    # fix 콜백은 parts[2]가 fix_type 문자열이므로 여기서 파싱하지 않음
    analysis_id = int(parts[3] if action == "fix" else parts[2])

    if action == "ok":
        _save_feedback(analysis_id, "CORRECT", None)
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ 검증 완료", callback_data="fb:done")
            ]])
        )

    elif action == "bad":
        keyboard = [
            [InlineKeyboardButton("시그널 오류 (BUY↔SELL↔HOLD)", callback_data=f"fb:fix:signal:{analysis_id}")],
            [InlineKeyboardButton("코인 오류 (잘못된 코인)",       callback_data=f"fb:fix:coin:{analysis_id}")],
            [InlineKeyboardButton("가격 오류 (진입가/손절 잘못됨)", callback_data=f"fb:fix:price:{analysis_id}")],
            [InlineKeyboardButton("전체 오류 (분석 자체가 잘못됨)", callback_data=f"fb:fix:all:{analysis_id}")],
        ]
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif action == "fix":
        fix_type = parts[2]
        analysis_id = int(parts[3])
        note_map = {
            "signal": "시그널 오류",
            "coin":   "코인 오류",
            "price":  "가격 오류",
            "all":    "전체 오류",
        }
        note = note_map.get(fix_type, fix_type)
        _save_feedback(analysis_id, "INCORRECT", note)

        # 현재 분석값 조회해서 수정 양식 구성
        s = _get_analysis_by_id(analysis_id)
        context.user_data["pending_correction"] = analysis_id
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))

        if s:
            def _f(v): return f"{float(v):,.0f}" if v else "-"
            template = (
                f"현재 분석 내용:\n"
                f"  시그널: `{s['signal_type']}` | 코인: `{s['coin_symbol'] or '없음'}`\n"
                f"  진입1: `{_f(s['entry_price_1'])}` / 진입2: `{_f(s['entry_price_2'])}` / 진입3: `{_f(s['entry_price_3'])}`\n"
                f"  손절: `{_f(s['stop_loss_price'])}` / 목표: `{_f(s['take_profit_price'])}`\n\n"
                "올바른 내용을 입력하세요:\n"
                "예) `ETH BUY 진입1=2150 진입2=2100 진입3=2050 손절=1950 목표=2400`\n"
                "_수정할 항목만 입력해도 됩니다_"
            )
        else:
            template = "올바른 내용을 자유롭게 입력하세요."

        await query.message.reply_text(
            f"❌ *{note}* 로 기록됐습니다.\n\n{template}\n\n_건너뛰려면 /skip_",
            parse_mode="Markdown",
        )

    elif action == "done":
        await query.answer("이미 처리됐습니다.")


def _get_analysis_by_id(analysis_id: int) -> dict | None:
    """분석 ID로 단일 분석 결과를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signal_type, coin_symbol,
                       entry_price_1, entry_price_2, entry_price_3,
                       stop_loss_price, take_profit_price
                FROM analyses WHERE id = %s
            """, (analysis_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "signal_type":       row[0],
                "coin_symbol":       row[1],
                "entry_price_1":     row[2],
                "entry_price_2":     row[3],
                "entry_price_3":     row[4],
                "stop_loss_price":   row[5],
                "take_profit_price": row[6],
            }
    finally:
        conn.close()


def _save_feedback(analysis_id: int, feedback: str, note: str | None) -> None:
    """analyses 테이블에 피드백을 저장한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE analyses SET feedback = %s, feedback_note = %s WHERE id = %s",
                (feedback, note, analysis_id),
            )
        conn.commit()
    finally:
        conn.close()


# ── 시장 분석 명령어 ──────────────────────────────────────────

_COIN_KEYWORDS: dict[str, list[str]] = {
    "BTC":    ["btc", "비트", "비트코인", "bitcoin"],
    "ETH":    ["eth", "이더", "이더리움", "ethereum"],
    "SOL":    ["sol", "솔라나", "solana"],
    "XRP":    ["xrp", "리플", "ripple"],
    "USDT.D": ["테더", "usdt.d", "테더도미넌스", "tether"],
}
_MARKET_TRIGGERS = ["어때", "분석", "시장", "봐줘", "살까", "팔까", "들어갈까", "어디서", "지금"]


def _detect_market_query(text: str) -> str | None:
    """메시지에서 코인 + 시장 질문을 감지하면 코인 심볼 반환, 없으면 None."""
    lower = text.lower()
    if not any(t in lower for t in _MARKET_TRIGGERS):
        return None
    for coin, keywords in _COIN_KEYWORDS.items():
        if any(k in lower for k in keywords):
            return coin
    return None


def _register_custom_alert_db(coin: str, price: float, direction: str, note: str) -> int:
    """custom_alerts 테이블에 수동 알림 등록 후 id 반환."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO custom_alerts (coin_symbol, target_price, direction, note)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (coin, round(price, 2), direction, note))
            alert_id = cur.fetchone()[0]
        conn.commit()
        return alert_id
    finally:
        conn.close()


def _has_open_position(coin: str) -> bool:
    """해당 코인의 OPEN 포지션이 있으면 True."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM positions WHERE coin_symbol = %s AND status = 'OPEN'",
                (coin,)
            )
            return cur.fetchone()[0] > 0
    finally:
        conn.close()


def _format_market_analysis(result: dict, coin: str) -> str:
    """시장 분석 결과를 텔레그램 메시지로 포맷."""
    trend_emoji = {"UPTREND": "📈", "DOWNTREND": "📉", "SIDEWAYS": "➡️"}.get(
        result.get("trend", ""), "📊"
    )
    trend_label = {"UPTREND": "상승 추세", "DOWNTREND": "하락 추세", "SIDEWAYS": "횡보"}.get(
        result.get("trend", ""), "분석 중"
    )

    def _fmt(v):
        if v is None:
            return "-"
        try:
            f = float(v)
            return f"{f:,.0f}" if f >= 100 else f"{f:,.4f}"
        except Exception:
            return str(v)

    lines = [f"📊 *{coin} 시장 분석*\n"]

    # 거시 방향성 차단 경고 (최상단 표시)
    macro = result.get("macro_block")
    if macro and macro.get("result_direction") == "BEARISH":
        tf_kr = {"WEEKLY": "주봉", "MONTHLY": "월봉", "DAILY": "일봉"}.get(
            macro.get("result_timeframe", ""), ""
        )
        target_str = ""
        if macro.get("result_target"):
            target_str = f"→ 하락 목표가: {float(macro['result_target']):,.0f}\n"
        macro_warning = (
            f"🚨 *[유튜버 거시 방향성 발동 중]*\n"
            f"조건: USDT.D {float(macro['trigger_level']):.2f}% 이상 "
            f"(현재 {float(macro['current_value']):.2f}%)\n"
            f"→ {coin} {tf_kr} 하락 시나리오 활성\n"
            f"{target_str}"
            f"⛔ *이 상황에서 {coin} 롱은 거시 방향을 역행합니다*\n"
            f"{'─' * 35}\n"
        )
        lines.insert(0, macro_warning)

    lines.append(f"{trend_emoji} 추세: {trend_label}")

    e_low  = result.get("entry_zone_low")
    e_high = result.get("entry_zone_high")
    if e_low and e_high:
        lines.append(f"🎯 진입 구간: {_fmt(e_low)} ~ {_fmt(e_high)}")
    elif e_low:
        lines.append(f"🎯 진입가: {_fmt(e_low)}")
    else:
        lines.append("🎯 진입: 현재 진입 보류 (애매한 구간)")

    lines.append(f"🛑 손절: {_fmt(result.get('stop_loss'))}")
    lines.append(f"✅ 1차 목표: {_fmt(result.get('take_profit_1'))}")
    lines.append(f"✅ 2차 목표: {_fmt(result.get('take_profit_2'))}")

    if result.get("pattern"):
        lines.append(f"\n🔍 패턴: {result['pattern']}")

    ks = result.get("key_support")
    kr = result.get("key_resistance")
    if ks or kr:
        lines.append(f"📌 지지: {_fmt(ks)} | 저항: {_fmt(kr)}")

    # 유튜버 신호
    youtuber = result.get("youtuber_signal")
    if youtuber:
        y = youtuber[0]
        lines.append(f"\n[유튜버] {y['signal_type']} 신호 ({'✅ 일치' if result.get('entry_recommended') else '⚠️ 확인 필요'})")
    else:
        lines.append("\n[유튜버] 해당 코인 분석 없음 (기술적 분석만)")

    if result.get("summary"):
        lines.append(f"\n💡 {result['summary']}")

    if e_low:
        lines.append("\n🔔 진입가 도달 시 자동 알림 등록됨")

    return "\n".join(lines)


async def _cmd_market(update, context) -> None:
    """/market [코인] — 4TF 기술적 분석 + 자동 알림 등록.
    예) /market BTC  /market ETH
    """
    from analysis.market_analyzer import analyze_market

    args = context.args
    coin = args[0].upper() if args else "BTC"
    if coin not in list(_COIN_KEYWORDS.keys()) + ["BTC", "ETH", "SOL", "XRP"]:
        await update.message.reply_text(f"지원 코인: BTC, ETH, SOL, XRP, USDT.D")
        return

    await update.message.reply_text(f"⏳ {coin} 4타임프레임 분석 중... (10~30초 소요)")

    try:
        result = await analyze_market(coin)
    except Exception as e:
        await update.message.reply_text(f"❌ 분석 실패: {e}")
        return

    text = _format_market_analysis(result, coin)
    await update.message.reply_text(text, parse_mode="Markdown")

    # 진입가 custom_alert 자동 등록
    entry = result.get("entry_zone_low")
    if entry and result.get("entry_recommended"):
        try:
            _register_custom_alert_db(coin, float(entry), "BELOW", f"{coin} 자동 진입 알림")
            await update.message.reply_text(
                f"🔔 {coin} {float(entry):,.0f} 하향 도달 시 자동 알림 등록됨\n"
                f"/alerts 로 확인 가능"
            )
        except Exception:
            pass

    # 오픈 포지션 있으면 TP 알림도 등록
    tp1 = result.get("take_profit_1")
    if tp1 and _has_open_position(coin):
        try:
            _register_custom_alert_db(coin, float(tp1), "ABOVE", f"{coin} 1차 익절 알림 (자동)")
            await update.message.reply_text(f"🔔 {coin} {float(tp1):,.0f} 1차 익절 자동 알림 등록됨")
        except Exception:
            pass


async def _handle_correction_or_chat(update, context) -> None:
    """피드백 수정 입력 대기 중이면 수정 처리, 시장 질문이면 분석, 아니면 GPT 대화."""
    analysis_id = context.user_data.get("pending_correction")
    text = update.message.text.strip()

    if analysis_id and not text.startswith("/"):
        context.user_data.pop("pending_correction")
        conn = _db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE analyses SET feedback_note = COALESCE(feedback_note, '') || ' | 수정: ' || %s WHERE id = %s",
                    (text, analysis_id),
                )
            conn.commit()
        finally:
            conn.close()
        await update.message.reply_text(f"✅ 수정 내용 저장 완료\n_{text[:100]}_", parse_mode="Markdown")
        return

    # 시장 질문 감지 ("BTC 어때?", "이더 지금 살까?" 등)
    detected_coin = _detect_market_query(text)
    if detected_coin:
        # /market 커맨드와 동일하게 처리
        context.args = [detected_coin]
        await _cmd_market(update, context)
        return

    await _handle_chat(update, context)


async def _callback_setmode(update, context) -> None:
    """/mode 버튼 클릭 처리 — settings + user_profiles 동시 갱신."""
    query = update.callback_query
    await query.answer()

    mode = query.data.split(":")[1]
    _set_mode(mode)
    _update_user_mode(query.from_user.id, mode)

    label = MODE_LABEL.get(mode, mode)
    lines = [f"✅ 매매 모드 변경: *{label}*"]

    coins = _get_active_coins()
    if coins:
        lines.append("\n*📊 현재 활성 시나리오*")
        for c in coins:
            emoji = SIGNAL_EMOJI.get(c["signal"], "⚪")
            s = _get_latest_scenario(c["coin"])
            if s:
                e1 = f"`${s['entry_price_1']:,.0f}`" if s.get("entry_price_1") else "-"
                sl = f"`${s['stop_loss_price']:,.0f}`" if s.get("stop_loss_price") else "-"
                tp = f"`${s['take_profit_price']:,.0f}`" if s.get("take_profit_price") else "-"
                tf = TIMEFRAME_LABEL.get(s.get("timeframe", ""), "")
                lines.append(
                    f"{emoji} *{c['coin']}* {tf}\n"
                    f"  진입1: {e1} | 손절: {sl} | 목표: {tp}"
                )
            else:
                lines.append(f"{emoji} *{c['coin']}*")
    else:
        lines.append("\n현재 활성 시나리오 없음")

    await query.edit_message_text("\n".join(lines), parse_mode="Markdown")


# ── 공통 포맷터 ───────────────────────────────────────────────

def _format_scenario(s: dict) -> str:
    """분석 결과를 Telegram Markdown 텍스트로 포맷한다."""
    coin = s["coin_symbol"]
    tf_label = TIMEFRAME_LABEL.get(s["timeframe"], s["timeframe"]) if s.get("timeframe") else ""

    pub_at = s.get("published_at")
    date_str = pub_at.strftime("%m/%d") if pub_at else (
        s["created_at"].strftime("%m/%d") if s.get("created_at") else ""
    )

    direction = _direction_label(s["signal_type"], bool(s.get("short_entry_price")))
    lines = [f"{direction} *{coin} 시나리오*"]
    if tf_label:
        ref = " _(참고용)_" if s.get("is_reference_only") else ""
        lines.append(f"차트: {tf_label}{ref}")
    if date_str:
        lines.append(f"📅 {date_str} 게시글 기준")
    lines.append("")

    risk      = _get_user_risk()
    entry_key = RISK_TO_ENTRY_KEY.get(risk, "entry_price_2")
    entry_val = s.get(entry_key) or s.get("entry_price_1") or s.get("entry_price_2")
    risk_name = RISK_LABEL.get(risk, "중립형")

    if entry_val:
        lines.append(f"*유튜버 제시 진입 목표가 ({risk_name})*")
        lines.append(f"  `${entry_val:,.0f}`")
    if s["stop_loss_price"]:
        lines.append(f"손절: `${s['stop_loss_price']:,.0f}`")
    if s["take_profit_price"]:
        lines.append(f"목표가: `${s['take_profit_price']:,.0f}`")
    if s.get("hold_status"):
        lines.append(f"\n⚠️ *현재 상태*: {s['hold_status']}")
    if s.get("summary"):
        lines.append(f"\n*요약*\n{s['summary']}")
    if s.get("invalidation"):
        lines.append(f"\n*무효 조건*\n{s['invalidation']}")

    return "\n".join(lines)


# ── GPT 대화 ─────────────────────────────────────────────────

def _fetch_chat_context_sync() -> str:
    """활성 시나리오·설정·최근 거래를 GPT 채팅 컨텍스트 문자열로 반환."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT mode, is_halted FROM settings WHERE id = 1")
            settings_row = cur.fetchone()

            cur.execute("""
                SELECT a.coin_symbol, a.signal_type, a.timeframe,
                       a.entry_price_1, a.entry_price_2, a.entry_price_3,
                       a.stop_loss_price, a.take_profit_price, a.absolute_stop,
                       a.summary, a.invalidation, p.published_at
                FROM analyses a
                JOIN posts p ON a.post_id = p.id
                WHERE a.is_active = TRUE AND a.signal_type IN ('BUY','SELL')
                  AND a.coin_symbol IS NOT NULL
                ORDER BY a.created_at DESC LIMIT 5
            """)
            scenarios = cur.fetchall()

            # 최근 7일 HOLD 업데이트 — 기존 시나리오에 영향을 주는 최신 게시글
            cur.execute("""
                SELECT a.coin_symbol, a.signal_type, a.timeframe,
                       a.summary, p.published_at
                FROM analyses a
                JOIN posts p ON a.post_id = p.id
                WHERE a.signal_type = 'HOLD'
                  AND a.coin_symbol IS NOT NULL
                  AND p.published_at >= NOW() - INTERVAL '7 days'
                ORDER BY p.published_at DESC LIMIT 6
            """)
            recent_holds = cur.fetchall()

            cur.execute("""
                SELECT symbol, side, price, status
                FROM trades ORDER BY id DESC LIMIT 3
            """)
            trades = cur.fetchall()

            cur.execute("""
                SELECT indicator, state, key_level, implication, created_at
                FROM market_context
                WHERE created_at >= NOW() - INTERVAL '7 days'
                ORDER BY created_at DESC LIMIT 5
            """)
            market_rows = cur.fetchall()
    finally:
        conn.close()

    def fmt(v):
        if v is None: return "-"
        return f"{float(v):,.0f}" if float(v) >= 1 else f"{float(v):,.4f}"

    lines = ["[현재 시스템 상태]"]
    if settings_row:
        lines.append(f"매매 모드: {settings_row[0]} | {'정지' if settings_row[1] else '운영 중'}")

    if scenarios:
        lines.append("\n[활성 투자 시나리오]")
        for s in scenarios:
            coin, sig, tf, e1, e2, e3, sl, tp, abs_stop, summary, inv, pub_at = s
            tf_str = {"DAILY": "일봉", "HOURLY": "시간봉", "WEEKLY": "주봉"}.get(tf or "", tf or "")
            date_str = pub_at.strftime("%m/%d") if pub_at else "-"
            lines.append(
                f"• {sig} {coin} ({tf_str}) — {date_str} 게시글 기준\n"
                f"  유튜버 제시 진입 목표가: {fmt(e1)} / {fmt(e2)} / {fmt(e3)}\n"
                f"  손절: {fmt(sl)} | 목표: {fmt(tp)} | 마지노선: {fmt(abs_stop)}\n"
                f"  요약: {(summary or '')[:120]}\n"
                f"  무효 조건: {(inv or '')[:80]}"
            )
    else:
        lines.append("\n[활성 투자 시나리오 없음]")

    if recent_holds:
        lines.append("\n[최근 시나리오 업데이트 (HOLD/관망)]")
        for coin, sig, tf, summary, pub_at in recent_holds:
            tf_str = {"DAILY": "일봉", "HOURLY": "시간봉", "WEEKLY": "주봉"}.get(tf or "", tf or "")
            date_str = pub_at.strftime("%m/%d") if pub_at else "-"
            lines.append(f"• {date_str} {coin or '?'} ({tf_str}): {(summary or '')[:100]}")

    if trades:
        lines.append("\n[최근 거래]")
        for symbol, side, price, status in trades:
            lines.append(f"• {symbol} {side} @ {fmt(price)} ({status})")

    if market_rows:
        lines.append("\n[최근 시장 지표]")
        indicator_labels = {"TETHER_D": "테더.D", "BTC_D": "BTC 도미넌스"}
        state_labels = {
            "BEARISH": "약세", "BULLISH": "강세", "NEUTRAL": "중립",
            "WARNING": "경고", "RISING": "상승", "FALLING": "하락",
        }
        for indicator, state, key_level, implication, _ in market_rows:
            label = indicator_labels.get(indicator, indicator)
            state_str = state_labels.get(state, state or "-")
            lines.append(f"• {label}: {state_str}")
            if key_level:
                lines.append(f"  구간: {key_level}")
            if implication:
                lines.append(f"  영향: {implication}")

    return "\n".join(lines)


def _get_analyses_for_coin(coin: str) -> list[dict]:
    """해당 코인의 활성 BUY/SELL 분석 전체를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.signal_type, a.timeframe,
                       a.entry_price_1, a.entry_price_2, a.entry_price_3, a.entry_price_4,
                       a.stop_loss_price, a.absolute_stop, a.take_profit_price,
                       a.summary, a.invalidation, a.risk_reward_ratio, p.published_at
                FROM analyses a
                JOIN posts p ON a.post_id = p.id
                WHERE a.is_active = TRUE
                  AND a.coin_symbol = %s
                  AND a.signal_type IN ('BUY', 'SELL')
                ORDER BY a.created_at DESC
                LIMIT 3
            """, (coin.upper(),))
            rows = cur.fetchall()
            return [
                {
                    "id":              r[0],
                    "signal_type":     r[1],
                    "timeframe":       r[2],
                    "entry_price_1":   float(r[3]) if r[3] else None,
                    "entry_price_2":   float(r[4]) if r[4] else None,
                    "entry_price_3":   float(r[5]) if r[5] else None,
                    "entry_price_4":   float(r[6]) if r[6] else None,
                    "stop_loss_price": float(r[7]) if r[7] else None,
                    "absolute_stop":   float(r[8]) if r[8] else None,
                    "take_profit":     float(r[9]) if r[9] else None,
                    "summary":         r[10],
                    "invalidation":    r[11],
                    "rr_ratio":        float(r[12]) if r[12] else None,
                    "published_at":    r[13],
                }
                for r in rows
            ]
    finally:
        conn.close()


def _classify_entry_position(entry_price: float, analysis: dict) -> str:
    """현재 진입가가 분석의 어느 성향 구간에 위치하는지 판단한다."""
    signal = analysis["signal_type"]
    e1 = analysis.get("entry_price_1")
    e2 = analysis.get("entry_price_2")
    e3 = analysis.get("entry_price_3")
    e4 = analysis.get("entry_price_4")

    levels = [(v, label) for v, label in [
        (e1, "안정형"), (e2, "중립형"), (e3, "공격형"), (e4, "초공격형")
    ] if v is not None]

    if not levels:
        return "진입 구간 정보 없음"

    # BUY: 높은 가격이 안정형, 낮은 가격이 공격형
    # SELL: 낮은 가격이 안정형, 높은 가격이 공격형
    if signal == "BUY":
        levels_sorted = sorted(levels, key=lambda x: x[0], reverse=True)  # 높은→낮은
    else:
        levels_sorted = sorted(levels, key=lambda x: x[0])  # 낮은→높은

    # 가장 가까운 레벨 찾기
    closest = min(levels_sorted, key=lambda x: abs(x[0] - entry_price))
    diff_pct = (entry_price - closest[0]) / closest[0] * 100

    if abs(diff_pct) <= 1.0:
        return f"{closest[1]} 구간 진입 (`${closest[0]:,.0f}` 기준, {diff_pct:+.1f}%)"

    # 구간 사이에 있는 경우 — 인접 두 레벨 사이
    for i in range(len(levels_sorted) - 1):
        hi_price, hi_label = levels_sorted[i]
        lo_price, lo_label = levels_sorted[i + 1]
        lo, hi = min(hi_price, lo_price), max(hi_price, lo_price)
        if lo <= entry_price <= hi:
            return f"{hi_label} ~ {lo_label} 사이 진입"

    # 범위 밖
    if signal == "BUY":
        top_price, top_label = levels_sorted[0]
        bot_price, bot_label = levels_sorted[-1]
        if entry_price > top_price:
            over_pct = (entry_price - top_price) / top_price * 100
            return f"분석 구간보다 {over_pct:.1f}% 높은 진입 ({top_label} 위)"
        else:
            under_pct = (bot_price - entry_price) / bot_price * 100
            return f"분석 구간보다 {under_pct:.1f}% 낮은 진입 ({bot_label} 아래)"
    else:
        top_price, top_label = levels_sorted[-1]
        bot_price, bot_label = levels_sorted[0]
        if entry_price < bot_price:
            return f"분석 구간보다 낮은 진입 ({bot_label} 아래)"
        else:
            return f"분석 구간보다 높은 진입 ({top_label} 위)"


def _format_position_analysis(pos: dict, analyses: list[dict]) -> str:
    """포지션 정보와 매칭된 분석을 종합해 응답 텍스트를 만든다."""
    coin      = pos["coin"]
    side      = pos["side"]
    entry     = pos["entry_price"]
    mark      = pos["mark_price"]
    pnl_pct   = pos["pnl_pct"]
    leverage  = pos["leverage"]
    liq_price = pos.get("liq_price")

    side_emoji = "🟢 Long" if side == "LONG" else "🔴 Short"
    pnl_emoji  = "📈" if pnl_pct >= 0 else "📉"

    lines = [
        f"*{coin} 포지션 분석*",
        "",
        f"방향: {side_emoji} {leverage}x | 진입가: `${entry:,.2f}`",
        f"현재가: `${mark:,.2f}` {pnl_emoji} `{pnl_pct:+.2f}%`",
    ]
    if liq_price:
        lines.append(f"청산가: `${liq_price:,.2f}`")

    if not analyses:
        lines += [
            "",
            "⚠️ 현재 이 코인의 활성 시나리오가 없습니다.",
            "시나리오 없이 진입한 포지션이거나, 분석이 만료됐을 수 있습니다.",
        ]
        return "\n".join(lines)

    for a in analyses:
        tf_label  = TIMEFRAME_LABEL.get(a["timeframe"] or "", a["timeframe"] or "")
        date_str  = a["published_at"].strftime("%m/%d") if a["published_at"] else "-"
        sig_emoji = SIGNAL_EMOJI.get(a["signal_type"], "⚪")
        entry_pos = _classify_entry_position(entry, a)

        lines += [
            "",
            f"━━━ {sig_emoji} {tf_label} 시나리오 ({date_str}) ━━━",
            f"📍 진입 위치: {entry_pos}",
        ]

        # 손절까지 거리
        sl = a.get("stop_loss_price")
        ab = a.get("absolute_stop")
        tp = a.get("take_profit")

        if sl:
            sl_dist = (entry - sl) / entry * 100 if side == "LONG" else (sl - entry) / entry * 100
            sl_remain = (mark - sl) / mark * 100 if side == "LONG" else (sl - mark) / mark * 100
            lines.append(
                f"🛑 손절까지: `{sl_remain:.1f}%` 남음 (손절가 `${sl:,.0f}`)"
            )
        if ab:
            ab_dist = (mark - ab) / mark * 100 if side == "LONG" else (ab - mark) / mark * 100
            lines.append(f"⛔ 마지노선: `${ab:,.0f}` ({ab_dist:.1f}% 거리)")
        if tp:
            tp_dist = (tp - mark) / mark * 100 if side == "LONG" else (mark - tp) / mark * 100
            lines.append(f"🎯 목표까지: `{tp_dist:.1f}%` 남음 (목표가 `${tp:,.0f}`)")
        if a.get("rr_ratio"):
            lines.append(f"손익비: `{a['rr_ratio']:.1f}R`")

        # 현재가가 손절 근처인지 경고
        if sl and side == "LONG" and mark < sl * 1.03:
            lines.append("⚠️ *현재가가 손절 구간 근처입니다. 주의 필요.*")
        elif sl and side == "SHORT" and mark > sl * 0.97:
            lines.append("⚠️ *현재가가 손절 구간 근처입니다. 주의 필요.*")

        # 청산가와 손절 관계
        if liq_price and sl:
            if side == "LONG" and liq_price > sl:
                lines.append("⚠️ *청산가가 손절가보다 높습니다 — 레버리지 재검토 권장*")
            elif side == "SHORT" and liq_price < sl:
                lines.append("⚠️ *청산가가 손절가보다 낮습니다 — 레버리지 재검토 권장*")

        if a.get("summary"):
            lines.append(f"\n💬 {a['summary'][:120]}")
        if a.get("invalidation"):
            lines.append(f"🚫 무효 조건: {a['invalidation'][:80]}")

    return "\n".join(lines)


async def _handle_photo(update, context) -> None:
    """포지션 스크린샷 수신 → GPT-4o Vision 분석 → 시나리오 매칭 응답."""
    import asyncio
    import base64
    from openai import AsyncOpenAI

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    # 가장 고해상도 사진 선택
    photo = update.message.photo[-1]
    file_obj = await context.bot.get_file(photo.file_id)
    image_bytes = await file_obj.download_as_bytearray()
    b64_image = base64.b64encode(image_bytes).decode()

    extraction_prompt = """\
이 이미지는 Bybit 선물 거래 포지션 화면입니다.
다음 정보를 JSON으로 추출하세요. 여러 포지션이 있으면 배열로.

{
  "positions": [
    {
      "coin": "ETH",           // 코인 심볼 (ETHUSDT → ETH)
      "side": "LONG",          // LONG 또는 SHORT
      "leverage": 3,           // 레버리지 배수 (숫자)
      "quantity": 0.04,        // 수량
      "entry_price": 2073.33,  // 평균 진입가
      "mark_price": 2108.86,   // 현재(마크) 가격
      "pnl_pct": 5.13,         // 수익률 % (양수=수익, 음수=손실)
      "liq_price": 1389.17     // 청산 예상가 (없으면 null)
    }
  ]
}

JSON만 반환하세요. 포지션 정보가 없으면 {"positions": []} 반환."""

    try:
        client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
        resp = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text",      "text": extraction_prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64_image}"}},
                ],
            }],
            max_tokens=400,
            temperature=0,
        )

        raw = resp.choices[0].message.content.strip()
        # ```json ... ``` 블록 제거
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        positions = data.get("positions", [])

    except Exception as e:
        await update.message.reply_text(f"포지션 추출 실패: {e}")
        return

    if not positions:
        await update.message.reply_text("포지션 정보를 인식하지 못했습니다.\nBybit 포지션 탭이 보이는 화면을 보내주세요.")
        return

    loop = asyncio.get_event_loop()
    reply_parts = []

    for pos in positions:
        coin = pos.get("coin", "").upper()
        if not coin:
            continue
        analyses = await loop.run_in_executor(None, _get_analyses_for_coin, coin)
        reply_parts.append(_format_position_analysis(pos, analyses))

    if reply_parts:
        await update.message.reply_text("\n\n".join(reply_parts), parse_mode="Markdown")
    else:
        await update.message.reply_text("인식된 포지션이 없습니다.")


async def _handle_chat(update, context) -> None:
    """명령어가 아닌 일반 텍스트 → GPT-4o mini와 대화."""
    import asyncio
    from openai import AsyncOpenAI

    user_id = update.effective_user.id
    user_message = update.message.text.strip()

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    loop = asyncio.get_event_loop()
    db_ctx = await loop.run_in_executor(None, _fetch_chat_context_sync)

    history = _chat_history.setdefault(user_id, [])

    messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT + "\n\n" + db_ctx}]
    messages.extend(history[-(  _MAX_HISTORY * 2):])
    messages.append({"role": "user", "content": user_message})

    try:
        client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=600,
        )
        reply = resp.choices[0].message.content

        history.append({"role": "user",      "content": user_message})
        history.append({"role": "assistant", "content": reply})
        if len(history) > _MAX_HISTORY * 2:
            _chat_history[user_id] = history[-(_MAX_HISTORY * 2):]

        await update.message.reply_text(reply)
    except Exception as e:
        await update.message.reply_text(f"오류가 발생했습니다: {e}")


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
            STEP_RISK:         [CallbackQueryHandler(_step_risk,         pattern="^risk:")],
            STEP_ASSET:        [MessageHandler(filters.TEXT & ~filters.COMMAND, _step_asset)],
            STEP_BTC_LEVERAGE: [CallbackQueryHandler(_step_btc_leverage, pattern="^lev:")],
            STEP_ETH_LEVERAGE: [CallbackQueryHandler(_step_eth_leverage, pattern="^lev:")],
            STEP_MODE:         [CallbackQueryHandler(_step_mode,         pattern="^tmode:")],
            STEP_AUTO_RATIO:   [CallbackQueryHandler(_step_auto_ratio,   pattern="^ratio:")],
            STEP_COINS:        [CallbackQueryHandler(_step_coins,        pattern="^(coin:|coins:done)")],
        },
        fallbacks=[CommandHandler("start", _cmd_start)],
    )

    app.add_handler(onboarding)
    app.add_handler(CommandHandler("coins",    _cmd_coins))
    app.add_handler(CommandHandler("scenario", _cmd_scenario))
    app.add_handler(CommandHandler("links",    _cmd_links))
    app.add_handler(CommandHandler("memo",     _cmd_memo))
    app.add_handler(CommandHandler("memos",    _cmd_memos))
    app.add_handler(CommandHandler("mode",        _cmd_mode))
    app.add_handler(CommandHandler("status",      _cmd_status))
    app.add_handler(CommandHandler("alert",       _cmd_alert))
    app.add_handler(CommandHandler("alerts",      _cmd_alerts))
    app.add_handler(CommandHandler("cancelalert", _cmd_cancelalert))
    app.add_handler(CommandHandler("market",      _cmd_market))

    app.add_handler(CallbackQueryHandler(_callback_scenario, pattern="^scenario:"))
    app.add_handler(CallbackQueryHandler(_callback_setmode,  pattern="^setmode:"))
    app.add_handler(CallbackQueryHandler(_callback_feedback,  pattern="^fb:"))

    # 포지션 스크린샷 → 시나리오 매칭
    app.add_handler(MessageHandler(filters.PHOTO, _handle_photo))

    # 피드백 수정 입력 → GPT 대화 순으로 우선순위
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_correction_or_chat))

    print("[telegram-bot] 시작 — 명령어 대기 중")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
