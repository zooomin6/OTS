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
                SELECT signal_type, coin_symbol,
                       entry_price_1, entry_price_2, entry_price_3, entry_price_4,
                       stop_loss_price, take_profit_price,
                       timeframe, is_reference_only,
                       summary, invalidation, created_at
                FROM analyses
                WHERE is_active = TRUE AND coin_symbol = %s
                  AND signal_type IN ('BUY', 'SELL')
                ORDER BY created_at DESC
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

    parts = query.data.split(":")  # fb:ok:123 or fb:bad:123
    action = parts[1]
    analysis_id = int(parts[2])

    if action == "ok":
        _save_feedback(analysis_id, "CORRECT", None)
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup([[
                {"text": "✅ 검증 완료", "callback_data": "fb:done"}
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


async def _handle_correction_or_chat(update, context) -> None:
    """피드백 수정 입력 대기 중이면 수정 처리, 아니면 GPT 대화로 전달."""
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
                SELECT coin_symbol, signal_type, timeframe,
                       entry_price_1, entry_price_2, entry_price_3,
                       stop_loss_price, take_profit_price, absolute_stop,
                       summary, invalidation
                FROM analyses
                WHERE is_active = TRUE AND signal_type IN ('BUY','SELL')
                  AND coin_symbol IS NOT NULL
                ORDER BY created_at DESC LIMIT 5
            """)
            scenarios = cur.fetchall()

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
            coin, sig, tf, e1, e2, e3, sl, tp, abs_stop, summary, inv = s
            tf_str = {"DAILY": "일봉", "HOURLY": "시간봉", "WEEKLY": "주봉"}.get(tf or "", tf or "")
            lines.append(
                f"• {sig} {coin} ({tf_str})\n"
                f"  진입: {fmt(e1)} / {fmt(e2)} / {fmt(e3)}\n"
                f"  손절: {fmt(sl)} | 목표: {fmt(tp)} | 마지노선: {fmt(abs_stop)}\n"
                f"  요약: {(summary or '')[:120]}\n"
                f"  무효 조건: {(inv or '')[:80]}"
            )
    else:
        lines.append("\n[활성 투자 시나리오 없음]")

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
    app.add_handler(CommandHandler("mode",     _cmd_mode))
    app.add_handler(CommandHandler("status",   _cmd_status))

    app.add_handler(CallbackQueryHandler(_callback_scenario, pattern="^scenario:"))
    app.add_handler(CallbackQueryHandler(_callback_setmode,  pattern="^setmode:"))
    app.add_handler(CallbackQueryHandler(_callback_feedback,  pattern="^fb:"))

    # 피드백 수정 입력 → GPT 대화 순으로 우선순위
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_correction_or_chat))

    print("[telegram-bot] 시작 — 명령어 대기 중")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
