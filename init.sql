-- =============================================================
-- AI 코인 투자 어시스턴트 — DB 스키마 초기화
-- PostgreSQL 14+
-- v2: coin_symbol, price_alerts, post_links, video_memos 추가
-- v3: 3분할 매수, 유튜버 구간, 기술적 지표 컬럼 추가
-- v4: news_articles, user_profiles 추가
-- v5: positions, economic_calendars 추가, timeframe/숏 관련 컬럼 추가
-- =============================================================

-- -----------------------------------------
-- 1. posts — 수집된 게시글
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS posts (
    id           BIGSERIAL    PRIMARY KEY,
    channel_id   VARCHAR(100) NOT NULL,
    post_id      VARCHAR(255) NOT NULL UNIQUE,
    content      TEXT         NOT NULL,
    post_type    VARCHAR(10)  NOT NULL DEFAULT 'text'
                     CHECK (post_type IN ('text', 'video', 'mixed')),
    image_urls   JSONB        NOT NULL DEFAULT '[]',
    content_hash VARCHAR(64),
    published_at TIMESTAMP    NOT NULL,
    collected_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_posts_post_id       ON posts (post_id);
CREATE INDEX IF NOT EXISTS idx_posts_published_at  ON posts (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_post_type     ON posts (post_type);

-- -----------------------------------------
-- 2. analyses — GPT 분석 결과
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS analyses (
    id                  BIGSERIAL      PRIMARY KEY,
    post_id             BIGINT         NOT NULL REFERENCES posts (id) ON DELETE CASCADE,
    signal_type         VARCHAR(10)    NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD')),
    coin_symbol         VARCHAR(20),
    -- 차트 시간 단위
    timeframe           VARCHAR(10)    CHECK (timeframe IN ('MONTHLY', 'WEEKLY', 'DAILY', 'HOURLY')),
    is_reference_only   BOOLEAN        NOT NULL DEFAULT FALSE,
    -- 유튜버 제시 구간
    youtuber_zone_low   DECIMAL(18, 2),
    youtuber_zone_high  DECIMAL(18, 2),
    -- 성향별 단일 진입가 (1=안정형, 2=중립형, 3=공격형, 4=초공격형)
    entry_price_1       DECIMAL(18, 2),
    entry_price_2       DECIMAL(18, 2),
    entry_price_3       DECIMAL(18, 2),
    entry_price_4       DECIMAL(18, 2),
    entry_ratio_1       SMALLINT,
    entry_ratio_2       SMALLINT,
    entry_ratio_3       SMALLINT,
    -- 손익 기준가
    absolute_stop       DECIMAL(18, 2),   -- 마지노선 (시즌 종료 레벨)
    stop_loss_price     DECIMAL(18, 2),
    take_profit_price   DECIMAL(18, 2),
    -- SELL 신호 숏 진입 (GPT 판단 시에만)
    short_entry_price   DECIMAL(18, 2),
    short_stop_loss     DECIMAL(18, 2),
    -- 기술적 지표
    risk_reward_ratio   DECIMAL(6, 2),
    current_rsi         DECIMAL(5, 2),
    rsi_signal          VARCHAR(10)    CHECK (rsi_signal IN ('OVERSOLD', 'NEUTRAL', 'OVERBOUGHT')),
    volume_signal       VARCHAR(10)    CHECK (volume_signal IN ('HIGH', 'NORMAL', 'LOW')),
    fib_level           DECIMAL(6, 3),
    -- 요약·시나리오
    scenario_json       JSONB          NOT NULL,
    summary             TEXT,
    invalidation        TEXT,
    raw_response        TEXT,
    feedback            VARCHAR(20),
    feedback_note       TEXT,
    is_active           BOOLEAN        NOT NULL DEFAULT TRUE,
    expires_at          TIMESTAMP,                -- DAILY=+5일, HOURLY=+24h, MONTHLY/WEEKLY=NULL
    created_at          TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analyses_post_id      ON analyses (post_id);
CREATE INDEX IF NOT EXISTS idx_analyses_signal_type  ON analyses (signal_type);
CREATE INDEX IF NOT EXISTS idx_analyses_coin_symbol  ON analyses (coin_symbol);
CREATE INDEX IF NOT EXISTS idx_analyses_timeframe    ON analyses (timeframe);
CREATE INDEX IF NOT EXISTS idx_analyses_is_active    ON analyses (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_analyses_created_at   ON analyses (created_at DESC);

-- -----------------------------------------
-- 3. price_alerts — 가격 도달 알림 설정
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS price_alerts (
    id           BIGSERIAL      PRIMARY KEY,
    analysis_id  BIGINT         NOT NULL REFERENCES analyses (id) ON DELETE CASCADE,
    coin_symbol  VARCHAR(20)    NOT NULL,
    target_price DECIMAL(18, 2) NOT NULL,
    alert_type   VARCHAR(20)    NOT NULL
                     CHECK (alert_type IN (
                         'ENTRY_1', 'ENTRY_2', 'ENTRY_3', 'ENTRY_4',
                         'ABSOLUTE_STOP', 'STOP_LOSS',
                         'TAKE_PROFIT', 'TAKE_PROFIT_2',
                         'SHORT_ENTRY'
                     )),
    status       VARCHAR(20)    NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'PENDING_SLOT', 'TRIGGERED', 'CANCELLED')),
    triggered_at TIMESTAMP,
    created_at   TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_price_alerts_analysis_id  ON price_alerts (analysis_id);
CREATE INDEX IF NOT EXISTS idx_price_alerts_coin_symbol  ON price_alerts (coin_symbol);
CREATE INDEX IF NOT EXISTS idx_price_alerts_status       ON price_alerts (status) WHERE status = 'PENDING';

-- -----------------------------------------
-- 4. post_links — 게시글 내 링크 (트레이딩뷰 등)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS post_links (
    id         BIGSERIAL    PRIMARY KEY,
    post_id    BIGINT       NOT NULL REFERENCES posts (id) ON DELETE CASCADE,
    url        TEXT         NOT NULL,
    link_type  VARCHAR(20)  NOT NULL
                   CHECK (link_type IN ('tradingview', 'youtube', 'other')),
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_post_links_post_id   ON post_links (post_id);
CREATE INDEX IF NOT EXISTS idx_post_links_link_type ON post_links (link_type);

-- -----------------------------------------
-- 5. video_memos — 영상 내용 직접 입력 메모
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS video_memos (
    id         BIGSERIAL PRIMARY KEY,
    post_id    BIGINT    REFERENCES posts (id) ON DELETE SET NULL,
    content    TEXT      NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_video_memos_post_id    ON video_memos (post_id);
CREATE INDEX IF NOT EXISTS idx_video_memos_created_at ON video_memos (created_at DESC);

-- -----------------------------------------
-- 6. news_articles — 실시간 뉴스 수집
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS news_articles (
    id             BIGSERIAL     PRIMARY KEY,
    source         VARCHAR(50)   NOT NULL,
    external_id    VARCHAR(255)  UNIQUE,
    title          TEXT          NOT NULL,
    summary        TEXT,
    url            TEXT          NOT NULL,
    published_at   TIMESTAMP     NOT NULL,
    sentiment      VARCHAR(10)   CHECK (sentiment IN ('BULLISH', 'BEARISH', 'NEUTRAL')),
    impact_level   VARCHAR(10)   CHECK (impact_level IN ('HIGH', 'MEDIUM', 'LOW')),
    related_coins  JSONB         NOT NULL DEFAULT '[]',
    gpt_analysis   TEXT,
    is_processed   BOOLEAN       NOT NULL DEFAULT FALSE,
    collected_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_published_at   ON news_articles (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_source         ON news_articles (source);
CREATE INDEX IF NOT EXISTS idx_news_impact         ON news_articles (impact_level);
CREATE INDEX IF NOT EXISTS idx_news_is_processed   ON news_articles (is_processed) WHERE is_processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_news_related_coins  ON news_articles USING GIN (related_coins);

-- -----------------------------------------
-- 7. user_profiles — 텔레그램 사용자 투자 성향
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS user_profiles (
    id                   BIGSERIAL     PRIMARY KEY,
    telegram_user_id     BIGINT        NOT NULL UNIQUE,
    telegram_username    VARCHAR(100),
    risk_tolerance       VARCHAR(20)   NOT NULL DEFAULT 'MODERATE'
                             CHECK (risk_tolerance IN ('CONSERVATIVE', 'MODERATE', 'AGGRESSIVE')),
    total_asset_krw      BIGINT,
    leverage             SMALLINT      NOT NULL DEFAULT 1
                             CHECK (leverage BETWEEN 1 AND 50),
    leverage_config      JSONB         NOT NULL DEFAULT '{}',
    trading_mode         VARCHAR(20)   NOT NULL DEFAULT 'SEMI_AUTO'
                             CHECK (trading_mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL', 'NOTIFY_ONLY')),
    auto_ratio           SMALLINT      NOT NULL DEFAULT 50
                             CHECK (auto_ratio BETWEEN 0 AND 100),
    preferred_coins      JSONB         NOT NULL DEFAULT '[]',
    onboarding_completed BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_telegram_id ON user_profiles (telegram_user_id);

-- -----------------------------------------
-- 8. positions — 오픈 포지션 상태 추적
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS positions (
    id                    BIGSERIAL      PRIMARY KEY,
    analysis_id           BIGINT         NOT NULL REFERENCES analyses (id),
    coin_symbol           VARCHAR(20)    NOT NULL,
    side                  VARCHAR(10)    NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    avg_entry_price       DECIMAL(18, 2) NOT NULL,
    initial_capital_usdt  DECIMAL(18, 2) NOT NULL,   -- 최초 진입 자본 (추가매수 금액 계산 기준)
    leverage              SMALLINT       NOT NULL,    -- 진입 당시 레버리지 스냅샷
    current_qty           DECIMAL(18, 8) NOT NULL,
    current_stop_loss     DECIMAL(18, 2),
    current_take_profit_1 DECIMAL(18, 2),
    current_take_profit_2 DECIMAL(18, 2),
    tp1_executed          BOOLEAN        NOT NULL DEFAULT FALSE,
    add_buy_count         SMALLINT       NOT NULL DEFAULT 0,
    bybit_position_idx    SMALLINT,                  -- 0=롱, 1=숏 (hedge mode)
    status                VARCHAR(10)    NOT NULL DEFAULT 'OPEN'
                              CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at             TIMESTAMP      NOT NULL DEFAULT NOW(),
    closed_at             TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_positions_analysis_id  ON positions (analysis_id);
CREATE INDEX IF NOT EXISTS idx_positions_coin_symbol  ON positions (coin_symbol);
CREATE INDEX IF NOT EXISTS idx_positions_status       ON positions (status) WHERE status = 'OPEN';

-- -----------------------------------------
-- 9. trades — 매매 실행 내역
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL      PRIMARY KEY,
    analysis_id     BIGINT         NOT NULL REFERENCES analyses (id),
    position_id     BIGINT         REFERENCES positions (id),
    symbol          VARCHAR(20)    NOT NULL,
    side            VARCHAR(10)    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty             DECIMAL(18, 8) NOT NULL,
    price           DECIMAL(18, 2),
    status          VARCHAR(20)    NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING', 'FILLED', 'FAILED', 'CANCELLED')),
    bybit_order_id  VARCHAR(100),
    stop_loss_price DECIMAL(18, 2),
    mode            VARCHAR(20)    NOT NULL CHECK (mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL')),
    executed_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trades_analysis_id  ON trades (analysis_id);
CREATE INDEX IF NOT EXISTS idx_trades_position_id  ON trades (position_id);
CREATE INDEX IF NOT EXISTS idx_trades_status       ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_executed_at  ON trades (executed_at DESC);

-- -----------------------------------------
-- 10. settings — 시스템 설정 (항상 1행 고정)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS settings (
    id                   INT            PRIMARY KEY DEFAULT 1,
    mode                 VARCHAR(20)    NOT NULL DEFAULT 'SEMI_AUTO'
                             CHECK (mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL')),
    max_trade_amount_krw INT            NOT NULL DEFAULT 100000,
    daily_loss_limit_krw INT            NOT NULL DEFAULT 300000,
    stop_loss_pct        DECIMAL(5, 4)  NOT NULL DEFAULT 0.03,
    is_halted            BOOLEAN        NOT NULL DEFAULT FALSE,
    updated_at           TIMESTAMP      NOT NULL DEFAULT NOW(),
    CONSTRAINT settings_single_row CHECK (id = 1)
);

INSERT INTO settings (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

-- -----------------------------------------
-- 11. daily_stats — 일별 통계
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS daily_stats (
    id                BIGSERIAL PRIMARY KEY,
    date              DATE      NOT NULL UNIQUE,
    total_trades      INT       NOT NULL DEFAULT 0,
    realized_pnl_krw  INT       NOT NULL DEFAULT 0,
    is_halted         BOOLEAN   NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats (date DESC);

-- -----------------------------------------
-- 12. economic_calendars — 경제지표 캘린더
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS economic_calendars (
    id          BIGSERIAL    PRIMARY KEY,
    source      VARCHAR(50)  NOT NULL DEFAULT 'finnhub',
    external_id VARCHAR(255) UNIQUE,
    event_name  TEXT         NOT NULL,
    event_date  DATE         NOT NULL,
    event_time  TIME,
    importance  VARCHAR(10)  NOT NULL CHECK (importance IN ('HIGH', 'MEDIUM', 'LOW')),
    description TEXT,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_econ_cal_event_date ON economic_calendars (event_date);
CREATE INDEX IF NOT EXISTS idx_econ_cal_importance ON economic_calendars (importance) WHERE importance = 'HIGH';

-- -----------------------------------------
-- 13. market_context — 시장 지표 (테더.D, BTC 도미넌스)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS market_context (
    id          BIGSERIAL    PRIMARY KEY,
    post_id     BIGINT       REFERENCES posts (id) ON DELETE CASCADE,
    indicator   VARCHAR(20)  NOT NULL,
    state       VARCHAR(50),
    key_level   TEXT,
    implication TEXT,
    summary     TEXT,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_context_post_id    ON market_context (post_id);
CREATE INDEX IF NOT EXISTS idx_market_context_created_at ON market_context (created_at DESC);
