-- =============================================================
-- AI 코인 투자 어시스턴트 — DB 스키마 초기화
-- PostgreSQL 14+
-- v2: coin_symbol, price_alerts, post_links, video_memos 추가
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
    id               BIGSERIAL      PRIMARY KEY,
    post_id          BIGINT         NOT NULL REFERENCES posts (id) ON DELETE CASCADE,
    signal_type      VARCHAR(10)    NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD')),
    coin_symbol      VARCHAR(20),
    entry_price_1    DECIMAL(18, 2),
    entry_price_2    DECIMAL(18, 2),
    stop_loss_price  DECIMAL(18, 2),
    take_profit_price DECIMAL(18, 2),
    scenario_json    JSONB          NOT NULL,
    summary          TEXT,
    invalidation     TEXT,
    raw_response     TEXT,
    is_active        BOOLEAN        NOT NULL DEFAULT TRUE,
    expires_at       TIMESTAMP,
    created_at       TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- scenario_json 구조 예시:
-- [
--   { "step": 1, "target_price": 95000, "action": "1차 매수", "condition": "95,000 도달 시" },
--   { "step": 2, "target_price": 92000, "action": "2차 매수", "condition": "92,000 도달 시" },
--   { "step": 3, "target_price": 105000, "action": "익절",   "condition": "목표가 도달 시" }
-- ]

CREATE INDEX IF NOT EXISTS idx_analyses_post_id      ON analyses (post_id);
CREATE INDEX IF NOT EXISTS idx_analyses_signal_type  ON analyses (signal_type);
CREATE INDEX IF NOT EXISTS idx_analyses_coin_symbol  ON analyses (coin_symbol);
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
                     CHECK (alert_type IN ('ENTRY_1', 'ENTRY_2', 'STOP_LOSS', 'TAKE_PROFIT')),
    status       VARCHAR(20)    NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'TRIGGERED', 'CANCELLED')),
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
-- 6. trades — 매매 실행 내역
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL      PRIMARY KEY,
    analysis_id     BIGINT         NOT NULL REFERENCES analyses (id),
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
CREATE INDEX IF NOT EXISTS idx_trades_status       ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_executed_at  ON trades (executed_at DESC);

-- -----------------------------------------
-- 7. settings — 시스템 설정 (항상 1행 고정)
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
-- 8. daily_stats — 일별 통계
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS daily_stats (
    id                BIGSERIAL PRIMARY KEY,
    date              DATE      NOT NULL UNIQUE,
    total_trades      INT       NOT NULL DEFAULT 0,
    realized_pnl_krw  INT       NOT NULL DEFAULT 0,
    is_halted         BOOLEAN   NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats (date DESC);
