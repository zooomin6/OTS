-- =============================================================
-- AI 코인 투자 어시스턴트 — DB 스키마 초기화
-- PostgreSQL 14+
-- 작성일: 2026-05-08
-- =============================================================

-- -----------------------------------------
-- 1. posts — 수집된 게시글
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS posts (
    id           BIGSERIAL    PRIMARY KEY,
    channel_id   VARCHAR(100) NOT NULL,
    post_id      VARCHAR(255) NOT NULL UNIQUE,
    content      TEXT         NOT NULL,
    published_at TIMESTAMP    NOT NULL,
    collected_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_posts_post_id       ON posts (post_id);
CREATE INDEX IF NOT EXISTS idx_posts_published_at  ON posts (published_at DESC);

-- -----------------------------------------
-- 2. analyses — GPT 분석 결과
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS analyses (
    id            BIGSERIAL   PRIMARY KEY,
    post_id       BIGINT      NOT NULL REFERENCES posts (id) ON DELETE CASCADE,
    signal_type   VARCHAR(10) NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD')),
    scenario_json JSONB       NOT NULL,
    summary       TEXT,
    invalidation  TEXT,
    raw_response  TEXT,
    created_at    TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- scenario_json 구조 예시:
-- [
--   { "step": 1, "target_price": 2450, "action": "1차 매도 (부분 실현)", "condition": "2,450 도달 시" },
--   { "step": 2, "target_price": null, "action": "관망",                 "condition": "2,150~2,200 지지 확인" },
--   { "step": 3, "target_price": 2700, "action": "재매수",               "condition": "지지 확인 후" }
-- ]

CREATE INDEX IF NOT EXISTS idx_analyses_post_id     ON analyses (post_id);
CREATE INDEX IF NOT EXISTS idx_analyses_signal_type ON analyses (signal_type);
CREATE INDEX IF NOT EXISTS idx_analyses_created_at  ON analyses (created_at DESC);

-- -----------------------------------------
-- 3. trades — 매매 실행 내역
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
    mode            VARCHAR(20)    NOT NULL CHECK (mode IN ('FULL_AUTO', 'SEMI_AUTO')),
    executed_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trades_analysis_id  ON trades (analysis_id);
CREATE INDEX IF NOT EXISTS idx_trades_status       ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_executed_at  ON trades (executed_at DESC);

-- -----------------------------------------
-- 4. settings — 시스템 설정 (항상 1행 고정)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS settings (
    id                   INT            PRIMARY KEY DEFAULT 1,
    mode                 VARCHAR(20)    NOT NULL DEFAULT 'SEMI_AUTO'
                             CHECK (mode IN ('FULL_AUTO', 'SEMI_AUTO')),
    max_trade_amount_krw INT            NOT NULL DEFAULT 100000,
    daily_loss_limit_krw INT            NOT NULL DEFAULT 300000,
    stop_loss_pct        DECIMAL(5, 4)  NOT NULL DEFAULT 0.03,
    is_halted            BOOLEAN        NOT NULL DEFAULT FALSE,
    updated_at           TIMESTAMP      NOT NULL DEFAULT NOW(),
    CONSTRAINT settings_single_row CHECK (id = 1)
);

-- 초기 설정값 삽입 (중복 실행 방지)
INSERT INTO settings (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

-- -----------------------------------------
-- 5. daily_stats — 일별 통계
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS daily_stats (
    id                BIGSERIAL PRIMARY KEY,
    date              DATE      NOT NULL UNIQUE,
    total_trades      INT       NOT NULL DEFAULT 0,
    realized_pnl_krw  INT       NOT NULL DEFAULT 0,
    is_halted         BOOLEAN   NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats (date DESC);
