-- v5: 자동매매 핵심 스키마 — positions, economic_calendars 신규 + 기존 테이블 확장
-- 적용: docker exec -i coin_postgres psql -U coinuser -d coin_assistant < migrations/v5_trading_schema.sql

-- -----------------------------------------
-- 1. posts — content_hash 추가 (게시글 수정 감지)
-- -----------------------------------------
ALTER TABLE posts
    ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);

-- -----------------------------------------
-- 2. analyses — timeframe / 숏 진입 관련 컬럼 추가
-- -----------------------------------------
ALTER TABLE analyses
    ADD COLUMN IF NOT EXISTS timeframe         VARCHAR(10)
        CHECK (timeframe IN ('MONTHLY', 'WEEKLY', 'DAILY', 'HOURLY')),
    ADD COLUMN IF NOT EXISTS is_reference_only BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS short_entry_price DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS short_stop_loss   DECIMAL(18, 2);

CREATE INDEX IF NOT EXISTS idx_analyses_timeframe ON analyses (timeframe);

-- -----------------------------------------
-- 3. price_alerts — 타입/상태 CHECK 재정의
-- -----------------------------------------
ALTER TABLE price_alerts
    DROP CONSTRAINT IF EXISTS price_alerts_type_check,
    DROP CONSTRAINT IF EXISTS price_alerts_status_check;

ALTER TABLE price_alerts
    ADD CONSTRAINT price_alerts_type_check
        CHECK (alert_type IN (
            'ENTRY_1', 'ENTRY_2', 'ENTRY_3', 'ENTRY_4',
            'ABSOLUTE_STOP', 'STOP_LOSS',
            'TAKE_PROFIT', 'TAKE_PROFIT_2',
            'SHORT_ENTRY'
        )),
    ADD CONSTRAINT price_alerts_status_check
        CHECK (status IN ('PENDING', 'PENDING_SLOT', 'TRIGGERED', 'CANCELLED'));

-- -----------------------------------------
-- 4. trades — position_id FK 추가
-- -----------------------------------------
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS position_id BIGINT;

-- positions 테이블 생성 후 FK를 걸어야 하므로 제약은 아래에서 추가

-- -----------------------------------------
-- 5. positions — 오픈 포지션 상태 추적 (신규)
-- -----------------------------------------
CREATE TABLE IF NOT EXISTS positions (
    id                     BIGSERIAL      PRIMARY KEY,
    analysis_id            BIGINT         NOT NULL REFERENCES analyses (id),
    coin_symbol            VARCHAR(20)    NOT NULL,
    side                   VARCHAR(10)    NOT NULL CHECK (side IN ('LONG', 'SHORT')),

    -- 진입 스냅샷 (추가매수 후 갱신)
    avg_entry_price        DECIMAL(18, 2) NOT NULL,
    initial_capital_usdt   DECIMAL(18, 2) NOT NULL,   -- 최초 진입 자본 (추가매수 금액 계산 기준)
    leverage               SMALLINT       NOT NULL,    -- 진입 당시 레버리지 (청산가 계산용)
    current_qty            DECIMAL(18, 8) NOT NULL,

    -- 현재 손익 기준가 (자동 업데이트)
    current_stop_loss      DECIMAL(18, 2),
    current_take_profit_1  DECIMAL(18, 2),
    current_take_profit_2  DECIMAL(18, 2),

    -- 상태 플래그
    tp1_executed           BOOLEAN        NOT NULL DEFAULT FALSE,
    add_buy_count          SMALLINT       NOT NULL DEFAULT 0,

    -- 바이빗 식별자
    bybit_position_idx     SMALLINT,                  -- 0=매수(롱), 1=매도(숏) in hedge mode

    status                 VARCHAR(10)    NOT NULL DEFAULT 'OPEN'
                               CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at              TIMESTAMP      NOT NULL DEFAULT NOW(),
    closed_at              TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_positions_analysis_id  ON positions (analysis_id);
CREATE INDEX IF NOT EXISTS idx_positions_coin_symbol  ON positions (coin_symbol);
CREATE INDEX IF NOT EXISTS idx_positions_status       ON positions (status) WHERE status = 'OPEN';

-- trades.position_id FK (positions 생성 후)
ALTER TABLE trades
    ADD CONSTRAINT IF NOT EXISTS trades_position_id_fk
        FOREIGN KEY (position_id) REFERENCES positions (id);

-- -----------------------------------------
-- 6. economic_calendars — 경제지표 캘린더 (신규)
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

CREATE INDEX IF NOT EXISTS idx_econ_cal_event_date  ON economic_calendars (event_date);
CREATE INDEX IF NOT EXISTS idx_econ_cal_importance  ON economic_calendars (importance) WHERE importance = 'HIGH';
