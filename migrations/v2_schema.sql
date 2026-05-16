-- =============================================================
-- v2 마이그레이션 — 기존 DB에 적용
-- 실행: docker exec -i coin_postgres psql -U coinuser -d coin_assistant < migrations/v2_schema.sql
-- =============================================================

-- posts: 이미지 URL, 게시글 타입 추가
ALTER TABLE posts
    ADD COLUMN IF NOT EXISTS post_type  VARCHAR(10) NOT NULL DEFAULT 'text'
        CHECK (post_type IN ('text', 'video', 'mixed')),
    ADD COLUMN IF NOT EXISTS image_urls JSONB NOT NULL DEFAULT '[]';

CREATE INDEX IF NOT EXISTS idx_posts_post_type ON posts (post_type);

-- analyses: 코인 심볼, 가격 구간, 활성 여부 추가
ALTER TABLE analyses
    ADD COLUMN IF NOT EXISTS coin_symbol       VARCHAR(20),
    ADD COLUMN IF NOT EXISTS entry_price_1     DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS entry_price_2     DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS stop_loss_price   DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS take_profit_price DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS is_active         BOOLEAN   NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS expires_at        TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_analyses_coin_symbol ON analyses (coin_symbol);
CREATE INDEX IF NOT EXISTS idx_analyses_is_active   ON analyses (is_active) WHERE is_active = TRUE;

-- settings: 모드에 MANUAL 추가
ALTER TABLE settings DROP CONSTRAINT IF EXISTS settings_mode_check;
ALTER TABLE settings ADD CONSTRAINT settings_mode_check
    CHECK (mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL'));

-- trades: 모드에 MANUAL 추가, FULL_AUTO → AUTO 이름 통일
ALTER TABLE trades DROP CONSTRAINT IF EXISTS trades_mode_check;
ALTER TABLE trades ADD CONSTRAINT trades_mode_check
    CHECK (mode IN ('AUTO', 'SEMI_AUTO', 'MANUAL'));

-- 신규: price_alerts
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

CREATE INDEX IF NOT EXISTS idx_price_alerts_analysis_id ON price_alerts (analysis_id);
CREATE INDEX IF NOT EXISTS idx_price_alerts_coin_symbol ON price_alerts (coin_symbol);
CREATE INDEX IF NOT EXISTS idx_price_alerts_status      ON price_alerts (status) WHERE status = 'PENDING';

-- 신규: post_links
CREATE TABLE IF NOT EXISTS post_links (
    id         BIGSERIAL   PRIMARY KEY,
    post_id    BIGINT      NOT NULL REFERENCES posts (id) ON DELETE CASCADE,
    url        TEXT        NOT NULL,
    link_type  VARCHAR(20) NOT NULL
                   CHECK (link_type IN ('tradingview', 'youtube', 'other')),
    created_at TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_post_links_post_id   ON post_links (post_id);
CREATE INDEX IF NOT EXISTS idx_post_links_link_type ON post_links (link_type);

-- 신규: video_memos
CREATE TABLE IF NOT EXISTS video_memos (
    id         BIGSERIAL PRIMARY KEY,
    post_id    BIGINT    REFERENCES posts (id) ON DELETE SET NULL,
    content    TEXT      NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_video_memos_post_id    ON video_memos (post_id);
CREATE INDEX IF NOT EXISTS idx_video_memos_created_at ON video_memos (created_at DESC);
