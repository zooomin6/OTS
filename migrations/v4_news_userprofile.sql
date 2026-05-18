-- v4: 실시간 뉴스 수집 테이블 + 텔레그램 사용자 투자 성향 테이블 추가

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

CREATE TABLE IF NOT EXISTS user_profiles (
    id                   BIGSERIAL     PRIMARY KEY,
    telegram_user_id     BIGINT        NOT NULL UNIQUE,
    telegram_username    VARCHAR(100),
    risk_tolerance       VARCHAR(20)   NOT NULL DEFAULT 'MODERATE'
                             CHECK (risk_tolerance IN ('CONSERVATIVE', 'MODERATE', 'AGGRESSIVE')),
    total_asset_krw      BIGINT,
    leverage             SMALLINT      NOT NULL DEFAULT 1
                             CHECK (leverage BETWEEN 1 AND 50),
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
