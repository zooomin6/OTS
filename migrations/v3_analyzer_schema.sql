-- v3: analyzer 고도화 — 성향별 진입가, 유튜버 구간, 기술적 지표 컬럼 추가

-- analyses 테이블에 새 컬럼 추가
ALTER TABLE analyses
    ADD COLUMN IF NOT EXISTS youtuber_zone_low   DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS youtuber_zone_high  DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS entry_price_3       DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS entry_price_4       DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS entry_ratio_1       SMALLINT,
    ADD COLUMN IF NOT EXISTS entry_ratio_2       SMALLINT,
    ADD COLUMN IF NOT EXISTS entry_ratio_3       SMALLINT,
    ADD COLUMN IF NOT EXISTS absolute_stop       DECIMAL(18, 2),
    ADD COLUMN IF NOT EXISTS current_rsi         DECIMAL(5, 2),
    ADD COLUMN IF NOT EXISTS risk_reward_ratio   DECIMAL(6, 2),
    ADD COLUMN IF NOT EXISTS rsi_signal          VARCHAR(10)
                                 CHECK (rsi_signal IN ('OVERSOLD', 'NEUTRAL', 'OVERBOUGHT')),
    ADD COLUMN IF NOT EXISTS volume_signal       VARCHAR(10)
                                 CHECK (volume_signal IN ('HIGH', 'NORMAL', 'LOW')),
    ADD COLUMN IF NOT EXISTS fib_level           DECIMAL(6, 3);

-- price_alerts: ENTRY_3 타입 지원을 위해 CHECK 제약 갱신
ALTER TABLE price_alerts
    DROP CONSTRAINT IF EXISTS price_alerts_alert_type_check;

ALTER TABLE price_alerts
    ADD CONSTRAINT price_alerts_alert_type_check
        CHECK (alert_type IN ('ENTRY_1', 'ENTRY_2', 'ENTRY_3', 'STOP_LOSS', 'TAKE_PROFIT'));
