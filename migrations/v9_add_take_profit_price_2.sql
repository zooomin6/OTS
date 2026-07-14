-- v9: analyses 테이블에 take_profit_price_2(2차 목표가) 컬럼 추가
-- 적용: Get-Content migrations/v9_add_take_profit_price_2.sql | docker exec -i coin_postgres psql -U coinuser -d coin_assistant

ALTER TABLE analyses
    ADD COLUMN IF NOT EXISTS take_profit_price_2 NUMERIC(18, 2);
