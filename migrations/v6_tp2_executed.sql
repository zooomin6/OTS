-- v6: positions 테이블에 tp2_executed 플래그 추가
-- 적용: docker exec -i coin_postgres psql -U coinuser -d coin_assistant < migrations/v6_tp2_executed.sql

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS tp2_executed BOOLEAN NOT NULL DEFAULT FALSE;
