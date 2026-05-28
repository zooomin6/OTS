-- v7: analyses 테이블에서 미사용 entry_ratio 컬럼 제거
-- 적용: Get-Content migrations/v7_drop_entry_ratio.sql | docker exec -i coin_postgres psql -U coinuser -d coin_assistant

ALTER TABLE analyses
    DROP COLUMN IF EXISTS entry_ratio_1,
    DROP COLUMN IF EXISTS entry_ratio_2,
    DROP COLUMN IF EXISTS entry_ratio_3;
