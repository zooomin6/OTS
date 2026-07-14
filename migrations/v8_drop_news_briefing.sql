-- v8: 뉴스 크롤러 / 일일 브리핑 기능 제거에 따라 미사용 테이블 삭제
-- 적용: Get-Content migrations/v8_drop_news_briefing.sql | docker exec -i coin_postgres psql -U coinuser -d coin_assistant

DROP TABLE IF EXISTS news_articles;
DROP TABLE IF EXISTS economic_calendars;
