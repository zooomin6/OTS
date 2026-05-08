"""
Selenium 기반 유튜브 멤버십 게시글 크롤러.
CRAWL_INTERVAL_SECONDS 간격으로 폴링 → Redis 중복 필터 → Kafka `post.new` 발행.
TODO: YoutubeCrawler 클래스 구현
"""
