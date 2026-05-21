# AI 코인 투자 어시스턴트

유튜버 멤버십 게시글을 AI가 자동 분석하여 투자 시나리오를 구조화하고, Bybit API를 통해 자동 매매까지 실행하는 개인용 투자 어시스턴트

**개발자**: 민규

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Language | Python 3.11+ |
| Framework | FastAPI |
| Crawler | Selenium |
| Message Queue | Apache Kafka |
| Cache | Redis |
| DB | PostgreSQL |
| ORM | SQLAlchemy |
| AI | OpenAI GPT-4o (Vision 포함) |
| 뉴스 | CryptoPanic API + RSS (CoinDesk, Cointelegraph) |
| 알림 | Telegram Bot API |
| 자동매매 | Bybit API (pybit) |
| Container | Docker + Docker Compose |

---

## 주요 기능

### Crawler — 게시글 수집
- Selenium으로 유튜버 멤버십 채널 1분 간격 폴링
- Redis 중복 필터로 동일 게시글 재처리 방지
- 새 게시글 감지 시 Kafka 토픽(`post.new`) 발행

### Analysis — AI 시나리오 분석
- GPT-4o (Vision)가 게시글 텍스트 + 차트 이미지를 함께 분석
- **차트 시간 단위(timeframe) 자동 감지** — 월봉/주봉은 참고 알림만, 일봉/시간봉은 자동매매 실행
- **투자 성향별 단일 진입가** 자동 배정 — 안정형(최상단) / 중립형 / 공격형 / 초공격형(최하단)
- 손절가 미제시 시 구간 하단 −3% 자동 설정, R:R 비율 자동 계산
- **SELL 신호**: 롱 청산 알림 + GPT 판단 시 숏 진입가/손절가 자동 제안
- 분석 결과(유튜버 구간·성향별 진입가·기술지표) PostgreSQL 영구 저장
- 일봉 분석은 5일, 시간봉 분석은 24시간 후 자동 만료

### News Monitor — 실시간 뉴스 분석 *(예정)*
- **CryptoPanic API** + RSS(CoinDesk, Cointelegraph) 실시간 수집
- 지정학적 이슈(전쟁·관세·규제·해킹) 등 코인 가격 영향 뉴스 자동 필터
- GPT-4o가 뉴스를 BULLISH/BEARISH/NEUTRAL + HIGH/MEDIUM/LOW 영향도로 분류
- HIGH 영향 뉴스는 텔레그램으로 즉시 알림

### Trading — 자동 매매
- **SEMI_AUTO 모드**: GPT 분석 결과를 텔레그램 버튼으로 확인 후 실행
- **AUTO 모드**: 리스크 체크 후 Bybit 자동 주문 + 손절 동시 등록
- **NOTIFY_ONLY 모드**: 매매 없이 알림만 수신
- 1회 거래 한도 / 일일 손실 한도 / 자동 손절 리스크 제어 내장

### Telegram Bot — 명령어
- `/start` — 봇 시작 + **투자 성향 온보딩** (처음 사용 시 자동 실행)
- `/status` — 수집 현황 + 오늘 거래 요약
- `/coins` — 현재 활성 코인 목록 조회
- `/scenario [코인]` — 최신 분석 시나리오 조회
- `/mode` — 매매 모드 변경 (AUTO / SEMI_AUTO / MANUAL / NOTIFY_ONLY)
- `/memo [내용]` — 영상 내용 메모 저장
- `/memos` — 최근 메모 목록 조회

### 사용자 온보딩 (첫 `/start` 시)
텔레그램 봇이 순서대로 질문하여 투자 프로필을 구성합니다:
1. **투자 성향** — 안정형 / 중립형 / 공격형
2. **총 투자 가능 자산** — 직접 입력 (원 단위)
3. **레버리지 배수** — 1x ~ 50x
4. **매매 방식** — 자동 / 반자동 / 수동 / 알림만
5. **자동매매 비중** — 자동+수동 혼합 시 자동 비중 (%)
6. **관심 코인** — BTC, ETH, SOL 등 선택

---

## API 명세

서버 실행 후 아래 주소에서 Swagger UI를 확인할 수 있습니다.

```
http://localhost:8000/docs
```

### 주요 엔드포인트

| Method | URL | 설명 |
|--------|-----|------|
| POST | `/analyze` | 텍스트 수동 입력 → GPT 분석 (테스트용) |
| GET | `/status` | 수집 + 시스템 상태 조회 |
| GET | `/history` | 최근 분석 이력 조회 |
| PATCH | `/settings` | 리스크 한도 / 모드 설정 변경 |
| GET | `/trades` | 매매 실행 내역 조회 |

---

## 실행 방법

### 1. 사전 요구사항
- Python 3.11+
- Docker + Docker Compose
- OpenAI API Key
- Telegram Bot Token + Chat ID
- Bybit API Key (테스트넷 권장)

### 2. 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 아래 항목을 설정합니다.

```env
OPENAI_API_KEY=sk-...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BYBIT_TESTNET=true
DATABASE_URL=postgresql://user:password@localhost:5432/coin_assistant
REDIS_URL=redis://localhost:6379
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### 3. DB 초기화

```bash
psql -U {유저명} -d coin_assistant -f init.sql
```

### 4. 빌드 및 실행

```bash
docker-compose up -d
```

---

## 프로젝트 구조

```
coin-assistant/
├── crawler/
│   └── youtube_crawler.py     # Selenium 1분 폴링 + Kafka 발행
├── analysis/
│   └── gpt_analyzer.py        # GPT-4o 시나리오 분석 (3분할·R:R·기술지표)
├── news/                      # (예정)
│   └── news_crawler.py        # CryptoPanic + RSS 실시간 뉴스 수집
├── trading/
│   ├── bybit_client.py        # Bybit API 주문 실행
│   └── risk_manager.py        # 리스크 제어 (한도 / 손절)
├── notification/
│   └── telegram_bot.py        # 텔레그램 봇 + 사용자 온보딩
├── db/
│   ├── models.py              # SQLAlchemy 모델
│   └── database.py            # async 엔진 + 세션 팩토리
├── migrations/
│   ├── v2_schema.sql           # v2: 이미지·링크·가격알림
│   ├── v3_analyzer_schema.sql  # v3: 성향별 진입가·유튜버 구간·기술지표
│   ├── v4_news_userprofile.sql # v4: 뉴스·사용자 프로필
│   └── v5_trading_schema.sql   # v5: positions·경제지표·timeframe·숏 진입
├── init.sql                   # DB 전체 스키마 (최신)
├── api/
│   └── main.py                # FastAPI 엔드포인트
├── .env.example
└── docker-compose.yml
```
