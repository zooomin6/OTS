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
| AI | OpenAI GPT-4o |
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
- GPT-4o가 게시글을 단계별 투자 시나리오로 재구성
- 각 단계별 목표가 / 행동 지침 / 시나리오 무효 조건 추출
- 분석 결과 PostgreSQL 영구 저장

### Trading — 자동 매매
- **Full-auto 모드** (`/work`): 리스크 체크 후 Bybit 자동 주문 + 손절 동시 등록
- **Semi-auto 모드** (`/manual`): 텔레그램 버튼으로 사용자 확인 후 실행
- 1회 거래 한도 / 일일 손실 한도 / 자동 손절 리스크 제어 내장

### Telegram Bot — 명령어
- `/start` — 봇 시작 + 상태 안내
- `/status` — 수집 현황 + 오늘 거래 요약
- `/history` — 최근 5건 분석 이력 조회
- `/pause` / `/resume` — 수집 일시정지 / 재개
- `/work` / `/manual` — Full-auto / Semi-auto 모드 전환
- `/limit` — 리스크 한도 설정 조회

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
psql -U {유저명} -d coin_assistant -f db/init.sql
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
│   └── gpt_analyzer.py        # GPT-4o 시나리오 분석
├── trading/
│   ├── bybit_client.py        # Bybit API 주문 실행
│   └── risk_manager.py        # 리스크 제어 (한도 / 손절)
├── notification/
│   └── telegram_bot.py        # 텔레그램 봇 명령어 + 알림
├── db/
│   ├── models.py              # SQLAlchemy 모델
│   └── init.sql               # DB 스키마 초기화
├── api/
│   └── main.py                # FastAPI 엔드포인트
├── .env.example
└── docker-compose.yml
```
