# OTS — 바이빗 자동매매 시스템 프로젝트 문서

> 새 대화 시작 시 이 파일을 먼저 읽고 시작할 것.

---

## 코딩 원칙 (모든 코드 작업에 적용)

### 1. 코딩 전에 먼저 생각할 것
가정하지 말 것. 혼란을 숨기지 말 것. 트레이드오프를 드러낼 것.

구현 전에:
- 가정을 명시적으로 말할 것. 불확실하면 물어볼 것.
- 여러 해석이 가능하면 제시할 것 — 조용히 하나를 골라서 진행하지 말 것.
- 더 간단한 방법이 있으면 말할 것. 필요하면 반대 의견도 낼 것.
- 불명확한 게 있으면 멈출 것. 무엇이 헷갈리는지 짚고 물어볼 것.

### 2. 단순함 우선
문제를 해결하는 최소한의 코드만. 추측성 코드는 없음.

- 요청받지 않은 기능은 추가하지 않음.
- 한 번만 쓰는 코드에 추상화를 만들지 않음.
- 요청하지 않은 "유연성"/"설정 가능성"은 추가하지 않음.
- 일어날 수 없는 상황에 대한 에러 처리는 하지 않음.
- 200줄로 짰는데 50줄로 가능하다면 다시 짤 것.
- "시니어 엔지니어가 보면 과하게 복잡하다고 할까?" — 그렇다면 단순화할 것.

### 3. 외과적 수정
필요한 부분만 건드릴 것. 자기가 만든 흔적만 치울 것.

기존 코드 수정 시:
- 인접한 코드/주석/포맷팅을 "개선"하지 말 것.
- 망가지지 않은 걸 리팩토링하지 말 것.
- 본인 스타일과 다르더라도 기존 스타일을 따를 것.
- 관련 없는 죽은 코드를 발견하면 언급만 하고 삭제하지 말 것.

내 변경으로 생긴 고아 코드는:
- 내 변경으로 더 이상 쓰이지 않게 된 import/변수/함수는 제거.
- 원래 있던 죽은 코드는 요청 없이는 제거하지 않음.

기준: 변경한 모든 줄은 사용자 요청과 직접 연결되어야 함.

### 4. 목표 기반 실행
성공 기준을 정의하고, 검증될 때까지 반복할 것.

작업을 검증 가능한 목표로 바꿀 것:
- "검증 추가" → "잘못된 입력에 대한 테스트를 작성하고, 통과시킴"
- "버그 수정" → "버그를 재현하는 테스트를 작성하고, 통과시킴"
- "X 리팩토링" → "리팩토링 전후 테스트가 통과하는지 확인"

다단계 작업은 짧게 계획을 먼저 말할 것:
```
1. [단계] → 검증: [확인 방법]
2. [단계] → 검증: [확인 방법]
3. [단계] → 검증: [확인 방법]
```
강한 성공 기준이 있으면 독립적으로 반복 가능. 약한 기준("일단 되게 해줘")은 계속 되묻게 됨.

---

## 프로젝트 한 줄 요약

유튜브 멤버십 게시글(유튜버 투자 신호)을 자동 크롤링 → GPT-4o로 분석 → 텔레그램 알림 → 바이빗 자동 주문까지 연결하는 풀 파이프라인 자동매매 시스템.

---

## 전체 아키텍처

```
YouTube 멤버십 게시글
        ↓ (Selenium 크롤러, 60초 주기)
    PostgreSQL (posts 테이블)
        ↓ (Kafka post.new 토픽)
    GPT-4o 분석기
     ├─ TradingView 링크 → Selenium 스크린샷 → GPT Vision
     └─ analyses 테이블 저장 + price_alerts 등록
        ↓
    텔레그램 알림 (BUY/SELL 신호)
        ↓
    Price Monitor (Bybit WebSocket)
     └─ price_alerts 가격 도달 → 텔레그램 알림
        ↓
    Trade Executor → 바이빗 지정가 주문
```

**인프라**: Docker Compose (PostgreSQL, Redis, Kafka, Selenium, 앱 서비스들)

---

## 실행 중인 Docker 서비스

| 컨테이너 | 역할 |
|---------|------|
| `coin_crawler` | YouTube 크롤러 (60초 주기) |
| `coin_analyzer` | GPT-4o 분석기 (Kafka 컨슈머) |
| `coin_telegram_bot` | 텔레그램 봇 (명령어 처리) |
| `coin_price_monitor` | Bybit WebSocket 가격 감시 |
| `coin_market_watch` | 테더.D/BTC 조건 감시 (별도 폴링) |
| `coin_api` | FastAPI (포트 8000) |
| `coin_selenium` | Chrome WebDriver (크롤러/스크린샷 공용) |
| `coin_postgres` | PostgreSQL (포트 5432) |
| `coin_redis` | Redis (포트 6379) |
| `coin_kafka` | Kafka (포트 29092) |

---

## 모듈별 상세 기능

### 1. 크롤러 (`crawler/youtube_crawler.py`)
- Selenium으로 YouTube 멤버십 게시글 수집 (로그인 쿠키 주입 방식)
- 게시글당: 본문 텍스트, 이미지 URL, TradingView 링크, 게시 시간 추출
- Redis로 중복 방지 (7일 TTL)
- 새 게시글 발견 시 Kafka `post.new` 토픽으로 발행

### 2. GPT 분석기 (`analysis/gpt_analyzer.py`)
- Kafka 컨슈머 → 게시글 DB 조회 → GPT-4o 분석
- **TradingView 링크 있으면**: Selenium으로 스크린샷 → GPT Vision으로 차트까지 분석
- 게시글 1개에 여러 코인/타임프레임 분석 있으면 `analyses` 배열에 분리 저장

**추출하는 정보:**
| 필드 | 설명 |
|------|------|
| signal_type | BUY / SELL / HOLD / SKIP |
| coin_symbol | BTC, ETH, USDT.D 등 (가격 범위로 자동 추론) |
| timeframe | MONTHLY / WEEKLY / DAILY / HOURLY |
| entry_price_1~4 | 안정형 / 중립형 / 공격형 / 초공격형 진입가 |
| stop_loss_price | 손절가 (유튜버 명시 or 구간 하단 ×0.97 자동계산) |
| take_profit_price | 1차 목표가 |
| take_profit_price_2 | 2차 목표가 |
| absolute_stop | 마지노선 (시즌 종료 수준) |
| short_entry_price | SELL 신호 시 숏 진입가 |
| youtuber_zone_low/high | 유튜버 제시 매수/매도 구간 |
| risk_reward_ratio | R:R 비율 (entry_price_2 기준) |
| current_rsi | 언급된 RSI 수치 |
| summary | 핵심 내용 2~3문장 요약 |
| invalidation | 분석 무효화 조건 |
| scenario | 단계별 시나리오 JSON |

**타임프레임별 처리:**
- MONTHLY/WEEKLY: `is_reference_only=TRUE` → 참고 메시지만, 자동매매 없음
- DAILY: 자동매매 + 5일 후 만료
- HOURLY: 자동매매 + 24시간 후 만료

**이 유튜버 특화 언어 패턴 학습됨:**
- "삼발이" = 3개 지지에서 반등, "갈겨보리자" = 강한 BUY 신호
- "주황 박스" = 차트 매수 구간, "4-3 자리" = 엘리어트 4번 파동 3번 눌림
- "트뷰" = TradingView 링크, "60분봉" = HOURLY
- SKIP: 유튜브 영상 썸네일 → DB 저장 없이 텔레그램으로 수동 확인 요청

### 3. 텔레그램 봇 (`notification/telegram_bot.py`)
- BUY/SELL 신호 발생 시 알림 (HOLD는 알림 없음)
- 알림 내용: 신호, 코인, 진입가(사용자 성향 맞춤), 손절, 목표, R:R, 기술지표, 요약
- 피드백 버튼 (✅ 맞음 / ❌ 틀림) → `analyses.feedback` 업데이트
- `/coins`, `/scenario`, `/positions` 등 명령어 처리

### 4. Price Monitor (`price_monitor/monitor.py`)
- Bybit WebSocket으로 실시간 가격 구독
- `price_alerts` 테이블의 PENDING 알림 감시 (60초마다 DB 재조회)
- 가격 도달 시: DB TRIGGERED 업데이트 + 텔레그램 알림 발송
- Redis로 중복 알림 방지 (30분 TTL)
- PENDING_SLOT: 포지션 꽉 찼을 때 슬롯 생기면 자동 전환

**alert_type 종류:**
`ENTRY_1~4`, `STOP_LOSS`, `ABSOLUTE_STOP`, `TAKE_PROFIT`, `TAKE_PROFIT_2`, `SHORT_ENTRY`

### 5. Trade Executor (`trading/trade_executor.py`)
- TRIGGERED price_alerts 폴링 (3초 주기)
- 모드별 동작:
  - AUTO: 리스크 체크 → 즉시 Bybit 지정가 주문
  - SEMI_AUTO: 텔레그램 확인 대기 → 승인 시 주문 (**버튼 미연동, 항상 취소됨**)
  - MANUAL/NOTIFY_ONLY: 알림만
- 추가매수: `positions` 테이블에서 기존 포지션 확인 → 평단가 재계산
- 1차 익절 시 50% 청산 + 손절가를 1차TP로 이동
- 2차 익절 시 나머지 50% 전량 청산

### 6. Position Sync (`trading/position_sync.py`)
- analyzer에서 BUY/SELL 저장 직후 호출
- 사용자 성향에 맞는 진입가로 바이빗 주문 또는 텔레그램 안내
- 모드: AUTO → 즉시 주문 / SEMI_AUTO, MANUAL, NOTIFY_ONLY → 안내만

### 7. Market Watch (`market_watch/watcher.py`)
- **테더.D 저항 반락 감시**: CoinGecko API 폴링 (60초)
  - USDT.D >= 7.83% 터치 후 하락 전환 → 텔레그램 알림 "BTC/ETH 진입 신호"
- **BTC 저항 돌파 감시**: Bybit REST API 폴링 (30초)
  - BTC >= 74,000 돌파 → 텔레그램 알림 "BTC 롱 진입 신호"
- Redis로 중복 알림 방지 (4시간 쿨다운)
- ⚠️ 감시 조건은 대화 중 수동으로 설정된 값 — 새 분석 나오면 수동 업데이트 필요

---

## 데이터베이스 주요 테이블

| 테이블 | 용도 |
|-------|------|
| `posts` | 크롤링된 유튜브 게시글 |
| `post_links` | 게시글 내 링크 (tradingview/youtube/other) |
| `analyses` | GPT 분석 결과 (BUY/SELL/HOLD) |
| `price_alerts` | 가격 도달 감시 목록 (PENDING→TRIGGERED) |
| `trades` | 실행된 주문 이력 |
| `positions` | 현재 오픈 포지션 (추가매수 추적) |
| `market_context` | 테더.D / BTC.D 시장 지표 이력 |
| `user_profiles` | 사용자 설정 (성향/레버리지/모드) |
| `settings` | 시스템 설정 (매매 모드/일일 손실 한도) |
| `daily_stats` | 날짜별 수익 통계 |

---

## 매매 전략 설정 (2026-06 개정 — ⚠️ 코드 반영 예정, 아래 "미완성" 참고)

| 항목 | 설정값 |
|------|-------|
| 주문 방식 | 지정가 (Limit Order) |
| 마진 | 격리마진 (Isolated) + 양방향 (Hedge) |
| 동시 포지션 | 최대 2개 (BTC + ETH) |
| 자산 배분 | BTC 50% / ETH 50% |
| 복리 | 전체 잔고 기준 재배분 |
| 레버리지 | BTC 5배 / ETH 3배 (안전 상한, 그대로 유지) |

### 진입 규칙 (개정)
- **단일 진입** — 지정가 1개만 (분할매수 안 함). 진입가 = `entry_price_1` (박스 윗쪽 = 유튜버 레벨 최상단).
- **진입 필터** — 유튜버가 약세 신호(곧 큰 하락 / USDT.D 상승 / "당분간 관망")를 주면 진입하지 않음 (HOLD).

### 익절 규칙 (개정)
- **1차 익절**: 1차 목표가 도달 시 **50% 매도**. **손절 이동 안 함** (손절선 그대로 유지).
- **남은 50%**: 계속 보유. **강제 2차 익절(TP2) 없음** — 눌리면 아래 추가매수로 대응.

### 추가매수(물타기) 규칙 (개정)
- **트리거**: 유튜버가 **강한 표현("무조건/반드시/꼭")** 으로 새 진입점을 제시할 때만 산다.
  - 단순히 트뷰 차트로 "여기 반등자리다" 정도 + 내 평단과 가까우면 → **안 산다**.
- **의미 조건**: 현재 **포지션이 -35~40% 이상 물렸을 때만** 추매 (조금 물렸을 때 사면 평단 효과 미미 → 안 함).

### 손절 / 약세 대응 규칙 (개정)
- 평소엔 손절 안 함 (롱 편향, 웬만하면 버팀).
- 유튜버가 **하락 신호**를 주면:
  - **기본: 즉시 전량 청산.**
  - **예외 — 마지노선(추세선)을 함께 제시할 때** (예: "주봉 아래 추세선이 마지노선"):
    - 마지노선이 진입가 대비 **가격 -20% 이상 아래** → 하락폭 큼 → **즉시 전량 청산**.
    - 마지노선이 **가격 -20% 이내** → 버틸 만함 → 청산 대신 **그 추세선에서 추가매수**.
  - ※ 여기서 20%는 **가격 차이** 기준 (레버리지 반영 포지션 % 아님).

---

## 사용자 성향별 진입가

**BUY 신호:**
- entry_price_1 (CONSERVATIVE/안정형): 유튜버 레벨 최상단 (일찍 진입)
- entry_price_2 (MODERATE/중립형): 유튜버 레벨 중간
- entry_price_3 (AGGRESSIVE/공격형): 유튜버 레벨 하단 (깊은 하락 기다림)
- entry_price_4 (초공격형): 유튜버가 "마지막 매수"로 명시한 경우만

**SELL 신호:** 방향 반전 (높은 가격이 안전한 숏 자리)

---

## 알림 우선순위

| 색상 | 상황 |
|------|------|
| 🔴 긴급 | 마지노선 도달, 청산 임박, 손절 주문 실패 |
| 🟠 중요 | 진입가 도달, 주문 실패 |
| 🟡 일반 | 새 분석, 부분 익절, 미체결 만료 |

---

## 현재 알려진 문제 / 미완성 항목

### ❌ 미완성
0. **매매 실행 로직이 개정 전략과 불일치** (최우선)
   - 위 "매매 전략 설정 (개정)"은 결정됐으나 코드는 아직 옛 방식(3분할 진입, 강제 TP2, 손절 이동).
   - `trading/position_sync.py`(진입) + `trading/trade_executor.py`(익절/추매/손절)를 개정 전략에 맞게 수정 필요.
   - 추가로 `trade_executor`는 docker-compose에 서비스로 등록된 적 없어 현재 실행 안 됨 → 역할 분리(진입=sync, 익절·추매·손절=executor) 후 등록 필요.

1. **SEMI_AUTO 텔레그램 버튼 연동** (코드 완료, 배포 전)
   - `_wait_for_confirmation`을 Redis 우체통 방식으로 재작성 + 봇에 `^exec:` 콜백 추가 완료.
   - 단 trade_executor가 아직 실행 안 되므로 실제 동작은 배포 후 확인 필요.

2. **Market Watch 감시 조건 수동 관리**
   - USDT.D 저항(7.83%), BTC 돌파(74,000) 하드코딩
   - 새 분석 나올 때마다 `market_watch/watcher.py` 수동 수정 필요

3. **바이빗 실거래 미테스트**
   - 현재 `.env`에 `BYBIT_TESTNET=true`
   - 실거래 전 TESTNET으로 흐름 검증 필요

### ⚠️ 개선 필요
1. **GPT 분석 정확도 부족** (사용자 피드백)
   - 정확한 매수대 제시 못하는 경우 많음
   - 신호가 느림 (유튜버 게시글 의존)
   - "똑똑한 트레이더 옆에 두고 쓰는" 수준으로 개선 필요

2. **게시글 수정 감지 미구현**
   - `posts.content_hash` 컬럼 있으나 크롤러에서 업데이트 안 함

3. **crawler Selenium 타임아웃으로 가끔 죽음**
   - exit code 137로 강제 종료됨
   - Docker restart policy `unless-stopped`로 자동 재시작되나 25시간 죽어있었던 사례 있음

---

## 운영 환경

- **배포 목표**: AWS EC2 t2.micro (현재는 로컬 Docker)
- **현재 상태**: 로컬 Windows에서 Docker Compose 실행 중
- **바이빗 모드**: 양방향(Hedge) + 격리마진(Isolated)
- **GPT 모델**: gpt-4o (temperature=0.3)
- **최근 분석 패턴 few-shot**: DB에서 최근 8개 CORRECT 피드백 분석 참고
- **GPT 대화 기록**: 최근 20개 메시지 유지

---

## 시뮬레이션 모드 (CLAUDE.md 원래 기능)

사용자가 시장 상황이나 유튜버 게시글 내용을 제시하면:
- 텔레그램 봇처럼 BUY/SELL/HOLD 판단과 근거를 응답
- GPT API 호출 없이 직접 판단
- 판단 후 맞는지 틀린지 피드백도 수용
