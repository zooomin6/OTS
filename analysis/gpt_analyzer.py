"""
GPT-4o 투자 시나리오 분석기.
Kafka post.new 컨슘 → DB에서 게시글 조회 → GPT-4o 분석 → analyses 저장 → Telegram 발송.
"""
from __future__ import annotations

import asyncio
import functools
import json
import os
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC_POST_NEW", "post.new")
KAFKA_GROUP        = "analysis-group"
DATABASE_URL       = os.environ["DATABASE_URL"]
OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# GPT-4o에게 줄 역할과 응답 형식 지시
SYSTEM_PROMPT = """\
당신은 국내 코인 투자 분석 어시스턴트입니다.
유튜브 멤버십 투자 게시글(텍스트 + 이미지 차트)을 분석하여 반드시 아래 JSON 형식만 반환하세요. 설명 텍스트 없이 JSON만.

{
  "analyses": [
    {
      "signal_type": "BUY" | "SELL" | "HOLD",
      "coin_symbol": "BTC" | "ETH" | "SOL" | "XRP" | 기타심볼 | null,
      "timeframe": "MONTHLY" | "WEEKLY" | "DAILY" | "HOURLY" | null,
      "youtuber_zone_low": 유튜버가 제시한 매수/매도구간 하단 (숫자) | null,
      "youtuber_zone_high": 유튜버가 제시한 매수/매도구간 상단 (숫자) | null,
      "entry_price_1": 안정형 진입가 (숫자) | null,
      "entry_price_2": 중립형 진입가 (숫자) | null,
      "entry_price_3": 공격형 진입가 (숫자) | null,
      "entry_price_4": 초공격형 진입가 — 마지막 매수/매도 (숫자) | null,
      "absolute_stop": 마지노선 — 이 아래면 시즌 종료 수준 (숫자) | null,
      "stop_loss_price": 손절가 (숫자) | null,
      "take_profit_price": 1차 목표 익절가 (숫자) | null,
      "take_profit_price_2": 2차 목표 익절가 (숫자) | null,
      "short_entry_price": SELL 신호 시 숏 진입 추천가 (숫자) | null,
      "short_stop_loss": SELL 신호 시 숏 손절가 (숫자) | null,
      "risk_reward_ratio": R:R 비율 (소수, 예: 2.5) | null,
      "current_rsi": 게시글에 언급된 RSI 현재값 (숫자, 예: 43.37) | null,
      "rsi_signal": "OVERSOLD" | "NEUTRAL" | "OVERBOUGHT" | null,
      "volume_signal": "HIGH" | "NORMAL" | "LOW" | null,
      "fib_level": 가장 가까운 피보나치 레벨 (예: 0.618) | null,
      "summary": "핵심 투자 내용 2~3문장 요약",
      "invalidation": "이 분석이 무효화되는 조건",
      "scenario": [
        {"step": 1, "action": "액션 설명", "condition": "진입·청산 조건", "target_price": null}
      ]
    }
  ],
  "market_indicators": {
    "tether_d": {
      "state": "BEARISH" | "BULLISH" | "NEUTRAL" | "WARNING" | null,
      "key_level": "현재 테더.D의 핵심 구간 설명" | null,
      "implication": "시장에 미치는 영향 한 문장" | null
    },
    "btc_dominance": {
      "state": "RISING" | "FALLING" | "NEUTRAL" | null,
      "implication": "BTC 도미넌스 변화가 알트에 미치는 영향" | null
    }
  }
}

**중요: 게시글에 여러 코인 또는 여러 타임프레임 분석이 포함된 경우, analyses 배열에 각각 분리해서 추가하세요.**
예) "1. 테더.D ... 2. 이더 60분 봉 ... 3. 비트 주봉 ..." → analyses 배열에 3개 항목
분석이 하나뿐이면 배열에 항목 1개. market_indicators는 최상위에 한 번만.

## 분석 원칙

### 1. 유튜버 신호 우선
- 유튜버가 제시한 가격들을 정확히 추출하세요. 텍스트에 명시된 숫자를 최우선으로 사용.
- 이미지 차트에 표시된 수치가 있으면 텍스트와 함께 참고.
- signal_type은 유튜버의 방향성(BUY/SELL/HOLD)을 따릅니다.

**이 유튜버의 기본 방향성: 롱(BUY) 편향**
- 기본적으로 롱(매수) 중심으로 분석합니다.
- **"지지 구간"** 언급 → 해당 가격에서 롱 진입 자리 = signal_type BUY, entry_price로 추출

**"저항 구간" 해석 규칙:**
- **"X 저항"**, **"X에서 저항"**, **"X까지 목표"** → 무조건 take_profit_price = X (롱 익절 자리)
  - 추가 설명 없이 "저항"이라고만 해도 → take_profit_price로 처리. SELL 신호 아님.
- **숏 진입**으로 해석하는 경우는 오직 유튜버가 명시적으로 언급할 때만:
  - "X에서 숏", "X에서 공매도", "X에서 매도 진입" → signal_type = SELL, short_entry_price = X
- **롱 익절 + 숏 동시**: "X에서 롱 끊고 숏으로 전환" → analyses 2개 분리 (BUY take_profit + SELL 신호)

### 2. 차트 시간 단위 (timeframe) 추출
- 게시글/차트에서 "월봉", "주봉", "일봉", "시봉/시간봉/1H/4H" 키워드를 찾아 판단.
- MONTHLY(월봉), WEEKLY(주봉): 참고용 분석 — 자동매매 주문 없음.
- DAILY(일봉), HOURLY(시간봉): 자동매매 실행 대상.
- 명확히 알 수 없으면 null.

### 3. 성향별 단일 진입가 배정

**BUY 신호일 때** — 각 성향의 사람은 자신의 레벨 하나에서만 매수합니다:
- **entry_price_1 (안정형)**: 유튜버 레벨 중 가장 높은 가격. 일찍 진입, 리스크 최소.
- **entry_price_2 (중립형)**: 유튜버 레벨 중 중간 가격.
- **entry_price_3 (공격형)**: 유튜버 레벨 중 하단 가격. 깊은 하락 기다림.
- **entry_price_4 (초공격형)**: 유튜버가 "마지막 매수" 또는 최저 레벨로 명시한 가격. 명시 없으면 null.

**SELL 신호일 때** — 방향이 반전됩니다 (숏은 높은 가격이 더 안전):
- **entry_price_1 (안정형)**: 유튜버 레벨 중 가장 낮은 가격. 충분히 내린 후 숏.
- **entry_price_2 (중립형)**: 유튜버 레벨 중 중간 가격.
- **entry_price_3 (공격형)**: 유튜버 레벨 중 가장 높은 가격. 일찍 숏 진입.
- **entry_price_4 (초공격형)**: 유튜버가 "가장 공격적 숏 자리"로 명시한 가격. 명시 없으면 null.

레벨이 4개보다 적으면 있는 것만 채우고 나머지는 null.
레벨이 1개뿐이면 entry_price_1에만 채우고 나머지 null.

### 4. 숏 진입가 (SELL 신호 전용)
- GPT가 숏 포지션 진입이 적절하다고 판단할 때만 short_entry_price를 채웁니다.
- 단순 롱 청산 알림에 그칠 경우 short_entry_price = null.
- short_stop_loss = short_entry_price × 1.03 (숏 손절은 진입가 위 +3%).
- 유튜버가 명시한 값 있으면 그 값 우선.

### 5. 마지노선 (absolute_stop) 추출
- 유튜버가 "시즌 종료", "추세선 붕괴", "절대 지지선" 등으로 표현한 가격.
- stop_loss_price와 다름: 여기 도달하면 단순 손절이 아니라 시장 방향 자체가 바뀐 것.
- 없으면 null.

### 6. 손절가 자동 계산
- 유튜버가 명시한 손절가 있으면 그대로 사용.
- BUY 신호 — 없으면: stop_loss_price = youtuber_zone_low × 0.97 (구간 하단 -3%)
- SELL 신호 — 없으면: stop_loss_price = youtuber_zone_high × 1.03 (구간 상단 +3%)

### 7. R:R 비율 계산
- risk_reward_ratio = (take_profit_price - entry_price_2) / (entry_price_2 - stop_loss_price)
- entry_price_2 기준 (중립형 기준값). 계산 불가능하면 null.
- **최소 목표가 거리 규칙**: take_profit_price는 entry_price_2 대비 최소 1.0% 이상 떨어져야 합니다.
  왕복 수수료(~0.11%) + 슬리피지를 고려하면 1% 미만 목표는 수익이 나지 않습니다.
  유튜버가 제시한 목표가가 1% 미만이면 nearest 저항선을 찾아 조정하거나 take_profit_price = null로 두세요.

### 8. 기술적 지표
- current_rsi: 유튜버가 텍스트에서 언급한 RSI 수치 (예: "rsi 43.37" → 43.37).
- rsi_signal: current_rsi 기준 30 이하 → OVERSOLD, 70 이상 → OVERBOUGHT, 나머지 → NEUTRAL.
- 거래량: 최근 캔들 거래량이 평균 대비 높으면 HIGH, 낮으면 LOW. 판단 불가 → null.
- 피보나치: 차트의 주요 되돌림 레벨 중 entry_price_2와 가장 가까운 값.

### 9. 무효화 조건
- BUY 신호: "종가 기준 {zone_low 또는 absolute_stop} 하향 이탈" 형식으로 반드시 포함.
- SELL 신호: "종가 기준 {zone_high} 상향 돌파" 형식으로 반드시 포함.

### 10. 유튜버 언어 패턴 해석

**지지·저항 표현** — 아래 표현이 나오면 해당 가격을 구간으로 인식:
- "지지", "지지선", "지지대", "지지 구간", "지지 존" → 해당 가격 = 매수 구간 하단
- "저항", "저항선", "저항대", "저항 구간", "저항 존" → 해당 가격 = 매도 구간 상단
- "매수 자리", "매수 구간", "매수 존", "롱 자리", "롱 진입" → BUY 진입 구간
- "매도 자리", "매도 구간", "숏 자리", "숏 진입" → SELL 진입 구간
- "목표가", "1차 목표", "2차 목표", "TP", "익절" → take_profit
- "손절", "SL", "스탑", "컷" → stop_loss_price
- "마지노선", "절대 지지", "추세 붕괴", "시즌 종료", "구조 붕괴" → absolute_stop

**시나리오 상태 표현**:
- "유효", "아직 유효", "시나리오 유효" → 기존 시나리오 유지. signal_type = HOLD.
- "무효", "시나리오 무효", "이탈" → 기존 시나리오 종료. is_active = FALSE 처리.
- "업데이트", "수정", "변경" → 기존 시나리오 덮어쓰기. 새 가격으로 갱신.
- "대기", "관망", "홀드" → HOLD.

**추가매수 표현**:
- "추가 매수", "추가매수", "분할 매수", "2차 진입", "3차 진입" → 해당 가격 = 다음 entry_price 레벨
- "마지막 매수", "최후 매수", "끝 자리" → entry_price_4

**게시글 유형 판단**:
- 차트 이미지 + 가격 언급 → 신규 시나리오 또는 업데이트
- 텍스트만 + "유효" → HOLD (기존 유지)
- "오늘 하루", "단기", "단타" → HOURLY
- "이번 주", "주간" → WEEKLY
- "이번 달", "월간" → MONTHLY

### 12. 유튜브 영상 썸네일 처리 (중요)
- 이미지가 유튜브 영상 썸네일인 경우(재생 버튼▶이 보이거나, 영상 제목 텍스트가 오버레이된 경우, 유튜브 UI 요소가 보이는 경우) **차트가 아니므로 분석하지 마세요.**
- 이 경우 signal_type을 반드시 **"SKIP"** 으로 설정하고, summary에 "영상 게시물 — 차트 분석 불가"라고 기입하세요.
- 나머지 필드는 모두 null로 두세요.
- analyses 배열에 SKIP 항목 하나만 반환하면 됩니다.

### 11. 코인 심볼 추론 (반드시 적용 — null 반환 최소화)

코인 이름이 명시되지 않아도 **가격 범위**로 반드시 추론하세요.

| 가격 범위 (USDT) | 코인 |
|---|---|
| 50,000 ~ 200,000 | BTC |
| 20,000 ~ 50,000 | BTC (약세장) |
| 1,800 ~ 5,000 | ETH |
| 500 ~ 1,800 | ETH (약세장) |
| 80 ~ 400 | SOL |
| 0.3 ~ 5 | XRP |
| 0.5 ~ 30 | LINK / AVAX / MATIC 등 중형 알트 |
| 0.001 ~ 0.1 | PEPE / WIF 등 밈코인 |

**도미넌스 지표 심볼 규칙 (반드시 적용)**:
- "테더.D", "테더도미넌스", "USDT.D", "usdt.d" 언급 → coin_symbol = "USDT.D"
- USDT.D 가격은 % 단위 (보통 4~8 사이). 게시글에 숫자가 나오면 youtuber_zone_low/high에 반드시 기입.
- "비트 도미넌스", "BTC.D" 언급 → coin_symbol = "BTC.D"
- "ETH/BTC" 차트 → coin_symbol = "ETH/BTC"
- 도미넌스/비율 지표는 timeframe을 차트 기준으로 추출 (주봉→WEEKLY, 4시간→HOURLY 등)

**추론 우선순위**:
1. 텍스트에 코인명 직접 언급 → 그대로 사용
2. 도미넌스 지표 언급 → 위 도미넌스 규칙 적용
3. entry_price 또는 stop_loss_price 범위 → 위 표 적용
4. absolute_stop 범위 → 위 표 적용 (entry 없을 때 fallback)
5. "비트/비트코인" → BTC, "이더/이더리움" → ETH
6. 이 채널은 ETH를 가장 많이 다룸 → 코인 미언급 + 가격 1500~5000 범위면 ETH 확정
7. 알트코인 게시글이나 가격 범위 불명확 시에만 null 허용

**예시**:
- "2050 아래에서 추가 매수" → 가격 2050 = ETH 범위 → coin_symbol = "ETH"
- "89,000 손절" → 가격 89,000 = BTC 범위 → coin_symbol = "BTC"
- "1920 아래부터 중요" → 가격 1920 = ETH 약세장 범위 → coin_symbol = "ETH"

### 12. 이 채널 특화 언어 패턴

이 유튜버의 고유 표현을 정확히 해석하세요:

**차트/분석 용어**:
- "트뷰", "트레이딩뷰" → TradingView 차트 링크
- "60분", "60분 봉" → HOURLY / "4시간", "4H" → HOURLY / "일봉" → DAILY / "주봉" → WEEKLY
- "주황 박스", "주황색 박스", "박스" → 유튜버가 차트에 표시한 매수/매도 구간
- "A, B, C", "1번, 2번, 3번" → 엘리어트 파동 레이블 (분석 레퍼런스)
- "마디", "3마디", "5마디" → 엘리어트 파동 카운트
- "브리핑한 대로", "저번에 말한", "이전 시나리오대로" → 기존 시나리오 유효(HOLD)
- "갈겨보리자", "매수를 갈겨", "갈기자" → BUY 신호 강도 강함
- "슈팅", "쏘는 그림" → 상승 돌파 예상
- "괴롭히고 있는" → 저항선 테스트 중 (HOLD)
- "우횡보", "횡보" → 방향성 없음(HOLD)

**매수 구조 표현**:
- "1차 매수", "1차로 매수" → entry_price_1 (안정형 첫 진입)
- "2차 매수", "추가로" → entry_price_2
- "마지막 자리", "끝 자리", "4-3 자리" → entry_price_4 (초공격형)
- "오늘 봉이 저가를 지키고 끝나면" → 확인 후 진입 조건 (조건부 BUY)
- "조금 더 안전하게 하고 싶은 분" → 안정형(entry_price_1) 표현

**지지 구조 표현**:
- "삼발이", "삼발이 기법", "삼발이 박격포 기법" → 3개 지지점에서 반등하는 패턴 = 지지가 견고함. signal_type = BUY, 지지 신뢰도 높음으로 해석
- "삼발이 나름 견고", "삼발이 견고하게 만들었음" → 3중 지지 확인 완료, 상승 전환 기대 (BUY 또는 HOLD 유지)

**시나리오 레이블**:
- "메인 시나리오" → 핵심 시나리오 유효
- "서브 시나리오", "대안" → 보조 시나리오 (is_reference_only 고려)
- "시나리오 살아있다", "아직 살아있음" → HOLD (기존 유지)

**매수 강도 표현** (강할수록 확신 높은 BUY):
- "갈겨보리자", "매수를 갈겨", "갈기세요", "조져보겠음", "1차로 갈기자" → BUY, 강한 확신
- "바겐세일", "바겐세일~" → 할인 구간 적극 매수 (BUY, 높은 확신)
- "개꿀", "포텐 좋다", "흐름이 좋음" → 긍정적 BUY
- "일단 1차매수", "그냥 1차" → 가벼운 첫 진입 BUY
- "없는데 잡고 싶다", "포지션 없는데 매수하고 싶다" → 진입 탐색 중 (BUY 예비)

**차트 구간 표현**:
- "주황색 구간", "주황 박스", "주황색 되돌림" → 차트에 표시된 핵심 매수/지지 구간
- "주황색 아래로 잠기면" → 해당 구간 이탈 = 손절 조건 (stop_loss)
- "A 위아래에서", "B 구간", "A 부근" → 차트 레이블 기준 진입 구간
- "위아래로 잘 비비면" → 해당 구간 횡보 예상, HOLD
- "체크 구간에서 시작" → 해당 가격 돌파 확인 후 진입 (조건부 BUY)

**엘리어트 파동 기반 진입 타이밍**:
- "4-3 자리", "4-3이라면" → 4번 파동의 3번째 눌림 = 강한 매수 자리
- "5마디 완성", "5파 완성" → 상승 끝물 주의
- "3마디", "A·B·C 완성" → 조정 완료 후 반등 기대
- "동그라미 12345" → 엘리어트 임펄스 파동 카운팅 중

**리스크 관리 표현**:
- "주황색 아래로 잠기면 짧게 손절" → tight stop, 구간 이탈 즉시 손절
- "본전 스탑 및 끌어올리기", "본전 스탑" → 진입 후 수익 시 SL을 진입가로 이동
- "오늘 봉이 어제 저가를 지키면", "봉 나오는 거 보고" → 캔들 종가 확인 조건부 진입
- "분할의 정도는 본인 리스크 감내도에 따라" → 비율 미지정, entry_ratio는 null

**이 유튜버의 기술적 분석 체계** (분석 판단에 활용):

① **다중 타임프레임 하향식 분석 (Top-Down)**
   - 항상 큰 봉(주봉/일봉)으로 큰 그림 먼저 → 작은 봉(60분/15분)으로 진입 타이밍
   - "주봉에서 추세선 지키면서", "일봉 단위로 크게 조정" → 큰 그림 기준
   - "60분 봉으로 진입" → 작은 봉 타이밍 진입

② **핵심 가격 레벨 ("맥점")**
   - "맥점" = 가장 중요한 지지/저항 가격 → entry_price_1 또는 youtuber_zone으로 추출
   - "안착" = 해당 가격 위에서 종가 마감 확인 → 확인 후 BUY 조건
   - "리테스트" = 이전 지지/저항 재방문 → 진입 기회
   - "매물대" = 과거 거래 집중 구간, 저항/지지로 작용

③ **엘리어트 파동 + 다이버전스 조합**
   - "12345" 상승 5파 완성 근처 → 조정 주의, HOLD 또는 약한 BUY
   - "ABC 조정 완료" → 다음 상승 시작, BUY 신호
   - "다이버전스 걸리면서" = RSI와 가격 방향 불일치 → 반전 신호 강화
   - "상승 다이버전스" → 하락 중 RSI 상승 = BUY 강화
   - "하락 다이버전스" → 상승 중 RSI 하락 = SELL/주의

④ **시장 지배력 지표 (도미넌스) 활용**
   - "OTHERS.D" = 알트코인 도미넌스. 상승 안착 → 알트 시즌 임박, BUY 강화
   - "USDT.D" = 달러 도미넌스. 하락 → 위험자산 선호 → BUY 환경
   - "ETH/BTC" = 이더리움 상대 강도. 상승 → ETH 강세 구간
   - "BTC.D" = 비트 도미넌스. 하락 → 알트 강세

⑤ **캔들 종가 확인 원칙**
   - "봉이 저가를 지키고 끝나는지" → 당일 봉 종가로 지지 확인 후 진입
   - "종가 기준 이탈" → 장중 이탈이 아닌 종가 기준 손절 판단
   - "일봉 아래 파란색 추세선" → 추세선 하향 이탈 = absolute_stop

⑥ **컬러 기반 차트 구간**
   - 주황색(orange) 박스/추세선 → 핵심 매수 구간 또는 단기 지지
   - 파란색(blue) 추세선 → 중장기 핵심 추세선, 이탈 시 시즌 종료 수준
   - 분홍색(pink) 박스 → 저항 구간
   - "색깔 아래로 잠기면" → 해당 색깔 구간 하향 이탈 = 손절 조건

**매크로/외부 리스크 표현** (분석 영향 없음, 무시):
- "전쟁 이슈", "트럼프 입놀림", "뉴스" → 외부 변수 언급, 시나리오 자체는 유효

### 15. 시장 지표 추출 (market_indicators)

게시글에 아래 지표가 언급되면 반드시 추출하세요.

**테더.D (USDT 도미넌스)**:
- "테더.D", "USDT.D", "테더 도미넌스" 언급 시 추출
- state 판단:
  - "A구간 위로 올라탔다", "상승 돌파", "위험 신호" → "WARNING" (알트 하락 압력)
  - "A구간 아래로 잠겼다", "하락 전환", "떨어지고 있어" → "BULLISH" (알트 상승 여건)
  - "횡보", "유지", "아직 판단 어려움" → "NEUTRAL"
  - "급등", "크게 상승" → "BEARISH" (알트 위험)
- key_level: 유튜버가 언급한 A구간, B추세선 등 핵심 레벨 설명
- implication: "테더.D 상승 → 알트 하락 압력" 또는 "테더.D 하락 → 알트 상승 여건" 형태

**BTC 도미넌스**:
- "BTC.D", "비트 도미넌스" 언급 시 추출
- 상승 → 알트 약세, 하락 → 알트 강세

지표 언급이 없으면 해당 필드 null.

**무시할 게시글 패턴** (signal_type=HOLD, 가격 추출 없음):
- "ㄱㄱㄱ" 연속 → 단순 반응/응원 게시글
- 생일 축하, 개인 일상 내용만 있는 경우
- "롯됐음", 짧은 감탄사만 있는 경우

**회고성 게시글 처리 (중요):**
다음 패턴이 포함된 게시글은 **새로운 매매 신호가 아닙니다**:
- "~이었죠", "~였죠", "~맞췄습니다", "~잡아드렸습니다", "~적중했습니다"
- "전세계에서 가장 정확하게 미리 잡아드렸습니다"
- "브리핑한 대로 됐죠", "말한 대로 됐습니다"
→ 이런 표현이 가격과 함께 나오면 signal_type = HOLD로 처리
→ 해당 가격은 **이미 지난 구간**이므로 entry_price로 추출하지 말 것
→ 단, 게시글 내에 "앞으로" 방향의 새로운 신호가 명시되면 그것만 추출

### 17. 매매 원칙 (실전 검증된 규칙)

**진입 원칙:**
- **명확한 지지선/추세선/채널 하단에서만 진입** — 중간 구간 진입 금지
- **저점이 높아지는 구조** (higher lows) = 매수 신호. 저점이 계속 올라오면 상승 에너지 응축 중
- **수평 지지 + 추세선 겹치는 구간** = 가장 강한 진입 자리 (confluence)
- **저항을 여러 번 테스트** = 조만간 돌파 가능성 높음 → BUY 신호 강화
- 가격이 이미 많이 올랐으면 **추격 금지** — 다음 조정 기다리기
- 진입 근거(지지선/채널/유튜버 명시)가 없으면 signal_type = HOLD

**삼각수렴 패턴 진입 원칙 (중요):**
- 삼각수렴은 방향이 불확실하다 → **이탈 방향 확인 후 진입**
- 하락 추세 중 삼각수렴 = **하방 이탈이 기본값**. 상방 돌파를 기대하고 먼저 진입하면 안 됨
- 상위 타임프레임(4시간/일봉)이 하락 추세이면 삼각수렴 하단 반등 매수 금지
- 삼각수렴 상방 이탈 확인 후 → 이탈한 가격 위에서 진입 (추격 아님, 확인 진입)

**다이버전스 진입 원칙:**
- 다이버전스는 반전 신호이지 반전 확정이 아님
- 다이버전스 + 지지선 + 유튜버 신호가 모두 겹칠 때 가장 신뢰도 높음
- 다이버전스만 단독으로는 진입 근거 부족 → 추가 조건 필요
- 확신이 있더라도 포지션 크기로 리스크 조절 (베팅 크기 ≠ 진입 여부)

**분할매수 원칙:**
- 1차 진입 후 추가매수는 **반드시 마지노선(absolute_stop) 설정 후**
- 마지노선 = 일봉 종가 기준 이탈 시 전체 손절 → 명확히 추출할 것
- 추세선 이탈은 추가매수 자리일 수 있음 (손절 기준 아님)
- 손절 없이 추가매수 = 절대 금지

**손절 원칙:**
- 손절 기준은 항상 **일봉 종가 기준** (장중 위크는 손절 아님)
- 진입 근거가 된 지지선/추세선이 일봉 종가로 이탈 → 즉시 손절
- 손절이 명확할수록 포지션 크기 자신감 있게 잡을 수 있음
- 지지선이 무너지면 "무조건" 지지라고 했던 곳도 손절 — "무조건"은 없음
- 손절 있어야 추가매수도 의미 있음

**놓친 기회 대응:**
- 진입 주문이 미체결되고 가격이 올라갔으면 → **추격 금지**
- 다음 조정 구간(이전 저항 → 지지 전환 구간)에서 새 진입 기다리기
- 놓친 기회를 쫓는 것보다 다음 기회를 기다리는 것이 항상 옳음

**상위 타임프레임 추세 원칙:**
- 4시간봉/일봉이 하락 추세이면 → 단기 반등 매수는 소량만, 큰 포지션 금지
- 하락 추세 중 지지선 = 참고 기준이지 절대 기준이 아님 (무너질 수 있음)
- 상위 TF 추세를 이기는 단기 신호는 없음 → 추세 확인이 먼저

### 18. 조건부 거시 방향성 추출 (최우선 처리)

유튜버가 아래 패턴을 제시하면 **반드시** `macro_rules` 배열을 추출하세요:

**인식 패턴:**
- "A가 X 이상/이하 [종가] 마감하면 → B는 [주봉/월봉] C 방향"
- "A가 X를 넘으면/깨지면 → B [주봉] 시나리오"
- "테더가 X 위로 올라가면 → 비트 주봉으로 봐야 한다"

**예시:**
"USDT.D가 7.83 이상 종가 마감하면 BTC 주봉 하락 시나리오"
→ trigger_coin="USDT.D", trigger_cond="CLOSE_ABOVE", trigger_level=7.83,
  result_coin="BTC", result_direction="BEARISH", result_timeframe="WEEKLY"

**응답 JSON 최상위에 macro_rules 배열 추가:**
```
"macro_rules": [
  {
    "trigger_coin": "USDT.D",
    "trigger_cond": "CLOSE_ABOVE" | "CLOSE_BELOW" | "BREAK_ABOVE" | "BREAK_BELOW",
    "trigger_level": 7.83,
    "result_coin": "BTC",
    "result_direction": "BEARISH" | "BULLISH",
    "result_timeframe": "WEEKLY" | "MONTHLY" | "DAILY",
    "result_target": 숫자 또는 null,
    "description": "원문 한 줄 요약"
  }
]
```

**이 규칙이 있을 때 처리 원칙:**
- 해당 result_coin의 BUY entry_price를 절대 제시하지 말 것
- 조건 충족 = 방향이 정해진 것 → "매수 타이밍 탐색"이 아니라 "방향 경고"
- signal_type은 HOLD 또는 SELL로 처리
- summary에 "거시 방향성 규칙 발동 시 [result_coin] [result_direction] — 롱 진입 금지" 명시

거시 방향성이 한번 발동되면 result_coin의 신규 매수 신호는 무효화됨을 인지할 것.

### 13. 차트 레이블 전용 게시글 처리

**텍스트에 구체적 가격 숫자가 없고 차트 레이블(A, B, C, 주황박스 등)만 언급된 경우**:
- signal_type은 맥락에 따라 BUY/SELL/HOLD 판단
- entry_price는 모두 null (차트 없이는 가격 확정 불가)
- summary에 "차트 이미지에서 A/B 구간 확인 필요" 명시
- 단, 텍스트 내 다른 가격(absolute_stop, SL 등)이 있으면 코인 추론에 활용

**이미지 없거나 이미지 URL 만료된 경우**:
- 텍스트만으로 분석 가능한 모든 정보를 추출
- "이미지 확인 불가" 등의 이유로 signal_type=HOLD로 낮추지 말 것
- 텍스트 맥락이 BUY면 BUY, 가격 추론 가능하면 entry_price 채울 것

### 14. 진입가 자동 추론 (가격 언급 있으나 entry_price 불명확할 때)

유튜버가 명확한 진입가를 제시하지 않아도 다음 규칙으로 entry_price_1을 채우세요:
- "X 부근에서 매수" → entry_price_1 = X
- "X 위에서 매수" → entry_price_1 = X × 1.005 (0.5% 위)
- "X 아래에서 매수" → entry_price_1 = X × 0.995 (0.5% 아래)
- "X ~ Y 구간 매수" → entry_price_1 = Y (상단, 안정형), entry_price_2 = (X+Y)/2, entry_price_3 = X (하단, 공격형)
- 이미 매수 중 "추가 매수 X" → entry_price_2 또는 entry_price_3 = X

### 16. 목표가(take_profit_price) 추론 강화

유튜버가 명확한 목표가를 제시하지 않아도 다음 순서로 추론하세요:

**1순위 — 직접 언급:**
- "X 목표", "X에서 익절", "X 도달 시 매도", "X까지 간다" → take_profit_price = X
- "1차 목표 X, 2차 목표 Y" → take_profit_price = X (2차는 scenario에 포함)
- "X~Y에서 매도", "X~Y 완벽한 타점" → take_profit_price = X (하단, 보수적 목표)

**2순위 — 저항선/전고점 (BUY 신호의 목표가):**
- "X 저항", "X에서 저항", "X까지 오르면 막힌다" → take_profit_price = X (롱의 익절 구간)
- "전고점", "이전 고점", "A 고점(위)" 등 가격이 함께 언급되면 → take_profit_price = 해당 가격
- "X 돌파하면 Y까지" → take_profit_price = Y
- 이 유튜버는 롱 편향이므로 "저항"은 SELL 신호가 아닌 BUY의 take_profit으로 처리

**3순위 — 차트 이미지 기반 (이미지 있을 때):**
- 차트에 분홍색 박스·저항선이 진입가 위에 표시되어 있으면 → 그 가격 = take_profit_price
- 차트 레이블 "A", "B" 등이 진입 후 위쪽에 있으면 → 가장 가까운 것 = take_profit_price

**4순위 — R:R 2:1 역산 (최후 수단, 위 3가지로 추출 불가할 때만):**
- entry_price_2와 stop_loss_price가 모두 있을 때:
  BUY: take_profit_price = entry_price_2 + (entry_price_2 - stop_loss_price) × 2
  SELL: take_profit_price = short_entry_price - (stop_loss_price - short_entry_price) × 2
- 이 방법 사용 시 summary 끝에 "(목표가: R:R 2:1 추정)" 추가

**null 허용:**
- 차트도 없고 저항선 언급도 없고 entry/SL도 불명확한 순수 시황 게시글 → null 유지
"""


# ── DB ───────────────────────────────────────────────────────

def _db_connect():
    import psycopg2
    from urllib.parse import urlparse
    url = (DATABASE_URL
           .replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgresql+psycopg://",  "postgresql://"))
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        dbname=p.path.lstrip("/"),
        options="-c client_encoding=UTF8",
    )


def _fetch_post_sync(post_db_id: int) -> dict | None:
    """DB id로 게시글을 조회한다. image_urls도 함께 반환해 Vision 분석에 활용한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, content, channel_id, image_urls, post_type FROM posts WHERE id = %s",
                (post_db_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id":         row[0],
                "content":    row[1],
                "channel_id": row[2],
                "image_urls": row[3] or [],   # JSONB → Python 리스트
                "post_type":  row[4],
            }
    finally:
        conn.close()


def _fetch_tv_links_sync(post_id: int) -> list[str]:
    """해당 게시물의 TradingView 링크 목록 반환."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT url FROM post_links WHERE post_id = %s AND link_type = 'tradingview' LIMIT 3",
                (post_id,),
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _screenshot_tv_sync(url: str) -> str | None:
    """Selenium Remote로 TradingView 차트 스크린샷 → data URI 반환."""
    import time
    try:
        from selenium import webdriver
        options = webdriver.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",
            options=options,
        )
        try:
            driver.get(url)
            time.sleep(7)  # 차트 렌더링 대기
            b64 = driver.get_screenshot_as_base64()
            return f"data:image/png;base64,{b64}"
        finally:
            driver.quit()
    except Exception as e:
        print(f"[analyzer] TradingView 스크린샷 실패 ({url}): {e}")
        return None


def _calc_expires_at(timeframe: str | None) -> str | None:
    """timeframe 기반으로 expires_at 문자열을 계산한다 (DB NOW() 기준 상대값)."""
    if timeframe == "DAILY":
        return "NOW() + INTERVAL '5 days'"
    if timeframe == "HOURLY":
        return "NOW() + INTERVAL '24 hours'"
    return None  # MONTHLY / WEEKLY / None → 만료 없음


def _save_macro_rules_sync(analysis_id: int, rules: list[dict]) -> None:
    """GPT가 추출한 거시 방향성 규칙을 macro_rules 테이블에 저장."""
    if not rules:
        return
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            # 테이블 없으면 생성
            cur.execute("""
                CREATE TABLE IF NOT EXISTS macro_rules (
                    id               BIGSERIAL PRIMARY KEY,
                    analysis_id      BIGINT REFERENCES analyses(id) ON DELETE CASCADE,
                    trigger_coin     VARCHAR(20) NOT NULL,
                    trigger_cond     VARCHAR(20) NOT NULL,
                    trigger_level    NUMERIC(18,4) NOT NULL,
                    result_coin      VARCHAR(20) NOT NULL,
                    result_direction VARCHAR(10) NOT NULL,
                    result_timeframe VARCHAR(10),
                    result_target    NUMERIC(18,2),
                    is_active        BOOLEAN DEFAULT TRUE,
                    description      TEXT,
                    created_at       TIMESTAMP DEFAULT NOW()
                )
            """)
            for rule in rules:
                try:
                    cur.execute("""
                        INSERT INTO macro_rules
                        (analysis_id, trigger_coin, trigger_cond, trigger_level,
                         result_coin, result_direction, result_timeframe, result_target, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        analysis_id,
                        rule.get("trigger_coin", ""),
                        rule.get("trigger_cond", "CLOSE_ABOVE"),
                        float(rule.get("trigger_level", 0)),
                        rule.get("result_coin", "BTC"),
                        rule.get("result_direction", "BEARISH"),
                        rule.get("result_timeframe"),
                        float(rule["result_target"]) if rule.get("result_target") else None,
                        rule.get("description", ""),
                    ))
                    print(f"[analyzer] 거시규칙 저장: {rule.get('trigger_coin')} "
                          f"{rule.get('trigger_cond')} {rule.get('trigger_level')} "
                          f"→ {rule.get('result_coin')} {rule.get('result_direction')}")
                except Exception as e:
                    print(f"[analyzer] 거시규칙 저장 실패: {e}")
        conn.commit()
    finally:
        conn.close()


def _save_analysis_sync(
    post_db_id: int,
    signal_type: str,
    coin_symbol: str | None,
    timeframe: str | None,
    is_reference_only: bool,
    youtuber_zone_low: float | None,
    youtuber_zone_high: float | None,
    entry_price_1: float | None,
    entry_price_2: float | None,
    entry_price_3: float | None,
    entry_price_4: float | None,
    absolute_stop: float | None,
    stop_loss_price: float | None,
    take_profit_price: float | None,
    short_entry_price: float | None,
    short_stop_loss: float | None,
    risk_reward_ratio: float | None,
    current_rsi: float | None,
    rsi_signal: str | None,
    volume_signal: str | None,
    fib_level: float | None,
    summary: str,
    invalidation: str,
    scenario_json: list,
    raw_response: str,
) -> int:
    """analyses 테이블에 저장하고 새 analysis id를 반환한다."""
    expires_expr = _calc_expires_at(timeframe)

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO analyses (
                    post_id, signal_type, coin_symbol,
                    timeframe, is_reference_only,
                    youtuber_zone_low, youtuber_zone_high,
                    entry_price_1, entry_price_2, entry_price_3, entry_price_4,
                    absolute_stop, stop_loss_price, take_profit_price,
                    short_entry_price, short_stop_loss,
                    risk_reward_ratio, current_rsi, rsi_signal, volume_signal, fib_level,
                    summary, invalidation, scenario_json, raw_response,
                    expires_at
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s::jsonb, %s,
                    {expires_expr if expires_expr else 'NULL'}
                )
                RETURNING id
                """,
                (
                    post_db_id, signal_type, coin_symbol,
                    timeframe, is_reference_only,
                    youtuber_zone_low, youtuber_zone_high,
                    entry_price_1, entry_price_2, entry_price_3, entry_price_4,
                    absolute_stop, stop_loss_price, take_profit_price,
                    short_entry_price, short_stop_loss,
                    risk_reward_ratio, current_rsi, rsi_signal, volume_signal, fib_level,
                    summary, invalidation,
                    json.dumps(scenario_json, ensure_ascii=False),
                    raw_response,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
    finally:
        conn.close()


def _save_market_context_sync(post_db_id: int, indicators: dict) -> None:
    """market_indicators 필드에서 테더.D 등 시장 지표를 market_context 테이블에 저장."""
    if not indicators:
        return
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            tether = indicators.get("tether_d") or {}
            if tether.get("state"):
                cur.execute(
                    """
                    INSERT INTO market_context (post_id, indicator, state, key_level, implication, summary)
                    VALUES (%s, 'TETHER_D', %s, %s, %s, %s)
                    """,
                    (post_db_id, tether.get("state"), tether.get("key_level"),
                     tether.get("implication"), tether.get("summary")),
                )
            btc_d = indicators.get("btc_dominance") or {}
            if btc_d.get("state"):
                cur.execute(
                    """
                    INSERT INTO market_context (post_id, indicator, state, implication)
                    VALUES (%s, 'BTC_D', %s, %s)
                    """,
                    (post_db_id, btc_d.get("state"), btc_d.get("implication")),
                )
        conn.commit()
    finally:
        conn.close()


def _create_price_alerts_sync(
    analysis_id: int,
    coin_symbol: str,
    entry_price_1: float | None,
    entry_price_2: float | None,
    entry_price_3: float | None,
    entry_price_4: float | None,
    stop_loss_price: float | None,
    take_profit_price: float | None,
    take_profit_price_2: float | None,
) -> None:
    """
    분석 결과에서 추출된 가격 수치를 price_alerts 테이블에 등록한다.
    Price Monitor 서비스가 이 테이블을 보고 바이빗 실시간 가격과 비교해 알림을 발송한다.
    """
    alerts = []
    if entry_price_1:
        alerts.append(("ENTRY_1",       entry_price_1))
    if entry_price_2:
        alerts.append(("ENTRY_2",       entry_price_2))
    if entry_price_3:
        alerts.append(("ENTRY_3",       entry_price_3))
    if entry_price_4:
        alerts.append(("ENTRY_4",       entry_price_4))
    if stop_loss_price:
        alerts.append(("STOP_LOSS",     stop_loss_price))
    if take_profit_price:
        alerts.append(("TAKE_PROFIT",   take_profit_price))
    if take_profit_price_2:
        alerts.append(("TAKE_PROFIT_2", take_profit_price_2))

    if not alerts:
        return

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            for alert_type, price in alerts:
                cur.execute(
                    """
                    INSERT INTO price_alerts (analysis_id, coin_symbol, target_price, alert_type)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (analysis_id, coin_symbol, price, alert_type),
                )
        conn.commit()
        print(f"[analyzer] 가격 알림 등록: {len(alerts)}개 ({coin_symbol})")
    finally:
        conn.close()


# ── 채널 패턴 학습 컨텍스트 ──────────────────────────────────

def _fetch_recent_context_sync(limit: int = 8) -> str:
    """
    최근 BUY/SELL 분석 결과를 few-shot 예시로 반환.
    BTC/ETH/USDT.D로 필터링하고 가상 P&L 정보를 포함한다.
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.content, a.signal_type, a.coin_symbol, a.timeframe,
                       a.entry_price_1, a.entry_price_2, a.entry_price_3,
                       a.stop_loss_price, a.take_profit_price, a.summary,
                       a.feedback, a.feedback_source, a.virtual_pnl_pct
                FROM analyses a
                JOIN posts p ON a.post_id = p.id
                WHERE a.signal_type IN ('BUY', 'SELL')
                  AND a.summary IS NOT NULL AND a.summary != ''
                  AND a.coin_symbol IN ('BTC', 'ETH', 'USDT.D')
                  AND (a.feedback IS NULL OR a.feedback = 'CORRECT')
                ORDER BY
                  CASE WHEN a.feedback = 'CORRECT' THEN 0 ELSE 1 END,
                  CASE WHEN a.virtual_pnl_pct IS NOT NULL THEN a.virtual_pnl_pct ELSE 0 END DESC,
                  a.created_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return ""

    examples = []
    for row in reversed(rows):  # 오래된 것부터 → 최신 순서로
        (content_preview, signal, coin, tf,
         e1, e2, e3, sl, _, summary,
         feedback, fb_source, pnl_pct) = row

        tf_str = tf or "미확인"
        e_str  = " / ".join(f"{v:,.0f}" for v in [e1, e2, e3] if v) or "-"
        sl_str = f"{sl:,.0f}" if sl else "-"

        # 라벨: 검증 여부 + P&L 표시
        if feedback == "CORRECT":
            if pnl_pct is not None:
                src = "AUTO" if fb_source == "AUTO" else "MANUAL"
                label = f" [검증됨✅{src} {float(pnl_pct):+.1f}%]"
            else:
                label = " [검증됨✅]"
        else:
            label = ""

        examples.append(
            f"[예시{label}] 게시글: {content_preview[:120]}...\n"
            f"→ 신호:{signal} 코인:{coin} 타임프레임:{tf_str} "
            f"진입가:{e_str} 손절:{sl_str}\n"
            f"→ 요약: {summary[:100]}"
        )

    return (
        "\n\n--- 이 채널의 최근 분석 패턴 참고 (few-shot) ---\n"
        + "\n\n".join(examples)
        + "\n--- 위 패턴을 참고해 아래 새 게시글을 분석하세요 ---\n"
    )


def _fetch_coin_active_context_sync(before_post_id: int) -> str:
    """
    현재 게시글 이전의 BTC/ETH/USDT.D 활성 시나리오를 시간순으로 반환.

    GPT가 이 컨텍스트를 보고:
    - 이미 진입 중인 포지션이 있는지 파악
    - 새 게시글의 가격이 기존 진입가 대비 높은지(TP/저항) 낮은지(추가매수) 판단
    - 주봉/월봉 기준 큰 그림(마지노선, 지지선) 인식
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    a.coin_symbol, a.signal_type, a.timeframe,
                    a.entry_price_1, a.entry_price_2,
                    a.stop_loss_price, a.take_profit_price,
                    a.absolute_stop,
                    a.youtuber_zone_low, a.youtuber_zone_high,
                    a.summary, a.created_at
                FROM analyses a
                WHERE a.coin_symbol IN ('BTC', 'ETH', 'USDT.D')
                  AND a.is_active = TRUE
                  AND a.post_id < %s
                  AND a.signal_type IN ('BUY', 'SELL', 'HOLD')
                ORDER BY a.coin_symbol, a.created_at ASC
                LIMIT 12
            """, (before_post_id,))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return ""

    from datetime import datetime as _dt

    now = _dt.now()
    current_coin = None
    blocks: list[str] = []
    coin_lines: list[str] = []

    def _flush():
        if current_coin and coin_lines:
            blocks.append(f"[{current_coin}]\n" + "\n".join(coin_lines))

    for row in rows:
        coin, signal, tf, e1, e2, sl, tp, abs_stop, zl, zh, summary, created_at = row

        if coin != current_coin:
            _flush()
            coin_lines = []
            current_coin = coin

        tf_str   = tf or "?"
        days_ago = (now - created_at).days if created_at else 0
        time_str = f"{days_ago}일 전" if days_ago >= 1 else "오늘"

        parts = [f"  {time_str} [{tf_str}] {signal}"]
        if e1:  parts.append(f"진입:{float(e1):,.0f}")
        if e2:  parts.append(f"/{float(e2):,.0f}")
        if sl:  parts.append(f"  손절:{float(sl):,.0f}")
        if tp:  parts.append(f"  목표:{float(tp):,.0f}")
        if abs_stop: parts.append(f"  마지노선:{float(abs_stop):,.0f}")
        if zl and zh: parts.append(f"  구간:{float(zl):,.0f}~{float(zh):,.0f}")

        if summary:
            parts.append(f"\n    요약: {summary[:80]}")

        coin_lines.append("".join(parts))

    _flush()

    if not blocks:
        return ""

    return (
        "\n\n=== 이전 게시글 기준 현재 활성 시나리오 (시간순, 맥락 파악용) ===\n"
        + "\n\n".join(blocks)
        + "\n\n"
        + "※ 주의: 위 시나리오에서 이미 진입한 포지션이 있다면,\n"
        + "   새 게시글의 가격 언급이 기존 진입가보다 높으면 → 목표가(take_profit) 또는 회고\n"
        + "   기존 진입가보다 낮으면 → 추가매수 또는 손절 구간\n"
        + "=== 위 맥락을 반드시 참고하세요 ===\n"
    )


# ── GPT-4o ───────────────────────────────────────────────────

def _parse_analysis_item(item: dict, raw: str) -> dict:
    """GPT 응답의 analyses 배열 항목 하나를 정규화한다."""
    timeframe = item.get("timeframe")
    if timeframe:
        timeframe = timeframe.upper()
        if timeframe not in ("MONTHLY", "WEEKLY", "DAILY", "HOURLY"):
            timeframe = None
    return {
        "signal_type":         (item.get("signal_type") or "HOLD").upper(),
        "coin_symbol":         item.get("coin_symbol"),
        "timeframe":           timeframe,
        "is_reference_only":   timeframe in ("MONTHLY", "WEEKLY"),
        "youtuber_zone_low":   item.get("youtuber_zone_low"),
        "youtuber_zone_high":  item.get("youtuber_zone_high"),
        "entry_price_1":       item.get("entry_price_1"),
        "entry_price_2":       item.get("entry_price_2"),
        "entry_price_3":       item.get("entry_price_3"),
        "entry_price_4":       item.get("entry_price_4"),
        "absolute_stop":       item.get("absolute_stop"),
        "stop_loss_price":     item.get("stop_loss_price"),
        "take_profit_price":   item.get("take_profit_price"),
        "take_profit_price_2": item.get("take_profit_price_2"),
        "short_entry_price":   item.get("short_entry_price"),
        "short_stop_loss":     item.get("short_stop_loss"),
        "risk_reward_ratio":   item.get("risk_reward_ratio"),
        "current_rsi":         item.get("current_rsi"),
        "rsi_signal":          item.get("rsi_signal"),
        "volume_signal":       item.get("volume_signal"),
        "fib_level":           item.get("fib_level"),
        "summary":             item.get("summary", ""),
        "invalidation":        item.get("invalidation", ""),
        "scenario":            item.get("scenario", []),
        "raw":                 raw,
    }


async def _analyze_with_gpt(
    content: str,
    image_urls: list[str] | None = None,
    post_db_id: int | None = None,
) -> tuple[list[dict], dict]:
    """
    게시글 텍스트와 이미지를 GPT-4o로 분석한다.
    반환: (analyses 리스트, market_indicators 딕트)
    게시글에 여러 코인/타임프레임이 있으면 analyses에 복수 항목이 담긴다.
    """
    import functools
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)

    loop = asyncio.get_event_loop()
    recent_ctx = await loop.run_in_executor(None, _fetch_recent_context_sync)

    # 이전 게시글 기반 코인별 활성 시나리오 컨텍스트
    coin_ctx = ""
    if post_db_id:
        coin_ctx_fn = functools.partial(_fetch_coin_active_context_sync, post_db_id)
        coin_ctx = await loop.run_in_executor(None, coin_ctx_fn)

    full_content = (recent_ctx + coin_ctx + content) if (recent_ctx or coin_ctx) else content

    if image_urls:
        user_content: list = [{"type": "text", "text": full_content}]
        for url in image_urls[:8]:
            user_content.append({
                "type": "image_url",
                "image_url": {"url": url, "detail": "high"},
            })
    else:
        user_content = full_content  # type: ignore[assignment]

    from openai import APIStatusError, APIConnectionError, APITimeoutError

    _RETRY_DELAYS = [5, 15, 30]
    response = None
    for attempt in range(len(_RETRY_DELAYS) + 1):
        try:
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_content},
                ],
                response_format={"type": "json_object"},
                temperature=0.3,
            )
            break
        except APIStatusError as e:
            if e.status_code == 429 and "insufficient_quota" in str(e):
                await _send_telegram_text(
                    "⚠️ *OpenAI 크레딧 소진*\n\n"
                    "API 할당량이 초과되었습니다. 분석이 중단됩니다.\n"
                    "platform.openai.com → Billing 에서 크레딧을 충전해 주세요."
                )
                raise
            if attempt >= len(_RETRY_DELAYS):
                raise
            wait = _RETRY_DELAYS[attempt]
            print(f"[analyzer] GPT API 에러 (status={e.status_code}), {wait}초 후 재시도 ({attempt + 1}/{len(_RETRY_DELAYS)})")
            await asyncio.sleep(wait)
        except (APIConnectionError, APITimeoutError) as e:
            if attempt >= len(_RETRY_DELAYS):
                raise
            wait = _RETRY_DELAYS[attempt]
            print(f"[analyzer] GPT 연결 실패 ({type(e).__name__}), {wait}초 후 재시도 ({attempt + 1}/{len(_RETRY_DELAYS)})")
            await asyncio.sleep(wait)
    raw = response.choices[0].message.content
    if not raw:
        raise ValueError(f"GPT 빈 응답 (finish_reason={response.choices[0].finish_reason})")

    parsed = json.loads(raw)
    market_indicators = parsed.get("market_indicators", {})
    macro_rules_raw   = parsed.get("macro_rules", [])

    analyses_raw = parsed.get("analyses")
    if analyses_raw and isinstance(analyses_raw, list):
        analyses = [_parse_analysis_item(a, raw) for a in analyses_raw]
    else:
        # 구형 단일 객체 포맷 fallback
        analyses = [_parse_analysis_item(parsed, raw)]

    return analyses, market_indicators, macro_rules_raw


# ── Telegram ─────────────────────────────────────────────────

def _get_user_risk_sync() -> str:
    """DB에서 사용자 risk_tolerance를 조회한다. 기본값 MODERATE."""
    if not TELEGRAM_CHAT_ID:
        return "MODERATE"
    try:
        import psycopg2
        from urllib.parse import urlparse
        url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
        p = urlparse(url)
        conn = psycopg2.connect(
            host=p.hostname, port=p.port or 5432,
            user=p.username, password=p.password,
            dbname=p.path.lstrip("/"),
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT risk_tolerance FROM user_profiles WHERE telegram_user_id = %s",
                (int(TELEGRAM_CHAT_ID),)
            )
            row = cur.fetchone()
        conn.close()
        return row[0] if row else "MODERATE"
    except Exception:
        return "MODERATE"


async def _send_telegram_text(text: str) -> None:
    """단순 텍스트 메시지를 Telegram으로 전송한다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    import httpx
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(url, json=payload)


async def _send_telegram(
    analysis_id: int,
    result: dict,
    content_preview: str,
) -> None:
    """분석 결과를 Telegram으로 알린다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    import httpx

    signal_type = result["signal_type"]
    short_entry = result.get("short_entry_price")

    if signal_type == "BUY":
        direction = "🟢 롱 진입"
    elif signal_type == "SELL":
        direction = "🔴 숏 진입" if short_entry else "🔴 롱 청산"
    else:
        direction = "🟡 관망"

    coin      = result.get("coin_symbol") or "?"
    zone_low  = result.get("youtuber_zone_low")
    zone_high = result.get("youtuber_zone_high")
    e1 = result.get("entry_price_1")   # 안정형
    e2 = result.get("entry_price_2")   # 중립형
    e3 = result.get("entry_price_3")   # 공격형
    abs_stop = result.get("absolute_stop")
    sl  = result.get("stop_loss_price")
    tp  = result.get("take_profit_price")
    rr  = result.get("risk_reward_ratio")
    cur_rsi = result.get("current_rsi")
    rsi = result.get("rsi_signal")
    vol = result.get("volume_signal")
    fib = result.get("fib_level")

    def _fmt(v):
        if v is None:
            return "-"
        return f"{v:,.0f}" if v >= 1000 else f"{v:,.4f}"

    timeframe        = result.get("timeframe")
    is_reference_only = result.get("is_reference_only", False)
    tf_label = {"MONTHLY": "월봉", "WEEKLY": "주봉", "DAILY": "일봉", "HOURLY": "시간봉"}.get(timeframe or "", "")

    lines = [f"{direction} *새 투자 신호 — {coin}*\n"]

    if tf_label:
        ref = " _(참고용 — 자동매매 없음)_" if is_reference_only else ""
        lines.append(f"📅 차트: {tf_label}{ref}\n")

    if zone_low and zone_high:
        lines.append(f"📌 유튜버 구간: {_fmt(zone_low)} ~ {_fmt(zone_high)}\n")

    # 사용자 성향에 맞는 진입가 하나만 표시
    risk_to_entry = {"CONSERVATIVE": e1, "MODERATE": e2, "AGGRESSIVE": e3}
    risk_label    = {"CONSERVATIVE": "안정형", "MODERATE": "중립형", "AGGRESSIVE": "공격형"}
    import asyncio
    loop = asyncio.get_event_loop()
    user_risk = await loop.run_in_executor(None, _get_user_risk_sync)
    entry_val     = risk_to_entry.get(user_risk) or e1 or e2
    if entry_val:
        lines.append(f"🎯 진입가 ({risk_label.get(user_risk, '중립형')}): {_fmt(entry_val)}\n")

    if sl or tp:
        lines.append(
            f"🛡 손절: {_fmt(sl)}  |  🏆 목표: {_fmt(tp)}"
            + (f"  (R:R {rr:.1f})" if rr else "")
            + "\n"
        )

    if abs_stop:
        lines.append(f"⛔ 마지노선: {_fmt(abs_stop)} (이탈 시 시즌 종료)\n")

    tech_parts = []
    if cur_rsi:
        tech_parts.append(f"RSI {cur_rsi} ({rsi or '?'})")
    elif rsi:
        tech_parts.append(f"RSI={rsi}")
    if vol:
        tech_parts.append(f"거래량={vol}")
    if fib:
        tech_parts.append(f"Fib {fib}")
    if tech_parts:
        lines.append("📊 기술지표: " + " | ".join(tech_parts) + "\n")

    summary_text = result.get("summary") or "-"
    invalid_text = result.get("invalidation") or "-"
    lines.append(f"\n*요약*\n{summary_text}\n")
    lines.append(f"*무효 조건*\n{invalid_text}\n")
    lines.append(f"\n원문: {content_preview[:80]}...")
    lines.append(f"\n분석 ID: \\#{analysis_id}")

    text = "\n".join(lines)

    # BUY/SELL 신호만 피드백 버튼 추가 (HOLD는 버튼 없음)
    reply_markup = None
    if signal_type in ("BUY", "SELL"):
        reply_markup = {
            "inline_keyboard": [[
                {"text": "✅ 맞음", "callback_data": f"fb:ok:{analysis_id}"},
                {"text": "❌ 틀림", "callback_data": f"fb:bad:{analysis_id}"},
            ]]
        }

    async with httpx.AsyncClient() as client:
        try:
            payload: dict = {
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": "Markdown",
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json=payload,
                timeout=10,
            )
        except Exception as e:
            print(f"[analyzer] Telegram 발송 실패: {e}")


# ── 파이프라인 ────────────────────────────────────────────────

async def _process(msg_value: bytes) -> None:
    """Kafka 메시지 1건: 게시글 조회 → GPT 분석 → DB 저장 → 가격알림 등록 → Telegram."""
    data = json.loads(msg_value)
    post_db_id   = data["post_id"]
    kafka_images = data.get("image_urls", [])

    loop = asyncio.get_event_loop()

    post = await loop.run_in_executor(None, _fetch_post_sync, post_db_id)
    if not post:
        print(f"[analyzer] 게시글 없음: id={post_db_id}")
        return

    image_urls = list(kafka_images or post.get("image_urls", []))

    # TradingView 링크 스크린샷 추가
    tv_fn = functools.partial(_fetch_tv_links_sync, post_db_id)
    tv_links = await loop.run_in_executor(None, tv_fn)
    if tv_links:
        print(f"[analyzer] TradingView 링크 {len(tv_links)}개 스크린샷 시작")
        for tv_url in tv_links:
            ss_fn = functools.partial(_screenshot_tv_sync, tv_url)
            b64_img = await loop.run_in_executor(None, ss_fn)
            if b64_img:
                image_urls.append(b64_img)
                print(f"[analyzer] TV 스크린샷 추가 완료: {tv_url}")

    print(f"[analyzer] 분석 시작: post_id={post_db_id}, 이미지 {len(image_urls)}개")

    analyses, market_indicators, macro_rules_raw = await _analyze_with_gpt(
        post["content"], image_urls=image_urls, post_db_id=post_db_id
    )
    print(f"[analyzer] 분석 결과: {len(analyses)}개 시나리오")

    # 시장 지표 저장 (게시글당 1번)
    if market_indicators:
        ctx_fn = functools.partial(_save_market_context_sync, post_db_id, market_indicators)
        await loop.run_in_executor(None, ctx_fn)

    # 각 분석 항목별로 저장 + 알림
    for result in analyses:
        print(f"[analyzer] 신호: {result['signal_type']} | 코인: {result['coin_symbol']} | TF: {result['timeframe']}")

        # 영상 썸네일 게시물 — DB 저장 없이 텔레그램으로 확인 요청
        if result["signal_type"] == "SKIP":
            skip_text = (
                f"🎬 *영상 게시물 감지*\n"
                f"post\\_id: {post_db_id}\n\n"
                f"이미지가 유튜브 영상 썸네일로 판단되어 자동 분석을 건너뛰었습니다.\n"
                f"영상 내용을 요약해서 알려주시면 수동으로 입력하겠습니다."
            )
            await _send_telegram_text(skip_text)
            continue

        save_fn = functools.partial(
            _save_analysis_sync,
            post_db_id,
            result["signal_type"],
            result["coin_symbol"],
            result["timeframe"],
            result["is_reference_only"],
            result["youtuber_zone_low"],
            result["youtuber_zone_high"],
            result["entry_price_1"],
            result["entry_price_2"],
            result["entry_price_3"],
            result["entry_price_4"],
            result["absolute_stop"],
            result["stop_loss_price"],
            result["take_profit_price"],
            result["short_entry_price"],
            result["short_stop_loss"],
            result["risk_reward_ratio"],
            result["current_rsi"],
            result["rsi_signal"],
            result["volume_signal"],
            result["fib_level"],
            result["summary"],
            result["invalidation"],
            result["scenario"],
            result["raw"],
        )
        analysis_id = await loop.run_in_executor(None, save_fn)
        print(f"[analyzer] 저장 완료: analysis_id={analysis_id}")

        # 거시 방향성 규칙 저장
        if macro_rules_raw:
            macro_fn = functools.partial(_save_macro_rules_sync, analysis_id, macro_rules_raw)
            await loop.run_in_executor(None, macro_fn)

        if result["coin_symbol"] and not result["is_reference_only"]:
            alerts_fn = functools.partial(
                _create_price_alerts_sync,
                analysis_id,
                result["coin_symbol"],
                result["entry_price_1"],
                result["entry_price_2"],
                result["entry_price_3"],
                result["entry_price_4"],
                result["stop_loss_price"],
                result["take_profit_price"],
                result["take_profit_price_2"],
            )
            await loop.run_in_executor(None, alerts_fn)

        # BUY/SELL만 Telegram 알림 (HOLD는 알림 없음 — 다중 분석 시 스팸 방지)
        if result["signal_type"] in ("BUY", "SELL"):
            await _send_telegram(analysis_id, result, post["content"])
            if not result["is_reference_only"]:
                try:
                    from trading.position_sync import sync_analysis
                    await sync_analysis(analysis_id)
                except Exception as e:
                    print(f"[analyzer] position sync 실패: {e}")


# ── 실행 루프 ─────────────────────────────────────────────────

async def run_consumer() -> None:
    """Kafka post.new 토픽을 구독하며 분석을 실행한다."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"[analyzer] 시작 — {KAFKA_TOPIC} 구독 중")
    try:
        async for msg in consumer:
            try:
                await _process(msg.value)
            except Exception as e:
                print(f"[analyzer] 처리 에러: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run_consumer())
