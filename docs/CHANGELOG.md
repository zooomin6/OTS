# OTS 코드 변경 내역 (CHANGELOG)

> **목적:** 바이브 코딩 보완용. AI가 수정한 코드를 **직접 읽고 이해하기 위한 대조 문서**.
> 각 항목은 `무엇을 / 왜 / 코드 / 읽는 법 / 검증` 순서.
> ⚠️ **실거래(`BYBIT_TESTNET=false`) 전에는 매매 핵심 코드(가드·주문 로직)를 이 문서로 대조하며 직접 검증할 것.**

---

## 2026-06-08 · 청산 가드 + 백테스트 도구

### 📌 배경 — 왜 이 작업을 했나

한 줄: **"유튜버 신호 추종 + 물타기"인데 가끔 청산빔에 망한다 → 원인을 데이터로 밝히고, 그 패턴을 코드로 차단.**

데이터로 확인한 사실 (추측 아님, 실측):
1. 자체 FIB 신호는 순손실 — BTC −19.6% / ETH −25.2% (`backtest/full_runner.py`)
2. 진짜 실패모드 = **청산빔**. 중첩 DCA + 높은 레버 → 유튜버의 *다음 지지에 닿기도 전에* 청산.
3. 3년치 16개 급락 통계 (`backtest/liq_sweep.py`):
   - BTC: 5배 생존 88% / 8배 50% / 10배 44%
   - ETH: 3배 75% / 5배 56% (**ETH가 BTC보다 위험** — 변동성 큼)
4. 결론: 안전 레버 상한 = **BTC 5배 / ETH 3배**. 그 위는 차단.

### 📂 이번에 바뀐 파일 (6개)

| # | 파일 | 종류 | 한 줄 |
|---|------|------|-------|
| 1 | `trading/risk_manager.py` | 수정 (+61) | 청산 가드 코어 로직 |
| 2 | `trading/trade_executor.py` | 수정 (+48) | 알림→주문 경로(TRIGGERED)에 가드 연결 |
| 3 | `trading/position_sync.py` | 수정 (+21) | 즉시진입(AUTO) 경로에 가드 연결 |
| 4 | `backtest/liq_check.py` | **신규** | 단일 사례 청산 검증 도구 |
| 5 | `backtest/liq_sweep.py` | **신규** | 다기간 생존율 통계 도구 |
| 6 | `backtest/signal_engine.py` | 실험 → 원복 | 추세필터 강화 시도했으나 효과 없어 **원복함 (이 커밋 미포함)** |

> ⚠️ `db/models.py`, `notification/telegram_bot.py`, `migrations/pending.md`는 **이번 세션 이전부터** 수정돼 있던 것 — 이 작업과 무관.

---

### 1️⃣ `trading/risk_manager.py` — 가드 코어 로직

**무엇을:** 청산 위험을 계산·판정하는 함수/메서드를 추가.

**왜:** 어딘가에서 "이 레버·이 진입가면 청산가가 얼마고, 그게 위험한가?"를 물어볼 곳이 필요. 그 두뇌를 여기 둠.

**추가된 코드 (상단 상수 + 함수):**
```python
# 청산 가드 (backtest/liq_sweep.py 3년치 16개 급락 통계 기반)
MMR                  = 0.005             # 격리마진 유지증거금률 근사
LEVERAGE_CAP         = {"BTC": 5, "ETH": 3}
DEFAULT_LEVERAGE_CAP = 3                  # 기타 코인 보수적

def liq_price_long(avg_entry: float, leverage: int, mmr: float = MMR) -> float:
    """격리마진 롱 청산가 근사: 평단 × (1 − 1/레버 + 유지증거금률)."""
    return avg_entry * (1 - 1.0 / leverage + mmr)

def safe_max_leverage(avg_entry: float, lowest_support: float, mmr: float = MMR) -> int | None:
    """청산가가 lowest_support 아래가 되는 최대 정수 레버리지."""
    if not avg_entry or not lowest_support or lowest_support >= avg_entry:
        return None
    denom = 1 + mmr - (lowest_support / avg_entry)
    if denom <= 0:
        return 999
    return max(1, int(1.0 / denom))
```

**추가된 코드 (RiskManager 클래스 메서드):**
```python
def leverage_cap(self, coin: str) -> int:
    return LEVERAGE_CAP.get((coin or "").upper(), DEFAULT_LEVERAGE_CAP)

def check_liquidation(self, coin, leverage, avg_entry=None, lowest_support=None, side="LONG"):
    coin = (coin or "").upper()
    cap = self.leverage_cap(coin)
    if leverage > cap:                                    # ① 레버 상한 초과
        return RiskCheckResult(False, f"{coin} 레버리지 {leverage}x > 안전상한 {cap}x …")
    if side == "LONG" and avg_entry and lowest_support:   # ② 청산가 vs 최저지지
        liq = liq_price_long(avg_entry, leverage)
        if liq >= lowest_support:
            smax = safe_max_leverage(avg_entry, lowest_support)
            return RiskCheckResult(False, f"{coin} 청산가 {liq:,.0f} ≥ 최저지지 {lowest_support:,.0f} …")
    return RiskCheckResult(True)
```

**읽는 법 (핵심):**
- **청산가 공식** `평단 × (1 − 1/레버 + 0.005)` 이 가드의 심장. 예) 평단 67,500·10배 → `67,500 × (1 − 0.1 + 0.005) = 60,800`. 즉 가격이 60,800까지만 빠져도 청산.
  - 레버 높을수록 `1/레버`가 작아져 → 청산가가 평단에 **가까워짐** → 더 위험. (10배 청산가 60,800 vs 5배 54,300)
- `check_liquidation`은 두 관문: **① 레버가 코인 상한 넘으면 차단**, **② 청산가가 유튜버 최저 지지보다 위면 차단**(지지 닿기 전 죽으니까).
- `safe_max_leverage`는 ②의 역산 — "지지 아래로 청산가 두려면 최대 몇 배?"

**검증:** BTC 10/8x·ETH 5x → 차단, BTC5·ETH3 → 통과. `safe_max_leverage(67500, 56000)=5` (스윕의 "5배 생존/6배 청산"과 일치).

---

### 2️⃣ `trading/trade_executor.py` — TRIGGERED 주문 경로에 연결

**무엇을:** (a) 유튜버 최저 지지를 DB에서 읽는 헬퍼 추가, (b) 주문 직전에 가드 2단 삽입.

**왜:** 가드 "두뇌"(1번)를 실제 주문이 나가는 길목에 끼워야 작동. 이 경로는 *가격 알림(price_alert)이 터져서* 주문하는 흐름.

**추가된 헬퍼:**
```python
def _get_lowest_support(analysis_id: int) -> float | None:
    """유튜버 최저 지지 = absolute_stop / entry_price_3 / youtuber_zone_low 중 최저값."""
    # analyses 테이블에서 세 값 조회 → 가장 낮은 값 반환 (없으면 None)
```

**삽입된 가드 (`_execute_alert` 안, `place_order` 직전):**
```python
rm = RiskManager()

# 가드 1: 레버 상한 초과 시 자동 하향 (차단 아니라 5x/3x로 낮춰서 계속 진행)
cap = rm.leverage_cap(coin)
if leverage > cap:
    await _send_telegram(f"⚠️ {coin} 레버리지 {leverage}x → {cap}x 자동 하향 …")
    leverage = cap

qty = _calc_qty(usdt_amount, price, leverage)   # ← 낮춘 레버로 수량 재계산
...
result = rm.check(trade_krw, …)                 # 기존 리스크 체크(정지/한도 등)
...

# 가드 2: 청산가가 최저지지 위면 주문 거부
if signal == "BUY":
    lowest_support = await loop.run_in_executor(None, _get_lowest_support, analysis_id)
    proj_avg = (… 추가매수면 예상 평단 …) if is_add_buy else price
    liq_res = rm.check_liquidation(coin, leverage, avg_entry=proj_avg, lowest_support=lowest_support)
    if not liq_res:
        await _send_telegram(f"🛑 청산 가드 차단 — {coin}\n{liq_res.reason}")
        _mark_alert_processed(alert["id"]); return    # ← 주문 안 하고 종료
```

**읽는 법 (핵심):**
- **가드 1은 "차단"이 아니라 "자동 하향".** BTC 8배 설정이어도 주문 자체는 막지 않고 **5배로 낮춰서** 진행 → 시스템이 멈추지 않음. 그래서 `qty`(수량)를 낮춘 레버로 다시 계산함.
- **가드 2는 "차단".** 낮춘 레버로도 청산가가 유튜버 지지 위면 → 그 자리는 안전하게 못 사니 주문 거부.
- 추가매수면 `proj_avg`(기존 평단 + 이번 체결을 섞은 예상 평단)로 계산 — 물타기 후 평단 기준으로 청산가를 봐야 정확.

**검증:** `_get_lowest_support(304)` → 1555(ETH 박스 바닥) 정상 반환. coin_api 재시작 반영.

---

### 3️⃣ `trading/position_sync.py` — 즉시진입(AUTO) 경로에 연결

**무엇을:** AUTO 모드에서 분석 직후 지정가 사다리를 거는 경로에 **같은 가드**를 삽입.

**왜:** 주문 경로가 두 개(②번 TRIGGERED, 이 ③번 즉시진입). 한쪽만 막으면 다른 쪽으로 새니까 둘 다 막아야 함.

**삽입된 가드 (`place_order` 루프 직전):**
```python
from trading.risk_manager import RiskManager
_rm  = RiskManager()
_cap = _rm.leverage_cap(coin)
if leverage > _cap:                              # 가드 1: 자동 하향
    await _send_telegram(f"⚠️ {coin} 레버리지 {leverage}x → {_cap}x 자동 하향 …")
    leverage = _cap
if signal == "BUY" and entry_prices:             # 가드 2: 청산가 vs 최저지지
    _ratios = [0.40, 0.35, 0.25, 0.0][:len(entry_prices)]
    _planned_avg = (… 사다리 비중 가중 평단 …)
    _supports = [float(v) for v in ([abs_stop] + entry_prices) if v]
    _liq = _rm.check_liquidation(coin, leverage, avg_entry=_planned_avg,
                                 lowest_support=(min(_supports) if _supports else None))
    if not _liq:
        await _send_telegram(f"🛑 청산 가드 차단 — {coin}\n{_liq.reason}")
        return
```

**읽는 법 (핵심):**
- 여기선 진입가가 사다리(`entry_prices` = e1/e2/e3)로 미리 정해져 있어서, **계획된 평단**(`_planned_avg`)을 비중(40/35/25%)으로 가중평균해 계산.
- 나머지 로직(상한 하향 + 청산가 체크)은 ②번과 동일. 구동 컨테이너는 `coin_analyzer`.

**검증:** 문법 OK. coin_analyzer 재시작 반영.

---

### 4️⃣ `backtest/liq_check.py` (신규) — 단일 사례 청산 검증

**무엇을:** 특정 DCA 사다리(예: BTC 72,417/71,150/60,000)를 **실제 Bybit 캔들 꼬리**에 대입해 레버리지별 청산 여부 출력.

**왜:** "이 자리 8배로 들어가면 청산빔에 죽나?"를 숫자로 보려고. 가드의 프로토타입.

**실행:**
```bash
docker exec coin_backtest python -m backtest.liq_check
docker exec coin_backtest python -m backtest.liq_check --ladder 72417,71150,60000 --box 56000,58000
```
**핵심 파라미터:** `LADDER`(진입 라인), `LEVERAGES`(스윕할 레버), `MMR`(유지증거금률). **DB 안 씀** (가격은 Bybit REST).

---

### 5️⃣ `backtest/liq_sweep.py` (신규) — 다기간 생존율 통계

**무엇을:** 3년치 캔들에서 스윙 고점→급락 에피소드를 자동 탐지, **고점 대비 상대 사다리**(−12/−14/−27%)를 대입해 레버리지별 **생존율**을 집계.

**왜:** 단일 사례는 운일 수 있으니, "BTC 5배 / ETH 3배"라는 안전선을 **통계로** 굳히려고. → 가드의 `LEVERAGE_CAP` 근거.

**실행:**
```bash
docker exec coin_backtest python -m backtest.liq_sweep            # BTC + ETH
```
**핵심 파라미터:** `DROPS`(상대 낙폭 사다리), `SWING_W`(고점 탐지 윈도우), `HORIZON`(추적일수).

---

### 6️⃣ `backtest/signal_engine.py` — 추세필터 강화 (실험 ⚠️ 미확정)

**무엇을:** FIB 진입의 추세 필터를 "종가 < 50일선×0.97"에서 "종가 < 50일선 **또는** 50일선 우하향"으로 강화.

**왜:** 역추세 매수가 하락장에서 깨지길래 더 강하게 막아보려 한 실험.

**diff:**
```python
-    # ── 추세 필터: 50일 SMA ──
+    # ── 추세 필터: 50일 SMA 위치 + 방향 ──
     sma50 = _sma(prev_candles, 50)
-    if sma50 and candle["close"] < sma50 * 0.97:
+    sma50_prev = _sma(prev_candles[:-10], 50) if len(prev_candles) >= 60 else None
+    downtrend = bool(
+        (sma50 and candle["close"] < sma50)
+        or (sma50 and sma50_prev and sma50 < sma50_prev)
+    )
+    if downtrend:
         fib_level = None
```

**상태 — 원복됨 (2026-06-08):** 백테스트 재실행 결과 **효과 없었음**(BTC 그대로 −19.6%, 오히려 유일한 수익 트레이드까지 걸러냄). 따라서 `git restore`로 **원복했고 이 커밋엔 포함하지 않음.** 신호엔진은 추후 "유튜버 주봉 프레임"으로 재설계 예정. (이 기록은 "왜 안 바꿨나"의 근거로 남겨둠.)

---

### ✅ 이 날짜의 검증 요약
- 가드 로직 단위 테스트 통과 (BTC8/10·ETH5 차단, BTC5·ETH3 통과)
- `py_compile` 전 파일 문법 OK
- 실제 postgres로 end-to-end (ETH 304 박스 최저지지 1555 인식)
- 라이브 반영: `coin_api`(②), `coin_analyzer`(③) 재시작

### ⏳ 미해결 / 다음
- `absolute_stop`·`take_profit`이 GPT 추출에서 자주 **NULL** → 가드가 `entry_price_3`로 폴백 중. 유튜버 구조선(마지노선) 추출 보강 필요.
- 시나리오당 **최대손실 %** 미정 (레버 확정됐으니 재논의 가능).
- 신호엔진 재설계(주봉 프레임/박스천장 진입) 미착수 — 가드만 먼저 깖.
- 인프라: Docker 7.5GiB로 14컨테이너 **OOM(exit 137) 반복** → 메모리 증설 권장.
