# DB 사용현황 감사 (Schema Usage Audit)

> **목적:** "요구가 바뀌며 쌓인 죽은 코드"를 *감이 아니라 grep 증거*로 식별 → 안전하게 정리.
> 방법: 각 컬럼/테이블 이름을 코드 전체에서 검색 → 참조 수와 맥락으로 분류.
> 기준일: 2026-06-08

## 분류 결과

| 대상 | 위치 | 코드 참조 | 판정 | 조치 |
|------|------|----------|------|------|
| `content_hash` | posts | **0곳** | ❌ DEAD | 삭제 안전 |
| `video_memos` | (테이블 전체) | **0곳** | ❌ DEAD | 테이블 삭제 안전 |
| `entry_ratio_1/2/3` | analyses | 1곳 + 폐기 마이그레이션(v7) 존재 | ❌ DEAD | 삭제 (v7 적용) |
| `volume_signal` | analyses | 저장만, 매매 미사용 | ⚠️ ZOMBIE | 정리 후보 (저장코드 같이) |
| `fib_level` (컬럼) | analyses | 저장만 (※ signal_engine 변수는 별개·사용중) | ⚠️ ZOMBIE | 정리 후보 |
| `entry_price_4` | analyses | **14곳+ 사용중** | ✅ LIVE | **삭제 금지** |
| `short_entry_price` | analyses | 22곳 | ✅ LIVE | 유지 |
| `invalidation` | analyses | 25곳 | ✅ LIVE | 유지 |
| `market_context` | (테이블) | 11곳 | ✅ LIVE | 유지 |
| `rsi_signal` / `raw_response` / `feedback_note` | analyses | 4~10곳 | ✅ LIVE | 유지 |
| 그 외 핵심 테이블 | posts/analyses/price_alerts/positions/trades/user_profiles/settings/news/economic_calendars/daily_stats | 다수 | ✅ LIVE | 유지 |

## ⚠️ 핵심 교훈

예전 정리 계획 `migrations/pending.md`는 **`entry_price_4`를 "삭제하라"** 고 적혀 있었다.
그러나 grep 결과 **14곳 이상에서 사용 중** (analyzer 저장/추출, api, telegram_bot, position_sync, trade_executor).
→ **그 계획대로 DROP 했으면 분석기·봇·매매가 전부 깨졌다.**

> **결론: 스키마 정리는 "기억/오래된 메모"가 아니라 grep 증거로 한다.**

## 안전한 삭제 절차 (DEAD 항목만)

1. **코드 참조 먼저 제거** (있으면) — ORM 모델 필드, raw SQL의 컬럼명
2. **DB 마이그레이션 실행** — `ALTER TABLE ... DROP COLUMN IF EXISTS ...`
3. **하나씩** — 한 번에 하나 지우고 서비스 동작 확인
4. 백업 후 진행

확정 삭제 대상 (코드 참조 0):
```sql
ALTER TABLE posts    DROP COLUMN IF EXISTS content_hash;
ALTER TABLE analyses DROP COLUMN IF EXISTS entry_ratio_1;
ALTER TABLE analyses DROP COLUMN IF EXISTS entry_ratio_2;
ALTER TABLE analyses DROP COLUMN IF EXISTS entry_ratio_3;
DROP TABLE IF EXISTS video_memos;
-- ⚠️ entry_price_4 는 절대 삭제 금지 (사용 중)
-- ⚠️ volume_signal / fib_level 은 ZOMBIE — GPT 저장코드까지 같이 정리할지 결정 후
```

## 미완성(🚧) — 죽은 게 아니라 "덜 만든 것" → 삭제 X, 로드맵으로

- **SEMI_AUTO 텔레그램 버튼** (`trade_executor.py`) — 5분 후 자동 취소, 버튼 미연동
- **market_watch 하드코딩 값** (USDT.D 7.83%, BTC 74,000) — 수동 관리 중
- **USDT.D 매크로 게이트** — CoinGecko 401로 비활성
- **absolute_stop / take_profit GPT 추출** — 자주 NULL (가드가 entry_price_3로 폴백)
