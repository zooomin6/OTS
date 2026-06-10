# OTS 작업 현황 (TODO)

> 최종 업데이트: 2026-06-08
> 이번 세션 = 청산 가드 구현 + 데이터 기반 진단 + 문서화.

---

## ✅ 완료

### 진단 (데이터로)
- [x] 자체 신호 백테스트 → 순손실 입증 (BTC −19.6% / ETH −25.2%)
- [x] 실패 원인 규명: 역추세 매수 + 청산빔
- [x] 3년치 16개 급락 통계 → 안전 레버 BTC5 / ETH3 산출

### 청산 가드 (구현·라이브)
- [x] `trading/risk_manager.py` — 가드 코어 (`check_liquidation`, `leverage_cap`)
- [x] `trading/trade_executor.py` — TRIGGERED 주문 경로 연결 + `_get_lowest_support`
- [x] `trading/position_sync.py` — AUTO 진입 경로 연결
- [x] 검증(단위·문법·end-to-end) + `coin_api`·`coin_analyzer` 재시작 반영

### 도구·문서
- [x] `backtest/liq_check.py` (단일 사례 청산 검증)
- [x] `backtest/liq_sweep.py` (다기간 생존율 통계)
- [x] `docs/CHANGELOG.md` · `docs/erd.dbml` · `docs/db_audit.md`
- [x] 노션 포트폴리오 페이지 (대시보드)

### 정리
- [x] 실험 코드(`signal_engine` 추세필터) 원복
- [x] OOM으로 죽은 핵심 9개 컨테이너 복구

---

## ⏳ 바로 마무리 (작은 것)
- [ ] 노션 마감 — 콜아웃 색상 · 배너 이미지 · ERD 이미지 임베드 · §5·§8 본인 언어로 재작성

## 🔧 인프라
- [ ] Docker 메모리 증설 (7.5 → 12~16GB) — OOM(exit 137) 재발 방지
- [ ] 죽은 5개 컨테이너 복구 — selenium · crawler · news_crawler · briefing · market_watch (메모리 올린 뒤)

## 🧹 코드·DB 정리 (감사 완료, 실행 남음)
- [ ] 죽은 스키마 삭제 — `content_hash` · `entry_ratio_1/2/3` · `video_memos` (절차: db_audit.md)
- [ ] 좀비 결정 — `volume_signal` · `fib_level` (저장 코드까지 정리할지)
- [ ] ⚠️ `entry_price_4`는 삭제 금지 (14곳 사용 중)

## 🚀 다음 큰 작업
- [ ] 신호엔진 재설계 — 유튜버 주봉 시나리오 프레임 + 박스 천장 진입 (합의만, 미착수)
- [ ] 코드 이해 감사 — 모듈별 정독으로 소유권 확보 (학습 경로)

## 📌 원래부터 미완성 (백로그)
- [ ] SEMI_AUTO 텔레그램 승인 버튼 미연동
- [ ] USDT.D 매크로 게이트 비활성 (CoinGecko 401)
- [ ] GPT `absolute_stop`/목표가 추출 자주 NULL → 추출 보강
- [ ] 바이빗 실거래 미검증 (현재 테스트넷)

---

**추천 순서:** ① Docker 메모리+컨테이너 정상화 → ② 신호엔진 재설계 *또는* 코드 이해 감사
