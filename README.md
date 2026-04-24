# Stock Screener — KOSPI / KOSDAQ

매일 장 마감 후 코스피/코스닥 전 종목을 자동 스캔하여 차트 패턴 후보를 추출하고,  
Claude AI 가 기술적 관점에서 점수와 근거를 제공하는 웹 스크리너.

---

## 구성

```
stock-screener/
├── .github/workflows/
│   └── screener.yml        # GitHub Actions 스케줄 (매일 16:10 KST)
├── scripts/
│   └── screener.py         # 데이터 수집 + 패턴 감지 + AI 분석
├── data/
│   └── results.json        # 스크리너 결과 (자동 갱신)
└── index.html              # 프론트엔드 대시보드
```

---

## 설치 방법

### 1. 레포지토리 생성

```bash
# GitHub 에서 Lorenzo-KR/Stock-Screener 신규 레포 생성 후
git clone https://github.com/Lorenzo-KR/Stock-Screener.git
cd Stock-Screener
```

### 2. 파일 복사 & 초기 커밋

```bash
# 이 파일들을 레포 루트에 복사 후
git add .
git commit -m "init: stock screener v1"
git push
```

### 3. GitHub Secret 등록

Settings → Secrets and variables → Actions → **New repository secret**

| Name | Value |
|------|-------|
| `ANTHROPIC_API_KEY` | `sk-ant-...` |

### 4. GitHub Pages 활성화

Settings → Pages → Source: **main branch / root**

### 5. 첫 실행 (수동)

Actions 탭 → "Stock Screener Daily" → **Run workflow**

---

## 자동 실행 스케줄

```
매주 월~금 16:10 KST (07:10 UTC)
```

장이 없는 날(공휴일)은 pykrx 가 빈 데이터를 반환하므로 결과가 갱신되지 않음.

---

## 감지 패턴

| 패턴 | 조건 |
|------|------|
| 골든크로스 | MA5가 MA20을 하향 돌파 (전일→당일) |
| 박스권 돌파 | 당일 종가 > 최근 20일 고가 |
| 거래량 급증+양봉 | 거래량 ≥ 20일 평균 × 2 & 양봉 마감 |
| 눌림목 | 전일 저가 MA20 ±2% 이내 + 당일 MA20 위 회복 |

---

## AI 분석

- 패턴 감지 후보 중 상위 60개를 Claude API 로 분석
- 점수 1~10 + 한 줄 근거 + 리스크 요인 제공
- API 호출 비용 절감을 위해 패턴 수·MA정배열·거래량비율 기준 상위만 선별

---

## 주의사항

> 이 스크리너는 차트 패턴 기반 후보 필터링 도구입니다.  
> 실제 투자 결정은 재무 분석, 업종 동향, 개인 리스크 허용 범위를 함께 고려하시기 바랍니다.
