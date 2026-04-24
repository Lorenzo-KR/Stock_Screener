"""
Stock Screener — KOSPI / KOSDAQ
매일 장 마감 후 GitHub Actions 에서 자동 실행
출력: data/results.json
"""

import json
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pykrx import stock

# ─────────────────────────────────────────────────────────────────
# 설정값
# ─────────────────────────────────────────────────────────────────
LOOKBACK_DAYS    = 100     # 수집 기간 (캘린더 일수, 영업일 약 60~70일 확보)
BREAKOUT_DAYS    = 20      # 박스권 돌파 기준 (N일 신고가)
VOLUME_MULT      = 2.0     # 거래량 급증 기준 (20일 평균 대비 배수)
MA_SHORT         = 5
MA_MID           = 20
MA_LONG          = 60
MIN_PRICE        = 1_000   # 동전주 제외 (원)
MIN_VOLUME       = 50_000  # 거래량 최소 기준
MAX_AI_TARGETS   = 60      # AI 분석 대상 최대 수 (비용 절감)
PYKRX_SLEEP      = 0.05    # 종목당 API 딜레이 (초)
AI_SLEEP         = 1.2     # AI 호출 간 딜레이 (초)


# ─────────────────────────────────────────────────────────────────
# 날짜 유틸
# ─────────────────────────────────────────────────────────────────
def get_date_range():
    end   = datetime.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


# ─────────────────────────────────────────────────────────────────
# 데이터 수집
# ─────────────────────────────────────────────────────────────────
def get_all_tickers():
    kospi  = [("KOSPI",  t) for t in stock.get_market_ticker_list(market="KOSPI")]
    kosdaq = [("KOSDAQ", t) for t in stock.get_market_ticker_list(market="KOSDAQ")]
    return kospi + kosdaq


def get_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        if df is None or len(df) < MA_LONG + 5:
            return None
        # pykrx 컬럼명 표준화
        df.columns = [c.strip() for c in df.columns]
        col_map = {
            "시가": "open", "고가": "high", "저가": "low",
            "종가": "close", "거래량": "volume",
            "거래대금": "amount", "등락률": "change"
        }
        df.rename(columns=col_map, inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].copy()
        df = df[df["volume"] > 0]
        return df if len(df) >= MA_LONG + 5 else None
    except Exception:
        return None


def get_ticker_name(ticker: str) -> str:
    try:
        return stock.get_market_ticker_name(ticker)
    except Exception:
        return ticker


# ─────────────────────────────────────────────────────────────────
# 기술적 지표 계산
# ─────────────────────────────────────────────────────────────────
def calc_indicators(df: pd.DataFrame) -> dict:
    c  = df["close"]
    v  = df["volume"]
    ma5  = c.rolling(MA_SHORT).mean()
    ma20 = c.rolling(MA_MID).mean()
    ma60 = c.rolling(MA_LONG).mean()
    vol_avg20 = v.rolling(20).mean()

    return {
        "close"      : c.iloc[-1],
        "open_last"  : df["open"].iloc[-1],
        "high_last"  : df["high"].iloc[-1],
        "low_last"   : df["low"].iloc[-1],
        "prev_close" : c.iloc[-2],
        "ma5"        : ma5.iloc[-1],
        "ma20"       : ma20.iloc[-1],
        "ma60"       : ma60.iloc[-1],
        "ma5_prev"   : ma5.iloc[-2],
        "ma20_prev"  : ma20.iloc[-2],
        "low_prev"   : df["low"].iloc[-2],
        "vol_last"   : v.iloc[-1],
        "vol_avg20"  : vol_avg20.iloc[-1],
        "high_20d"   : df["high"].iloc[-BREAKOUT_DAYS - 1 : -1].max(),
        "close_20d"  : c.iloc[-20:].tolist(),
        "vol_20d"    : v.iloc[-20:].tolist(),
    }


# ─────────────────────────────────────────────────────────────────
# 패턴 감지
# ─────────────────────────────────────────────────────────────────
PATTERN_LABELS = {
    "golden_cross"    : "골든크로스",
    "breakout"        : "박스권 돌파",
    "volume_surge"    : "거래량 급증+양봉",
    "pullback_support": "눌림목",
}


def detect_patterns(ind: dict) -> list[str]:
    flags = []

    # 1. 골든크로스: 전일 MA5 < MA20, 당일 MA5 > MA20
    if ind["ma5_prev"] < ind["ma20_prev"] and ind["ma5"] >= ind["ma20"]:
        flags.append("golden_cross")

    # 2. 박스권 돌파: 당일 종가 > 최근 20일(전일까지) 고가
    if not np.isnan(ind["high_20d"]) and ind["close"] > ind["high_20d"]:
        flags.append("breakout")

    # 3. 거래량 급증 + 양봉
    if (
        ind["vol_avg20"] > 0
        and ind["vol_last"] >= ind["vol_avg20"] * VOLUME_MULT
        and ind["close"] > ind["open_last"]
    ):
        flags.append("volume_surge")

    # 4. 눌림목: 전일 저가 MA20 ±2% 이내, 당일 종가 MA20 위 회복
    if (
        not np.isnan(ind["ma20_prev"])
        and ind["low_prev"] <= ind["ma20_prev"] * 1.02
        and ind["close"] > ind["ma20"]
    ):
        flags.append("pullback_support")

    return flags


# ─────────────────────────────────────────────────────────────────
# AI 분석 (Claude API)
# ─────────────────────────────────────────────────────────────────
def score_with_ai(client, name: str, market: str, patterns: list, ind: dict) -> tuple:
    """Claude API 로 차트 매력도 1~10 점 + 근거 + 리스크 반환"""
    pattern_str = ", ".join([PATTERN_LABELS.get(p, p) for p in patterns])
    aligned = (ind["ma5"] > ind["ma20"] > ind["ma60"])
    vol_ratio = (ind["vol_last"] / ind["vol_avg20"]) if ind["vol_avg20"] > 0 else 0

    prompt = f"""당신은 한국 주식 기술적 분석 전문가입니다. 아래 종목 데이터를 차트 관점에서 분석하세요.

[종목 정보]
이름: {name} ({market})
현재가: {int(ind['close']):,}원 (전일대비 {((ind['close']/ind['prev_close'])-1)*100:+.2f}%)
감지 패턴: {pattern_str}
MA 정배열: {'YES (MA5 > MA20 > MA60)' if aligned else 'NO'}
거래량 비율(20일 평균 대비): {vol_ratio:.1f}배
MA5: {ind['ma5']:.0f} | MA20: {ind['ma20']:.0f} | MA60: {ind['ma60']:.0f}
최근 20일 종가: {[int(x) for x in ind['close_20d']]}

[평가 기준]
- 패턴 신뢰도 및 조합 강도
- MA 정배열 여부와 이격도
- 거래량 지지 수준
- 추세 지속 가능성
- 리스크 요소 (고점 근접, 급등 후 과열 등)

반드시 JSON 형식으로만 응답하세요 (다른 텍스트 절대 포함 금지):
{{"score": 7, "reason": "한 문장 근거 (60자 이내)", "risk": "핵심 리스크 (30자 이내)"}}"""

    try:
        import anthropic as _anthropic
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = message.content[0].text.strip()
        data = json.loads(raw)
        return (
            int(data.get("score", 5)),
            str(data.get("reason", "")),
            str(data.get("risk", "")),
        )
    except Exception as e:
        print(f"    AI 분석 오류 ({name}): {e}")
        return 5, "AI 분석 불가", ""


# ─────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────
def main():
    start_date, end_date = get_date_range()
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 스크리너 시작 | 기간: {start_date} ~ {end_date}")

    tickers = get_all_tickers()
    print(f"  전체 종목: {len(tickers):,}개")

    candidates = []

    for i, (market, ticker) in enumerate(tickers):
        if i % 300 == 0:
            print(f"  진행: {i:,} / {len(tickers):,}")

        df = get_ohlcv(ticker, start_date, end_date)
        if df is None:
            time.sleep(PYKRX_SLEEP)
            continue

        ind = calc_indicators(df)

        # 기본 필터: 최소 가격·거래량
        if ind["close"] < MIN_PRICE or ind["vol_last"] < MIN_VOLUME:
            time.sleep(PYKRX_SLEEP)
            continue

        patterns = detect_patterns(ind)
        if not patterns:
            time.sleep(PYKRX_SLEEP)
            continue

        name = get_ticker_name(ticker)

        candidates.append({
            "ticker"      : ticker,
            "name"        : name,
            "market"      : market,
            "patterns"    : patterns,
            "close"       : int(ind["close"]),
            "change_pct"  : round((ind["close"] / ind["prev_close"] - 1) * 100, 2),
            "volume_ratio": round(ind["vol_last"] / ind["vol_avg20"], 1) if ind["vol_avg20"] > 0 else 0,
            "ma_aligned"  : bool(ind["ma5"] > ind["ma20"] > ind["ma60"]),
            "close_20d"   : [int(x) for x in ind["close_20d"]],
            "ai_score"    : None,
            "ai_reason"   : "",
            "ai_risk"     : "",
        })
        time.sleep(PYKRX_SLEEP)

    print(f"  패턴 감지 후보: {len(candidates)}개")

    # ── AI 분석 ──────────────────────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key and candidates:
        try:
            import anthropic as _anthropic
            client = _anthropic.Anthropic(api_key=api_key)

            # 패턴 수 많은 순 → MA정배열 → 거래량비율 순 정렬 후 상위만 AI 분석
            candidates.sort(
                key=lambda x: (len(x["patterns"]), x["ma_aligned"], x["volume_ratio"]),
                reverse=True,
            )
            ai_targets = candidates[:MAX_AI_TARGETS]

            print(f"  AI 분석 시작: {len(ai_targets)}개")
            for j, c in enumerate(ai_targets):
                print(f"    [{j+1:02d}/{len(ai_targets)}] {c['name']} ({c['market']})")
                df = get_ohlcv(c["ticker"], start_date, end_date)
                if df is not None:
                    ind = calc_indicators(df)
                    score, reason, risk = score_with_ai(client, c["name"], c["market"], c["patterns"], ind)
                    c["ai_score"]  = score
                    c["ai_reason"] = reason
                    c["ai_risk"]   = risk
                time.sleep(AI_SLEEP)
        except ImportError:
            print("  anthropic 패키지 없음 — AI 분석 건너뜀")
    else:
        print("  ANTHROPIC_API_KEY 없음 — AI 분석 건너뜀")

    # ── 최종 정렬: AI 점수 → 패턴 수 → 거래량비율 ────────────────
    candidates.sort(
        key=lambda x: (x["ai_score"] or 0, len(x["patterns"]), x["volume_ratio"]),
        reverse=True,
    )

    output = {
        "updated_at"    : datetime.now().strftime("%Y-%m-%d %H:%M KST"),
        "total_scanned" : len(tickers),
        "total_found"   : len(candidates),
        "candidates"    : candidates,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ 저장 완료: data/results.json ({len(candidates)}개 종목)")


if __name__ == "__main__":
    main()
