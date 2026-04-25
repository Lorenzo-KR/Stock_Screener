"""
Historical Analog Pattern Matcher
현재 종목의 가격/거래량 패턴과 가장 유사한 과거 시점 K개를 찾아
다음 1일 상승 확률 및 기대 수익률을 산출합니다.

방법: K-최근접이웃(KNN) on time-series feature vectors
"""

import numpy as np
import pandas as pd

WINDOW      = 20     # 패턴 인식 윈도우 (영업일)
TOP_K       = 25     # 유사 사례 수
MIN_HISTORY = 150    # 분석에 필요한 최소 행 수
SKIP_RECENT = 20     # 최근 N일은 유사 사례에서 제외 (데이터 누수 방지)


# ─────────────────────────────────────────────────────────────────
# 특징 벡터 생성
# ─────────────────────────────────────────────────────────────────
def _make_feature(df: pd.DataFrame, t: int) -> np.ndarray | None:
    """t번째 행 기준 특징 벡터 반환.
    구성:
      - 정규화된 WINDOW일 가격 시퀀스 (형태/모양 포착)
      - 최근 5일 거래량 비율 시퀀스 (수급 강도 포착)
      - MA 위치 지표 (추세 맥락)
      - 단기 모멘텀 (1d, 5d, 10d)
      - 최근 변동성
    """
    if t < WINDOW + 1:
        return None

    c = df["close"].values.astype(float)
    v = df["volume"].values.astype(float)

    base = c[t - WINDOW]
    if base <= 0:
        return None

    # 1. 정규화 가격 시퀀스 (WINDOW일 형태)
    price_seq = c[t - WINDOW : t + 1] / base - 1   # shape: (WINDOW+1,)

    # 2. 거래량 비율 시퀀스 (최근 5일 / 20일 평균)
    vol_avg = v[t - WINDOW : t].mean()
    if vol_avg <= 0:
        return None
    vol_seq = v[t - 5 : t + 1] / vol_avg           # shape: (6,)

    # 3. MA 위치 (현재가 대비)
    ma5  = c[t - 5  : t + 1].mean()
    ma20 = c[t - 20 : t + 1].mean()
    cur  = c[t]
    if ma5 <= 0 or ma20 <= 0 or cur <= 0:
        return None
    ma5_pos  = (cur - ma5)  / ma5
    ma20_pos = (cur - ma20) / ma20
    ma_cross = (ma5 - ma20) / ma20

    # 4. 단기 모멘텀
    mom1d  = c[t] / c[t - 1]  - 1 if c[t - 1]  > 0 else 0.0
    mom5d  = c[t] / c[t - 5]  - 1 if c[t - 5]  > 0 else 0.0
    mom10d = c[t] / c[t - 10] - 1 if c[t - 10] > 0 else 0.0

    # 5. 최근 변동성
    rets = np.diff(c[t - 10 : t + 1]) / c[t - 10 : t]
    vol  = float(rets.std()) if len(rets) > 1 else 0.0

    fv = np.concatenate([
        price_seq,                              # 21개 — 가장 중요
        np.clip(vol_seq, 0, 10),               # 6개
        [ma5_pos, ma20_pos, ma_cross,           # 3개
         mom1d, mom5d, mom10d, vol],            # 4개
    ])

    if np.any(np.isnan(fv)) or np.any(np.isinf(fv)):
        return None
    return fv


def _cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


# ─────────────────────────────────────────────────────────────────
# 종목별 Analog 점수 계산
# ─────────────────────────────────────────────────────────────────
def compute_analog(df: pd.DataFrame) -> dict | None:
    """
    현재(마지막 행) 패턴과 유사한 과거 K개를 찾아
    다음날 1일 상승 확률 및 기대 수익률 반환.
    반환: {"win_prob_1d": float, "avg_ret_1d": float, "n_analogs": int}
    """
    n = len(df)
    if n < MIN_HISTORY:
        return None

    t_now = n - 1
    fv_now = _make_feature(df, t_now)
    if fv_now is None:
        return None

    c = df["close"].values.astype(float)

    results = []
    # 과거 시점 순회 (최근 SKIP_RECENT일 제외 → 미래 데이터 누수 방지)
    end_t = n - 1 - SKIP_RECENT
    for t in range(WINDOW + 1, end_t):
        if t + 1 >= n:
            break
        fv = _make_feature(df, t)
        if fv is None:
            continue
        sim = _cosine_sim(fv_now, fv)
        next_ret = c[t + 1] / c[t] - 1 if c[t] > 0 else 0.0
        results.append((sim, next_ret))

    if len(results) < TOP_K // 2:
        return None

    results.sort(reverse=True)
    top = results[:TOP_K]

    rets     = [r for _, r in top]
    win_prob = sum(1 for r in rets if r > 0) / len(rets)
    avg_ret  = float(np.mean(rets)) * 100

    return {
        "win_prob_1d": round(win_prob, 3),
        "avg_ret_1d":  round(avg_ret, 2),
        "n_analogs":   len(top),
    }


# ─────────────────────────────────────────────────────────────────
# 전 종목 일괄 처리
# ─────────────────────────────────────────────────────────────────
def run_analog_screener(
    ticker_data: dict[str, tuple[str, pd.DataFrame]],
    top_n: int = 300,
    adapter=None,
) -> list[dict]:
    """
    전 종목 Analog 분석 → 1일 상승 확률 상위 top_n 반환.
    반환 리스트: [{"ticker", "market", "win_prob_1d", "avg_ret_1d", ...}, ...]
    """
    # Analog 분석은 3년치 전체 데이터 필요 — DB에서 별도 조회
    if adapter is not None:
        print("  Analog용 3년치 데이터 조회 중...")
        full_data = adapter.fetch_recent_ohlcv(days=1100)
        print(f"  조회 완료: {len(full_data):,}개 종목")
    else:
        full_data = ticker_data

    scored = []
    total  = len(full_data)

    for i, (ticker, (market, df)) in enumerate(full_data.items()):
        if i % 500 == 0:
            print(f"  Analog 진행: {i:,}/{total:,}")

        result = compute_analog(df)
        if result is None:
            continue

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last
        close     = int(last["close"])
        chg_pct   = round((last["close"] / prev["close"] - 1) * 100, 2) if prev["close"] > 0 else 0.0
        vol_avg20 = df["volume"].iloc[-20:].mean()
        vol_ratio = round(last["volume"] / vol_avg20, 1) if vol_avg20 > 0 else 0.0

        scored.append({
            "ticker"      : ticker,
            "market"      : market,
            "close"       : close,
            "change_pct"  : chg_pct,
            "volume_ratio": vol_ratio,
            "close_20d"   : [int(x) for x in df["close"].iloc[-20:].tolist()],
            "win_prob_1d" : result["win_prob_1d"],
            "avg_ret_1d"  : result["avg_ret_1d"],
            "n_analogs"   : result["n_analogs"],
        })

    scored.sort(key=lambda x: x["win_prob_1d"], reverse=True)
    return scored[:top_n]
