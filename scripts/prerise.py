"""
Pre-Rise Pattern Screener
과거 3년 데이터에서 크게 움직인(±3% 이상) 시점만 추출하여
현재 패턴과 가장 유사한 K개를 찾고, 그 중 상승 비율을 반환합니다.

기존 Analog(1일 전 종목 대상)와 달리:
  - '강한 움직임(±3%)' 시점만 비교 대상으로 삼아 노이즈 제거
  - 5일 / 20일 두 기간의 상승 확률 산출
  - 기술적 패턴 필터와 무관하게 전 종목 스캔
"""

import numpy as np
import pandas as pd

from analog import _make_feature, WINDOW, _cosine_sim

# ─── 설정 ────────────────────────────────────────────────────────
MOVE_THRESHOLD = 0.03     # ±3% 이상 움직인 시점만 라이브러리에 포함
TOP_K          = 30       # 유사 사례 수
MIN_HISTORY    = 60
MIN_LIB_SIZE   = 500
MAX_LIB_SIZE   = 200_000  # 메모리 제한


# ─────────────────────────────────────────────────────────────────
# 강한 움직임 라이브러리 구축
# ─────────────────────────────────────────────────────────────────
def build_mover_library(
    ticker_data: dict,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    5일 후 ±3% 이상 움직인 역사적 시점만 추출.

    반환:
      fvs       — (N, D) float32  특징벡터
      rets_5d   — (N,)   float32  5일 수익률
      rets_20d  — (N,)   float32  20일 수익률 (데이터 없으면 NaN)
    """
    fvs, r5, r20 = [], [], []

    for ticker, (market, df) in ticker_data.items():
        n = len(df)
        if n < WINDOW + 25:
            continue

        c = df["close"].values.astype(float)
        # 최소 5일 미래 확보, 20일 미래는 가능한 만큼
        for t in range(WINDOW + 1, n - 6):
            if c[t] <= 0 or c[t + 5] <= 0:
                continue
            ret5 = c[t + 5] / c[t] - 1
            if abs(ret5) < MOVE_THRESHOLD:
                continue

            fv = _make_feature(df, t)
            if fv is None:
                continue

            ret20 = (c[t + 20] / c[t] - 1) if t + 20 < n and c[t + 20] > 0 else float("nan")

            fvs.append(fv)
            r5.append(ret5)
            r20.append(ret20)

    if not fvs:
        empty = np.empty(0, dtype=np.float32)
        return np.empty((0, 1), dtype=np.float32), empty, empty

    fvs_arr = np.array(fvs, dtype=np.float32)
    r5_arr  = np.array(r5,  dtype=np.float32)
    r20_arr = np.array(r20, dtype=np.float32)

    if len(fvs_arr) > MAX_LIB_SIZE:
        idx = np.random.choice(len(fvs_arr), MAX_LIB_SIZE, replace=False)
        fvs_arr = fvs_arr[idx]
        r5_arr  = r5_arr[idx]
        r20_arr = r20_arr[idx]

    return fvs_arr, r5_arr, r20_arr


# ─────────────────────────────────────────────────────────────────
# 단일 종목 점수 산출
# ─────────────────────────────────────────────────────────────────
def _score_one(
    df: pd.DataFrame,
    lib_fvs: np.ndarray,
    lib_norms: np.ndarray,
    lib_r5: np.ndarray,
    lib_r20: np.ndarray,
) -> dict | None:
    t_now  = len(df) - 1
    fv_now = _make_feature(df, t_now)
    if fv_now is None:
        return None

    fv = fv_now.astype(np.float32)
    norm = np.linalg.norm(fv)
    if norm < 1e-9:
        return None

    # 벡터화 코사인 유사도
    sims = lib_fvs @ fv / (lib_norms * norm + 1e-9)

    k = min(TOP_K, len(sims))
    top_idx = np.argpartition(sims, -k)[-k:]

    top_r5  = lib_r5[top_idx]
    top_r20 = lib_r20[top_idx]

    win5  = float(np.mean(top_r5  > 0))
    win20 = float(np.nanmean(top_r20 > 0)) if not np.all(np.isnan(top_r20)) else float("nan")
    avg5  = float(np.mean(top_r5)) * 100
    avg20 = float(np.nanmean(top_r20)) * 100 if not np.all(np.isnan(top_r20)) else float("nan")

    return {
        "win_prob_5d" : round(win5,  3),
        "win_prob_20d": round(win20, 3) if not np.isnan(win20) else None,
        "avg_ret_5d"  : round(avg5,  2),
        "avg_ret_20d" : round(avg20, 2) if not np.isnan(avg20) else None,
    }


# ─────────────────────────────────────────────────────────────────
# 전 종목 일괄 처리
# ─────────────────────────────────────────────────────────────────
def run_prerise_screener(
    ticker_data: dict,
    top_n: int = 500,
    adapter=None,
) -> list[dict]:
    """
    전 종목 대상으로 5일·20일 상승 확률 산출.
    win_prob_5d 기준 상위 top_n 반환.
    """
    if adapter is not None:
        print("  PreRise용 3년치 데이터 조회 중...")
        full_data = adapter.fetch_recent_ohlcv(days=1100)
        print(f"  조회 완료: {len(full_data):,}개 종목")
    else:
        full_data = ticker_data

    print(f"  강한 움직임(±{MOVE_THRESHOLD*100:.0f}%) 라이브러리 구축 중...")
    lib_fvs, lib_r5, lib_r20 = build_mover_library(full_data)
    n_lib = len(lib_fvs)

    if n_lib < MIN_LIB_SIZE:
        print(f"  라이브러리 부족 ({n_lib}개) — PreRise 스킵")
        return []

    up_cnt = int(np.sum(lib_r5 > 0))
    dn_cnt = n_lib - up_cnt
    print(f"  라이브러리: {n_lib:,}개 (상승 {up_cnt:,}개 / 하락 {dn_cnt:,}개)")

    lib_norms = np.linalg.norm(lib_fvs, axis=1).astype(np.float32)

    scored = []
    total  = len(full_data)

    for i, (ticker, (market, df)) in enumerate(full_data.items()):
        if i % 500 == 0:
            print(f"  PreRise 진행: {i:,}/{total:,}")

        result = _score_one(df, lib_fvs, lib_norms, lib_r5, lib_r20)
        if result is None:
            continue

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last
        chg_pct   = round((last["close"] / prev["close"] - 1) * 100, 2) if prev["close"] > 0 else 0.0
        vol_avg20 = df["volume"].iloc[-20:].mean()
        vol_ratio = round(last["volume"] / vol_avg20, 1) if vol_avg20 > 0 else 0.0

        scored.append({
            "ticker"      : ticker,
            "market"      : market,
            "close"       : int(last["close"]),
            "change_pct"  : chg_pct,
            "volume_ratio": vol_ratio,
            "close_20d"   : [int(x) for x in df["close"].iloc[-20:].tolist()],
            "win_prob_5d" : result["win_prob_5d"],
            "win_prob_20d": result["win_prob_20d"],
            "avg_ret_5d"  : result["avg_ret_5d"],
            "avg_ret_20d" : result["avg_ret_20d"],
        })

    scored.sort(key=lambda x: x["win_prob_5d"], reverse=True)
    return scored[:top_n]
