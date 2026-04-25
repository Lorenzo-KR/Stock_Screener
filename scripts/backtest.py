"""
백테스트 엔진 — KOSPI / KOSDAQ
최근 3년간 패턴 발생 시점 → 5일/20일 실제 수익률 통계 산출
월 1회 GitHub Actions 실행 | 출력: data/backtest_stats.json
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pykrx import stock

BACKTEST_YEARS = 3
FORWARD_5D     = 5
FORWARD_20D    = 20
MA_SHORT       = 5
MA_MID         = 20
MA_LONG        = 60
BREAKOUT_DAYS  = 20
VOLUME_MULT    = 2.0
MIN_PRICE      = 1_000
MIN_VOLUME     = 50_000
MIN_SAMPLES    = 30       # 통계 신뢰 최소 샘플 수
PYKRX_SLEEP    = 0.05


def get_date_range():
    end   = datetime.today()
    start = end - timedelta(days=int(BACKTEST_YEARS * 365.25) + 30)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def get_all_tickers():
    kospi  = [("KOSPI",  t) for t in stock.get_market_ticker_list(market="KOSPI")]
    kosdaq = [("KOSDAQ", t) for t in stock.get_market_ticker_list(market="KOSDAQ")]
    return kospi + kosdaq


def get_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        if df is None or len(df) < MA_LONG + FORWARD_20D + 5:
            return None
        df.columns = [c.strip() for c in df.columns]
        df.rename(columns={
            "시가": "open", "고가": "high", "저가": "low",
            "종가": "close", "거래량": "volume",
        }, inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].copy()
        df = df[df["volume"] > 0].reset_index(drop=True)
        return df if len(df) >= MA_LONG + FORWARD_20D + 5 else None
    except Exception:
        return None


def detect_patterns_at(df: pd.DataFrame, i: int) -> list[str]:
    c, v = df["close"], df["volume"]

    ma5       = c.iloc[i - MA_SHORT + 1 : i + 1].mean()
    ma20      = c.iloc[i - MA_MID   + 1 : i + 1].mean()
    ma5_prev  = c.iloc[i - MA_SHORT : i].mean()
    ma20_prev = c.iloc[i - MA_MID   : i].mean()
    vol_avg20 = v.iloc[i - MA_MID   : i].mean()
    high_20d  = df["high"].iloc[i - BREAKOUT_DAYS : i].max()

    close    = c.iloc[i]
    open_    = df["open"].iloc[i]
    low_prev = df["low"].iloc[i - 1]
    vol_last = v.iloc[i]

    pats = []
    if ma5_prev < ma20_prev and ma5 >= ma20:
        pats.append("golden_cross")
    if not np.isnan(high_20d) and close > high_20d:
        pats.append("breakout")
    if vol_avg20 > 0 and vol_last >= vol_avg20 * VOLUME_MULT and close > open_:
        pats.append("volume_surge")
    if not np.isnan(ma20_prev) and low_prev <= ma20_prev * 1.02 and close > ma20:
        pats.append("pullback_support")
    return pats


def run_backtest_for_ticker(df: pd.DataFrame) -> list[dict]:
    records = []
    for i in range(MA_LONG + 1, len(df) - FORWARD_20D):
        if df["close"].iloc[i] < MIN_PRICE or df["volume"].iloc[i] < MIN_VOLUME:
            continue
        pats = detect_patterns_at(df, i)
        if not pats:
            continue
        entry = df["close"].iloc[i]
        ret5  = (df["close"].iloc[i + FORWARD_5D]  / entry - 1) * 100
        ret20 = (df["close"].iloc[i + FORWARD_20D] / entry - 1) * 100
        records.append({
            "patterns": tuple(sorted(pats)),
            "ret5":  ret5,
            "ret20": ret20,
        })
    return records


def compute_stats(all_records: list[dict]) -> dict:
    bucket: dict[tuple, list] = defaultdict(list)
    for rec in all_records:
        pats = rec["patterns"]
        for p in pats:
            bucket[(p,)].append((rec["ret5"], rec["ret20"]))
        if len(pats) > 1:
            bucket[pats].append((rec["ret5"], rec["ret20"]))

    stats = {}
    for key, rets in bucket.items():
        if len(rets) < MIN_SAMPLES:
            continue
        r5  = [r[0] for r in rets]
        r20 = [r[1] for r in rets]
        stats["+".join(key)] = {
            "count":          len(rets),
            "win_rate_5d":    round(sum(1 for r in r5  if r > 0) / len(r5),  3),
            "avg_return_5d":  round(float(np.mean(r5)),  2),
            "med_return_5d":  round(float(np.median(r5)), 2),
            "win_rate_20d":   round(sum(1 for r in r20 if r > 0) / len(r20), 3),
            "avg_return_20d": round(float(np.mean(r20)), 2),
            "med_return_20d": round(float(np.median(r20)), 2),
        }
    return stats


def main():
    start_date, end_date = get_date_range()
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 백테스트 시작 | {start_date} ~ {end_date}")

    tickers = get_all_tickers()
    print(f"  전체 종목: {len(tickers):,}개")

    all_records: list[dict] = []
    ok = 0

    for i, (market, ticker) in enumerate(tickers):
        if i % 200 == 0:
            print(f"  진행: {i:,}/{len(tickers):,} | 누적 레코드: {len(all_records):,}")
        df = get_ohlcv(ticker, start_date, end_date)
        if df is not None:
            all_records.extend(run_backtest_for_ticker(df))
            ok += 1
        time.sleep(PYKRX_SLEEP)

    print(f"  완료: {ok:,}종목 분석 | 총 레코드: {len(all_records):,}개")

    stats = compute_stats(all_records)
    print("  [패턴별 5일 기대수익률]")
    for k, v in sorted(stats.items(), key=lambda x: x[1]["avg_return_5d"], reverse=True):
        print(f"    {k:<45} 승률={v['win_rate_5d']:.1%}  평균={v['avg_return_5d']:+.2f}%  n={v['count']:,}")

    output = {
        "generated_at":   datetime.now().strftime("%Y-%m-%d %H:%M KST"),
        "lookback_years": BACKTEST_YEARS,
        "total_records":  len(all_records),
        "pattern_stats":  stats,
    }
    os.makedirs("data", exist_ok=True)
    with open("data/backtest_stats.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("  ✓ data/backtest_stats.json 저장 완료")


if __name__ == "__main__":
    main()
