"""
DB 초기화 — 전 종목 3년치 OHLCV를 Supabase에 적재 (최초 1회 실행)
이미 데이터가 있으면 아무것도 하지 않습니다.
"""

import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests
from pykrx import stock

sys.path.insert(0, os.path.dirname(__file__))
import db

YEARS       = 3


def get_all_tickers(market_code: int) -> list[str]:
    """네이버 금융에서 종목 코드 목록 조회 (0=KOSPI, 1=KOSDAQ)"""
    tickers = []
    for page in range(1, 60):
        try:
            r = requests.get(
                "https://finance.naver.com/sise/sise_market_sum.nhn",
                params={"sosok": market_code, "page": page},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            codes = re.findall(r"code=([0-9]{6})", r.text)
            if not codes:
                break
            tickers.extend(codes)
        except Exception:
            break
    return list(set(tickers))
PYKRX_SLEEP = 0.05
FLUSH_SIZE  = 10_000


def get_date_range():
    end   = datetime.today()
    start = end - timedelta(days=int(YEARS * 365.25) + 30)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def fetch_ohlcv(ticker: str, start: str, end: str):
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        if df is None or len(df) < 5:
            return None
        df.columns = [c.strip() for c in df.columns]
        df.rename(columns={
            "시가": "open", "고가": "high", "저가": "low",
            "종가": "close", "거래량": "volume",
        }, inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].copy()
        return df[df["volume"] > 0]
    except Exception:
        return None


def main():
    adapter = db.get_adapter()
    mode    = "Supabase" if os.environ.get("SUPABASE_URL") else "DuckDB(로컬)"
    print(f"  DB 모드: {mode}")

    last = adapter.get_last_ohlcv_date()
    if last:
        print(f"DB에 이미 데이터 있음 (최신: {last}). init_db 건너뜀.")
        print("증분 업데이트는 screener.py가 자동으로 처리합니다.")
        return

    start_date, end_date = get_date_range()
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] DB 초기화 시작 | {start_date} ~ {end_date}")

    kospi  = [("KOSPI",  t) for t in get_all_tickers(0)]
    kosdaq = [("KOSDAQ", t) for t in get_all_tickers(1)]
    tickers = kospi + kosdaq
    print(f"  전체 종목: {len(tickers):,}개")

    buffer: list[dict] = []
    ok = 0
    total_rows = 0

    for i, (market, ticker) in enumerate(tickers):
        if i % 200 == 0:
            print(f"  진행: {i:,}/{len(tickers):,} | 저장된 행: {total_rows:,}")

        df = fetch_ohlcv(ticker, start_date, end_date)
        if df is not None:
            for dt, row in df.iterrows():
                buffer.append({
                    "ticker": ticker,
                    "market": market,
                    "date":   dt.strftime("%Y-%m-%d"),
                    "open":   int(row["open"]),
                    "high":   int(row["high"]),
                    "low":    int(row["low"]),
                    "close":  int(row["close"]),
                    "volume": int(row["volume"]),
                })
            ok += 1

        if len(buffer) >= FLUSH_SIZE:
            total_rows += adapter.upsert_ohlcv(buffer)
            buffer.clear()
            print(f"    → DB upsert ({total_rows:,}행 누적)")

        time.sleep(PYKRX_SLEEP)

    if buffer:
        total_rows += adapter.upsert_ohlcv(buffer)

    print(f"  ✓ DB 초기화 완료: {ok:,}종목, {total_rows:,}행 저장")


if __name__ == "__main__":
    main()
