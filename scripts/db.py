"""Supabase 헬퍼 — OHLCV 저장/조회 및 signals 기록"""

import os
from datetime import date, datetime, timedelta

import pandas as pd

OHLCV_BATCH  = 2_000
SIGNAL_BATCH = 500
PAGE_SIZE    = 10_000   # Supabase 요청당 최대 행 수


def get_client():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def get_last_ohlcv_date(sb) -> date | None:
    res = (sb.table("ohlcv")
             .select("date")
             .order("date", desc=True)
             .limit(1)
             .execute())
    if res.data:
        return datetime.strptime(res.data[0]["date"], "%Y-%m-%d").date()
    return None


def upsert_ohlcv(sb, rows: list[dict]) -> int:
    """OHLCV 배치 upsert. 저장된 행 수 반환."""
    for i in range(0, len(rows), OHLCV_BATCH):
        sb.table("ohlcv").upsert(rows[i : i + OHLCV_BATCH]).execute()
    return len(rows)


def fetch_recent_ohlcv(sb, days: int = 90) -> dict[str, tuple[str, pd.DataFrame]]:
    """최근 N일 전 종목 OHLCV를 페이지네이션으로 조회.
    반환: {ticker: (market, df)}  — df 인덱스는 datetime, 컬럼은 open/high/low/close/volume
    """
    since  = (date.today() - timedelta(days=days)).isoformat()
    rows: list[dict] = []
    offset = 0

    while True:
        res = (sb.table("ohlcv")
                 .select("ticker,market,date,open,high,low,close,volume")
                 .gte("date", since)
                 .order("ticker")
                 .order("date")
                 .range(offset, offset + PAGE_SIZE - 1)
                 .execute())
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not rows:
        return {}

    df_all = pd.DataFrame(rows)
    df_all["date"] = pd.to_datetime(df_all["date"])

    result: dict[str, tuple[str, pd.DataFrame]] = {}
    for (ticker, market), grp in df_all.groupby(["ticker", "market"]):
        df = (grp.drop(columns=["ticker", "market"])
                 .set_index("date")
                 .sort_index()
                 .astype({"open": int, "high": int, "low": int,
                           "close": int, "volume": int}))
        result[str(ticker)] = (str(market), df)
    return result


def upsert_signals(sb, signals: list[dict]):
    for i in range(0, len(signals), SIGNAL_BATCH):
        sb.table("signals").upsert(signals[i : i + SIGNAL_BATCH]).execute()
