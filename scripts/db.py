"""
DB 어댑터 — 환경변수에 따라 DuckDB(로컬) 또는 Supabase(클라우드) 자동 선택
  - SUPABASE_URL 없음 → DuckDBAdapter  (data/stock.db)
  - SUPABASE_URL 있음 → SupabaseAdapter
"""

import os
from datetime import date, timedelta
from abc import ABC, abstractmethod

import pandas as pd

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/stock.db")

DDL = """
CREATE TABLE IF NOT EXISTS ohlcv (
    ticker  VARCHAR NOT NULL,
    market  VARCHAR NOT NULL,
    date    DATE    NOT NULL,
    open    INTEGER,
    high    INTEGER,
    low     INTEGER,
    close   INTEGER,
    volume  BIGINT,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS signals (
    date         DATE    NOT NULL,
    ticker       VARCHAR NOT NULL,
    name         VARCHAR NOT NULL,
    market       VARCHAR NOT NULL,
    patterns     VARCHAR[],
    close        INTEGER,
    change_pct   DOUBLE,
    volume_ratio DOUBLE,
    ma_aligned   BOOLEAN,
    hist_score   INTEGER,
    hist_win5    DOUBLE,
    hist_ret5    DOUBLE,
    ai_score     INTEGER,
    ai_reason    VARCHAR,
    ai_risk      VARCHAR,
    PRIMARY KEY (date, ticker)
);
"""


# ─────────────────────────────────────────────────────────────────
# 추상 인터페이스
# ─────────────────────────────────────────────────────────────────
class DBAdapter(ABC):

    @abstractmethod
    def get_last_ohlcv_date(self) -> date | None:
        """DB에 저장된 가장 최근 날짜"""

    @abstractmethod
    def upsert_ohlcv(self, rows: list[dict]) -> int:
        """OHLCV 배치 upsert. 저장된 행 수 반환."""

    @abstractmethod
    def fetch_recent_ohlcv(self, days: int = 90) -> dict[str, tuple[str, pd.DataFrame]]:
        """최근 N일 전 종목 OHLCV → {ticker: (market, df)}"""

    @abstractmethod
    def upsert_signals(self, signals: list[dict]):
        """오늘 스크리닝 결과 저장"""


# ─────────────────────────────────────────────────────────────────
# DuckDB 어댑터 (로컬)
# ─────────────────────────────────────────────────────────────────
class DuckDBAdapter(DBAdapter):

    def __init__(self, path: str = DUCKDB_PATH):
        import duckdb
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = duckdb.connect(path)
        self.conn.execute(DDL)

    def get_last_ohlcv_date(self) -> date | None:
        row = self.conn.execute(
            "SELECT MAX(date) FROM ohlcv"
        ).fetchone()
        return row[0] if row and row[0] else None

    def upsert_ohlcv(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        self.conn.execute("""
            INSERT INTO ohlcv (ticker, market, date, open, high, low, close, volume)
            SELECT ticker, market, date, open, high, low, close, volume FROM df
            ON CONFLICT (ticker, date) DO UPDATE SET
                open   = excluded.open,
                high   = excluded.high,
                low    = excluded.low,
                close  = excluded.close,
                volume = excluded.volume
        """)
        return len(rows)

    def fetch_recent_ohlcv(self, days: int = 90) -> dict[str, tuple[str, pd.DataFrame]]:
        df = self.conn.execute(f"""
            SELECT ticker, market, date, open, high, low, close, volume
            FROM ohlcv
            WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY ticker, date
        """).df()

        if df.empty:
            return {}

        df["date"] = pd.to_datetime(df["date"])
        result: dict[str, tuple[str, pd.DataFrame]] = {}
        for (ticker, market), grp in df.groupby(["ticker", "market"]):
            g = grp.drop(columns=["ticker", "market"]).set_index("date").sort_index()
            result[str(ticker)] = (str(market), g)
        return result

    def upsert_signals(self, signals: list[dict]):
        if not signals:
            return
        df = pd.DataFrame(signals)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        self.conn.execute("""
            INSERT INTO signals
            SELECT * FROM df
            ON CONFLICT (date, ticker) DO UPDATE SET
                name         = excluded.name,
                market       = excluded.market,
                patterns     = excluded.patterns,
                close        = excluded.close,
                change_pct   = excluded.change_pct,
                volume_ratio = excluded.volume_ratio,
                ma_aligned   = excluded.ma_aligned,
                hist_score   = excluded.hist_score,
                hist_win5    = excluded.hist_win5,
                hist_ret5    = excluded.hist_ret5,
                ai_score     = excluded.ai_score,
                ai_reason    = excluded.ai_reason,
                ai_risk      = excluded.ai_risk
        """)


# ─────────────────────────────────────────────────────────────────
# Supabase 어댑터 (클라우드, 나중에)
# ─────────────────────────────────────────────────────────────────
class SupabaseAdapter(DBAdapter):

    PAGE_SIZE   = 10_000
    OHLCV_BATCH = 2_000
    SIG_BATCH   = 500

    def __init__(self):
        from supabase import create_client
        self.sb = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_KEY"],
        )

    def get_last_ohlcv_date(self) -> date | None:
        from datetime import datetime
        res = (self.sb.table("ohlcv")
               .select("date")
               .order("date", desc=True)
               .limit(1)
               .execute())
        if res.data:
            return datetime.strptime(res.data[0]["date"], "%Y-%m-%d").date()
        return None

    def upsert_ohlcv(self, rows: list[dict]) -> int:
        for i in range(0, len(rows), self.OHLCV_BATCH):
            self.sb.table("ohlcv").upsert(rows[i : i + self.OHLCV_BATCH]).execute()
        return len(rows)

    def fetch_recent_ohlcv(self, days: int = 90) -> dict[str, tuple[str, pd.DataFrame]]:
        since  = (date.today() - timedelta(days=days)).isoformat()
        rows   = []
        offset = 0
        while True:
            res = (self.sb.table("ohlcv")
                   .select("ticker,market,date,open,high,low,close,volume")
                   .gte("date", since)
                   .order("ticker").order("date")
                   .range(offset, offset + self.PAGE_SIZE - 1)
                   .execute())
            if not res.data:
                break
            rows.extend(res.data)
            if len(res.data) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

        if not rows:
            return {}

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        result: dict[str, tuple[str, pd.DataFrame]] = {}
        for (ticker, market), grp in df.groupby(["ticker", "market"]):
            g = grp.drop(columns=["ticker", "market"]).set_index("date").sort_index()
            result[str(ticker)] = (str(market), g)
        return result

    def upsert_signals(self, signals: list[dict]):
        for i in range(0, len(signals), self.SIG_BATCH):
            self.sb.table("signals").upsert(signals[i : i + self.SIG_BATCH]).execute()


# ─────────────────────────────────────────────────────────────────
# 팩토리 — 환경변수에 따라 자동 선택
# ─────────────────────────────────────────────────────────────────
def get_adapter() -> DBAdapter:
    if os.environ.get("SUPABASE_URL"):
        return SupabaseAdapter()
    return DuckDBAdapter()
