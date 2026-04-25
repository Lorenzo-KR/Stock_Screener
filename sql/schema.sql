-- Supabase SQL Editor에서 실행하세요

CREATE TABLE IF NOT EXISTS ohlcv (
    ticker  TEXT    NOT NULL,
    market  TEXT    NOT NULL,
    date    DATE    NOT NULL,
    open    INTEGER,
    high    INTEGER,
    low     INTEGER,
    close   INTEGER,
    volume  BIGINT,
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS ohlcv_date_idx   ON ohlcv(date);
CREATE INDEX IF NOT EXISTS ohlcv_ticker_idx ON ohlcv(ticker);

CREATE TABLE IF NOT EXISTS signals (
    date         DATE        NOT NULL,
    ticker       TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    market       TEXT        NOT NULL,
    patterns     TEXT[]      NOT NULL DEFAULT '{}',
    close        INTEGER,
    change_pct   NUMERIC(8,2),
    volume_ratio NUMERIC(8,2),
    ma_aligned   BOOLEAN,
    hist_score   SMALLINT,
    hist_win5    NUMERIC(5,3),
    hist_ret5    NUMERIC(8,2),
    ai_score     SMALLINT,
    ai_reason    TEXT,
    ai_risk      TEXT,
    PRIMARY KEY (date, ticker)
);

CREATE INDEX IF NOT EXISTS signals_date_idx   ON signals(date);
CREATE INDEX IF NOT EXISTS signals_ticker_idx ON signals(ticker);
