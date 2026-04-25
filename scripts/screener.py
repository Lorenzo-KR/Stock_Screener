"""
Stock Screener — KOSPI / KOSDAQ
매일 장 마감 후 GitHub Actions 에서 자동 실행
- Supabase 설정 시: 어제 데이터만 증분 수집, DB에서 90일치 조회
- Supabase 미설정 시: pykrx로 80일치 직접 수집 (fallback)
출력: data/results.json  +  Supabase signals 테이블
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta

import re

import numpy as np
import pandas as pd
import requests
from pykrx import stock

sys.path.insert(0, os.path.dirname(__file__))

# ─────────────────────────────────────────────────────────────────
# 설정값
# ─────────────────────────────────────────────────────────────────
LOOKBACK_DAYS  = 80       # Supabase 미사용 시 fallback 수집 기간 (캘린더일)
DB_OHLCV_DAYS  = 130      # DB 패턴 감지용 조회 기간 (캘린더일, 영업일 약 90일 확보)
BREAKOUT_DAYS  = 20
VOLUME_MULT    = 2.0
MA_SHORT       = 5
MA_MID         = 20
MA_LONG        = 60
MIN_PRICE      = 1_000
MIN_VOLUME     = 50_000
MAX_AI_TARGETS = 60
PYKRX_SLEEP    = 0.05
AI_SLEEP       = 1.2


_TICKER_NAME_CACHE: dict[str, str] = {}
_ETF_TICKERS: set[str] = {}


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


def build_name_cache() -> None:
    """네이버 금융 종목 목록에서 ticker → 종목명 캐시를 미리 구축"""
    global _TICKER_NAME_CACHE
    for market_code in (0, 1):
        for page in range(1, 60):
            try:
                r = requests.get(
                    "https://finance.naver.com/sise/sise_market_sum.nhn",
                    params={"sosok": market_code, "page": page},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )
                # 종목 링크 패턴: code=123456 뒤에 클래스 등 속성 있을 수 있으므로 [^>]* 사용
                pairs = re.findall(r'code=([0-9]{6})[^>]*>([^<]{1,30})</a>', r.text)
                if not pairs:
                    break
                for code, name in pairs:
                    name = name.strip()
                    if name and code not in _TICKER_NAME_CACHE:
                        _TICKER_NAME_CACHE[code] = name
            except Exception:
                break
    print(f"  종목명 캐시 구축: {len(_TICKER_NAME_CACHE):,}개")


def build_etf_set() -> None:
    """네이버 금융 ETF/ETN API에서 전체 코드 세트 구축"""
    global _ETF_TICKERS
    codes: set[str] = set()
    for url, key in [
        ("https://finance.naver.com/api/sise/etfItemList.nhn", "etfItemList"),
        ("https://finance.naver.com/api/sise/etnItemList.nhn", "etnItemList"),
    ]:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            items = r.json()["result"][key]
            codes.update(item["itemcode"] for item in items)
        except Exception:
            pass
    _ETF_TICKERS = codes
    print(f"  ETF/ETN 목록 구축: {len(_ETF_TICKERS):,}개")


def is_etf(ticker: str, name: str) -> bool:
    if ticker in _ETF_TICKERS:
        return True
    # ETF/ETN 목록 구축 실패 시 이름 기반 판별
    name_upper = name.upper()
    if " ETN" in name_upper or " ETF" in name_upper:
        return True
    etf_prefixes = (
        "KODEX", "TIGER", "KBSTAR", "RISE", "HANARO", "ACE", "KOSEF",
        "ARIRANG", "SOL", "TIMEFOLIO", "KINDEX", "SMART", "TREX", "PLUS",
        "FOCUS", "MASTER", "TRUE", "QV", "WOORI",
    )
    return any(name_upper.startswith(p) for p in etf_prefixes)


# ─────────────────────────────────────────────────────────────────
# 백테스트 통계 로드 & 점수 산출
# ─────────────────────────────────────────────────────────────────
def load_backtest_stats() -> dict:
    try:
        with open("data/backtest_stats.json", "r", encoding="utf-8") as f:
            return json.load(f).get("pattern_stats", {})
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def hist_score_from_stats(patterns: list, stats: dict) -> tuple[int | None, dict | None]:
    if not stats or not patterns:
        return None, None
    key = "+".join(sorted(patterns))
    s   = stats.get(key)
    if s is None:
        for p in patterns:
            cand = stats.get(p)
            if cand and (s is None or cand["avg_return_5d"] > s["avg_return_5d"]):
                s = cand
    if s is None:
        return None, None
    ev    = s["win_rate_5d"] * s["avg_return_5d"]
    score = max(1, min(10, round(1 + (ev + 2) / 7 * 9)))
    return score, s


# ─────────────────────────────────────────────────────────────────
# pykrx 유틸
# ─────────────────────────────────────────────────────────────────
def last_trading_day() -> str:
    """어제 또는 가장 최근 영업일 (yyyymmdd)"""
    d = datetime.today() - timedelta(days=1)
    # 주말 보정
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def get_ticker_name(ticker: str) -> str:
    # 네이버 금융 캐시 우선 사용
    if ticker in _TICKER_NAME_CACHE:
        return _TICKER_NAME_CACHE[ticker]
    try:
        name = stock.get_market_ticker_name(ticker)
        if isinstance(name, str) and name:
            return name
        # pykrx 버전에 따라 Series/DataFrame 반환 가능 — 비어있으면 ticker 반환
        if hasattr(name, "empty") and name.empty:
            return ticker
        if hasattr(name, "iloc"):
            return str(name.iloc[0])
    except Exception:
        pass
    return ticker


def get_ohlcv_pykrx(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """pykrx에서 단일 종목 OHLCV (fallback용)"""
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        if df is None or len(df) < MA_LONG + 5:
            return None
        df.columns = [c.strip() for c in df.columns]
        df.rename(columns={
            "시가": "open", "고가": "high", "저가": "low",
            "종가": "close", "거래량": "volume",
        }, inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].copy()
        df = df[df["volume"] > 0]
        return df if len(df) >= MA_LONG + 5 else None
    except Exception:
        return None


def fetch_yesterday_batch(date_str: str) -> list[dict]:
    """pykrx 배치 API — 특정 날짜 전 종목 OHLCV (2번 호출로 전 시장 커버)"""
    rows = []
    col_map = {
        "시가": "open", "고가": "high", "저가": "low",
        "종가": "close", "거래량": "volume",
    }
    date_str_fmt = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(date_str, market=market)
            if df is None or df.empty:
                continue
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns=col_map, inplace=True)
            needed = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            if not needed:
                continue
            df = df[needed]
            for ticker, row in df.iterrows():
                if row.get("close", 0) <= 0 or row.get("volume", 0) <= 0:
                    continue
                rows.append({
                    "ticker": str(ticker),
                    "market": market,
                    "date":   date_str_fmt,
                    "open":   int(row.get("open",  0)),
                    "high":   int(row.get("high",  0)),
                    "low":    int(row.get("low",   0)),
                    "close":  int(row.get("close", 0)),
                    "volume": int(row.get("volume",0)),
                })
        except Exception:
            pass  # 배치 실패 시 조용히 넘어감 — 기존 DB 데이터로 스크리닝 계속
    return rows


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
    if ind["ma5_prev"] < ind["ma20_prev"] and ind["ma5"] >= ind["ma20"]:
        flags.append("golden_cross")
    if not np.isnan(ind["high_20d"]) and ind["close"] > ind["high_20d"]:
        flags.append("breakout")
    if (ind["vol_avg20"] > 0
            and ind["vol_last"] >= ind["vol_avg20"] * VOLUME_MULT
            and ind["close"] > ind["open_last"]):
        flags.append("volume_surge")
    if (not np.isnan(ind["ma20_prev"])
            and ind["low_prev"] <= ind["ma20_prev"] * 1.02
            and ind["close"] > ind["ma20"]):
        flags.append("pullback_support")
    return flags


# ─────────────────────────────────────────────────────────────────
# AI 분석
# ─────────────────────────────────────────────────────────────────
def score_with_ai(client, name: str, market: str, patterns: list, ind: dict,
                  stat: dict | None = None) -> tuple:
    pattern_str = ", ".join([PATTERN_LABELS.get(p, p) for p in patterns])
    aligned   = (ind["ma5"] > ind["ma20"] > ind["ma60"])
    vol_ratio = (ind["vol_last"] / ind["vol_avg20"]) if ind["vol_avg20"] > 0 else 0

    stat_text = ""
    if stat:
        stat_text = f"""
[백테스트 통계 (최근 3년, n={stat['count']:,}건)]
5일 승률: {stat['win_rate_5d']:.1%} | 5일 평균수익: {stat['avg_return_5d']:+.1f}%
20일 승률: {stat['win_rate_20d']:.1%} | 20일 평균수익: {stat['avg_return_20d']:+.1f}%
"""

    prompt = f"""당신은 한국 주식 기술적 분석 전문가입니다. 아래 종목 데이터를 차트 관점에서 분석하세요.

[종목 정보]
이름: {name} ({market})
현재가: {int(ind['close']):,}원 (전일대비 {((ind['close']/ind['prev_close'])-1)*100:+.2f}%)
감지 패턴: {pattern_str}
MA 정배열: {'YES (MA5 > MA20 > MA60)' if aligned else 'NO'}
거래량 비율(20일 평균 대비): {vol_ratio:.1f}배
MA5: {ind['ma5']:.0f} | MA20: {ind['ma20']:.0f} | MA60: {ind['ma60']:.0f}
최근 20일 종가: {[int(x) for x in ind['close_20d']]}{stat_text}
[평가 기준]
- 백테스트 통계 대비 현재 차트 맥락의 신뢰도
- 패턴 조합 강도 및 MA 정배열 여부
- 거래량 지지 수준과 추세 지속 가능성
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
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 스크리너 시작")

    # ── DB 어댑터 연결 ────────────────────────────────────────────
    adapter = None
    use_db  = False
    try:
        import db as _db
        adapter = _db.get_adapter()
        use_db  = True
        mode = "Supabase(클라우드)" if os.environ.get("SUPABASE_URL") else "DuckDB(로컬)"
        print(f"  DB 연결 성공 — {mode}")
    except Exception as e:
        print(f"  DB 미사용 — fallback 모드 ({e})")

    # ── 종목명 캐시 & ETF 목록 구축 ──────────────────────────────────
    build_name_cache()
    build_etf_set()

    # ── 백테스트 통계 로드 ────────────────────────────────────────
    bt_stats = load_backtest_stats()
    if bt_stats:
        print(f"  백테스트 통계: {len(bt_stats)}개 패턴")

    # ── OHLCV 데이터 수집 ─────────────────────────────────────────
    ticker_data: dict[str, tuple[str, pd.DataFrame]] = {}  # {ticker: (market, df)}

    if use_db:
        # [DB 모드] 어제 데이터만 pykrx 배치 fetch → upsert → DB에서 90일 조회
        yesterday = last_trading_day()
        print(f"  어제({yesterday}) 데이터 배치 수집 중...")
        new_rows = fetch_yesterday_batch(yesterday)
        if new_rows:
            adapter.upsert_ohlcv(new_rows)
            print(f"  DB upsert: {len(new_rows):,}행")
        else:
            print("  어제 데이터 없음 (휴장일 가능성)")

        print(f"  DB에서 최근 {DB_OHLCV_DAYS}일 OHLCV 조회 중...")
        ticker_data = adapter.fetch_recent_ohlcv(days=DB_OHLCV_DAYS)
        print(f"  조회 완료: {len(ticker_data):,}개 종목")
        total_scanned = len(ticker_data)
    else:
        # [Fallback 모드] pykrx 개별 종목 fetch
        end_date   = datetime.today().strftime("%Y%m%d")
        start_date = (datetime.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
        print(f"  pykrx 개별 수집: {start_date} ~ {end_date}")

        kospi  = [("KOSPI",  t) for t in get_all_tickers(0)]
        kosdaq = [("KOSDAQ", t) for t in get_all_tickers(1)]
        all_tickers = kospi + kosdaq
        total_scanned = len(all_tickers)
        print(f"  전체 종목: {total_scanned:,}개")

        for i, (market, ticker) in enumerate(all_tickers):
            if i % 300 == 0:
                print(f"  진행: {i:,}/{total_scanned:,}")
            df = get_ohlcv_pykrx(ticker, start_date, end_date)
            if df is not None:
                ticker_data[ticker] = (market, df)
            time.sleep(PYKRX_SLEEP)

    # ── 패턴 감지 ────────────────────────────────────────────────
    candidates = []
    for ticker, (market, df) in ticker_data.items():
        if len(df) < MA_LONG + 5:
            continue
        ind = calc_indicators(df)
        if ind["close"] < MIN_PRICE or ind["vol_last"] < MIN_VOLUME:
            continue
        patterns = detect_patterns(ind)
        if not patterns:
            continue

        name = get_ticker_name(ticker)
        hs, stat = hist_score_from_stats(patterns, bt_stats)

        candidates.append({
            "ticker"      : ticker,
            "name"        : name,
            "market"      : market,
            "is_etf"      : is_etf(ticker, name),
            "patterns"    : patterns,
            "close"       : int(ind["close"]),
            "change_pct"  : round((ind["close"] / ind["prev_close"] - 1) * 100, 2),
            "volume_ratio": round(ind["vol_last"] / ind["vol_avg20"], 1) if ind["vol_avg20"] > 0 else 0,
            "ma_aligned"  : bool(ind["ma5"] > ind["ma20"] > ind["ma60"]),
            "close_20d"   : [int(x) for x in ind["close_20d"]],
            "hist_score"  : hs,
            "hist_win5"   : stat["win_rate_5d"]   if stat else None,
            "hist_ret5"   : stat["avg_return_5d"] if stat else None,
            "hist_n"      : stat["count"]          if stat else None,
            "ai_score"    : None,
            "ai_reason"   : "",
            "ai_risk"     : "",
            "_ind"        : ind,   # AI 분석용 임시 필드
        })

    print(f"  패턴 감지 후보: {len(candidates)}개")

    # ── AI 분석 ──────────────────────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key and candidates:
        try:
            import anthropic as _anthropic
            client = _anthropic.Anthropic(api_key=api_key)

            candidates.sort(
                key=lambda x: (x["hist_score"] or 0, len(x["patterns"]), x["volume_ratio"]),
                reverse=True,
            )
            ai_targets = candidates[:MAX_AI_TARGETS]
            print(f"  AI 분석 시작: {len(ai_targets)}개")

            for j, c in enumerate(ai_targets):
                print(f"    [{j+1:02d}/{len(ai_targets)}] {c['name']} ({c['market']})")
                _, stat = hist_score_from_stats(c["patterns"], bt_stats)
                score, reason, risk = score_with_ai(
                    client, c["name"], c["market"], c["patterns"], c["_ind"], stat
                )
                c["ai_score"]  = score
                c["ai_reason"] = reason
                c["ai_risk"]   = risk
                time.sleep(AI_SLEEP)
        except ImportError:
            print("  anthropic 패키지 없음 — AI 분석 건너뜀")
    else:
        print("  ANTHROPIC_API_KEY 없음 — AI 분석 건너뜀")

    # _ind 임시 필드 제거
    for c in candidates:
        c.pop("_ind", None)

    # ── Analog 분석 (KNN: 유사 과거 패턴 → 1일 상승 확률) ───────────
    try:
        import analog as _analog
        print(f"  Analog 분석 시작 ({len(ticker_data):,}개 종목)...")
        analog_results = _analog.run_analog_screener(ticker_data, top_n=500, adapter=adapter if use_db else None)
        analog_map = {r["ticker"]: r for r in analog_results}

        for c in candidates:
            ar = analog_map.get(c["ticker"])
            c["win_prob_1d"] = ar["win_prob_1d"] if ar else None
            c["avg_ret_1d"]  = ar["avg_ret_1d"]  if ar else None

        print(f"  Analog 완료: {len(analog_results)}개 점수 산출")
    except Exception as e:
        print(f"  Analog 분석 오류: {e}")
        for c in candidates:
            c["win_prob_1d"] = None
            c["avg_ret_1d"]  = None

    # ── PreRise 분석 (강한 상승/하락 패턴 KNN → 5일·20일 상승 확률) ─
    try:
        import prerise as _prerise
        print(f"  PreRise 분석 시작...")
        prerise_results = _prerise.run_prerise_screener(
            ticker_data, top_n=500, adapter=adapter if use_db else None
        )
        prerise_map = {r["ticker"]: r for r in prerise_results}

        for c in candidates:
            pr = prerise_map.get(c["ticker"])
            c["win_prob_5d"]  = pr["win_prob_5d"]  if pr else None
            c["win_prob_20d"] = pr["win_prob_20d"] if pr else None
            c["avg_ret_5d"]   = pr["avg_ret_5d"]   if pr else None
            c["avg_ret_20d"]  = pr["avg_ret_20d"]  if pr else None

        print(f"  PreRise 완료: {len(prerise_results)}개 점수 산출")
    except Exception as e:
        print(f"  PreRise 분석 오류: {e}")
        import traceback; traceback.print_exc()
        for c in candidates:
            c["win_prob_5d"]  = None
            c["win_prob_20d"] = None
            c["avg_ret_5d"]   = None
            c["avg_ret_20d"]  = None

    # ── 최종 정렬: 5일 상승확률 → 1일 상승확률 → 백테스트 점수 ────
    candidates.sort(
        key=lambda x: (
            x.get("win_prob_5d")  or 0,
            x.get("win_prob_1d")  or 0,
            x["hist_score"] or 0,
            x["ai_score"]   or 0,
        ),
        reverse=True,
    )

    # ── DB signals 저장 ──────────────────────────────────────────
    if use_db and candidates:
        today = datetime.today().strftime("%Y-%m-%d")
        signal_rows = [{
            "date"        : today,
            "ticker"      : c["ticker"],
            "name"        : c["name"],
            "market"      : c["market"],
            "patterns"    : c["patterns"],
            "close"       : c["close"],
            "change_pct"  : c["change_pct"],
            "volume_ratio": c["volume_ratio"],
            "ma_aligned"  : c["ma_aligned"],
            "hist_score"  : c["hist_score"],
            "hist_win5"   : c["hist_win5"],
            "hist_ret5"   : c["hist_ret5"],
            "ai_score"    : c["ai_score"],
            "ai_reason"   : c["ai_reason"],
            "ai_risk"     : c["ai_risk"],
        } for c in candidates]
        adapter.upsert_signals(signal_rows)
        print(f"  DB signals 저장: {len(signal_rows)}건")

    # ── results.json 저장 ─────────────────────────────────────────
    output = {
        "updated_at"   : datetime.now().strftime("%Y-%m-%d %H:%M KST"),
        "total_scanned": total_scanned,
        "total_found"  : len(candidates),
        "candidates"   : candidates,
    }
    os.makedirs("data", exist_ok=True)
    with open("data/results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✓ data/results.json 저장 ({len(candidates)}개 종목)")


if __name__ == "__main__":
    main()
