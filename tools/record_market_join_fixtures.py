"""Record the ESPN, Kalshi and Polymarket Gamma payloads that tests/odds/test_market_join.py replays offline.

Run once per refresh. It makes about 40 sequential requests, 0.5 s apart::

    uv run python tools/record_market_join_fixtures.py --start 20261118 --days 7

Writes ``tests/fixtures/market_join/<league>/{espn_scoreboard,kalshi_markets,gamma_events}.json``,
each trimmed to the keys the join and its parsers read (see the fixtures README).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import time

import requests

from sportsdataverse.odds.market_join import MARKET_LEAGUES

ESPN = "https://site.api.espn.com/apis/site/v2/sports"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA = "https://gamma-api.polymarket.com"
OUT = pathlib.Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "market_join"

_KALSHI_KEYS = (
    "ticker",
    "event_ticker",
    "title",
    "yes_sub_title",
    "status",
    "occurrence_datetime",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "no_bid_dollars",
    "no_ask_dollars",
    "last_price_dollars",
)
_GAMMA_KEYS = ("id", "slug", "title", "eventDate", "startTime", "usId", "active", "closed")
_GAMMA_TEAM_KEYS = ("abbreviation", "alias", "name", "ordering")
_ESPN_TEAM_KEYS = ("id", "abbreviation", "displayName", "shortDisplayName", "name", "location")


def _get(session: requests.Session, url: str, **params: str):
    time.sleep(0.5)
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _espn_event(event: dict) -> dict:
    comp = (event.get("competitions") or [{}])[0]
    competitors = [
        {
            "id": c.get("id"),
            "homeAway": c.get("homeAway"),
            "team": {k: (c.get("team") or {}).get(k) for k in _ESPN_TEAM_KEYS},
        }
        for c in comp.get("competitors") or []
    ]
    return {
        **{k: event.get(k) for k in ("id", "uid", "date", "name", "shortName", "status")},
        "competitions": [
            {
                "id": comp.get("id"),
                "date": comp.get("date"),
                "neutralSite": comp.get("neutralSite"),
                "competitors": competitors,
            }
        ],
    }


def record(session: requests.Session, league: str, start: dt.date, days: int) -> None:
    cfg = MARKET_LEAGUES[league]
    out = OUT / league
    out.mkdir(parents=True, exist_ok=True)

    events = []
    for i in range(days):
        day = (start + dt.timedelta(days=i)).strftime("%Y%m%d")
        body = _get(session, f"{ESPN}/{cfg.espn_path}/scoreboard", dates=day, limit="500")
        events += [_espn_event(e) for e in body.get("events", [])]
    (out / "espn_scoreboard.json").write_text(json.dumps({"events": events}, indent=1) + "\n", encoding="utf-8")

    markets, cursor = [], ""
    for _ in range(20):
        page = {"cursor": cursor} if cursor else {}
        body = _get(session, f"{KALSHI}/markets", series_ticker=cfg.kalshi_series, status="open", limit="1000", **page)
        markets += [{k: m.get(k) for k in _KALSHI_KEYS} for m in body.get("markets", [])]
        cursor = body.get("cursor") or ""
        if not cursor:
            break
    (out / "kalshi_markets.json").write_text(json.dumps({"markets": markets}, indent=1) + "\n", encoding="utf-8")

    gamma = []
    for page_no in range(6):
        batch = _get(
            session,
            f"{GAMMA}/events",
            series_id=str(cfg.gamma_series_id),
            active="true",
            closed="false",
            limit="100",
            offset=str(page_no * 100),
        )
        for e in batch:
            row = {k: e.get(k) for k in _GAMMA_KEYS}
            row["teams"] = [{k: t.get(k) for k in _GAMMA_TEAM_KEYS} for t in e.get("teams") or []]
            gamma.append(row)
        if len(batch) < 100:
            break
    (out / "gamma_events.json").write_text(json.dumps(gamma, indent=1) + "\n", encoding="utf-8")
    print(f"{league}: {len(events)} ESPN events, {len(markets)} Kalshi markets, {len(gamma)} Gamma events")


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--start", required=True, help="first ESPN date, YYYYMMDD")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--leagues", nargs="+", default=sorted(MARKET_LEAGUES))
    args = ap.parse_args()
    start = dt.datetime.strptime(args.start, "%Y%m%d").date()
    session = requests.Session()
    session.headers["User-Agent"] = "sportsdataverse-py fixture recorder"
    for league in args.leagues:
        record(session, league, start, args.days)


if __name__ == "__main__":
    main()
