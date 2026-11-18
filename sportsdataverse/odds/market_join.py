"""Join ESPN games to Kalshi and Polymarket game markets.

ESPN, Kalshi and Polymarket each spell a team their own way, and none of them
carries the others' ids, so the join works on what all three share: the two teams
and the day. The rules:

* **Teams, in either order.** A neutral-site game can list the "home" team second
  on one venue and first on another, so a pair matches as listed or swapped.
* **A team** matches on its code after :data:`MARKET_ALIASES` (ESPN ``GS`` is Kalshi
  ``GSW``), else on its name (Kalshi ``yes_sub_title``, Gamma ``name`` / ``alias``)
  against ESPN's location, display name, short name and nickname.
* **Kalshi** event tickers carry the US-Eastern date the game was first scheduled
  for (``KXNBAGAME-26NOV22BOSGSW``). A ticker on the game's Eastern date wins; a
  postponed game keeps its original ticker date, so an unclaimed ticker within
  ``max_date_gap_days`` is the fallback, nearest first.
* **Polymarket Gamma** events carry ``eventDate`` (the local game date) and
  ``startTime`` (UTC). The event date must equal the Eastern date, or the start
  time must fall within ``max_start_gap_hours`` of ESPN's. Only game events count
  (slug ``{sport}-{away}-{home}-{date}``); the derivative events Gamma files in the
  same series (``-exact-margin``, ``-player-props``) are skipped.
* **Ties.** Kalshi settles a tied NFL game at $0.50 on each team's contract, so
  the tie is priced into both sides and ``home_prob`` reads as a win counting a tie
  as half. A league where a draw needs its own price sets ``draw_possible``, and
  its probabilities stay null without one.

Each ESPN game gets at most one market event and each market event serves at most
one game. ``matched_by`` says how a row was joined, ``+``-separated: how the teams
matched (``code``, ``alias`` or ``name``), the date rule, and ``swapped`` when the
venue lists the teams in the other order. It is null for a game with no market.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from collections.abc import Iterable, Sequence
from typing import Any

import polars as pl

from sportsdataverse.wexp.market import fair_win_probs


__all__ = [
    "MARKET_ALIASES",
    "MARKET_LEAGUES",
    "MarketLeague",
    "join_espn_kalshi",
    "join_espn_polymarket",
    "parse_kalshi_event_ticker",
]


@dataclass(frozen=True)
class MarketLeague:
    """Where one league's games live on each venue.

    Attributes:
        espn_path: ``{sport}/{league}`` on ESPN's Site v2 API.
        kalshi_series: the Kalshi series of game-winner markets.
        gamma_series_id: the Polymarket Gamma ``series_id`` of the league's game events.
        draw_possible: a game can end level, so a two-way price is incomplete.
    """

    espn_path: str
    kalshi_series: str
    gamma_series_id: str | None
    draw_possible: bool = False


#: Leagues whose series were checked against recorded markets (2026-11-18).
MARKET_LEAGUES: dict[str, MarketLeague] = {
    "nba": MarketLeague("basketball/nba", "KXNBAGAME", "10345"),
    "nfl": MarketLeague("football/nfl", "KXNFLGAME", "12185"),
    "nhl": MarketLeague("hockey/nhl", "KXNHLGAME", "10346"),
}

#: ESPN abbreviation -> the venue's code, where they differ. Identity (any case) is the default.
#: Only pairs seen in the recorded markets are listed (tests/fixtures/market_join, 2026-11-18).
#: A pair missing here still matches on the team name, and ``matched_by`` then says ``name``.
MARKET_ALIASES: dict[str, dict[str, dict[str, str]]] = {
    "nba": {
        "GS": {"kalshi": "GSW", "gamma": "GSW"},
        "NO": {"kalshi": "NOP", "gamma": "NOP"},
        "NY": {"kalshi": "NYK", "gamma": "NYK"},
        "SA": {"kalshi": "SAS", "gamma": "SAS"},
        "UTAH": {"kalshi": "UTA", "gamma": "UTA"},
        "WSH": {"kalshi": "WAS", "gamma": "WAS"},
    },
    "nfl": {
        "JAX": {"kalshi": "JAC"},
        "LAR": {"gamma": "LA"},
        "WSH": {"kalshi": "WAS", "gamma": "WAS"},
    },
    "nhl": {
        "CGY": {"gamma": "CAL"},
        "LA": {"gamma": "LAK"},
        "MTL": {"gamma": "MON"},
        "UTA": {"gamma": "UTAH"},
        "VGK": {"gamma": "LAS"},
    },
}

_TICKER = re.compile(r"^(?P<series>[A-Z0-9]+)-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<dd>\d{2})(?P<blob>[A-Z0-9]+)$")
_MONTHS = {m: i for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), start=1)}


def parse_kalshi_event_ticker(event_ticker: str, codes: Iterable[str]) -> tuple[dt.date, str, str] | None:
    """``KXNBAGAME-26NOV22BOSGSW`` and its two outcome codes -> ``(2026-11-22, "BOS", "GSW")``.

    The ticker joins the away and home codes with no separator, so the two outcome
    codes (each market ticker's suffix) say where to split.

    Args:
        event_ticker: a Kalshi game event ticker.
        codes: the outcome codes of the event's markets (``BOS``, ``GSW``).

    Returns:
        ``(scheduled_date, away_code, home_code)``, or ``None`` when the ticker is not
        a dated game ticker or its team blob is not an ordered pair of ``codes``.

    Example:
        Quick start::

            from sportsdataverse.odds import parse_kalshi_event_ticker

            parse_kalshi_event_ticker("KXNBAGAME-26NOV22BOSGSW", ["GSW", "BOS"])
    """
    match = _TICKER.match(event_ticker or "")
    if not match or match["mon"] not in _MONTHS:
        return None
    try:
        day = dt.date(2000 + int(match["yy"]), _MONTHS[match["mon"]], int(match["dd"]))
    except ValueError:
        return None
    blob = re.sub(r"\d+$", "", match["blob"])
    known = {c.upper() for c in codes if c}
    for away in sorted(known, key=len, reverse=True):  # longest first: LAC is not LA + C
        home = blob[len(away) :]
        if blob.startswith(away) and home in known and home != away:
            return day, away, home
    return None


@dataclass
class _Side:
    code: str
    labels: tuple[str, ...]


@dataclass
class _Game:
    game_id: Any
    start: dt.datetime | None
    date_et: dt.date | None
    away: _Side
    home: _Side


def _norm(text: Any) -> str:
    """Lowercase, punctuation-free, with a trailing ``St`` read as ``State`` (Kalshi ``Washington St.``)."""
    out = re.sub(r"[^a-z0-9 ]+", " ", str(text or "").lower())
    out = re.sub(r"\s+", " ", out).strip()
    return re.sub(r" st$", " state", out)


def _alias(league: str, venue: str, espn_code: str) -> str:
    return MARKET_ALIASES.get(league, {}).get(espn_code.upper(), {}).get(venue, espn_code).upper()


def _side_match(league: str, venue: str, espn: _Side, code: str, labels: Sequence[str]) -> str | None:
    """How an ESPN team matches a venue's (code, labels): ``code``, ``alias``, ``name`` or ``None``."""
    if code and espn.code and code.upper() == espn.code.upper():
        return "code"
    if code and espn.code and code.upper() == _alias(league, venue, espn.code):
        return "alias"
    names = {_norm(x) for x in espn.labels} - {""}
    if any(_norm(label) in names for label in labels if label):
        return "name"
    return None


_RANK = {"code": 0, "alias": 1, "name": 2}


def _pair_match(league: str, venue: str, game: _Game, away: _Side, home: _Side) -> tuple[str, bool] | None:
    """``(team rule, swapped)`` when the venue's pair is the game's, as listed or swapped."""
    for swapped, (a, h) in ((False, (away, home)), (True, (home, away))):
        ma = _side_match(league, venue, game.away, a.code, a.labels)
        mh = _side_match(league, venue, game.home, h.code, h.labels)
        if ma and mh:
            return max(ma, mh, key=_RANK.__getitem__), swapped
    return None


def _utc(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _eastern_dates(starts: list[dt.datetime | None]) -> list[dt.date | None]:
    """US-Eastern calendar date of each UTC start, through polars' bundled tz database (no tzdata needed)."""
    s = pl.Series("s", starts, dtype=pl.Datetime("us", "UTC"))
    return s.dt.convert_time_zone("America/New_York").dt.date().to_list()


_ESPN_COLUMNS = ("game_id", "date", "home_abbreviation", "away_abbreviation")


def _espn_games(espn: pl.DataFrame) -> list[_Game]:
    """Games from an ESPN scoreboard frame (``espn_<league>_scoreboard()``, parsed)."""
    if espn.height == 0:  # a day with no games parses to a zero-column frame
        return []
    missing = [c for c in _ESPN_COLUMNS if c not in espn.columns]
    if missing:
        raise ValueError(f"the ESPN frame lacks {missing}; pass a parsed espn_<league>_scoreboard() frame")
    rows = espn.to_dicts()
    starts = [_utc(r.get("date")) for r in rows]
    dates = _eastern_dates(starts)

    def side(r: dict, prefix: str) -> _Side:
        labels = tuple(
            str(r.get(f"{prefix}_{k}") or "") for k in ("location", "display_name", "short_display_name", "name")
        )
        return _Side(str(r.get(f"{prefix}_abbreviation") or ""), labels)

    return [
        _Game(r["game_id"], s, d, side(r, "away"), side(r, "home")) for r, s, d in zip(rows, starts, dates, strict=True)
    ]


def _num(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _mid(bid: float | None, ask: float | None) -> float | None:
    """Bid/ask midpoint; a one-sided book (no bid, or Kalshi's 1.00 empty ask) has none."""
    if bid is None or ask is None or bid <= 0 or ask >= 1 or ask < bid:
        return None
    return (bid + ask) / 2


@dataclass
class _KalshiEvent:
    ticker: str
    date: dt.date
    away: _Side
    home: _Side
    markets: dict[str, dict]


def _kalshi_events(kalshi: pl.DataFrame) -> list[_KalshiEvent]:
    if kalshi.is_empty() or not {"ticker", "event_ticker"} <= set(kalshi.columns):
        return []
    grouped: dict[str, dict[str, dict]] = {}
    for row in kalshi.to_dicts():
        event, ticker = str(row.get("event_ticker") or ""), str(row.get("ticker") or "")
        if event and ticker.startswith(f"{event}-"):
            grouped.setdefault(event, {})[ticker[len(event) + 1 :].upper()] = row
    events = []
    for event, markets in grouped.items():
        parsed = parse_kalshi_event_ticker(event, markets)
        if parsed is None:
            continue
        day, away, home = parsed
        sides = [_Side(c, (str(markets[c].get("yes_sub_title") or ""),)) for c in (away, home)]
        events.append(_KalshiEvent(event, day, sides[0], sides[1], markets))
    return events


def _assign(games: list[_Game], candidates: dict[int, list[tuple[int, int, str]]]) -> dict[int, tuple[int, str]]:
    """One market per game and one game per market: lowest rank first, then game order.

    ``candidates[g]`` holds ``(rank, market_index, matched_by)``; rank 0 is an exact date.
    """
    flat = sorted((rank, g, m, how) for g, opts in candidates.items() for rank, m, how in opts)
    taken_games: set = set()
    taken_markets: set = set()
    out: dict[int, tuple[int, str]] = {}
    for _rank, g, m, how in flat:
        if g in taken_games or m in taken_markets:
            continue
        taken_games.add(g)
        taken_markets.add(m)
        out[g] = (m, how)
    return out


def _how(team_rule: str, date_rule: str, swapped: bool) -> str:
    return "+".join([team_rule, date_rule, *(["swapped"] if swapped else [])])


def join_espn_kalshi(
    espn: pl.DataFrame,
    kalshi: pl.DataFrame,
    league: str,
    *,
    max_date_gap_days: int = 3,
    method: str = "multiplicative",
    return_as_pandas: bool = False,
) -> pl.DataFrame:
    """Join ESPN games to Kalshi game-winner markets, one row per ESPN game.

    Args:
        espn: a parsed ESPN scoreboard (``espn_<league>_scoreboard(dates=...)``).
        kalshi: parsed Kalshi markets of the league's winner series
            (``kalshi_markets(params={"series_ticker": MARKET_LEAGUES[league].kalshi_series, "status": "open"})``,
            every page).
        league: a key of :data:`MARKET_LEAGUES` (``nba``, ``nfl``, ``nhl``).
        max_date_gap_days: the furthest a ticker's scheduled date may sit from the
            game's Eastern date and still match (a postponed game keeps its ticker).
        method: de-vig method for ``home_prob`` / ``away_prob`` (``multiplicative``
            or ``shin``, as in :mod:`sportsdataverse.wexp.market`).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per ESPN game: ``game_id``, ``game_date_et``, ``away_abbreviation``,
        ``home_abbreviation``, ``kalshi_event_ticker``, ``kalshi_scheduled_date``,
        ``kalshi_away_ticker``, ``kalshi_home_ticker``, the four ``*_yes_bid`` /
        ``*_yes_ask`` prices (0 to 1), ``home_prob`` / ``away_prob`` (de-vigged
        bid/ask midpoints), ``prob_note`` (why the probabilities are null) and
        ``matched_by``. A game no market matched keeps its row with null market
        columns.

    Raises:
        ValueError: ``league`` is not in :data:`MARKET_LEAGUES`, or ``espn`` lacks
            the scoreboard columns.

    Example:
        Quick start::

            import polars as pl

            from sportsdataverse.nba import espn_nba_scoreboard
            from sportsdataverse.odds import join_espn_kalshi, kalshi_markets

            games = espn_nba_scoreboard(dates="20261122")
            markets = kalshi_markets(limit="1000", params={"series_ticker": "KXNBAGAME", "status": "open"})
            joined = join_espn_kalshi(games, markets, "nba")

        Pipeline next step (one line)::

            joined.filter(pl.col("matched_by").is_null())

    See Also:
        * `Kalshi API docs`_ -- the keyless market data the prices come from.

    .. _Kalshi API docs: https://docs.kalshi.com/
    """
    cfg = _league(league)
    games = _espn_games(espn)
    events = _kalshi_events(kalshi)
    candidates: dict[int, list[tuple[int, int, str]]] = {}
    for g, game in enumerate(games):
        for m, event in enumerate(events):
            pair = _pair_match(league, "kalshi", game, event.away, event.home)
            if pair is None or game.date_et is None:
                continue
            gap = abs((event.date - game.date_et).days)
            if gap > max_date_gap_days:
                continue
            date_rule = "scheduled_date" if gap == 0 else "nearest_date"
            candidates.setdefault(g, []).append((gap, m, _how(pair[0], date_rule, pair[1])))
    matched = _assign(games, candidates)

    rows = []
    for g, game in enumerate(games):
        row: dict[str, Any] = {
            "game_id": game.game_id,
            "game_date_et": game.date_et,
            "away_abbreviation": game.away.code,
            "home_abbreviation": game.home.code,
        }
        hit = matched.get(g)
        if hit is not None:
            row.update(_kalshi_columns(events[hit[0]], hit[1], cfg, method))
        rows.append(row)
    return _frame(rows, _KALSHI_SCHEMA, espn, return_as_pandas)


def _kalshi_columns(ev: _KalshiEvent, how: str, cfg: MarketLeague, method: str) -> dict[str, Any]:
    """The market columns of one matched game; prices follow the team, so a swapped pair is unswapped."""
    away_side, home_side = (ev.home, ev.away) if how.endswith("+swapped") else (ev.away, ev.home)
    away_m, home_m = ev.markets[away_side.code], ev.markets[home_side.code]
    prices = {
        "away_yes_bid": _num(away_m.get("yes_bid_dollars")),
        "away_yes_ask": _num(away_m.get("yes_ask_dollars")),
        "home_yes_bid": _num(home_m.get("yes_bid_dollars")),
        "home_yes_ask": _num(home_m.get("yes_ask_dollars")),
    }
    probs, note = fair_win_probs(
        _mid(prices["home_yes_bid"], prices["home_yes_ask"]),
        _mid(prices["away_yes_bid"], prices["away_yes_ask"]),
        draw_possible=cfg.draw_possible,
        method=method,
    )
    return {
        "kalshi_event_ticker": ev.ticker,
        "kalshi_scheduled_date": ev.date,
        "kalshi_away_ticker": away_m.get("ticker"),
        "kalshi_home_ticker": home_m.get("ticker"),
        **prices,
        "home_prob": probs[0] if probs else None,
        "away_prob": probs[1] if probs else None,
        "prob_note": note,
        "matched_by": how,
    }


def join_espn_polymarket(
    espn: pl.DataFrame,
    gamma_events: pl.DataFrame,
    league: str,
    *,
    max_start_gap_hours: float = 6.0,
    return_as_pandas: bool = False,
) -> pl.DataFrame:
    """Join ESPN games to Polymarket Gamma game events, one row per ESPN game.

    Args:
        espn: a parsed ESPN scoreboard (``espn_<league>_scoreboard(dates=...)``).
        gamma_events: parsed Gamma events of the league's series
            (``polymarket_gamma_events(params={"series_id": MARKET_LEAGUES[league].gamma_series_id})``).
            Their ``teams`` column is the JSON list Gamma sends.
        league: a key of :data:`MARKET_LEAGUES`.
        max_start_gap_hours: how far Gamma's ``startTime`` may sit from ESPN's start
            when the event date does not match.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per ESPN game: ``game_id``, ``game_date_et``, ``away_abbreviation``,
        ``home_abbreviation``, ``gamma_event_id``, ``gamma_slug``,
        ``polymarket_us_slug`` (Gamma's ``usId`` as the Polymarket US gateway's market
        slug, ``aec-nba-sa-sac-2026-11-06``) and ``matched_by``. A game no event matched keeps its row with nulls.

    Raises:
        ValueError: ``league`` is not in :data:`MARKET_LEAGUES`, or ``espn`` lacks
            the scoreboard columns.

    Example:
        Quick start::

            from sportsdataverse.nfl import espn_nfl_scoreboard
            from sportsdataverse.odds import join_espn_polymarket, polymarket_gamma_events

            games = espn_nfl_scoreboard(dates="20261122")
            events = polymarket_gamma_events(closed="false", limit="100", params={"series_id": "12185"})
            joined = join_espn_polymarket(games, events, "nfl")

    See Also:
        * `Polymarket docs`_ -- the Gamma events API.

    .. _Polymarket docs: https://docs.polymarket.com/
    """
    _league(league)
    games = _espn_games(espn)
    events = _gamma_events(gamma_events)
    candidates: dict[int, list[tuple[int, int, str]]] = {}
    for g, game in enumerate(games):
        for m, ev in enumerate(events):
            pair = _pair_match(league, "gamma", game, ev["away"], ev["home"])
            if pair is None:
                continue
            if ev["date"] is not None and ev["date"] == game.date_et:
                rank, date_rule = 0, "event_date"
            elif (
                ev["start"] is not None
                and game.start is not None
                and abs((ev["start"] - game.start).total_seconds()) <= max_start_gap_hours * 3600
            ):
                rank, date_rule = 1, "start_time"
            else:
                continue
            candidates.setdefault(g, []).append((rank, m, _how(pair[0], date_rule, pair[1])))
    matched = _assign(games, candidates)
    rows = []
    for g, game in enumerate(games):
        row: dict[str, Any] = {
            "game_id": game.game_id,
            "game_date_et": game.date_et,
            "away_abbreviation": game.away.code,
            "home_abbreviation": game.home.code,
        }
        hit = matched.get(g)
        if hit:
            ev = events[hit[0]]
            us = ev["us_id"]
            row.update(
                gamma_event_id=ev["id"],
                gamma_slug=ev["slug"],
                polymarket_us_slug=(us if us.startswith("aec-") else f"aec-{us}") if us else None,
                matched_by=hit[1],
            )
        rows.append(row)
    return _frame(rows, _GAMMA_SCHEMA, espn, return_as_pandas)


#: A Gamma game event's slug: ``{sport}-{away}-{home}-{YYYY-MM-DD}``. The series also holds derivative
#: events on the same teams and date (``...-exact-margin``, ``...-player-props``), which are not the game.
_GAMMA_GAME_SLUG = re.compile(r"^[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$")


def _gamma_events(gamma: pl.DataFrame) -> list[dict]:
    if gamma.is_empty() or "teams" not in gamma.columns:
        return []
    out = []
    for row in gamma.to_dicts():
        if not _GAMMA_GAME_SLUG.match(str(row.get("slug") or "")):
            continue
        teams = row.get("teams")
        if isinstance(teams, str):
            try:
                teams = json.loads(teams)
            except ValueError:
                teams = None
        teams = [t for t in teams or [] if isinstance(t, dict)]
        away = next((t for t in teams if t.get("ordering") == "away"), teams[0] if teams else None)
        home = next((t for t in teams if t.get("ordering") == "home"), teams[1] if len(teams) > 1 else None)
        if not away or not home or away is home:
            continue

        def side(t: dict) -> _Side:
            return _Side(str(t.get("abbreviation") or ""), (str(t.get("name") or ""), str(t.get("alias") or "")))

        event_date = row.get("event_date")
        try:
            day = dt.date.fromisoformat(str(event_date)) if event_date else None
        except ValueError:
            day = None
        out.append(
            {
                "id": None if row.get("id") is None else str(row["id"]),
                "slug": row.get("slug"),
                "us_id": str(row.get("us_id") or "") or None,
                "date": day,
                "start": _utc(row.get("start_time")),
                "away": side(away),
                "home": side(home),
            }
        )
    return out


def _league(league: str) -> MarketLeague:
    try:
        return MARKET_LEAGUES[league]
    except KeyError:
        raise ValueError(f"league {league!r} is not one of {sorted(MARKET_LEAGUES)}") from None


_BASE_SCHEMA = {"game_date_et": pl.Date, "away_abbreviation": pl.String, "home_abbreviation": pl.String}
_KALSHI_SCHEMA = {
    **_BASE_SCHEMA,
    "kalshi_event_ticker": pl.String,
    "kalshi_scheduled_date": pl.Date,
    "kalshi_away_ticker": pl.String,
    "kalshi_home_ticker": pl.String,
    "away_yes_bid": pl.Float64,
    "away_yes_ask": pl.Float64,
    "home_yes_bid": pl.Float64,
    "home_yes_ask": pl.Float64,
    "home_prob": pl.Float64,
    "away_prob": pl.Float64,
    "prob_note": pl.String,
    "matched_by": pl.String,
}
_GAMMA_SCHEMA = {
    **_BASE_SCHEMA,
    "gamma_event_id": pl.String,
    "gamma_slug": pl.String,
    "polymarket_us_slug": pl.String,
    "matched_by": pl.String,
}


def _frame(rows: list[dict], schema: dict[str, Any], espn: pl.DataFrame, as_pandas: bool) -> Any:
    """Rows in the documented schema; ``game_id`` keeps the ESPN frame's dtype, so it joins back."""
    full = {"game_id": espn.schema.get("game_id", pl.Int64), **schema}
    df = pl.DataFrame([{k: r.get(k) for k in full} for r in rows], schema=full, orient="row")
    return df.to_pandas() if as_pandas else df
