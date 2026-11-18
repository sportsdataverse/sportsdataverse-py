"""ESPN -> Kalshi / Polymarket join, replayed on markets recorded 2026-11-18 (tests/fixtures/market_join)."""

import datetime as dt
import json
import pathlib

import polars as pl
import pytest

from sportsdataverse._common_espn_parsers import parse_scoreboard
from sportsdataverse.odds import join_espn_kalshi, join_espn_polymarket, parse_kalshi_event_ticker
from sportsdataverse.odds.kalshi_parsers import parse_kalshi
from sportsdataverse.odds.polymarket_parsers import parse_polymarket

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "market_join"
LEAGUES = ["nba", "nfl", "nhl"]


def _load(league, name):
    return json.loads((FIXTURES / league / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module", params=LEAGUES)
def recorded(request):
    league = request.param
    espn = parse_scoreboard(_load(league, "espn_scoreboard"))
    kalshi = parse_kalshi(_load(league, "kalshi_markets"))
    gamma = parse_polymarket(_load(league, "gamma_events"))
    return league, espn, kalshi, gamma


def _ticker_date(kalshi, ticker):
    codes = [m.rsplit("-", 1)[1] for m in kalshi["ticker"].to_list() if m.startswith(f"{ticker}-")]
    parsed = parse_kalshi_event_ticker(ticker, codes)
    return parsed[0] if parsed else None


def _kalshi_dates(kalshi):
    days = set()
    for t in kalshi["event_ticker"].unique().to_list():
        parsed = parse_kalshi_event_ticker(
            t, [m.rsplit("-", 1)[1] for m in kalshi["ticker"].to_list() if m.startswith(t)]
        )
        if parsed:
            days.add(parsed[0])
    return days


def test_kalshi_join_matches_every_game_kalshi_lists(recorded):
    """Each ESPN game on a day Kalshi lists tickets for is matched, and only once each way."""
    league, espn, kalshi, _ = recorded
    out = join_espn_kalshi(espn, kalshi, league)
    assert out.height == espn.height
    assert out["game_id"].n_unique() == out.height  # no ESPN game matches two markets
    hit = out.filter(pl.col("matched_by").is_not_null())
    assert hit["kalshi_event_ticker"].n_unique() == hit.height  # no market serves two games
    listed = out.filter(pl.col("game_date_et").is_in(sorted(_kalshi_dates(kalshi))))
    # ponytail: the last listed day can be partly listed; every earlier day must be complete.
    full = listed.filter(pl.col("game_date_et") < listed["game_date_et"].max())
    assert full.height > 0 and full["matched_by"].null_count() == 0
    assert listed["matched_by"].is_not_null().mean() >= 0.85
    # Market side: every Kalshi game ticker dated inside the ESPN week found its game.
    week = set(out["game_date_et"].to_list())
    in_week = {t for t in kalshi["event_ticker"].unique().to_list() if _ticker_date(kalshi, t) in week}
    assert in_week and in_week <= set(hit["kalshi_event_ticker"].to_list())


def test_kalshi_join_reads_prices_from_the_right_team(recorded):
    league, espn, kalshi, _ = recorded
    out = join_espn_kalshi(espn, kalshi, league).filter(pl.col("matched_by").is_not_null())
    for r in out.iter_rows(named=True):
        assert r["kalshi_home_ticker"].startswith(r["kalshi_event_ticker"])
        if r["home_prob"] is not None:
            assert abs(r["home_prob"] + r["away_prob"] - 1) < 1e-9
            assert 0 < r["home_prob"] < 1


def test_gamma_join_matches_every_game(recorded):
    league, espn, _, gamma = recorded
    out = join_espn_polymarket(espn, gamma, league)
    assert out["game_id"].n_unique() == out.height
    hit = out.filter(pl.col("matched_by").is_not_null())
    assert hit["gamma_slug"].n_unique() == hit.height
    assert hit.height == espn.height  # Gamma lists the whole recorded week in all three leagues
    # Every matched event Gamma gave a usId carries its US slug (.all() alone would skip nulls);
    # the recorded NHL series carries no usId at all.
    with_us = gamma.filter(pl.col("us_id").is_not_null())["slug"].to_list() if "us_id" in gamma.columns else []
    has_us = hit.filter(pl.col("gamma_slug").is_in(with_us))
    assert has_us["polymarket_us_slug"].null_count() == 0
    assert has_us["polymarket_us_slug"].str.starts_with("aec-").all()
    assert (has_us.height > 0) == (league != "nhl")
    # Only game events: the series' -exact-margin / -player-props events share the teams and date.
    assert hit["gamma_slug"].str.contains(r"^[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$").all()


def test_gamma_join_does_not_depend_on_event_order(recorded):
    league, espn, _, gamma = recorded
    forward = join_espn_polymarket(espn, gamma, league)
    backward = join_espn_polymarket(espn, gamma.reverse(), league)
    assert forward.equals(backward)


def test_aliases_are_what_joins_the_differing_codes():
    espn = parse_scoreboard(_load("nba", "espn_scoreboard"))
    out = join_espn_kalshi(espn, parse_kalshi(_load("nba", "kalshi_markets")), "nba")
    gs = out.filter((pl.col("home_abbreviation") == "GS") | (pl.col("away_abbreviation") == "GS"))
    gs = gs.filter(pl.col("matched_by").is_not_null())
    assert gs.height > 0 and gs["matched_by"].str.starts_with("alias+").all()
    assert gs["kalshi_event_ticker"].str.contains("GSW").all()


def test_neutral_site_order_is_matched_swapped():
    """A venue that lists the teams in the other order still joins, and says so."""
    espn = parse_scoreboard(_load("nfl", "espn_scoreboard"))
    flipped = espn.rename({c: c.replace("home_", "__tmp_") for c in espn.columns if c.startswith("home_")})
    flipped = flipped.rename({c: c.replace("away_", "home_") for c in flipped.columns if c.startswith("away_")})
    flipped = flipped.rename({c: c.replace("__tmp_", "away_") for c in flipped.columns if c.startswith("__tmp_")})
    out = join_espn_kalshi(flipped, parse_kalshi(_load("nfl", "kalshi_markets")), "nfl")
    assert out["matched_by"].null_count() == 0
    assert out["matched_by"].str.ends_with("+swapped").all()
    # the prices follow the team, not the listed side
    straight = join_espn_kalshi(espn, parse_kalshi(_load("nfl", "kalshi_markets")), "nfl")
    assert out["home_yes_bid"].to_list() == straight["away_yes_bid"].to_list()


def test_postponed_game_falls_back_to_the_nearest_unclaimed_ticker():
    espn = parse_scoreboard(_load("nfl", "espn_scoreboard"))
    kalshi = parse_kalshi(_load("nfl", "kalshi_markets"))
    moved = espn.head(1).with_columns(
        (pl.col("date").str.to_datetime(time_zone="UTC") + pl.duration(days=2)).dt.strftime("%Y-%m-%dT%H:%MZ")
    )
    out = join_espn_kalshi(moved, kalshi, "nfl")
    assert out["matched_by"].item().endswith("nearest_date")
    assert join_espn_kalshi(moved, kalshi, "nfl", max_date_gap_days=1)["matched_by"].item() is None


def test_kalshi_event_ticker_split():
    assert parse_kalshi_event_ticker("KXNBAGAME-26NOV22BOSGSW", ["GSW", "BOS"]) == (dt.date(2026, 11, 22), "BOS", "GSW")
    assert parse_kalshi_event_ticker("KXNFLGAME-26NOV29NELAC", ["NE", "LAC"]) == (dt.date(2026, 11, 29), "NE", "LAC")
    assert parse_kalshi_event_ticker("KXNBAGAME-26NOV22BOSGSW", ["BOS"]) is None
    assert parse_kalshi_event_ticker("not-a-ticker", ["BOS", "GSW"]) is None


def test_unknown_league_and_unparsed_frame_raise():
    espn = parse_scoreboard(_load("nba", "espn_scoreboard"))
    with pytest.raises(ValueError, match="not one of"):
        join_espn_kalshi(espn, pl.DataFrame(), "epl")
    with pytest.raises(ValueError, match="lacks"):
        join_espn_kalshi(pl.DataFrame({"game_id": [1]}), pl.DataFrame(), "nba")


def test_no_markets_keeps_every_game_with_null_market_columns():
    espn = parse_scoreboard(_load("nhl", "espn_scoreboard"))
    out = join_espn_kalshi(espn, pl.DataFrame(), "nhl")
    assert out.height == espn.height and out["matched_by"].null_count() == out.height
    assert out.schema["game_id"] == espn.schema["game_id"]


def test_a_day_with_no_games_returns_the_documented_empty_frame():
    """parse_scoreboard turns an empty ``events`` list into a zero-column frame; the join must not raise on it."""
    empty = parse_scoreboard({"events": []})
    kalshi = parse_kalshi(_load("nba", "kalshi_markets"))
    out = join_espn_kalshi(empty, kalshi, "nba")
    assert out.height == 0 and out.schema["game_id"] == pl.Int64 and "matched_by" in out.columns
    out = join_espn_polymarket(empty, parse_polymarket(_load("nba", "gamma_events")), "nba")
    assert out.height == 0 and "polymarket_us_slug" in out.columns
