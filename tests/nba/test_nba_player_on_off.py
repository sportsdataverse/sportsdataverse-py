"""BB-D5: NBA / WNBA player on/off and WOWY pairs, on committed real captures.

Fixtures (``tests/fixtures/bb_lineup_seasons``, README there; none synthetic): every 2024-25
regular-season game of three NBA teams from the ``nba_stats_*`` releases, stats.nba.com's
``teamplayeronoffsummary`` for those three teams (the oracle), and four 2005 WNBA games from the
two-half era.

The acceptance is the additive identity: a player's on and off possessions add up to his team's,
and so do the four WOWY splits of a pair. The team totals here come from the possessions release
and the play-by-play's final scores, not from the impact rows the builders read. The oracle gates
are the values observed on these teams; never lower one to make a change pass. The
``bb-player-on-off`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

import datetime as dt
import importlib
from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.lineup_seasons import lineup_seasons
from sportsdataverse._common.player_on_off import (
    WOWY_SIDES,
    player_on_off,
    player_on_off_schema,
    player_wowy,
    player_wowy_schema,
)
from sportsdataverse._common.rate_gates import ON_OFF_MIN_N
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_impact import nba_impact_rows
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
from sportsdataverse.nba.nba_player_on_off import nba_player_on_off, nba_player_wowy
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_player_on_off import wnba_player_on_off, wnba_player_wowy

adapter = importlib.import_module("sportsdataverse.nba.nba_player_on_off")

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_lineup_seasons"
#: The three NBA teams whose whole regular season the fixture holds.
NBA_TEAMS = (1610612750, 1610612755, 1610612756)
_TOTALS = ("poss", "opp_poss", "pts", "opp_pts", "minutes")


def _read(name: str, lg: str = "nba", season: int = 2025) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{lg}_{name}_{season}.parquet")


@cache
def _rows(lg: str = "nba", season: int = 2025) -> pl.DataFrame:
    league_id = "00" if lg == "nba" else "10"
    names = ("pbp", "game_lineups", "possessions", "schedule")
    return nba_impact_rows(*[_read(n, lg, season) for n in names], league_id=league_id)


@cache
def _rosters() -> pl.DataFrame:
    """The fixture games' box-score rows (DNPs included) and inactive lists, as the adapter reads them."""
    box = _read("player_boxscores").select("team_id", pl.col("person_id").alias("player_id"), "game_id")
    return pl.concat([box, _read("game_rosters").select("team_id", "player_id", "game_id")])


@cache
def _on_off(level: str | None = None, *, tenure: bool = True, rosters: bool = False) -> pl.DataFrame:
    """The span build (``tenure=True``, the off side over his span) or the whole-season one.

    ``rosters=True`` adds the listed games the adapter reads (its build); without them the span
    runs from his first game on the floor.
    """
    dates = adapter._game_dates(_read("schedule"))
    return player_on_off(
        _rows(),
        player_dtype=pl.Int64(),
        min_poss=400,
        level=level,
        game_dates=dates,
        tenure=tenure,
        rosters=_rosters() if rosters else None,
    )


@cache
def _traded(*, tenure: bool = True, rosters: bool = True) -> pl.DataFrame:
    """The fixture plus ``nba_*_2025_trades``: the three other-team games that bound three traded players' spans.

    ``rosters=True`` (the default) is the adapter's build; ``False`` opens each span at his first game on the floor.
    """
    names = ("pbp", "game_lineups", "possessions", "schedule")
    frames = [pl.concat([_read(n), pl.read_parquet(FIX / f"nba_{n}_2025_trades.parquet")]) for n in names]
    dates = adapter._game_dates(frames[3])
    return player_on_off(
        nba_impact_rows(*frames),
        player_dtype=pl.Int64(),
        min_poss=400,
        level="nba",
        game_dates=dates,
        tenure=tenure,
        rosters=_rosters() if rosters else None,
    )


def _oracle() -> pl.DataFrame:
    """stats.nba.com's on and off rows side by side (``<col>_off`` for the off side)."""
    oracle = _read("on_off_summary")
    on = oracle.filter(court_status="On").drop("court_status")
    off = oracle.filter(court_status="Off").drop("court_status", "gp")
    return on.join(off, on=["team_id", "player_id"], suffix="_off", validate="1:1")


@cache
def _wowy() -> pl.DataFrame:
    return player_wowy(_rows(), player_dtype=pl.Int64())


def _team_totals() -> pl.DataFrame:
    """Each fixture team's season possessions (the possessions release) and points (pbp final scores)."""
    sched = _read("schedule").select(pl.col("game_id").cast(pl.Utf8), "home_team_id", "away_team_id")
    finals = (
        _read("pbp")
        .group_by(pl.col("game_id").cast(pl.Utf8))
        .agg(pl.col("score_home", "score_away").cast(pl.Int64, strict=False).max())
        .join(sched, on="game_id", validate="1:1")
    )
    pts = (
        pl.concat(
            [
                finals.select(pl.col(f"{s}_team_id").alias("team_id"), pl.col(f"score_{s}").cast(pl.Int64).alias("pts"))
                for s in ("home", "away")
            ]
        )
        .group_by("team_id")
        .agg(pl.col("pts").sum().alias("team_pts"))
    )
    poss = (
        _read("possessions")
        .filter(pl.col("game_id").cast(pl.Utf8).is_in(sched["game_id"].implode()))
        .group_by(pl.col("offense_team_id").cast(pl.Int64).alias("team_id"))
        .agg(pl.len().cast(pl.Int64).alias("team_poss"))
    )
    return pts.join(poss, on="team_id", validate="1:1").filter(pl.col("team_id").is_in(NBA_TEAMS))


def test_on_plus_off_is_the_team_season() -> None:
    """The whole-season build (``tenure=False``, college's): on + off is the team's season, against independent totals."""
    out = _on_off(tenure=False).filter(pl.col("team_id").is_in(NBA_TEAMS))
    team = _team_totals()
    assert team.height == 3
    joined = out.join(team, on="team_id", how="left", validate="m:1")
    assert joined.height == 70  # every player who took the floor for the three teams
    assert (joined["team_games"] == 82).all() and (joined["games"] <= 82).all()
    assert (joined["poss_on"] + joined["poss_off"] == joined["team_poss"]).all()
    assert (joined["pts_on"] + joined["pts_off"] == joined["team_pts"]).all()
    for c in _TOTALS:  # one team total per team for every total
        per_team = joined.group_by("team_id").agg((pl.col(f"{c}_on") + pl.col(f"{c}_off")).round(4).n_unique())
        assert (per_team[f"{c}_on"] == 1).all(), c
    assert joined.filter(pl.any_horizontal(pl.col(f"{c}_off") < 0 for c in _TOTALS)).height == 0
    # off is the whole team season, the games he missed included: 82 games, 3,961 to 3,966 minutes
    assert (
        joined.group_by("team_id").agg((pl.col("minutes_on") + pl.col("minutes_off")).first())["minutes_on"].min()
        > 3960
    )


def test_matches_stats_nba_on_off_summary() -> None:
    """stats.nba.com's ``teamplayeronoffsummary`` on the players whose off side spans the team season.

    Read on the whole-season build, so the 26 whose stats.nba.com window is shorter can be found; the
    tenure build's match on them is :func:`test_span_follows_traded_players_like_stats_nba`.
    """
    o = _oracle()
    ours = _on_off(tenure=False)
    assert o.schema["player_id"] == ours.schema["player_id"] and o.schema["team_id"] == ours.schema["team_id"]
    j = o.join(ours, on=["team_id", "player_id"], how="left", validate="1:1")
    assert j.height == 70 and j["poss_on"].null_count() == 0  # every stats.nba.com player is here
    assert (j["games"] - j["gp"]).abs().max() <= 4  # observed 4; median 0
    gate = 400
    # On the floor nothing depends on how the off side is counted: every player over the gate on it.
    on_gate = j.filter(pl.col("poss_on") + pl.col("opp_poss_on") >= gate)
    assert on_gate.height == 56
    d_on = (on_gate["net_rtg_on"] - on_gate["net_rating"]).abs()
    assert d_on.median() <= 0.53 and d_on.max() <= 3.57  # observed 0.521 / 3.561
    assert on_gate.select(pl.corr("net_rtg_on", "net_rating")).item() >= 0.983  # observed 0.9831
    # The clock: our stints against its box minutes (whole minutes), a season apart by 17 at most.
    d_min = on_gate["minutes_on"] - on_gate["min"]
    assert abs(d_min.median()) <= 0.16 and d_min.abs().median() <= 1.45  # observed 0.159 / 1.441
    assert d_min.abs().max() <= 17.1  # observed 17.09
    # stats.nba.com credits a possession to every player on the floor during it, so its ratings run
    # lower on both ends; in the net rating the two offsets cancel.
    assert 2.9 <= (on_gate["off_rtg_on"] - on_gate["off_rating"]).median() <= 3.0  # observed 2.976
    assert 2.9 <= (on_gate["def_rtg_on"] - on_gate["def_rating"]).median() <= 3.0  # observed 2.923
    # stats.nba.com's off-court covers only the games a player was on the roster: on 26 of the 70 its
    # on + off falls short of the team's season (a mid-season trade or signing), never beyond it.
    gap = pl.col("min") + pl.col("min_off") - pl.col("minutes_on") - pl.col("minutes_off")
    tenure = gap.abs() <= 3
    short = j.filter(~tenure)
    assert short.height == 26 and short.select(gap.max()).item() < -1500  # observed -1541 to -3913
    assert (short["games"] < short["team_games"]).all()
    full = on_gate.filter(tenure & (pl.col("poss_off") + pl.col("opp_poss_off") >= gate))
    assert full.height == 41
    d_off = (full["net_rtg_off"] - full["net_rating_off"]).abs()
    d_diff = (full["net_rtg_diff"] - (full["net_rating"] - full["net_rating_off"])).abs()
    assert d_off.median() <= 0.28 and d_off.max() <= 1.70  # observed 0.270 / 1.695
    assert d_diff.median() <= 0.69 and d_diff.max() <= 3.96  # observed 0.684 / 3.958
    on_off_gap = pl.col("net_rating") - pl.col("net_rating_off")
    assert full.select(pl.corr(pl.col("net_rtg_diff"), on_off_gap)).item() >= 0.961  # observed 0.9611
    # The build the NBA / WNBA callers get (tenure) on the same 41, and on all 56 gated both ways.
    served = o.join(_traded(), on=["team_id", "player_id"], how="left", validate="1:1")
    same = served.join(full.select("team_id", "player_id"), on=["team_id", "player_id"], how="semi")
    assert same.height == 41
    # Here stats.nba.com's window is the season and the listed games make the span the season too, so
    # these are the whole-season numbers (0.813 / 2.998 / 0.9642 when a late debut opened the span).
    t_diff = (same["net_rtg_diff"] - (same["net_rating"] - same["net_rating_off"])).abs()
    assert t_diff.median() <= 0.69 and t_diff.max() <= 3.96  # observed 0.684 / 3.958
    assert same.select(pl.corr(pl.col("net_rtg_diff"), on_off_gap)).item() >= 0.961  # observed 0.9611
    both = served.filter(
        pl.col("poss_on") + pl.col("opp_poss_on") >= gate, pl.col("poss_off") + pl.col("opp_poss_off") >= gate
    )
    assert both.height == 56
    # 0.9626 and 1.051 / 4.528 from game dates alone
    assert both.select(pl.corr(pl.col("net_rtg_diff"), on_off_gap)).item() >= 0.968  # observed 0.9681
    b_diff = (both["net_rtg_diff"] - (both["net_rating"] - both["net_rating_off"])).abs()
    assert b_diff.median() <= 0.81 and b_diff.max() <= 3.96  # observed 0.801 / 3.958


def test_span_off_is_the_team_over_his_span() -> None:
    """The off side (``*_off``, repeated in ``*_off_span``): the team's games in his span less his stints."""
    out = _on_off().filter(pl.col("team_id").is_in(NBA_TEAMS))
    season = _on_off(tenure=False).filter(pl.col("team_id").is_in(NBA_TEAMS))
    assert out.height == 70 and out["team_games_span"].null_count() == 0
    dates = adapter._game_dates(_read("schedule"))
    poss = (
        _read("possessions")
        .with_columns(pl.col("game_id").cast(pl.Utf8))
        .join(dates, on="game_id", validate="m:1")
        .group_by(pl.col("offense_team_id").cast(pl.Int64).alias("team_id"), "game_date")
        .agg(pl.len().cast(pl.Int64).alias("n"))
    )
    span = (
        out.select("team_id", "player_id", "span_first_date", "span_last_date")
        .join(poss, on="team_id")
        .filter(pl.col("game_date") >= pl.col("span_first_date"), pl.col("game_date") <= pl.col("span_last_date"))
        .group_by("team_id", "player_id")
        .agg(pl.col("n").sum().alias("span_poss"), pl.len().alias("span_games"))
    )
    j = out.join(span, on=["team_id", "player_id"], validate="1:1")
    assert j.height == 70 and (j["poss_on"] + j["poss_off"] == j["span_poss"]).all()
    assert (j["span_games"] == j["team_games_span"]).all() and (j["games"] <= j["team_games_span"]).all()
    s = out.join(season, on=["team_id", "player_id"], suffix="_season", validate="1:1")
    for c in _TOTALS:
        assert (s[f"{c}_off_span"] == s[f"{c}_off"]).all(), c  # the span columns repeat the off side
        assert (s[f"{c}_off"] <= s[f"{c}_off_season"] + 1e-6).all() and (s[f"{c}_off"] >= 0).all(), c
    assert (s["net_rtg_diff_span"] == s["net_rtg_diff"]).all()
    # one team, on the floor in its first game: the span is the season (28 of the 70 here)
    whole = s.filter(pl.col("team_games_span") == pl.col("team_games"))
    assert whole.height == 28
    assert all((whole[f"{c}_off"] == whole[f"{c}_off_season"]).all() for c in ("poss", "opp_poss", "pts", "opp_pts"))
    assert (whole["net_rtg_diff"] == whole["net_rtg_diff_season"]).all()
    # the other 42 are off over fewer games than the whole season
    assert s.filter(pl.col("poss_off") < pl.col("poss_off_season")).height == 42
    # a signing starts at his debut: PHI's 1630600 played only the season finale
    late = out.filter(player_id=1630600).row(0, named=True)
    assert late["team_games_span"] == 1 and late["span_first_date"] == late["span_last_date"]


def test_span_follows_traded_players_like_stats_nba() -> None:
    """The gate: on three real 2024-25 trades the span's off side tracks stats.nba.com's roster-limited one."""
    season = _traded(tenure=False).select(
        "team_id",
        "player_id",
        pl.col("net_rtg_off").alias("net_rtg_off_season"),
        pl.col("minutes_off").alias("minutes_off_season"),
    )
    j = _oracle().join(_traded(), on=["team_id", "player_id"], how="left", validate="1:1")
    j = j.join(season, on=["team_id", "player_id"], how="left", validate="1:1")
    row = {r["player_id"]: r for r in j.to_dicts()}
    # Quentin Grimes, Dallas to Philadelphia: the span opens the day after his last Dallas game
    # (2025-02-02), at PHI's next game; stats.nba.com's off-court minutes are 694.
    g = row[1629656]
    assert g["span_first_date"] == dt.date(2025, 2, 4) and g["team_games_span"] == 34 and g["games"] == 28
    assert abs(g["minutes_off"] - g["min_off"]) <= 1  # observed 693.75 v 694
    assert abs(g["net_rtg_off"] - g["net_rating_off"]) <= 0.13  # observed 0.123; the season's is 1.00
    # Jusuf Nurkic and Josh Okogie, Phoenix to Charlotte: it closes at their last game listed for
    # Phoenix (2025-02-05, 2025-01-14), days before their first Charlotte game (2025-02-12,
    # 2025-01-20); Okogie's opens on opening night, where he was listed, not at his 2024-11-08 debut.
    n, o = row[203994], row[1629006]
    assert n["span_last_date"] == dt.date(2025, 2, 5) and o["span_last_date"] == dt.date(2025, 1, 14)
    assert o["span_first_date"] == dt.date(2024, 10, 23)
    for r in (g, n, o):
        assert abs(r["minutes_off"] - r["min_off"]) <= 16.1  # observed 0.25 / 6.84 / 16.09
        assert abs(r["net_rtg_off"] - r["net_rating_off"]) < abs(r["net_rtg_off_season"] - r["net_rating_off"])
    # From game dates alone the two spans run to the day before the Charlotte debut, and Okogie's
    # opens at his debut: 156 and 277 off-court minutes from stats.nba.com's.
    dated = {r["player_id"]: r for r in _traded(rosters=False).to_dicts() if r["team_id"] == 1610612756}
    assert dated[203994]["span_last_date"] == dt.date(2025, 2, 11) and dated[1629006]["span_last_date"] == dt.date(
        2025, 1, 18
    )
    assert abs(dated[1629006]["minutes_off"] - o["min_off"]) > 270  # observed 276.9
    # The bound reads every game of the season: without his last Dallas game the fixture's latest
    # Dallas game of his is 2024-10-25 (against Minnesota), and the span opens the next day.
    alone = _on_off().filter(player_id=1629656).row(0, named=True)
    assert alone["span_first_date"] == dt.date(2024, 10, 26)
    # Across the 70: on the 26 whose stats.nba.com off side is short of the team's season (traded,
    # signed or waived), the span cuts the median off-court net rating miss from 2.96 (the whole
    # season) to 0.59; on the 44 whose is the whole season it is the whole season's 0.261, since the
    # listed games (DNPs, inactives) open a late debut's span on opening night. From game dates alone
    # (``rosters=False``) the span opens at the debut and closes only at a later team's first game:
    # 1.27 on the 26 and 0.40 on the 44 (BB-D5's late-debut gap, sportsdataverse-py #1218).
    gap = (pl.col("min") + pl.col("min_off") - pl.col("minutes_on") - pl.col("minutes_off_season")).abs() > 3
    miss = {
        s or "tenure": (pl.col(f"net_rtg_off{s}") - pl.col("net_rating_off")).abs().median() for s in ("", "_season")
    }
    short = j.filter(gap).select(**miss).row(0, named=True)
    full = j.filter(~gap).select(**miss).row(0, named=True)
    assert j.filter(gap).height == 26 and short["tenure"] <= 0.59 and short["_season"] >= 2.95  # observed 0.587 / 2.960
    assert full["_season"] <= 0.27 and full["tenure"] <= 0.27  # observed 0.261 / 0.261
    # off-court minutes: a median 2.9 from stats.nba.com's over the 70 (128.4 from game dates alone)
    assert (j["minutes_off"] - j["min_off"]).abs().median() <= 3.0  # observed 2.90
    j0 = (
        _oracle()
        .join(_traded(rosters=False), on=["team_id", "player_id"], validate="1:1")
        .join(season, on=["team_id", "player_id"], validate="1:1")
    )
    short0 = j0.filter(gap).select(**miss).row(0, named=True)
    full0 = j0.filter(~gap).select(**miss).row(0, named=True)
    assert short0["tenure"] >= 1.26 and full0["tenure"] >= 0.39  # observed 1.265 / 0.399
    assert (j0["minutes_off"] - j0["min_off"]).abs().median() >= 128  # observed 128.4


def test_inactive_lists_alone_keep_the_open_end() -> None:
    """Every fixture game has an inactive list, but it never names a DNP-CD: no span may close on it."""
    dates = adapter._game_dates(_read("schedule"))
    inactive = _read("game_rosters").select("team_id", "player_id", "game_id")
    out = player_on_off(_rows(), player_dtype=pl.Int64(), min_poss=400, game_dates=dates, tenure=True, rosters=inactive)
    j = out.join(_on_off(), on=["team_id", "player_id"], validate="1:1", suffix="_d")
    # a player listed for a later team closes there, rightly (28 here); the rest must keep the dates-only end
    box = _read("player_boxscores").select(pl.col("person_id").alias("player_id"), "team_id")
    teams = pl.concat([inactive.select("player_id", "team_id"), box]).unique().group_by("player_id").len()
    one = j.join(teams.filter(pl.col("len") == 1), on="player_id", how="semi")
    assert one.height == 448
    # 80 close early when any listed row makes a game's list whole
    assert one.filter(pl.col("span_last_date") < pl.col("span_last_date_d")).height == 0


def test_span_columns_are_null_without_dates() -> None:
    out = player_on_off(_rows(), player_dtype=pl.Int64(), min_poss=400)
    span = [c for c in player_on_off_schema(pl.Int64()) if "span" in c]
    assert len(span) == 3 + 9 + 3
    assert out.select(pl.col(span).null_count()).row(0) == (out.height,) * len(span)
    assert out.drop(span).equals(_on_off(tenure=False).drop(span))  # the dates move nothing else
    # without dates there is no span, so the tenure build keeps the whole season
    assert player_on_off(_rows(), player_dtype=pl.Int64(), min_poss=400, tenure=True).equals(out)


def test_an_undated_game_keeps_its_team_on_the_whole_season() -> None:
    """With one game undated its team cannot place it in a span, so on and off stay on the same games."""
    dates = adapter._game_dates(_read("schedule"))
    team = NBA_TEAMS[0]
    sides = _rows().filter((pl.col("home_team_id") == team) | (pl.col("away_team_id") == team))
    gone = sides["game_id"].cast(pl.Utf8).unique().sort()[0]
    out = player_on_off(
        _rows(),
        player_dtype=pl.Int64(),
        min_poss=400,
        game_dates=dates.filter(pl.col("game_id") != gone),
        tenure=True,
    )
    season, tenure = _on_off(tenure=False), _on_off()
    cols = [f"{c}_off" for c in ("poss", "opp_poss", "pts", "opp_pts")]
    same = out.filter(team_id=team).select("player_id", *cols).sort("player_id")
    assert same.equals(season.filter(team_id=team).select("player_id", *cols).sort("player_id"))
    other = NBA_TEAMS[1]
    assert out.filter(team_id=other).select(cols).equals(tenure.filter(team_id=other).select(cols))


def test_wowy_splits_add_up_to_the_team() -> None:
    w = _wowy().filter(pl.col("team_id").is_in(NBA_TEAMS))
    team = _team_totals()
    j = w.join(team, on="team_id", validate="m:1")
    assert j.select((sum(pl.col(f"poss_{s}") for s in WOWY_SIDES) == pl.col("team_poss")).all()).item()
    assert j.select((sum(pl.col(f"pts_{s}") for s in WOWY_SIDES) == pl.col("team_pts")).all()).item()
    assert j.filter(pl.any_horizontal(pl.col(f"{c}_{s}") < 0 for c in _TOTALS for s in WOWY_SIDES)).height == 0
    # every pair of each team's players, the smaller id first: 20, 30 and 20 players
    assert w.height == 190 + 435 + 190 and (w["player_id_a"] < w["player_id_b"]).all()
    # 255 of them never shared the floor: zeros together, not nulls
    assert w.filter(pl.col("poss_both") == 0).height == 255
    assert w.select(pl.sum_horizontal(pl.col(f"{c}_{s}").null_count() for c in _TOTALS for s in WOWY_SIDES)).item() == 0
    # a rating with no possessions under it is null, never NaN or inf: 255 pairs never together
    ratings = [f"{r}_{s}" for r in ("off_rtg", "def_rtg", "net_rtg") for s in WOWY_SIDES]
    assert w.select(pl.sum_horizontal(pl.col(ratings).is_nan().sum())).item() == 0
    assert w.select(((pl.col("poss_both") == 0) == pl.col("off_rtg_both").is_null()).all()).item()
    # A's together + A alone is A's on-court season
    on = _on_off().select("team_id", pl.col("player_id").alias("player_id_a"), "poss_on", "pts_on")
    a = w.join(on, on=["team_id", "player_id_a"], validate="m:1")
    assert (a["poss_both"] + a["poss_a_only"] == a["poss_on"]).all()
    assert (a["pts_both"] + a["pts_a_only"] == a["pts_on"]).all()


def test_wowy_together_reuses_the_lineup_seasons_pairs() -> None:
    """A pair's possessions together are BB-D4's two-man unit plus the stints with an unknown fifth."""
    w = _wowy().filter(pl.col("team_id").is_in(NBA_TEAMS))
    pairs = lineup_seasons(
        _rows(), nba_lineup_count_rows(_read("possessions"), _read("schedule")), player_dtype=pl.Int64(), min_poss=400
    ).filter((pl.col("n_players") == 2) & pl.col("team_id").is_in(NBA_TEAMS))
    j = pairs.join(
        w, left_on=["team_id", "player_1", "player_2"], right_on=["team_id", "player_id_a", "player_id_b"], how="left"
    )
    assert j["poss_both"].null_count() == 0  # every two-man unit is a WOWY pair
    assert (j["poss_both"] >= j["poss"]).all()
    # equal unless the pair shared a stint with a gap in the source lineup: 533 of 560 pairs here
    assert j.filter(pl.col("poss_both") == pl.col("poss")).height == 533


def test_a_game_without_possessions_adds_nothing() -> None:
    """Its points and minutes would otherwise land on players with no possessions for them."""
    pbp, lineups = _read("pbp"), _read("game_lineups")
    poss, schedule = _read("possessions"), _read("schedule")
    no_poss = schedule["game_id"].sort().to_list()[1]
    assert poss.filter(pl.col("game_id") == no_poss).height > 0
    gapped = nba_impact_rows(pbp, lineups, poss.filter(pl.col("game_id") != no_poss), schedule)
    clean = nba_impact_rows(pbp, lineups, poss, schedule.filter(pl.col("game_id") != no_poss))
    assert player_on_off(gapped, player_dtype=pl.Int64(), min_poss=400).equals(
        player_on_off(clean, player_dtype=pl.Int64(), min_poss=400)
    )
    assert player_wowy(gapped, player_dtype=pl.Int64()).equals(player_wowy(clean, player_dtype=pl.Int64()))


def test_ranks_follow_the_gate_and_direction() -> None:
    out = _on_off()
    ranked = out.filter(pl.col("net_rtg_diff_rank").is_not_null())
    both = pl.min_horizontal(pl.col("poss_on") + pl.col("opp_poss_on"), pl.col("poss_off") + pl.col("opp_poss_off"))
    assert ranked.height == out.filter(both >= 400).height and ranked.height > 0
    best_def = ranked.sort("def_rtg_diff_rank").row(0, named=True)
    assert best_def["def_rtg_diff"] == ranked["def_rtg_diff"].min()  # lower is better on defense
    best_def_on = ranked.sort("def_rtg_on_rank").row(0, named=True)
    assert best_def_on["def_rtg_on"] == ranked["def_rtg_on"].min()
    best_net = ranked.sort("net_rtg_diff_rank").row(0, named=True)
    assert best_net["net_rtg_diff"] == ranked["net_rtg_diff"].max()
    assert out.select((pl.col("net_rtg_diff_n") == pl.min_horizontal("net_rtg_on_n", "net_rtg_off_n")).all()).item()
    assert (out["net_rtg_on_n"] == out["poss_on"] + out["opp_poss_on"]).all()
    assert (out["def_rtg_on_n"] == out["opp_poss_on"]).all() and (out["off_rtg_off_n"] == out["poss_off"]).all()


def test_wnba_half_seasons_count_the_whole_game() -> None:
    """2005 WNBA games (two 20-minute halves): on + off minutes are the games' lengths."""
    rows = _rows("wnba", 2005)
    out = player_on_off(rows, player_dtype=pl.Int64(), min_poss=200)
    team = out.group_by("team_id").agg((pl.col("minutes_on") + pl.col("minutes_off")).round(4).unique())
    assert team["minutes_on"].list.len().max() == 1
    # two 20-minute halves, then 5-minute overtimes (periods 3+): three regulation games and one 2OT
    periods = _read("pbp", "wnba", 2005).group_by(pl.col("game_id").cast(pl.Utf8)).agg(pl.col("period").max())
    games = _read("schedule", "wnba", 2005).with_columns(pl.col("game_id").cast(pl.Utf8)).join(periods, on="game_id")
    lengths: dict[int, float] = {}
    for g in games.to_dicts():
        for t in (g["home_team_id"], g["away_team_id"]):
            lengths[t] = lengths.get(t, 0.0) + 40.0 + 5.0 * (g["period"] - 2)
    assert sorted(set(games["period"])) == [2, 4]
    got = {r["team_id"]: r["minutes_on"][0] for r in team.to_dicts()}
    assert got == pytest.approx(lengths)


def test_public_builders_read_the_start_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(lg: str, name: str, want: int):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [want], f"{name}: called with {seasons}"
            return _read(name, lg)

        return load

    names = {"pbp": "pbp", "game_lineups": "game_lineups", "possessions": "possessions", "schedules": "schedule"}
    for loader, name in names.items():
        monkeypatch.setattr(nba_loaders, f"load_nba_stats_{loader}", fixture("nba", name, 2024))
        monkeypatch.setattr(wnba_loaders, f"load_wnba_stats_{loader}", fixture("wnba", name, 2025))
    for loader in ("player_boxscores", "game_rosters"):  # the listed games the span opens on
        monkeypatch.setattr(nba_loaders, f"load_nba_stats_{loader}", fixture("nba", loader, 2024))
    # the WNBA box release starts at 2026, so 2025 reads only the inactive lists (none here)
    monkeypatch.setattr(wnba_loaders, "load_wnba_stats_game_rosters", lambda seasons: pl.DataFrame())
    seen: list[object] = []
    real = adapter.player_on_off

    def spy(*args: object, **kwargs: object) -> pl.DataFrame:
        seen.append(kwargs["level"])
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter, "player_on_off", spy)
    assert nba_player_on_off(2025).equals(_on_off(level="nba", rosters=True))
    assert not _on_off(level="nba", rosters=True).equals(_on_off(level="nba"))  # the listed games move spans
    assert not _on_off(level="nba").equals(_on_off())  # the adapter's level gate changes ranks
    assert nba_player_wowy(2025).equals(_wowy())
    assert nba_player_on_off(2025, season_type="preseason").height == 0  # the fixture has no preseason game
    assert set(seen) == {"nba"}
    seen.clear()
    wnba = wnba_player_on_off(2025)
    assert seen == ["wnba"]  # each adapter passes its own level: a stricter one would pass the null checks below
    ranked = wnba.filter(pl.col("net_rtg_on_rank").is_not_null())
    assert ranked.filter(pl.col("poss_off") + pl.col("opp_poss_off") < 200).height == 0  # the WNBA floor is 200
    thin = wnba.filter(pl.col("net_rtg_on_n") < ON_OFF_MIN_N["wnba"]["net_rtg_on"])
    assert thin.height > 0 and thin["net_rtg_on_rank"].null_count() == thin.height  # the WNBA sample gate
    assert wnba_player_wowy(2025).height > 0


def test_empty_input_and_bad_args() -> None:
    empty_rows = nba_impact_rows(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    on_off = player_on_off(empty_rows, player_dtype=pl.Int64(), min_poss=1)
    assert on_off.height == 0 and on_off.schema == pl.Schema(player_on_off_schema(pl.Int64()))
    wowy = player_wowy(empty_rows, player_dtype=pl.Int64())
    assert wowy.height == 0 and wowy.schema == pl.Schema(player_wowy_schema(pl.Int64()))
    with pytest.raises(ValueError, match="league_id"):
        nba_player_on_off(2025, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        nba_player_wowy(2025, season_type="Regular Season")
