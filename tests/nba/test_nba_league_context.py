"""BB-D2: league ranks, percentiles, cohort percentiles, ``_n`` and league averages on real fixtures.

Fixtures (all committed captures, none synthetic):

* ``nba_prediction/{,wnba_}results_2024`` + ``{,wnba_}team_box_2024`` -> ``nba_team_ratings`` 2024
  (32 NBA rows: 30 teams + the two one-game ESPN All-Star teams; 14 WNBA rows: 12 + 2).
* ``nba_prediction/team_ratings_oracle_2024`` -- stats.nba.com NET/DEF ratings, 30 teams.
* ``crosswalk_basketball/nba_team_crosswalk_2026`` -- ``nba_conference`` per ESPN team id.
* ``mbb_player_value/player_seasons_2025`` -- 9,805 college player-seasons with ``position``
  (PF 7 rows, SF 10: one cohort under the ten-player floor, one exactly on it).

The three mutation rows in ``tests/mutants/registry.yaml`` (group ``bb-league-context``) go red
here: the floor, the lower-is-better direction and the league-average row's qualifier.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.league_context import (
    LEAGUE_AVERAGES_SCHEMA,
    MIN_COHORT_PLAYERS,
    MIN_COHORT_TEAMS,
    attach_cohort_percentiles,
    attach_ranks,
    league_averages,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
PRED = FIXTURES / "nba_prediction"


def _ratings(monkeypatch: pytest.MonkeyPatch, league_id: str = "00") -> pl.DataFrame:
    pre = "" if league_id == "00" else "wnba_"
    results = pl.read_parquet(PRED / f"{pre}results_2024.parquet")
    team_box = pl.read_parquet(PRED / f"{pre}team_box_2024.parquet")
    mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")
    # patch the loader lookup itself, so the WNBA run keeps the WNBA constants
    monkeypatch.setattr(mod, "_league_loaders", lambda lg: (lambda s: results, lambda s: team_box))
    return mod.nba_team_ratings(2024, league_id=league_id)


@pytest.mark.parametrize(("league_id", "teams", "rows"), [("00", 30, 32), ("10", 12, 14)])
def test_team_ratings_rank_lower_is_better_first(monkeypatch, league_id, teams, rows) -> None:
    """The lowest DRtg ranks 1 with the top percentile; the one-game All-Star rows are unranked."""
    out = _ratings(monkeypatch, league_id)
    assert out.height == rows
    ranked = out.filter(pl.col("adj_def_rtg_rank").is_not_null())
    assert ranked.height == teams
    assert out.filter(pl.col("games") == 1)["adj_def_rtg_rank"].null_count() == rows - teams

    by_drtg = ranked.sort("adj_def_rtg")
    assert by_drtg["adj_def_rtg_rank"].to_list() == [float(i) for i in range(1, teams + 1)]
    assert by_drtg["adj_def_rtg_pct"][0] == pytest.approx(100 * teams / (teams + 1))
    assert by_drtg["adj_def_rtg_pct"][-1] == pytest.approx(100 / (teams + 1))
    # higher-is-better runs the other way
    by_ortg = ranked.sort("adj_off_rtg", descending=True)
    assert by_ortg["adj_off_rtg_rank"][0] == 1.0
    # percentile is non-decreasing in the good direction
    assert by_drtg["adj_def_rtg_pct"].is_sorted(descending=True)


def test_team_ratings_sample_sizes(monkeypatch) -> None:
    """``_n`` is the sample of the rate: summed box possessions for ratings, games for pace."""
    out = _ratings(monkeypatch)
    assert out["adj_pace_n"].to_list() == out["games"].to_list()
    assert out["raw_pace_n"].to_list() == out["games"].to_list()
    assert (out["adj_off_rtg_n"] == out["adj_def_rtg_n"]).all()
    # ~96-100 possessions a game for the 30 ranked 2023-24 teams (All-Star games run faster)
    ranked = out.filter(pl.col("adj_off_rtg_rank").is_not_null())
    per_game = (ranked["adj_off_rtg_n"] / ranked["games"]).to_list()
    assert all(90 < p < 110 for p in per_game)
    assert out.schema["adj_off_rtg_n"] == pl.Int64


def test_team_ratings_league_average_row(monkeypatch) -> None:
    """The league-average row's ``n`` is the ``_pct`` denominator (qualified teams only)."""
    mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")
    out = _ratings(monkeypatch)
    avg = league_averages(
        out,
        ["adj_def_rtg", "adj_net_rtg"],
        level="nba",
        entity="team",
        category="team_ratings",
        qualifier=mod._RATINGS_QUALIFIER,
    )
    assert dict(avg.schema) == LEAGUE_AVERAGES_SCHEMA
    row = avg.filter(pl.col("metric") == "adj_def_rtg").row(0, named=True)
    assert row["n"] == 30
    n = row["n"]
    assert out["adj_def_rtg_pct"].max() == pytest.approx(100 * n / (n + 1))
    q = out.filter(pl.col("adj_def_rtg_rank").is_not_null())["adj_def_rtg"]
    assert row["mean"] == pytest.approx(q.mean())
    assert row["median"] == pytest.approx(q.median())
    assert row["sd"] == pytest.approx(q.std())
    assert row["season"] == 2024 and row["qualifier_min"] is None


def test_team_floor_nulls_rank_and_pct() -> None:
    """Four real teams are under the five-team floor; five are ranked."""
    oracle = pl.read_parquet(PRED / "team_ratings_oracle_2024.parquet").with_columns(season=pl.lit(2024))
    four = attach_ranks(oracle.head(4), ["net_rating"], min_cohort=MIN_COHORT_TEAMS)
    assert four["net_rating_rank"].null_count() == 4 and four["net_rating_pct"].null_count() == 4
    five = attach_ranks(oracle.head(5), ["net_rating"], min_cohort=MIN_COHORT_TEAMS)
    assert five["net_rating_rank"].null_count() == 0
    assert sorted(five["net_rating_rank"].to_list()) == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_conference_cohort_percentiles(monkeypatch) -> None:
    """East/West ``_conf_pct`` ranks within the conference, in the rating's own direction."""
    out = _ratings(monkeypatch)
    xw = pl.read_parquet(FIXTURES / "crosswalk_basketball" / "nba_team_crosswalk_2026.parquet").select(
        team_id=pl.col("espn_team_id").cast(pl.Int64).cast(pl.Utf8), conference=pl.col("nba_conference")
    )
    assert out.schema["team_id"] == xw.schema["team_id"]
    teams = out.join(xw, on="team_id", how="left", validate="m:1")
    assert teams.filter(pl.col("conference").is_not_null()).height == 30  # All-Star teams unmatched
    conf = attach_cohort_percentiles(
        teams, ["adj_def_rtg"], cohort="conference", suffix="_conf_pct", min_cohort=MIN_COHORT_TEAMS
    )
    for name in ("East", "West"):
        c = conf.filter(pl.col("conference") == name).sort("adj_def_rtg")
        assert c.height == 15
        assert c["adj_def_rtg_conf_pct"][0] == pytest.approx(100 * 15 / 16)
        assert c["adj_def_rtg_conf_pct"].is_sorted(descending=True)
    assert conf.filter(pl.col("conference").is_null())["adj_def_rtg_conf_pct"].null_count() == 2


def test_player_position_cohort_floor() -> None:
    """College PF (7 player-seasons) is under the ten-player floor; SF (exactly 10) is not."""
    players = pl.read_parquet(FIXTURES / "mbb_player_value" / "player_seasons_2025.parquet")
    ranked = attach_ranks(players, ["points"], min_cohort=MIN_COHORT_PLAYERS)
    assert ranked["points_rank"].null_count() == players["points"].null_count()
    pos = attach_cohort_percentiles(
        ranked, ["points"], cohort="position", suffix="_pos_pct", min_cohort=MIN_COHORT_PLAYERS
    )
    counts = dict(pos.group_by("position").agg(pl.col("points_pos_pct").count()).iter_rows())
    assert counts["PF"] == 0
    assert counts["SF"] == 10
    assert counts["G"] == pos.filter(pl.col("position") == "G").height


def test_stray_lower_is_better_raises() -> None:
    oracle = pl.read_parquet(PRED / "team_ratings_oracle_2024.parquet").with_columns(season=pl.lit(2024))
    with pytest.raises(ValueError, match="def_rtg"):
        attach_ranks(oracle, ["def_rating"], min_cohort=MIN_COHORT_TEAMS, lower_is_better=["def_rtg"])


def test_empty_frames_keep_the_schema() -> None:
    oracle = pl.read_parquet(PRED / "team_ratings_oracle_2024.parquet").with_columns(season=pl.lit(2024)).head(0)
    ranked = attach_ranks(oracle, ["def_rating"], min_cohort=MIN_COHORT_TEAMS, lower_is_better=["def_rating"])
    assert ranked.schema["def_rating_rank"] == pl.Float64 and ranked.schema["def_rating_pct"] == pl.Float64
    avg = league_averages(oracle, ["def_rating"], level="nba", entity="team", category="t")
    assert avg.height == 0 and dict(avg.schema) == LEAGUE_AVERAGES_SCHEMA
