"""BB-D3: NBA / WNBA season team rates from possessions, on committed real captures.

Fixtures (``tests/fixtures/bb_team_season_rates``, README there; none synthetic):

* ``nba_possessions_2026`` / ``nba_schedule_2026`` -- the 2025-26 ``nba_stats_possessions`` and
  ``nba_stats_schedules`` releases (regular season plus preseason), projected to the columns read.
* ``nba_team_stats_advanced_2026`` -- stats.nba.com ``team_stats_advanced`` (Regular Season), the
  net-rating oracle (BB-D3's done-when: every team within 1.0).
* ``nba_possessions_2024`` / ``nba_schedule_2024`` + ``nba_prediction/clutch_team_2024`` --
  stats.nba.com's clutch dashboard, the oracle for the clutch window.
* ``wnba_*_2025`` -- the WNBA twins and ``team_stats_advanced``.

The ``bb-team-season-rates`` rows of ``tests/mutants/registry.yaml`` go red here: the points
formula, the possession count, the for/against swap, the home/away split, and the clutch
window's clock, margin and period.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.league_context import LEAGUE_AVERAGES_SCHEMA
from sportsdataverse._common.team_season_rates import (
    SPLITS,
    TEAM_SEASON_RATES_SCHEMA,
    team_season_rates,
    team_season_rates_league_averages,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
RATES = FIXTURES / "bb_team_season_rates"
# The package re-exports the function under the module's name, so import the module itself.
mod = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")


def _fixture_loader(name: str, expect: list[int]):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(RATES / name)

    def load(seasons: list[int]) -> pl.DataFrame:
        assert list(seasons) == expect, f"{name}: loader called with {seasons}, expected {expect}"
        return frame

    return load


def _nba(monkeypatch: pytest.MonkeyPatch, season: int, **kwargs: object) -> pl.DataFrame:
    # The stats loaders take the START year; the table is named by the END year.
    monkeypatch.setattr(
        mod, "load_nba_stats_possessions", _fixture_loader(f"nba_possessions_{season}.parquet", [season - 1])
    )
    monkeypatch.setattr(
        mod, "load_nba_stats_schedules", _fixture_loader(f"nba_schedule_{season}.parquet", [season - 1])
    )
    return mod.nba_team_season_rates(season, **kwargs)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def nba_2026() -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        return _nba(mp, 2026)


def _oracle(name: str) -> pl.DataFrame:
    return pl.read_parquet(RATES / name)


def _join_oracle(rates: pl.DataFrame, oracle: pl.DataFrame) -> pl.DataFrame:
    left = rates.filter(split="all")
    assert left.schema["team_id"] == oracle.schema["team_id"], "team_id dtype differs from the oracle"
    out = left.join(oracle, on="team_id", how="inner", validate="1:1", suffix="_oracle")
    assert out.height == left.height == oracle.height
    return out


def test_schema_and_shape(nba_2026: pl.DataFrame) -> None:
    assert dict(nba_2026.schema) == TEAM_SEASON_RATES_SCHEMA
    assert nba_2026.height == 30 * len(SPLITS)
    assert nba_2026["season"].unique().to_list() == [2026]
    assert nba_2026.group_by("split").len()["len"].to_list() == [30] * len(SPLITS)


def test_nba_2026_net_rating_parity(nba_2026: pl.DataFrame) -> None:
    """BB-D3 done-when: every team's net rating within 1.0 of stats.nba.com team_stats_advanced."""
    j = _join_oracle(nba_2026, _oracle("nba_team_stats_advanced_2026.parquet"))
    delta = (j["net_rtg"] - j["net_rating"]).abs()
    assert delta.max() <= 1.0, j.select("team_name", "net_rtg", "net_rating").filter(delta > 1.0)
    # The ratings move with stats.nba.com on both ends, not only in the difference.
    assert (j["off_rtg"] - j["off_rating"]).abs().max() <= 1.5
    assert (j["def_rtg"] - j["def_rating"]).abs().max() <= 1.5


def test_nba_2026_possessions_pace_and_four_factors(nba_2026: pl.DataFrame) -> None:
    j = _join_oracle(nba_2026, _oracle("nba_team_stats_advanced_2026.parquet"))
    assert (j["games"] == j["gp"]).all()
    # Possession rows, one per possession: within 2% of stats.nba.com's possession count.
    assert ((j["poss"] - j["poss_oracle"]).abs() / j["poss_oracle"]).max() <= 0.02
    assert (j["pace"] - j["pace_oracle"]).abs().max() <= 1.0
    assert (j["efg_pct"] - j["efg_pct_oracle"]).abs().max() <= 0.002
    assert (j["tov_pct"] - j["tm_tov_pct"]).abs().max() <= 0.005
    assert (j["orb_pct"] - j["oreb_pct"]).abs().max() <= 0.02


def test_for_and_against_balance(nba_2026: pl.DataFrame) -> None:
    """Every possession is one team's offense and the other's defense, league-wide."""
    for split in ("all", "clutch"):
        s = nba_2026.filter(split=split)
        assert s["poss"].sum() == s["opp_poss"].sum()
        assert s["pts"].sum() == s["opp_pts"].sum()
    # pts / opp_pts are what the team / its opponents scored: the schedule's box score, short only
    # the few free throws the possession engine leaves unattributed (never over the box).
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(season_type="regular-season")
    box = (
        pl.concat(
            [
                sched.select(
                    pl.col("home_team_id").alias("team_id"),
                    pl.col("home_pts").alias("for"),
                    pl.col("away_pts").alias("against"),
                ),
                sched.select(
                    pl.col("away_team_id").alias("team_id"),
                    pl.col("away_pts").alias("for"),
                    pl.col("home_pts").alias("against"),
                ),
            ]
        )
        .group_by("team_id")
        .agg(pl.col("for").sum(), pl.col("against").sum())
    )
    j = nba_2026.filter(split="all").join(box, on="team_id", validate="1:1")
    assert j.height == 30
    for ours, theirs in (("pts", "for"), ("opp_pts", "against")):
        short = (j[theirs] - j[ours]) / j[theirs]
        assert short.min() >= 0 and short.max() <= 0.01, (ours, short.min(), short.max())


def test_home_away_split_against_the_box_score(nba_2026: pl.DataFrame) -> None:
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(season_type="regular-season")
    home_box = sched.group_by(pl.col("home_team_id").alias("team_id")).agg(pl.len().alias("home_games"))
    wide = (
        nba_2026.filter(pl.col("split").is_in(["all", "home", "away"]))
        .pivot(on="split", index="team_id", values=["games", "poss", "pts"])
        .join(home_box, on="team_id", validate="1:1")
    )
    assert (wide["games_home"] == wide["home_games"]).all()
    assert (wide["games_home"] + wide["games_away"] == wide["games_all"]).all()
    assert (wide["poss_home"] + wide["poss_away"] == wide["poss_all"]).all()
    assert (wide["pts_home"] + wide["pts_away"] == wide["pts_all"]).all()
    # Home-court advantage is real in 2025-26: the league's home split outscores its away split.
    home = nba_2026.filter(split="home")
    away = nba_2026.filter(split="away")
    assert home["net_rtg"].mean() > 1.0 > -1.0 > away["net_rtg"].mean()


def test_season_type_filter(monkeypatch: pytest.MonkeyPatch, nba_2026: pl.DataFrame) -> None:
    assert nba_2026.filter(split="all")["games"].max() == 82
    pre = _nba(monkeypatch, 2026, season_type="preseason")
    assert 0 < pre.filter(split="all")["games"].max() <= 6


def test_clutch_window_against_stats_nba_2024(monkeypatch: pytest.MonkeyPatch) -> None:
    """The clutch split matches stats.nba.com's clutch dashboard (last 5:00, within 5)."""
    rates = _nba(monkeypatch, 2024).filter(split="clutch")
    oracle = pl.read_parquet(FIXTURES / "nba_prediction" / "clutch_team_2024.parquet").with_columns(
        pl.col("team_id").cast(pl.Int64)
    )
    assert rates.schema["team_id"] == oracle.schema["team_id"]
    j = rates.join(oracle, on="team_id", validate="1:1")
    assert j.height == 30
    assert j.select(pl.corr("poss", "clutch_poss")).item() >= 0.99
    assert abs((j["poss"] - j["clutch_poss"]).mean()) <= 30
    assert j.select(pl.corr("net_rtg", "clutch_net_rating")).item() >= 0.97


def test_close_games_are_the_clutch_games(nba_2026: pl.DataFrame) -> None:
    wide = nba_2026.pivot(on="split", index="team_id", values="games")
    assert (wide["close"] == wide["clutch"]).all()
    assert (wide["close"] < wide["all"]).all()
    # ~7 clutch possessions per clutch game: the last five minutes of the 4th quarter and overtime.
    clutch = nba_2026.filter(split="clutch")
    per_game = (clutch["poss"] / clutch["games"]).mean()
    assert 5.0 <= per_game <= 10.0, per_game


def test_ranks_percentiles_and_sample_sizes(nba_2026: pl.DataFrame) -> None:
    all_ = nba_2026.filter(split="all")
    assert all_.filter(pl.col("net_rtg_rank") == 1)["net_rtg"].item() == all_["net_rtg"].max()
    assert all_.filter(pl.col("def_rtg_rank") == 1)["def_rtg"].item() == all_["def_rtg"].min()
    assert all_.filter(pl.col("opp_efg_pct_rank") == 1)["opp_efg_pct"].item() == all_["opp_efg_pct"].min()
    assert all_.filter(pl.col("tov_pct_rank") == 1)["tov_pct"].item() == all_["tov_pct"].min()
    assert all_.filter(pl.col("opp_tov_pct_rank") == 1)["opp_tov_pct"].item() == all_["opp_tov_pct"].max()
    assert all_["net_rtg_pct"].max() == pytest.approx(100 * 30 / 31)
    assert (all_["off_rtg_n"] == all_["poss"]).all()
    assert (all_["def_rtg_n"] == all_["opp_poss"]).all()
    assert (all_["net_rtg_n"] == all_["poss"] + all_["opp_poss"]).all()
    assert (all_["pace_n"] == all_["games"]).all()
    # eFG% and FT rate share the FGA denominator; ORB% counts chances, not possessions.
    assert (all_["efg_pct_n"] == all_["ft_rate_n"]).all()
    assert (all_["efg_pct_n"] > all_["poss"] * 0.8).all()
    assert (all_["orb_pct_n"] < all_["poss"]).all()


def test_league_averages_n_is_the_cohort(nba_2026: pl.DataFrame) -> None:
    avg = team_season_rates_league_averages(nba_2026, level="nba")
    assert dict(avg.schema) == LEAGUE_AVERAGES_SCHEMA
    assert sorted(avg["category"].unique()) == sorted(f"team_season_rates_{s}" for s in SPLITS)
    assert (avg["n"] == 30).all()
    net = avg.filter(category="team_season_rates_all", metric="net_rtg")
    assert net["mean"].item() == pytest.approx(nba_2026.filter(split="all")["net_rtg"].mean())


def test_wnba_2025_parity(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.wnba import wnba_loaders
    from sportsdataverse.wnba.wnba_team_season_rates import wnba_team_season_rates

    monkeypatch.setattr(
        wnba_loaders, "load_wnba_stats_possessions", _fixture_loader("wnba_possessions_2025.parquet", [2025])
    )
    monkeypatch.setattr(
        wnba_loaders, "load_wnba_stats_schedules", _fixture_loader("wnba_schedule_2025.parquet", [2025])
    )
    rates = wnba_team_season_rates(2025)
    assert rates.height == 13 * len(SPLITS)
    j = _join_oracle(rates, _oracle("wnba_team_stats_advanced_2025.parquet"))
    assert (j["games"] == j["gp"]).all()
    assert (j["net_rtg"] - j["net_rating"]).abs().max() <= 1.0
    assert (j["pace"] - j["pace_per40"]).abs().max() <= 1.5
    playoffs = wnba_team_season_rates(2025, season_type="playoffs")
    assert playoffs.filter(split="all").height == 8


def test_empty_input_keeps_the_schema() -> None:
    from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA

    out = team_season_rates(pl.DataFrame(schema=RATE_ROWS_SCHEMA), minutes_per_game=48)
    assert out.height == 0 and dict(out.schema) == TEAM_SEASON_RATES_SCHEMA
    rows = mod.nba_possession_rate_rows(pl.DataFrame(), pl.DataFrame())
    assert dict(rows.schema) == RATE_ROWS_SCHEMA
    # a counts frame missing columns raises even when there are no possessions to rate
    with pytest.raises(KeyError, match="counts missing"):
        team_season_rates(pl.DataFrame(schema=RATE_ROWS_SCHEMA), minutes_per_game=40, counts=pl.DataFrame())


def test_bad_league_id_raises() -> None:
    with pytest.raises(ValueError, match="league_id"):
        mod.nba_team_season_rates(2026, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        mod.nba_team_season_rates(2026, season_type="regular_season")
