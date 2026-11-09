"""BB-D19: NBA / WNBA team strength (possession ridge, SOS, SOV) on committed real captures.

Fixtures: BB-D3's ``tests/fixtures/bb_team_season_rates`` (README there; none synthetic) -- the
2025-26 NBA and 2025 WNBA possession and schedule releases, and stats.nba.com
``team_stats_advanced`` (Regular Season), the net-rating reference.

Tolerances are the observed values at the committed penalty (NBA 1600, WNBA 800) with a margin:

* adjusted net vs stats.nba.com net: worst |diff| NBA 2.07 (gate 2.5), WNBA 3.21 (gate 3.75);
  correlation 0.9991 / 0.9988 (gate 0.998 / 0.997). The gap is the ridge's shrinkage toward
  the average plus the schedule adjustment: BB-D3's raw net is within 0.47 / 0.74, and the
  nearly unpenalized fit (``lam=1``) within 0.85 / 1.24 (gate 1.0 / 1.5).
* the fitted home term (home offense over a neutral court, points per 100): NBA 2026 0.86,
  NBA 2024 1.07, WNBA 2025 1.60 (gate 0.5..2.0).
* NBA strength of schedule: every team in -0.53..0.35 (gate |sos| < 1.0); a balanced schedule
  makes its mean the mean adjusted net, 0.

The ``bb-team-strength`` rows of ``tests/mutants/registry.yaml`` go red here: the ridge's sign,
the home term's sign and its removal, the SOS opponent weighting and the SOV definition.
"""

from __future__ import annotations

import importlib
from collections import defaultdict
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA
from sportsdataverse._common.team_strength import (
    GAMES_SCHEMA,
    RATINGS_SCHEMA,
    RIDGE_LAMBDA,
    TEAM_STRENGTH_SCHEMA,
    possession_ridge_ratings,
    ridge_design,
    team_strength,
)

RATES = Path(__file__).resolve().parents[1] / "fixtures" / "bb_team_season_rates"
# The package re-exports the functions under the modules' names, so import the modules themselves.
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
mod = importlib.import_module("sportsdataverse.nba.nba_team_strength")


def _fixture_loader(name: str, expect: list[int]):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(RATES / name)

    def load(seasons: list[int]) -> pl.DataFrame:
        assert list(seasons) == expect, f"{name}: loader called with {seasons}, expected {expect}"
        return frame

    return load


def _strength(lg: str, season: int) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        if lg == "nba":
            # The stats loaders take the START year; the table is named by the END year.
            mp.setattr(
                tsr, "load_nba_stats_possessions", _fixture_loader(f"nba_possessions_{season}.parquet", [season - 1])
            )
            mp.setattr(tsr, "load_nba_stats_schedules", _fixture_loader(f"nba_schedule_{season}.parquet", [season - 1]))
            return mod.nba_team_strength(season)
        from sportsdataverse.wnba import wnba_loaders
        from sportsdataverse.wnba.wnba_team_strength import wnba_team_strength

        mp.setattr(
            wnba_loaders, "load_wnba_stats_possessions", _fixture_loader(f"wnba_possessions_{season}.parquet", [season])
        )
        mp.setattr(
            wnba_loaders, "load_wnba_stats_schedules", _fixture_loader(f"wnba_schedule_{season}.parquet", [season])
        )
        return wnba_team_strength(season)


def _rows(lg: str, season: int) -> pl.DataFrame:
    return tsr.nba_possession_rate_rows(
        pl.read_parquet(RATES / f"{lg}_possessions_{season}.parquet"),
        pl.read_parquet(RATES / f"{lg}_schedule_{season}.parquet"),
        league_id="00" if lg == "nba" else "10",
    )


@pytest.fixture(scope="module")
def nba_2026() -> pl.DataFrame:
    return _strength("nba", 2026)


@pytest.fixture(scope="module")
def wnba_2025() -> pl.DataFrame:
    return _strength("wnba", 2025)


def _join_oracle(strength: pl.DataFrame, lg: str, season: int) -> pl.DataFrame:
    oracle = pl.read_parquet(RATES / f"{lg}_team_stats_advanced_{season}.parquet")
    assert strength.schema["team_id"] == oracle.schema["team_id"], "team_id dtype differs from the oracle"
    out = strength.join(oracle, on="team_id", how="inner", validate="1:1")
    assert out.height == strength.height == oracle.height
    return out


def test_schema_and_carried_rates(nba_2026: pl.DataFrame) -> None:
    assert dict(nba_2026.schema) == TEAM_STRENGTH_SCHEMA
    assert nba_2026.height == 30 and nba_2026["team_id"].n_unique() == 30
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(tsr, "load_nba_stats_possessions", _fixture_loader("nba_possessions_2026.parquet", [2025]))
        mp.setattr(tsr, "load_nba_stats_schedules", _fixture_loader("nba_schedule_2026.parquet", [2025]))
        rates = tsr.nba_team_season_rates(2026).filter(split="all").drop("split")
    shared = rates.columns
    assert nba_2026.select(shared).equals(rates.sort("season", "team_id").select(shared))


@pytest.mark.parametrize(
    ("lg", "season", "max_diff", "min_corr"), [("nba", 2026, 2.5, 0.998), ("wnba", 2025, 3.75, 0.997)]
)
def test_adjusted_net_parity_with_stats_nba(lg: str, season: int, max_diff: float, min_corr: float) -> None:
    j = _join_oracle(_strength(lg, season), lg, season)
    diff = (j["adj_net_rtg"] - j["net_rating"]).abs()
    assert diff.max() <= max_diff, diff.max()
    assert np.corrcoef(j["adj_net_rtg"], j["net_rating"])[0, 1] >= min_corr
    # each side tracks its own raw rating; a flipped coefficient sign would anti-correlate
    assert np.corrcoef(j["adj_off_rtg"], j["off_rating"])[0, 1] >= 0.98
    assert np.corrcoef(j["adj_def_rtg"], j["def_rating"])[0, 1] >= 0.98
    # shrinkage pulls toward the average: the adjusted spread is under stats.nba.com's
    assert j["adj_net_rtg"].std() < j["net_rating"].std()


@pytest.mark.parametrize(("lg", "season", "max_diff"), [("nba", 2026, 1.0), ("wnba", 2025, 1.5)])
def test_unpenalized_fit_is_close_to_raw_net(lg: str, season: int, max_diff: float) -> None:
    """The penalty, not the design, is what separates adjusted net from stats.nba.com's raw net."""
    fit = possession_ridge_ratings(_rows(lg, season), lam=1.0)
    j = _join_oracle(fit, lg, season)
    assert (j["adj_net_rtg"] - j["net_rating"]).abs().max() <= max_diff


@pytest.mark.parametrize(("lg", "season"), [("nba", 2026), ("nba", 2024), ("wnba", 2025)])
def test_home_court_term(lg: str, season: int) -> None:
    fit = possession_ridge_ratings(_rows(lg, season), lam=RIDGE_LAMBDA["00" if lg == "nba" else "10"])
    hfa = fit["hfa"].unique()
    assert hfa.len() == 1
    assert 0.5 <= hfa[0] <= 2.0, hfa[0]


def test_ridge_is_centred_and_consistent(nba_2026: pl.DataFrame) -> None:
    s = nba_2026
    assert abs(s["adj_net_rtg"].mean()) < 1e-9  # type: ignore[arg-type]
    assert abs(s["adj_off_rtg"].mean() - s["adj_def_rtg"].mean()) < 1e-9  # type: ignore[operator]
    assert ((s["adj_off_rtg"] - s["adj_def_rtg"]) - s["adj_net_rtg"]).abs().max() < 1e-9
    # the intercept is the league's points per 100 (within the regression to it)
    league = 100 * s["pts"].sum() / s["poss"].sum()
    assert abs(s["adj_off_rtg"].mean() - league) < 0.5  # type: ignore[operator]


def _hand_record(schedule: pl.DataFrame, rating: dict[int, float]) -> dict[int, dict[str, list[float]]]:
    """Every completed regular-season game, walked in plain Python: opponent ratings per team."""
    out: dict[int, dict[str, list[float]]] = defaultdict(lambda: {"all": [], "won": [], "lost": []})
    games = schedule.filter(season_type="regular-season")
    for home, away, hp, ap in games.select("home_team_id", "away_team_id", "home_pts", "away_pts").rows():
        for team, opp, won in ((home, away, hp > ap), (away, home, ap > hp)):
            out[team]["all"].append(rating[opp])
            out[team]["won" if won else "lost"].append(rating[opp])
    return out


@pytest.mark.parametrize(("lg", "season"), [("nba", 2026), ("wnba", 2025)])
def test_sos_and_sov_by_hand(lg: str, season: int) -> None:
    s = _strength(lg, season)
    rating = dict(zip(s["team_id"], s["adj_net_rtg"]))
    hand = _hand_record(pl.read_parquet(RATES / f"{lg}_schedule_{season}.parquet"), rating)
    assert set(hand) == set(s["team_id"])
    for team, sos, sov, wins, losses in s.select("team_id", "sos", "sov", "wins", "losses").rows():
        rec = hand[team]
        assert wins == len(rec["won"]) and losses == len(rec["lost"])
        assert abs(sos - np.mean(rec["all"])) < 1e-9, team  # one weight per game, repeats included
        assert abs(sov - np.mean(rec["won"])) < 1e-9, team  # the wins only


def test_sos_and_sov_sanity(nba_2026: pl.DataFrame) -> None:
    s = nba_2026
    assert (s["wins"] + s["losses"] == s["games"]).all() and (s["games"] == 82).all()
    assert (s["sos_n"] == s["games"]).all() and (s["sov_n"] == s["wins"]).all()
    # 82 games each, every team the same number of times an opponent: mean SOS = mean adjusted net = 0
    assert abs(s["sos"].mean()) < 1e-9  # type: ignore[arg-type]
    assert s["sos"].abs().max() < 1.0  # type: ignore[operator]
    # wins come against the weaker part of a schedule
    assert (s["sov"] < s["sos"]).all()
    assert s.sort("sos", descending=True)["sos_rank"][0] == 1.0
    assert s.sort("adj_def_rtg")["adj_def_rtg_rank"][0] == 1.0
    assert s.sort("adj_net_rtg", descending=True)["adj_net_rtg_rank"][0] == 1.0
    assert (s["adj_net_rtg_n"] == s["poss"] + s["opp_poss"]).all()


def test_season_type_and_empty_input(monkeypatch: pytest.MonkeyPatch, nba_2026: pl.DataFrame) -> None:
    monkeypatch.setattr(tsr, "load_nba_stats_possessions", _fixture_loader("nba_possessions_2026.parquet", [2025]))
    monkeypatch.setattr(tsr, "load_nba_stats_schedules", _fixture_loader("nba_schedule_2026.parquet", [2025]))
    pre = mod.nba_team_strength(2026, season_type="preseason")
    # the preseason has the 30 teams plus visiting international clubs, a few games each
    assert pre.height > 30 and (pre["games"] < 10).all()
    empty = team_strength(
        tsr.team_season_rates(pl.DataFrame(schema=RATE_ROWS_SCHEMA), minutes_per_game=48),
        possession_ridge_ratings(pl.DataFrame(schema=RATE_ROWS_SCHEMA), lam=1.0),
        pl.DataFrame(schema=GAMES_SCHEMA),
    )
    assert empty.height == 0 and dict(empty.schema) == TEAM_STRENGTH_SCHEMA


def test_join_key_dtype_is_checked(nba_2026: pl.DataFrame) -> None:
    rows = _rows("nba", 2026)
    rates = tsr.team_season_rates(rows, minutes_per_game=48)
    ratings = possession_ridge_ratings(rows, lam=1.0).with_columns(pl.col("team_id").cast(pl.Utf8))
    with pytest.raises(TypeError, match="team_id"):
        team_strength(rates, ratings.select(list(RATINGS_SCHEMA)), pl.DataFrame(schema=GAMES_SCHEMA))


def test_bad_league_id_raises() -> None:
    with pytest.raises(ValueError, match="league_id"):
        mod.nba_team_strength(2026, league_id="20")


def test_bad_season_type_raises_before_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_load(seasons: list[int]) -> pl.DataFrame:
        raise AssertionError("loader called before season_type was checked")

    monkeypatch.setattr(tsr, "load_nba_stats_possessions", no_load)
    monkeypatch.setattr(tsr, "load_nba_stats_schedules", no_load)
    with pytest.raises(ValueError, match="season_type"):
        mod.nba_team_strength(2026, season_type="regular")


def test_ridge_design_rejects_null_points() -> None:
    rows = _rows("wnba", 2025)
    first = (
        (pl.int_range(pl.len()) == 0)
        & pl.col("offense_team_id").is_not_null()
        & pl.col("defense_team_id").is_not_null()
    )
    broken = rows.with_columns(pl.when(first).then(None).otherwise(pl.col("points")).alias("points"))
    with pytest.raises(ValueError, match="null points"):
        ridge_design(broken)


def test_ridge_design_rejects_a_null_home_flag() -> None:
    rows = _rows("wnba", 2025)
    assert ridge_design(rows).height > 0
    first = pl.int_range(pl.len()) == 0
    broken = rows.with_columns(
        pl.when(first).then(None).otherwise(pl.col("offense_is_home")).alias("offense_is_home"),
        pl.when(first).then(False).otherwise(pl.col("neutral_site")).alias("neutral_site"),
    )
    with pytest.raises(ValueError, match="null offense_is_home"):
        ridge_design(broken)
    # On a neutral site the flag is not read, so a null there is fine.
    assert (
        ridge_design(
            broken.with_columns(pl.when(first).then(True).otherwise(pl.col("neutral_site")).alias("neutral_site"))
        ).height
        == ridge_design(rows).height
    )


def test_adapters_pass_their_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """The carried BB-D3 ranks are gated by the adapter's own league (``rate_gates.TEAM_MIN_N``)."""
    seen: list[str] = []
    real = mod.team_season_rates

    def spy(*args: object, **kwargs: object) -> pl.DataFrame:
        seen.append(str(kwargs["level"]))
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(mod, "team_season_rates", spy)
    _strength("nba", 2026)
    _strength("wnba", 2025)
    assert seen == ["nba", "wnba"]
