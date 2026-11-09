"""BB-D3: men's and women's college season team rates from possessions, on committed real captures.

Fixtures (``tests/fixtures/bb_team_season_rates``, README there; none synthetic): every 2024-25
game of a Big Ten team (18 men's, 18 women's) from the ``ncaa_{mbb,wbb}_possessions`` and
``ncaa_{mbb,wbb}_team_box`` releases, and those games' rows of the ESPN schedule
(``neutral_site``). The non-conference opponents play one to three of these games each, so they
are in the table but below the ranking gate.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.rate_gates import TEAM_MIN_N
from sportsdataverse._common.team_season_rates import SPLITS, TEAM_SEASON_RATES_SCHEMA

RATES = Path(__file__).resolve().parents[1] / "fixtures" / "bb_team_season_rates"
# The package re-exports the function under the module's name, so import the module itself.
mod = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")
FOUR_FACTORS = ("efg_pct", "tov_pct", "orb_pct", "ft_rate", "opp_efg_pct", "opp_tov_pct", "opp_orb_pct", "opp_ft_rate")


def _loader(name: str):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(RATES / name)

    def load(seasons: object) -> pl.DataFrame:
        assert seasons == 2025, seasons
        return frame

    return load


def _rates(lg: str) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        if lg == "mbb":
            mp.setattr(mod, "load_ncaa_mbb_possessions", _loader("ncaa_mbb_possessions_2025.parquet"))
            mp.setattr(mod, "load_ncaa_mbb_team_box", _loader("ncaa_mbb_team_box_2025.parquet"))
            mp.setattr(mod, "load_mbb_schedule", _loader("mbb_schedule_2025.parquet"))
            return mod.mbb_team_season_rates(2025)
        from sportsdataverse.wbb import wbb_loaders
        from sportsdataverse.wbb.wbb_team_season_rates import wbb_team_season_rates

        mp.setattr(wbb_loaders, "load_ncaa_wbb_possessions", _loader("ncaa_wbb_possessions_2025.parquet"))
        mp.setattr(wbb_loaders, "load_ncaa_wbb_team_box", _loader("ncaa_wbb_team_box_2025.parquet"))
        mp.setattr(wbb_loaders, "load_wbb_schedule", _loader("wbb_schedule_2025.parquet"))
        return wbb_team_season_rates(2025)


@pytest.fixture(scope="module", params=["mbb", "wbb"])
def college(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    return request.param, _rates(request.param)


def _big_ten(lg: str) -> pl.Series:
    """The 18 Big Ten teams: in every fixture game is one, so only they have 20+ team-box rows."""
    box = pl.read_parquet(RATES / f"ncaa_{lg}_team_box_2025.parquet")
    games = box.group_by(pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id")).agg(pl.len().alias("n"))
    return games.filter(pl.col("n") >= 20)["team_id"].sort()


def test_schema_and_cohort(college: tuple[str, pl.DataFrame]) -> None:
    lg, rates = college
    assert dict(rates.schema) == TEAM_SEASON_RATES_SCHEMA
    assert rates.group_by("split").len().height == len(SPLITS)
    big_ten = _big_ten(lg)
    assert big_ten.len() == 18
    ranked = rates.filter((pl.col("split") == "all") & pl.col("net_rtg_rank").is_not_null())
    # Only the Big Ten teams clear the half-the-most-games gate; their opponents get null ranks.
    assert ranked["team_id"].sort().to_list() == big_ten.to_list()
    assert ranked["net_rtg_pct"].max() == pytest.approx(100 * 18 / 19)
    assert rates.filter(split="all").height > 18


def test_neutral_site_games_are_neither_home_nor_away(college: tuple[str, pl.DataFrame]) -> None:
    lg, rates = college
    wide = rates.pivot(on="split", index="team_id", values="games").filter(
        pl.col("team_id").is_in(_big_ten(lg).implode())
    )
    sched = pl.read_parquet(RATES / f"{lg}_schedule_2025.parquet").filter(pl.col("neutral_site"))
    neutral = pl.concat(
        [sched.select(pl.col("home_id").alias("team_id")), sched.select(pl.col("away_id").alias("team_id"))]
    )
    neutral = neutral.group_by(pl.col("team_id").cast(pl.Int64)).agg(pl.len().alias("neutral"))
    j = wide.join(neutral, on="team_id", how="left").with_columns(pl.col("neutral").fill_null(0))
    assert j["neutral"].sum() > 18  # every Big Ten tournament game is neutral
    assert (j["home"] + j["away"] + j["neutral"] == j["all"]).all()


def test_four_factors_are_game_level(college: tuple[str, pl.DataFrame]) -> None:
    lg, rates = college
    bt = rates.filter(pl.col("team_id").is_in(_big_ten(lg).implode()))
    clutch = bt.filter(split="clutch")
    assert clutch.select(pl.col(c).is_null().all() for c in FOUR_FACTORS).row(0) == (True,) * len(FOUR_FACTORS)
    assert (clutch["efg_pct_n"] == 0).all()
    for split in ("all", "home", "away", "close"):
        s = bt.filter(split=split)
        assert s.select(pl.col(c).is_not_null().all() for c in FOUR_FACTORS).row(0) == (True,) * len(FOUR_FACTORS)
    all_ = bt.filter(split="all")
    assert all_["efg_pct"].is_between(0.40, 0.62).all()
    assert all_["tov_pct"].is_between(0.10, 0.30).all()
    assert all_["orb_pct"].is_between(0.15, 0.45).all()


def test_clutch_window_uses_the_final_period(college: tuple[str, pl.DataFrame]) -> None:
    """~6-9 clutch possessions per clutch game: the last 5:00 of the 2nd half / 4th quarter and OT."""
    lg, rates = college
    bt = rates.filter(pl.col("team_id").is_in(_big_ten(lg).implode()))
    clutch = bt.filter(split="clutch")
    per_game = clutch["poss"].sum() / clutch["games"].sum()
    assert 5.0 <= per_game <= 10.0, (lg, per_game)
    wide = bt.pivot(on="split", index="team_id", values="games")
    assert (wide["close"] == wide["clutch"]).all()
    assert (wide["close"] < wide["all"]).all()


def test_for_and_against_balance(college: tuple[str, pl.DataFrame]) -> None:
    lg, rates = college
    all_ = rates.filter(split="all")
    poss = pl.read_parquet(RATES / f"ncaa_{lg}_possessions_2025.parquet")
    # A team's points are the possessions it had the ball, its opponents' the ones it defended.
    by_team = poss.group_by(pl.col("poss_team_espn_team_id").cast(pl.Int64).alias("team_id")).agg(
        pl.col("pts").sum().alias("scored"), pl.len().alias("n")
    )
    j = all_.join(by_team, on="team_id", validate="1:1")
    assert j.height == all_.height
    assert (j["pts"] == j["scored"]).all() and (j["poss"] == j["n"]).all()
    bt = all_.filter(pl.col("team_id").is_in(_big_ten(lg).implode()))
    assert (bt["opp_pts"] != bt["pts"]).all()
    assert bt["net_rtg"].mean() > 0  # the Big Ten outscores its schedule, non-conference included


def test_bad_league_raises() -> None:
    with pytest.raises(ValueError, match="league"):
        mod.mbb_team_season_rates(2025, league="coed")


def test_sample_gates_and_level_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each adapter passes its own level, and a rate under that level's gate gets no rank."""
    seen: list[str] = []
    real = mod.team_season_rates

    def spy(*args: object, **kwargs: object) -> pl.DataFrame:
        seen.append(str(kwargs["level"]))
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(mod, "team_season_rates", spy)
    rated = [c.removesuffix("_rank") for c in TEAM_SEASON_RATES_SCHEMA if c.endswith("_rank")]
    for lg in ("mbb", "wbb"):
        rates = _rates(lg)
        for m in rated:
            under = rates.filter(pl.col(f"{m}_n") < TEAM_MIN_N[lg][m])
            assert under[f"{m}_rank"].null_count() == under.height, (lg, m)
        # the Big Ten's clutch possessions (dozens a team) clear no rating gate
        clutch = rates.filter(split="clutch")
        assert clutch["off_rtg_rank"].null_count() == clutch.height
        assert rates.filter(split="all")["off_rtg_rank"].is_not_null().sum() == 18
    assert seen == ["mbb", "wbb"]
