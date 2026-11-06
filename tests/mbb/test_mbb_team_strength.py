"""BB-D19: men's and women's college team strength on committed real captures.

Fixtures, none synthetic:

* BB-D3's ``tests/fixtures/bb_team_season_rates`` -- every 2024-25 game of a Big Ten team from the
  ``ncaa_{mbb,wbb}_possessions`` and ``ncaa_{mbb,wbb}_team_box`` releases.
* ``tests/fixtures/bb_team_strength`` (README there) -- the same games' rows of the ESPN
  schedule, with final scores, and the ``mbb_ratings`` / ``wbb_ratings`` release rows of every
  team in them.

College adjusted ratings ARE the published ratings, so their parity tolerance is zero; SOS is
checked against the existing :func:`sportsdataverse.mbb.mbb_strength_of_schedule.strength_of_schedule`
(also zero), and SOV by hand. The ``_ridge`` ratings are the shared possession ridge on the same
possession rows, so they too match it exactly.
"""

from __future__ import annotations

import importlib
from collections import defaultdict
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.team_strength import (
    COLLEGE_RIDGE_LAMBDA,
    COLLEGE_TEAM_STRENGTH_SCHEMA,
    TEAM_STRENGTH_SCHEMA,
    possession_ridge_ratings,
)
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
RATES = FIXTURES / "bb_team_season_rates"
STRENGTH = FIXTURES / "bb_team_strength"
# The package re-exports the functions under the modules' names, so import the modules themselves.
tsr = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")
mod = importlib.import_module("sportsdataverse.mbb.mbb_team_strength")
LEAGUE = {"mbb": "mens", "wbb": "womens"}


def _loader(path: Path):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(path)

    def load(seasons: object) -> pl.DataFrame:
        assert seasons == 2025, seasons
        return frame

    return load


def _strength(lg: str) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        if lg == "mbb":
            mp.setattr(tsr, "load_ncaa_mbb_possessions", _loader(RATES / "ncaa_mbb_possessions_2025.parquet"))
            mp.setattr(tsr, "load_ncaa_mbb_team_box", _loader(RATES / "ncaa_mbb_team_box_2025.parquet"))
            mp.setattr(tsr, "load_mbb_schedule", _loader(STRENGTH / "mbb_schedule_2025.parquet"))
            mp.setattr(mod, "load_mbb_ratings", _loader(STRENGTH / "mbb_ratings_2025.parquet"))
            return mod.mbb_team_strength(2025)
        from sportsdataverse.wbb import wbb_loaders
        from sportsdataverse.wbb.wbb_team_strength import wbb_team_strength

        mp.setattr(wbb_loaders, "load_ncaa_wbb_possessions", _loader(RATES / "ncaa_wbb_possessions_2025.parquet"))
        mp.setattr(wbb_loaders, "load_ncaa_wbb_team_box", _loader(RATES / "ncaa_wbb_team_box_2025.parquet"))
        mp.setattr(wbb_loaders, "load_wbb_schedule", _loader(STRENGTH / "wbb_schedule_2025.parquet"))
        mp.setattr(wbb_loaders, "load_wbb_ratings", _loader(STRENGTH / "wbb_ratings_2025.parquet"))
        return wbb_team_strength(2025)


@pytest.fixture(scope="module", params=["mbb", "wbb"])
def college(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    return request.param, _strength(request.param)


def _ranked(strength: pl.DataFrame) -> pl.DataFrame:
    return strength.filter(pl.col("adj_net_rtg_rank").is_not_null())


def test_schema_and_cohort(college: tuple[str, pl.DataFrame]) -> None:
    _, s = college
    assert dict(s.schema) == COLLEGE_TEAM_STRENGTH_SCHEMA
    # the published-ratings columns keep their place; the ridge ones come after
    assert list(s.columns[: len(TEAM_STRENGTH_SCHEMA)]) == list(TEAM_STRENGTH_SCHEMA)
    assert s["team_id"].n_unique() == s.height
    # only the 18 Big Ten teams play a full schedule in these games, so only they are ranked
    assert _ranked(s).height == 18
    assert _ranked(s)["sos_rank"].sort().to_list() == [float(i) for i in range(1, 19)]


def test_record_and_possession_games_come_from_two_sources(college: tuple[str, pl.DataFrame]) -> None:
    """``games`` counts NCAA possession games, ``wins``/``losses`` ESPN's completed games.

    Men's: they agree for every Big Ten team. Women's: 12 of 18 differ, by -1 to +3 (games one
    source has and the other lacks), so the two are not interchangeable.
    """
    lg, s = college
    gap = (_ranked(s)["games"] - _ranked(s)["wins"] - _ranked(s)["losses"]).abs()
    assert gap.max() <= (0 if lg == "mbb" else 3)


def test_adjusted_ratings_are_the_college_ratings(college: tuple[str, pl.DataFrame]) -> None:
    lg, s = college
    ratings = pl.read_parquet(STRENGTH / f"{lg}_ratings_2025.parquet").with_columns(pl.col("team_id").cast(pl.Int64))
    assert s.schema["team_id"] == ratings.schema["team_id"]
    j = s.join(ratings, on=["season", "team_id"], how="left", validate="1:1")
    assert j["adj_em"].null_count() == 0  # every team in the table is rated
    for ours, theirs in (("adj_off_rtg", "adj_o"), ("adj_def_rtg", "adj_d"), ("adj_net_rtg", "adj_em")):
        assert (j[ours] - j[theirs]).abs().max() == 0.0


def _results(lg: str) -> pl.DataFrame:
    return pl.read_parquet(STRENGTH / f"{lg}_schedule_2025.parquet").filter(pl.col("status_type_completed"))


def test_sos_matches_mbb_strength_of_schedule(college: tuple[str, pl.DataFrame]) -> None:
    lg, s = college
    results = _results(lg).select(
        "game_id",
        pl.col("season").cast(pl.Int64),
        pl.col("home_id").cast(pl.Utf8).alias("home_team_id"),
        pl.col("away_id").cast(pl.Utf8).alias("away_team_id"),
        "home_score",
        "away_score",
        "neutral_site",
    )
    ref = strength_of_schedule(results, pl.read_parquet(STRENGTH / f"{lg}_ratings_2025.parquet"), league=LEAGUE[lg])
    ref = ref.select(pl.col("team_id").cast(pl.Int64), pl.col("sos").alias("sos_ref"))
    j = _ranked(s).join(ref, on="team_id", how="inner", validate="1:1")
    assert j.height == 18
    assert (j["sos"] - j["sos_ref"]).abs().max() < 1e-9


def test_sov_and_record_by_hand(college: tuple[str, pl.DataFrame]) -> None:
    lg, s = college
    ratings = pl.read_parquet(STRENGTH / f"{lg}_ratings_2025.parquet")
    rating = dict(ratings.select(pl.col("team_id").cast(pl.Int64), "adj_em").iter_rows())
    hand: dict[int, dict[str, list[float]]] = defaultdict(lambda: {"all": [], "won": []})
    for home, away, hs, as_ in _results(lg).select("home_id", "away_id", "home_score", "away_score").rows():
        for team, opp, won in ((home, away, hs > as_), (away, home, as_ > hs)):
            hand[team]["all"].append(rating[opp])
            if won:
                hand[team]["won"].append(rating[opp])
    for team, games, wins, losses, sos, sov, sov_n in (
        _ranked(s).select("team_id", "games", "wins", "losses", "sos", "sov", "sov_n").rows()
    ):
        rec = hand[team]
        assert wins + losses == len(rec["all"]) and wins == len(rec["won"]) == sov_n
        assert abs(sos - np.mean(rec["all"])) < 1e-9
        assert abs(sov - np.mean(rec["won"])) < 1e-9
        # a Big Ten schedule's wins come against its weaker opponents
        assert sov < sos


def _rows(lg: str) -> pl.DataFrame:
    return tsr.ncaa_possession_rate_rows(
        pl.read_parquet(RATES / f"ncaa_{lg}_possessions_2025.parquet"),
        pl.read_parquet(STRENGTH / f"{lg}_schedule_2025.parquet"),
        league=LEAGUE[lg],
    )


def test_ridge_ratings_are_the_possession_ridge(college: tuple[str, pl.DataFrame]) -> None:
    lg, s = college
    rows = _rows(lg)
    want = possession_ridge_ratings(rows, lam=COLLEGE_RIDGE_LAMBDA[LEAGUE[lg]])
    assert s.schema["team_id"] == want.schema["team_id"]
    j = s.join(want, on=["season", "team_id"], how="inner", validate="1:1", suffix="_want")
    # every team with a possession against a Division I opponent is rated by the ridge
    assert j.height == want.height == s.filter(pl.col("adj_net_rtg_ridge").is_not_null()).height
    for m in ("adj_off_rtg", "adj_def_rtg", "adj_net_rtg"):
        assert (j[f"{m}_ridge"] - j[f"{m}_want"]).abs().max() == 0.0
    # a second method, not a copy of the published one: shrunk, so its spread is smaller
    ranked = _ranked(s)
    assert (ranked["adj_net_rtg_ridge"] - ranked["adj_net_rtg"]).abs().max() > 1.0
    assert ranked["adj_net_rtg_ridge"].std() < ranked["adj_net_rtg"].std()
    # possessions against a non-Division I opponent (no ESPN team id) are left out of the fit
    both = rows.filter(pl.col("offense_team_id").is_not_null() & pl.col("defense_team_id").is_not_null())
    assert both.height < rows.height
    assert want.height == both["offense_team_id"].n_unique()


def test_ridge_ranks_and_sample_sizes(college: tuple[str, pl.DataFrame]) -> None:
    _, s = college
    ranked = _ranked(s)
    assert ranked["adj_net_rtg_ridge_rank"].sort().to_list() == [float(i) for i in range(1, 19)]
    best = ranked.sort("adj_net_rtg_ridge", descending=True)["adj_net_rtg_ridge_rank"][0]
    stingiest = ranked.sort("adj_def_rtg_ridge")["adj_def_rtg_ridge_rank"][0]
    assert best == 1.0 and stingiest == 1.0  # adj_def_rtg_ridge ranks low first
    assert (ranked["adj_off_rtg_ridge_n"] == ranked["poss"]).all()
    assert (ranked["adj_net_rtg_ridge_n"] == ranked["poss"] + ranked["opp_poss"]).all()


def test_ridge_is_centred_and_consistent(college: tuple[str, pl.DataFrame]) -> None:
    """Each side's ridge coefficients sum to zero, so over the rated teams the mean net is 0.

    The intercept is the neutral-court average against an average opponent of the fit, below the
    fixture's raw points per 100 (103.27 vs 106.66 men's, 90.17 vs 95.39 women's): the Big Ten
    teams, above average, take most of its possessions.
    """
    lg, s = college
    rated = s.filter(pl.col("adj_net_rtg_ridge").is_not_null())
    assert abs(rated["adj_net_rtg_ridge"].mean()) < 1e-9  # type: ignore[arg-type]
    off, de = rated["adj_off_rtg_ridge"].mean(), rated["adj_def_rtg_ridge"].mean()
    assert abs(off - de) < 1e-9  # type: ignore[operator]
    assert ((rated["adj_off_rtg_ridge"] - rated["adj_def_rtg_ridge"]) - rated["adj_net_rtg_ridge"]).abs().max() < 1e-9
    rows = _rows(lg).filter(pl.col("offense_team_id").is_not_null() & pl.col("defense_team_id").is_not_null())
    raw = 100 * rows["points"].sum() / rows.height
    assert 0 < raw - off < 6  # type: ignore[operator]


def test_bad_league_raises() -> None:
    with pytest.raises(ValueError, match="league"):
        mod.mbb_team_strength(2025, league="coed")
