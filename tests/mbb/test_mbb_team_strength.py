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
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.team_strength import (
    COLLEGE_RIDGE_LAMBDA,
    COLLEGE_RIDGE_PRIOR_LAMBDA,
    COLLEGE_RIDGE_PRIOR_REGRESSION,
    COLLEGE_TEAM_STRENGTH_SCHEMA,
    TEAM_STRENGTH_SCHEMA,
    last_season_prior,
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


def _loader(path: Path, prev: Path | None = None):  # type: ignore[no-untyped-def]
    """A loader serving ``path`` for 2025 and, when given, ``prev`` for 2024 (the prior's season)."""
    frames = {2025: pl.read_parquet(path)} | ({2024: pl.read_parquet(prev)} if prev else {})

    def load(seasons: object) -> pl.DataFrame:
        assert seasons in frames, seasons
        return frames[seasons]  # type: ignore[index]

    return load


def _strength(lg: str, prior: str | None = "last_season") -> pl.DataFrame:
    poss = _loader(RATES / f"ncaa_{lg}_possessions_2025.parquet", RATES / f"ncaa_{lg}_possessions_2024.parquet")
    sched = _loader(STRENGTH / f"{lg}_schedule_2025.parquet", RATES / f"{lg}_schedule_2024.parquet")
    with pytest.MonkeyPatch.context() as mp:
        if lg == "mbb":
            mp.setattr(tsr, "load_ncaa_mbb_possessions", poss)
            mp.setattr(tsr, "load_ncaa_mbb_team_box", _loader(RATES / "ncaa_mbb_team_box_2025.parquet"))
            mp.setattr(tsr, "load_mbb_schedule", sched)
            mp.setattr(mod, "load_mbb_ratings", _loader(STRENGTH / "mbb_ratings_2025.parquet"))
            return mod.mbb_team_strength(2025, prior=prior)
        from sportsdataverse.wbb import wbb_loaders
        from sportsdataverse.wbb.wbb_team_strength import wbb_team_strength

        mp.setattr(wbb_loaders, "load_ncaa_wbb_possessions", poss)
        mp.setattr(wbb_loaders, "load_ncaa_wbb_team_box", _loader(RATES / "ncaa_wbb_team_box_2025.parquet"))
        mp.setattr(wbb_loaders, "load_wbb_schedule", sched)
        mp.setattr(wbb_loaders, "load_wbb_ratings", _loader(STRENGTH / "wbb_ratings_2025.parquet"))
        return wbb_team_strength(2025, prior=prior)


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


def _rows(lg: str, season: int = 2025) -> pl.DataFrame:
    sched = STRENGTH / f"{lg}_schedule_2025.parquet" if season == 2025 else RATES / f"{lg}_schedule_{season}.parquet"
    return tsr.ncaa_possession_rate_rows(
        pl.read_parquet(RATES / f"ncaa_{lg}_possessions_{season}.parquet"), pl.read_parquet(sched), league=LEAGUE[lg]
    )


def _prior(lg: str) -> pl.DataFrame:
    lam, r = COLLEGE_RIDGE_PRIOR_LAMBDA[LEAGUE[lg]], COLLEGE_RIDGE_PRIOR_REGRESSION[LEAGUE[lg]]
    return last_season_prior(_rows(lg, 2024), lam=lam, regression=r)


def test_ridge_ratings_are_the_possession_ridge(college: tuple[str, pl.DataFrame]) -> None:
    lg, s = college
    rows = _rows(lg)
    want = possession_ridge_ratings(rows, lam=COLLEGE_RIDGE_PRIOR_LAMBDA[LEAGUE[lg]], prior=_prior(lg))
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


@pytest.mark.parametrize("lg", ["mbb", "wbb"])
def test_ridge_is_centred_and_consistent(lg: str) -> None:
    """Without the prior each side's ridge coefficients sum to zero, so over the rated teams the mean net is 0.

    The intercept is the neutral-court average against an average opponent of the fit, below the
    fixture's raw points per 100 (103.27 vs 106.66 men's, 90.17 vs 95.39 women's): the Big Ten
    teams, above average, take most of its possessions.
    """
    s = _strength(lg, prior=None)
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
    with pytest.raises(ValueError, match="prior"):
        mod.mbb_team_strength(2025, prior="fade")


@pytest.mark.parametrize("lg", ["mbb", "wbb"])
def test_the_prior_seeds_from_last_season(lg: str) -> None:
    """The default ridge shrinks toward r x last season's rating; prior=None is the plain ridge."""
    plain = _strength(lg, prior=None).drop_nulls("adj_net_rtg_ridge").sort("team_id")
    seeded = _strength(lg).drop_nulls("adj_net_rtg_ridge").sort("team_id")
    want = possession_ridge_ratings(_rows(lg), lam=COLLEGE_RIDGE_LAMBDA[LEAGUE[lg]]).sort("team_id")
    assert (plain["adj_net_rtg_ridge"] - want["adj_net_rtg"]).abs().max() == 0.0
    # the prior alone moves the ratings: against the same penalty without it (silent-no-op guard)
    same_lam = possession_ridge_ratings(_rows(lg), lam=COLLEGE_RIDGE_PRIOR_LAMBDA[LEAGUE[lg]]).sort("team_id")
    assert (seeded["adj_net_rtg_ridge"] - same_lam["adj_net_rtg"]).abs().max() > 0.5


@pytest.mark.parametrize("lg", ["mbb", "wbb"])
def test_a_season_with_no_previous_season_is_the_no_prior_fit(lg: str) -> None:
    """Before the releases start there is no prior, so the season gets the no-prior penalty too."""
    rows = _rows(lg).with_columns(pl.lit(2010, dtype=pl.Int64).alias("season"))
    got = mod._ridge(rows, LEAGUE[lg], "last_season")
    want = possession_ridge_ratings(rows, lam=COLLEGE_RIDGE_LAMBDA[LEAGUE[lg]])
    assert got.equals(want)


@pytest.mark.parametrize("lg", ["mbb", "wbb"])
def test_the_prior_constants_are_the_committed_fit(lg: str) -> None:
    """COLLEGE_RIDGE_PRIOR_* are tools/fit_college_ridge_prior.py's chosen values (its --out JSON)."""
    fit = json.loads((STRENGTH / f"ridge_prior_{lg}.json").read_text(encoding="utf-8"))
    assert not set(fit["train"]) & set(fit["holdout"])
    assert COLLEGE_RIDGE_PRIOR_LAMBDA[LEAGUE[lg]] == fit["chosen_lam"]
    assert COLLEGE_RIDGE_PRIOR_REGRESSION[LEAGUE[lg]] == fit["chosen_regression"]
    # the change ships only because it beats the no-prior ridge out of sample, in every held-out season
    ho = fit["holdout_scored_once"]
    assert ho["all"]["chosen_vs_shipped"]["diff"] < 0
    assert all(ho[f"season_{s}"]["chosen_vs_shipped"]["diff"] < 0 for s in fit["holdout"])


@pytest.mark.parametrize("lg", ["mbb", "wbb"])
def test_last_season_prior_is_the_regressed_previous_fit(lg: str) -> None:
    """The prior is last season's ridge deviations times r, from one season only."""
    lam, r = COLLEGE_RIDGE_PRIOR_LAMBDA[LEAGUE[lg]], COLLEGE_RIDGE_PRIOR_REGRESSION[LEAGUE[lg]]
    prev = _rows(lg, 2024)
    assert prev["season"].unique().to_list() == [2024]
    final = possession_ridge_ratings(prev, lam=lam)
    want = final.select(
        "team_id",
        (r * (pl.col("adj_off_rtg") - pl.col("adj_off_rtg").mean())).alias("off_dev"),
        (r * (pl.col("adj_def_rtg") - pl.col("adj_def_rtg").mean())).alias("def_dev"),
    )
    got = last_season_prior(prev, lam=lam, regression=r)
    assert got.schema["team_id"] == want.schema["team_id"] == pl.Int64
    assert got.height == want.height > 0
    for c in ("off_dev", "def_dev"):
        assert (got[c] - want[c]).abs().max() < 1e-12
    with pytest.raises(ValueError, match="one season"):
        last_season_prior(pl.concat([prev, _rows(lg)]), lam=lam, regression=r)


def test_adapters_pass_their_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """The carried BB-D3 ranks are gated by the adapter's own league (``rate_gates.TEAM_MIN_N``)."""
    seen: list[str] = []
    real = mod.team_season_rates

    def spy(*args: object, **kwargs: object) -> pl.DataFrame:
        seen.append(str(kwargs["level"]))
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(mod, "team_season_rates", spy)
    for lg in ("mbb", "wbb"):
        _strength(lg)
    assert seen == ["mbb", "wbb"]
