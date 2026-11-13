"""College pregame projections (GOP Q1 / Q4) on the real 2026-27 men's season through 2026-11-20.

The fixtures are the released schedule, team box and preseason prior
(tools/models/mbb_prediction/build_pregame_projection_fixture.py; tests/fixtures/mbb_prediction/README.md).
"""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import mbb_pregame_projection
from sportsdataverse.mbb.mbb_game_predict import predict_margin, win_prob_from_margin
from sportsdataverse.mbb.mbb_pregame_projection import (
    PREGAME_PROJECTION_SCHEMA,
    college_pregame_projections,
    mbb_pregame_projections,
)
from sportsdataverse.mbb.mbb_team_ratings import (
    _blend_ratings,
    _normalize_schedule,
    adjust_efficiency,
    raw_game_efficiency,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_prediction"
SCHED = pl.read_parquet(FIX / "pregame_projection_schedule_2027.parquet")
BOX = pl.read_parquet(FIX / "pregame_projection_team_box_2027.parquet")
PRIOR = pl.read_parquet(FIX / "pregame_projection_prior_2027.parquet")


@pytest.fixture(scope="module")
def eff() -> pl.DataFrame:
    return raw_game_efficiency(_normalize_schedule(SCHED), BOX)


@pytest.fixture(scope="module")
def off() -> pl.DataFrame:
    return college_pregame_projections(SCHED, BOX, prior=PRIOR, prior_games=0)


@pytest.fixture(scope="module")
def on() -> pl.DataFrame:
    return college_pregame_projections(SCHED, BOX, prior=PRIOR)


def _em(eff: pl.DataFrame, cutoff: datetime.date, **kw: object) -> dict[str, float]:
    return dict(adjust_efficiency(eff.filter(pl.col("date") < cutoff), **kw).select("team_id", "adj_em").iter_rows())


def test_one_row_per_game_in_the_schema(off: pl.DataFrame) -> None:
    assert off.schema == pl.Schema(PREGAME_PROJECTION_SCHEMA)
    assert off.height == SCHED["game_id"].n_unique() and off["game_id"].is_unique().all()
    assert (off["as_of_date"] <= off["game_date"]).all()


def test_blend_off_is_the_cards_number(off: pl.DataFrame, eff: pl.DataFrame) -> None:
    """Prior off: each game is #434's closed form on the season-to-date AdjEM through the day before."""
    rated = off.filter(pl.col("rated_home_prob").is_not_null())
    assert rated.height > 700
    assert (off["prior_games"] == 0).all() and (off["home_rating"] == off["home_rating_raw"]).fill_null(True).all()
    checked = 0
    for (cutoff,), sub in rated.group_by("as_of_date"):
        em = _em(eff, cutoff)
        for g in sub.iter_rows(named=True):
            h, a = em[g["home_team_id"]], em[g["away_team_id"]]
            assert g["home_rating"] == h and g["away_rating"] == a
            margin = predict_margin(h, a, g["neutral_site"])
            assert g["exp_home_margin"] == pytest.approx(margin, abs=1e-12)
            assert g["rated_home_prob"] == pytest.approx(win_prob_from_margin(margin), abs=1e-12)
            checked += 1
    assert checked == rated.height


def test_upcoming_games_are_rated_on_every_game_played(off: pl.DataFrame, eff: pl.DataFrame) -> None:
    last = eff["date"].max()
    ahead = off.filter(pl.col("game_date") > last)
    assert ahead.height > 100
    assert (ahead["as_of_date"] == last + datetime.timedelta(days=1)).all()
    em = _em(eff, last + datetime.timedelta(days=1))
    g = ahead.filter(pl.col("rated_home_prob").is_not_null()).row(0, named=True)
    assert g["home_rating"] == em[g["home_team_id"]]


def test_same_day_games_never_rate_each_other() -> None:
    """Moving one day's scores must leave that day's projections alone and move the next day's."""
    days = SCHED.filter(pl.col("status_type_completed") == True)["game_date"].unique().sort()  # noqa: E712
    day, nxt = days[len(days) // 2], days[len(days) // 2 + 1]
    on_day = SCHED.filter(pl.col("game_date") == day)["game_id"]
    moved = BOX.with_columns(
        pl.when(pl.col("game_id").is_in(on_day.implode())).then(pl.col("team_score") + 25).otherwise("team_score")
    )
    base = college_pregame_projections(SCHED, BOX, prior=PRIOR)
    cut = college_pregame_projections(SCHED, moved, prior=PRIOR)

    def probs(df: pl.DataFrame, d: datetime.date) -> list[float | None]:
        return df.filter(pl.col("game_date") == d).sort("game_id")["rated_home_prob"].to_list()

    assert probs(base, day) == probs(cut, day)
    assert probs(base, nxt) != probs(cut, nxt)


def test_blend_is_the_season_sims_prior(on: pl.DataFrame, eff: pl.DataFrame) -> None:
    """Prior on: 4 games of last season's regressed rating, the engine's own fixed point."""
    assert (on["prior_games"] == 4.0).all()
    cutoff = on.filter(pl.col("home_games") > 0)["as_of_date"].min()
    em = _em(eff, cutoff, prior=PRIOR, prior_weight=4.0)
    for g in on.filter(pl.col("as_of_date") == cutoff).drop_nulls("rated_home_prob").iter_rows(named=True):
        assert g["home_rating"] == em[g["home_team_id"]]
        assert g["exp_home_margin"] == pytest.approx(
            predict_margin(em[g["home_team_id"]], em[g["away_team_id"]], g["neutral_site"])
        )
    # a team that has not played is its prior: off_dev - def_dev
    first = on.filter(pl.col("as_of_date") == on["as_of_date"].min()).drop_nulls("home_rating").row(0, named=True)
    p = PRIOR.filter(pl.col("team_id") == first["home_team_id"]).row(0, named=True)
    assert first["home_games"] == 0 and first["home_rating"] == pytest.approx(p["off_dev"] - p["def_dev"])


def test_what_the_blend_changes(off: pl.DataFrame, on: pl.DataFrame) -> None:
    """Observed on the fixture (2026-11-13): the blend rates every D-I opener and halves the extremes."""
    both = off.select("game_id", pl.col("rated_home_prob").alias("p0")).join(
        on.select("game_id", pl.col("rated_home_prob").alias("p4")), on="game_id"
    )
    assert on["rated_home_prob"].is_not_null().sum() > off["rated_home_prob"].is_not_null().sum()
    rated = both.drop_nulls()

    def extreme(c: str) -> float:
        return float((~rated[c].is_between(0.01, 0.99)).mean())

    assert extreme("p4") < 0.5 * extreme("p0")
    # the spread of ratings shrinks toward last season's
    assert on["home_rating"].std() < off["home_rating"].std()


def test_blend_columns_on_the_ratings_table(eff: pl.DataFrame) -> None:
    raw = adjust_efficiency(eff)
    blend = _blend_ratings(eff, raw, PRIOR, league="mens")
    assert (blend["prior_games"] == 4.0).all() and blend.height >= raw.height
    want = dict(adjust_efficiency(eff, prior=PRIOR, prior_weight=4.0).select("team_id", "adj_em").iter_rows())
    assert dict(blend.select("team_id", "adj_em_blend").iter_rows()) == want
    none = _blend_ratings(eff, raw, PRIOR.clear(), league="mens")
    assert (none["prior_games"] == 0.0).all() and none["adj_em_blend"].to_list() == raw["adj_em"].to_list()


def test_wiring_reads_the_prior_only_when_blending(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []
    monkeypatch.setattr(mbb_pregame_projection, "_league_loaders", lambda league: (lambda s: SCHED, lambda s: BOX))

    def prior(season: int, schedule: pl.DataFrame, *, league: str) -> pl.DataFrame:
        calls.append(season)
        return PRIOR

    monkeypatch.setattr(mbb_pregame_projection, "load_preseason_prior", prior)
    out = mbb_pregame_projections(2027)
    assert calls == [2027] and (out["prior_games"] == 4.0).all()
    assert (mbb_pregame_projections(2027, prior_games=0)["prior_games"] == 0.0).all() and calls == [2027]
    with pytest.raises(ValueError, match="league"):
        mbb_pregame_projections(2027, league="mbb")
