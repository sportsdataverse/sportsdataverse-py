"""Season Monte-Carlo tests (T2.1 Phase 4).

Task 4.1 exercises the ratings-driven ``compute_results`` closure against the
``cfb_simulations`` engine contract (fills each unplayed week-``week_num`` game's
``result`` = sampled home margin), with a seeded rng for determinism.
"""

from __future__ import annotations

import datetime
import sys

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds, make_ratings_compute_results

_mod = sys.modules["sportsdataverse.cfb.cfb_season_odds"]


def test_ratings_compute_results_fills_and_favors_strong_team() -> None:
    """The closure returns {teams, games} and fills the target week's result."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.35, -0.35]})
    cr = make_ratings_compute_results(ratings)
    teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
    games = pl.DataFrame(
        {"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"], "neutral": [0], "result": [None]}
    )
    out = cr(teams, games, 1, rng=np.random.default_rng(0))
    assert set(out.keys()) == {"teams", "games"}
    assert out["games"].filter(pl.col("week") == 1)["result"].item() is not None


def test_strong_home_team_wins_on_average() -> None:
    """A (>> B) at home wins the large majority of a 400-sim batch of the same game.

    The rating differential is DERIVED from the target margin rather than
    hardcoded. A previous version fixed adj_net at +/-0.35, which was a ~33
    point favorite under net_points_scale=44.5367 and only ~20 under the
    refit 24.6578 -- so the test silently changed what it was asserting when
    the constants moved. Deriving it keeps the intent ("a 30-point home
    favorite wins nearly always") stable across any future refit.
    """
    from sportsdataverse.cfb.cfb_prediction_constants import get_constants

    c = get_constants()
    target_margin = 30.0
    half = (target_margin - c.hfa_points) / c.net_points_scale / 2.0
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [half, -half]})
    cr = make_ratings_compute_results(ratings)
    n = 400
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    games = pl.DataFrame(
        {
            "sim": list(range(1, n + 1)),
            "week": [1] * n,
            "home_team": ["A"] * n,
            "away_team": ["B"] * n,
            "neutral": [0] * n,
            "result": [None] * n,
        }
    )
    out = cr(teams, games, 1, rng=np.random.default_rng(1))
    res = out["games"]["result"]
    assert res.null_count() == 0
    assert res.mean() > 15.0  # A ~25pt favorite -> strongly positive home margin
    assert (res > 0).mean() > 0.85


def test_only_target_week_is_filled() -> None:
    """Games in other weeks keep their null result; teams pass through unchanged."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.2, -0.2]})
    cr = make_ratings_compute_results(ratings)
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    games = pl.DataFrame(
        {
            "sim": [1, 1],
            "week": [1, 2],
            "home_team": ["A", "A"],
            "away_team": ["B", "B"],
            "neutral": [0, 0],
            "result": [None, None],
        }
    )
    out = cr(teams, games, 1, rng=np.random.default_rng(0))
    g = out["games"]
    assert g.filter(pl.col("week") == 1)["result"].item() is not None
    assert g.filter(pl.col("week") == 2)["result"].item() is None
    assert out["teams"].equals(teams)


def test_neutral_site_drops_home_field() -> None:
    """Equal teams: a home game favors home; a neutral one is a coin flip (mean ~0)."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.0, 0.0]})
    cr = make_ratings_compute_results(ratings)
    n = 600
    base = {
        "sim": list(range(1, n + 1)),
        "week": [1] * n,
        "home_team": ["A"] * n,
        "away_team": ["B"] * n,
        "result": [None] * n,
    }
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    home = cr(teams, pl.DataFrame({**base, "neutral": [0] * n}), 1, rng=np.random.default_rng(2))
    neut = cr(teams, pl.DataFrame({**base, "neutral": [1] * n}), 1, rng=np.random.default_rng(2))
    assert home["games"]["result"].mean() > neut["games"]["result"].mean()
    assert abs(neut["games"]["result"].mean()) < 2.0


def test_postseason_games_never_tie() -> None:
    """A POST game re-breaks a sampled 0 (single-elim can't tie)."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.0, 0.0]})
    cr = make_ratings_compute_results(ratings)
    n = 500
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    games = pl.DataFrame(
        {
            "sim": list(range(1, n + 1)),
            "week": [15] * n,
            "game_type": ["POST"] * n,
            "home_team": ["A"] * n,
            "away_team": ["B"] * n,
            "neutral": [1] * n,
            "result": [None] * n,
        }
    )
    out = cr(teams, games, 15, rng=np.random.default_rng(3))
    assert (out["games"]["result"] == 0).sum() == 0


# --- Task 4.2: public cfb_season_odds wrapper --------------------------------

_IDS = [str(i) for i in range(1, 9)]  # 8 teams, ids "1".."8"
_CONF = {**{i: "X" for i in _IDS[:4]}, **{i: "Y" for i in _IDS[4:]}}


def _fake_ratings() -> pl.DataFrame:
    """Team "1" clearly best, descending to "8" (ESPN-id-style Utf8 keys)."""
    nets = [0.45, 0.20, 0.05, -0.05, 0.15, 0.00, -0.15, -0.35]
    return pl.DataFrame({"season": [2023] * 8, "team_id": _IDS, "adj_net": nets})


def _fake_schedule() -> pl.DataFrame:
    """Intra-conference round-robins, all unplayed (real-loader id/points columns)."""
    rows = []
    wk = 1
    for conf_ids in (_IDS[:4], _IDS[4:]):
        for i in range(len(conf_ids)):
            for j in range(i + 1, len(conf_ids)):
                rows.append((conf_ids[i], conf_ids[j], wk))
                wk = wk % 12 + 1
    return pl.DataFrame(
        {
            "season": [2023] * len(rows),
            "week": [w for _, _, w in rows],
            "season_type": ["regular"] * len(rows),
            "home_id": [int(h) for h, _, _ in rows],
            "away_id": [int(a) for _, a, _ in rows],
            "home_conference": [_CONF[h] for h, _, _ in rows],
            "away_conference": [_CONF[a] for _, a, _ in rows],
            "home_points": [None] * len(rows),
            "away_points": [None] * len(rows),
            "neutral_site": [False] * len(rows),
        }
    )


def _patch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_mod, "cfb_ratings", lambda *a, **k: _fake_ratings())
    monkeypatch.setattr(_mod, "load_cfb_schedule", lambda *a, **k: _fake_schedule())


def test_season_odds_schema_and_probabilities(monkeypatch: pytest.MonkeyPatch) -> None:
    """Output carries the documented columns; every probability is in [0, 1]."""
    _patch(monkeypatch)
    out = cfb_season_odds(2023, n_sims=300, playoff_seeds=4, seed=0)
    assert out.columns == [
        "season",
        "team_id",
        "exp_wins",
        "conf_title_prob",
        "playoff_prob",
        "first_round_bye_prob",
        "cfp_champ_prob",
    ]
    assert out.schema["team_id"] == pl.Utf8
    assert out.height == 8
    for col in ("conf_title_prob", "playoff_prob", "first_round_bye_prob", "cfp_champ_prob"):
        assert out[col].min() >= 0.0 and out[col].max() <= 1.0, col


def test_dominant_team_leads_conference_odds(monkeypatch: pytest.MonkeyPatch) -> None:
    """The best team in a conference has the highest conf-title probability there."""
    _patch(monkeypatch)
    out = cfb_season_odds(2023, n_sims=400, playoff_seeds=4, seed=1)
    conf_x = out.filter(pl.col("team_id").is_in(_IDS[:4]))
    top = conf_x.sort("conf_title_prob", descending=True).row(0, named=True)
    assert top["team_id"] == "1"
    assert top["conf_title_prob"] > 0.5


def test_season_odds_return_as_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """``return_as_pandas=True`` yields a pandas frame with the same columns."""
    import pandas as pd

    _patch(monkeypatch)
    out = cfb_season_odds(2023, n_sims=100, playoff_seeds=4, seed=0, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert list(out.columns)[:2] == ["season", "team_id"]


def test_season_odds_rejects_multiple_seasons() -> None:
    """Multiple seasons raise ValueError -- the cfb_simulations engine is
    single-season and would otherwise mix weeks across seasons."""
    with pytest.raises(ValueError, match="one season at a time"):
        cfb_season_odds([2022, 2023])


# --- #333: the simulated universe is the FBS field ---------------------------

_FBS_IDS = [str(i) for i in range(1, 5)]  # "1".."4" -- a two-conference FBS field
_NON_FBS_IDS = ["90", "91", "92", "93"]  # FCS/D2 cupcakes on the FBS schedule
_FBS_CONF = {**{i: "X" for i in _FBS_IDS[:2]}, **{i: "Y" for i in _FBS_IDS[2:]}}


def _mixed_schedule() -> pl.DataFrame:
    """FBS round-robin (unplayed) plus one played non-FBS body-bag game per FBS team.

    Mirrors the real shape from issue #333: the schedule carries every opponent an FBS
    team played, and those non-FBS programs are unrated -- so the sampler scored them
    as median FBS teams and the engine let them into the playoff field.
    """
    rows: list[tuple[str, str, int, str, object, object]] = []
    wk = 1
    for i in range(len(_FBS_IDS)):
        for j in range(i + 1, len(_FBS_IDS)):
            rows.append((_FBS_IDS[i], _FBS_IDS[j], wk, "fbs", None, None))
            wk = wk % 10 + 1
    for k, fid in enumerate(_FBS_IDS):
        rows.append((fid, _NON_FBS_IDS[k], 11, "fcs", 42, 7))
    return pl.DataFrame(
        {
            "season": [2023] * len(rows),
            "week": [w for _, _, w, _, _, _ in rows],
            "season_type": ["regular"] * len(rows),
            "start_date": ["2023-09-02T18:00:00.000Z"] * len(rows),
            "home_id": [int(h) for h, _, _, _, _, _ in rows],
            "away_id": [int(a) for _, a, _, _, _, _ in rows],
            "home_division": ["fbs"] * len(rows),
            "away_division": [ad for _, _, _, ad, _, _ in rows],
            "home_conference": [_FBS_CONF.get(h) for h, _, _, _, _, _ in rows],
            "away_conference": [_FBS_CONF.get(a) for _, a, _, _, _, _ in rows],
            "home_points": [hp for _, _, _, _, hp, _ in rows],
            "away_points": [ap for _, _, _, _, _, ap in rows],
            "neutral_site": [False] * len(rows),
        }
    )


def _fbs_ratings() -> pl.DataFrame:
    return pl.DataFrame({"season": [2023] * 4, "team_id": _FBS_IDS, "adj_net": [0.45, 0.20, 0.05, -0.05]})


def _patch_fbs(monkeypatch: pytest.MonkeyPatch, sched: pl.DataFrame | None = None) -> None:
    monkeypatch.setattr(_mod, "cfb_ratings", lambda *a, **k: _fbs_ratings())
    monkeypatch.setattr(_mod, "load_cfb_schedule", lambda *a, **k: sched if sched is not None else _mixed_schedule())


def test_non_fbs_teams_are_not_simulated(monkeypatch: pytest.MonkeyPatch) -> None:
    """No unrated non-FBS program reaches the output or the championship board.

    Regression for #333, where 571 FCS/D2/D3/NAIA teams were scored as league-average
    (0.0) and took ~21% of the championship probability on the real 2023 slate.
    """
    _patch_fbs(monkeypatch)
    out = cfb_season_odds(2023, n_sims=200, playoff_seeds=2, seed=0)
    assert set(out["team_id"].to_list()) == set(_FBS_IDS)
    assert out.filter(pl.col("team_id").is_in(_NON_FBS_IDS)).height == 0
    # The whole championship mass now lands on the FBS field.
    assert out["cfp_champ_prob"].sum() == pytest.approx(1.0, abs=1e-9)


def test_non_fbs_games_still_count_toward_fbs_records(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-FBS opponents are dropped as CONTENDERS, not as opponents.

    Asserted as a DIFFERENTIAL against the same slate with the body-bag games deleted.
    Both runs share a team/conference structure, so the engine generates the same
    postseason games and adds the same total wins -- the only remaining difference is
    the one banked non-FBS win per team. Filtering `games` instead of `teams` would
    erase those wins.
    """
    _patch_fbs(monkeypatch)
    with_non = cfb_season_odds(2023, n_sims=200, playoff_seeds=2, seed=0)

    monkeypatch.setattr(
        _mod, "load_cfb_schedule", lambda *a, **k: _mixed_schedule().filter(pl.col("away_division") == "fbs")
    )
    without = cfb_season_odds(2023, n_sims=200, playoff_seeds=2, seed=0)

    delta = with_non["exp_wins"].mean() - without["exp_wins"].mean()
    assert delta == pytest.approx(1.0, abs=1e-9), "the banked non-FBS win vanished"


def test_empty_fbs_filter_raises_rather_than_shipping_an_empty_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An id-namespace mismatch must raise, not silently return a zero-row board.

    Exercises the ratings-membership fallback (schedule without division columns)
    against ratings whose ids do not intersect the schedule's -- the float-origin
    ``"123.0"`` / zero-padding class of id bug. Without the guard it surfaces as an
    empty board presented as a result.
    """
    sched = _mixed_schedule().drop("home_division", "away_division")
    ratings = pl.DataFrame({"season": [2023] * 4, "team_id": ["1.0", "2.0", "3.0", "4.0"], "adj_net": [0.0] * 4})
    monkeypatch.setattr(_mod, "cfb_ratings", lambda *a, **k: ratings)
    monkeypatch.setattr(_mod, "load_cfb_schedule", lambda *a, **k: sched)
    with pytest.raises(ValueError, match="FBS filter emptied"):
        cfb_season_odds(2023, n_sims=10, playoff_seeds=2, seed=0)


# --- #334: as_of_date bounds the GAME SET, not just the ratings vintage ------

_AS_OF = datetime.date(2023, 10, 1)


def _dated_schedule(*, late_home: int = 42, late_away: int = 7) -> pl.DataFrame:
    """A COMPLETED season: FBS round-robin played before the boundary, return leg after.

    Every game carries a real score -- that is the #334 setup. The post-boundary scores
    are parameterized so a test can flip them and assert the forecast does not move.
    """
    pairs = [(a, b) for i, a in enumerate(_FBS_IDS) for b in _FBS_IDS[i + 1 :]]
    rows = [(h, a, w, f"2023-09-0{min(w, 9)}") for w, (h, a) in enumerate(pairs, start=1)]
    rows += [(a, h, w + 6, f"2023-11-0{min(w, 9)}") for w, (h, a) in enumerate(pairs, start=1)]
    is_late = [d >= "2023-10" for _, _, _, d in rows]
    return pl.DataFrame(
        {
            "season": [2023] * len(rows),
            "week": [w for _, _, w, _ in rows],
            "season_type": ["regular"] * len(rows),
            "start_date": [f"{d}T18:00:00.000Z" for _, _, _, d in rows],
            "home_id": [int(h) for h, _, _, _ in rows],
            "away_id": [int(a) for _, a, _, _ in rows],
            "home_division": ["fbs"] * len(rows),
            "away_division": ["fbs"] * len(rows),
            "home_conference": [_FBS_CONF[h] for h, _, _, _ in rows],
            "away_conference": [_FBS_CONF[a] for _, a, _, _ in rows],
            "home_points": [late_home if late else 21 for late in is_late],
            "away_points": [late_away if late else 14 for late in is_late],
            "neutral_site": [False] * len(rows),
        }
    )


def test_as_of_date_simulates_post_boundary_games(monkeypatch: pytest.MonkeyPatch) -> None:
    """A completed-season forecast from mid-season is a FORECAST, not a replay.

    Regression for #334: with every score present, probabilities came back exactly
    0.0/1.0 and exp_wins were exact integers -- the signature of nothing simulated.
    """
    _patch_fbs(monkeypatch, _dated_schedule())
    out = cfb_season_odds(2023, as_of_date=_AS_OF, n_sims=300, playoff_seeds=2, seed=0)
    non_integer = out.filter(pl.col("exp_wins") != pl.col("exp_wins").round(0))
    assert non_integer.height > 0, "no game was simulated -- as_of_date did not bound the game set"
    spread = out.filter((pl.col("playoff_prob") > 0.02) & (pl.col("playoff_prob") < 0.98))
    assert spread.height > 0, "playoff_prob is degenerate -- the post-boundary slate was replayed"


def _same_board(a: pl.DataFrame, b: pl.DataFrame) -> bool:
    """Compare two odds frames by VALUE, keyed on team_id, ignoring row order.

    ``cfb_season_odds`` sorts on probabilities alone, so tied teams come back in an
    arbitrary order run to run (polars ``unique`` does not promise input order either).
    ``DataFrame.equals`` therefore reports a difference where none exists -- comparing
    it directly makes a leakage assertion flaky in both directions.
    """
    return a.sort("team_id").equals(b.sort("team_id"))


def test_as_of_date_ignores_post_boundary_results(monkeypatch: pytest.MonkeyPatch) -> None:
    """THE leakage gate: flipping every post-boundary score changes nothing.

    Asserts on the OUTPUT, not that a filter ran. The control half proves the
    perturbation is real -- without ``as_of_date`` the same flip moves the board.
    """
    _patch_fbs(monkeypatch, _dated_schedule(late_home=42, late_away=7))
    forecast = cfb_season_odds(2023, as_of_date=_AS_OF, n_sims=200, playoff_seeds=2, seed=0)
    replay = cfb_season_odds(2023, n_sims=200, playoff_seeds=2, seed=0)

    _patch_fbs(monkeypatch, _dated_schedule(late_home=7, late_away=42))  # every result flipped
    forecast_flipped = cfb_season_odds(2023, as_of_date=_AS_OF, n_sims=200, playoff_seeds=2, seed=0)
    replay_flipped = cfb_season_odds(2023, n_sims=200, playoff_seeds=2, seed=0)

    assert _same_board(forecast, forecast_flipped), "post-as_of_date results leaked into the forecast"
    assert not _same_board(replay, replay_flipped), "control failed: the perturbation is a no-op"


def test_as_of_date_boundary_is_exclusive_of_the_future() -> None:
    """A game kicking off ON ``as_of_date`` is unknowable and must be simulated.

    Matches ``cfb_ratings``, which fits on ``date < as_of_date``. An inclusive boundary
    would hand the forecast a day of real results.
    """
    sched = _dated_schedule().with_columns(
        pl.when(pl.col("week") == 1)
        .then(pl.lit("2023-10-01T18:00:00.000Z"))
        .otherwise(pl.col("start_date"))
        .alias("start_date"),
        # the wrapper re-keys home_team/away_team onto the ESPN ids before conversion
        pl.col("home_id").cast(pl.Utf8).alias("home_team"),
        pl.col("away_id").cast(pl.Utf8).alias("away_team"),
    )
    games = _mod.cfb_games_from_schedule(_mod._mask_after(sched, _AS_OF))
    wk1 = games.filter(pl.col("week") == 1)
    assert wk1.height > 0
    assert wk1["result"].null_count() == wk1.height, "a game ON as_of_date was treated as played"
    # ...while a game the day before stays played.
    wk2 = games.filter(pl.col("week") == 2)
    assert wk2["result"].null_count() == 0, "a game BEFORE as_of_date was masked"


def test_as_of_date_drops_unplayed_postseason_matchups(monkeypatch: pytest.MonkeyPatch) -> None:
    """A masked bowl/CFP row is removed -- the matchup is itself an outcome.

    Keeping it would leak the real bracket into the forecast; the engine regenerates
    the postseason from each sim's own standings.
    """
    sched = _dated_schedule().with_columns(
        pl.when(pl.col("week") > 6).then(pl.lit("postseason")).otherwise(pl.col("season_type")).alias("season_type")
    )
    _patch_fbs(monkeypatch, sched)
    out = cfb_season_odds(2023, as_of_date=_AS_OF, n_sims=100, playoff_seeds=2, seed=0)
    assert set(out["team_id"].to_list()) == set(_FBS_IDS)
    assert out["cfp_champ_prob"].sum() == pytest.approx(1.0, abs=1e-9)
    assert out.filter((pl.col("playoff_prob") > 0.02) & (pl.col("playoff_prob") < 0.98)).height > 0


def test_season_odds_is_bit_reproducible_under_row_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same seed + same teams + DIFFERENT schedule row order => identical odds.

    The team universe is built with polars `.unique()`, which carries no order
    guarantee, and that row order is what the seeded RNG draws map onto. So the
    seed makes the DRAWS reproducible while the ORDERING stays free, and a
    tiebreak-sensitive team can move by ~1/n_sims between identical calls.

    Shuffling the input rows is a deterministic stand-in for that: it changes
    what `.unique(keep='first')` sees first, exactly as a different scan order
    would. Reproducibility is what `seed=` promises callers."""
    sched = _fake_schedule()

    monkeypatch.setattr(_mod, "cfb_ratings", lambda *a, **k: _fake_ratings())
    monkeypatch.setattr(_mod, "load_cfb_schedule", lambda *a, **k: sched)
    first = cfb_season_odds(2023, n_sims=200, playoff_seeds=4, seed=7)

    reversed_sched = sched.reverse()
    monkeypatch.setattr(_mod, "load_cfb_schedule", lambda *a, **k: reversed_sched)
    second = cfb_season_odds(2023, n_sims=200, playoff_seeds=4, seed=7)

    assert first.sort("team_id").equals(second.sort("team_id")), (
        "identical seed produced different odds under a different input row order"
    )
