"""Season Monte-Carlo tests (T2.1 Phase 4).

Task 4.1 exercises the ratings-driven ``compute_results`` closure against the
``cfb_simulations`` engine contract (fills each unplayed week-``week_num`` game's
``result`` = sampled home margin), with a seeded rng for determinism.
"""

from __future__ import annotations

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
    """A (>> B) at home wins the large majority of a 400-sim batch of the same game."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.35, -0.35]})
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
