"""Guards of tools/compare_college_team_strength.py: the as-of split and the paired scoring (no network)."""

import datetime as dt
import importlib.util
from pathlib import Path

import numpy as np
import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "compare_college_team_strength", _ROOT / "tools" / "compare_college_team_strength.py"
)
cmp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cmp)

START = dt.date(2024, 11, 4)


def _season(monkeypatch: pytest.MonkeyPatch) -> list[tuple[dt.date | None, dt.date | None]]:
    """One game a day for 30 days between two teams; ``_ratings`` records the latest date it saw."""
    days = [START + dt.timedelta(days=i) for i in range(30)]
    sched = pl.DataFrame(
        {
            "game_id": [str(i) for i in range(30)],
            "date": days,
            "completed": [True] * 30,
            "season_type": [2] * 30,
            "home_team_id": ["1", "2"] * 15,
            "away_team_id": ["2", "1"] * 15,
            "home_score": [70 + i % 3 for i in range(30)],
            "away_score": [69] * 30,
            "neutral_site": [False] * 30,
        }
    )
    eff = pl.DataFrame({"date": days})
    rows = pl.DataFrame({"date": days})
    teams = pl.DataFrame({"team_id": ["1", "2"]})
    seen: list[tuple[dt.date | None, dt.date | None]] = []

    def ratings(lg: str, e: pl.DataFrame, r: pl.DataFrame, lam: float) -> dict[str, dict[str, float]]:
        seen.append((e["date"].max(), r["date"].max()))
        return {"fixed": {"1": 2.0, "2": -2.0}, "ridge": {"1": 1.0, "2": -1.0}}

    monkeypatch.setattr(cmp, "load_college", lambda lg, s, c: (sched, eff, teams))
    monkeypatch.setattr(cmp, "load_rows", lambda lg, s, c: rows)
    monkeypatch.setattr(cmp, "_ratings", ratings)
    return seen


def test_ratings_never_see_the_games_they_score(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _season(monkeypatch)
    games, cuts = cmp.season_frames("mbb", 2025, Path("."), 200.0)
    assert seen[0] == (START + dt.timedelta(days=29),) * 2  # the season-end fit sees everything
    as_of = seen[1:]
    assert len(as_of) == cuts.height == 4  # weeks 1-4; week 0 is skipped
    for (week, eff_max, rows_max), (w,) in zip(
        [(k + 1, *dates) for k, dates in enumerate(as_of)], cuts.select("week").rows(), strict=True
    ):
        cut = START + dt.timedelta(days=7 * week)
        assert w == week
        assert eff_max < cut and rows_max < cut  # strictly before the cutoff, both methods
        scored = games.filter(pl.col("week") == week)
        assert scored.height == 7 or week == 4  # the 7 days from the cutoff (2 left in week 4)
    assert games["game_id"].n_unique() == games.height  # each game scored once
    assert games.height == 30 - 7


def test_paired_difference_is_ridge_minus_fixed() -> None:
    frame = pl.DataFrame(
        {
            "season": [2025] * 4,
            "week": [1, 1, 2, 2],
            "y": [1.0, 0.0, 1.0, 1.0],
            "margin": [5.0, -3.0, 1.0, 9.0],
            "p_fixed": [0.7, 0.4, 0.5, 0.8],
            "p_ridge": [0.6, 0.3, 0.55, 0.9],
            "mu_fixed": [4.0, -1.0, 0.0, 6.0],
            "mu_ridge": [3.0, -2.0, 2.0, 8.0],
        }
    )
    out = cmp.score(frame)
    for key in ("log_loss", "brier", "margin_mae"):
        paired = out[f"ridge_minus_fixed_{key}"]
        assert np.isclose(paired["diff"], out["ridge"][key] - out["fixed"][key])
        assert paired["weeks"] == 2
    # absolute margin errors: fixed 1, 2, 1, 3 and ridge 2, 1, 1, 1; the clustered SE by hand
    d = np.array([1.0, -1.0, 0.0, -2.0])
    by_week = np.array([d[:2].sum(), d[2:].sum()]) - 2 * d.mean()
    assert np.isclose(out["ridge_minus_fixed_margin_mae"]["se_week"], np.sqrt((by_week**2).sum()) / 4)


@pytest.mark.parametrize(
    ("train", "holdout", "message"),
    [
        (["2016"], ["2016", "2025"], "train and holdout seasons must be disjoint"),
        (["2024"], ["2023", "2025"], "penalty's fit seasons"),
        (["2024", "2024"], ["2025"], "--train repeats a season"),
        (["2024"], ["2025", "2025"], "--holdout repeats a season"),
    ],
)
def test_holdout_must_be_unseen(
    train: list[str],
    holdout: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pinned here, so a later refit's fit seasons cannot send the second case on to compare() (and the network)
    monkeypatch.setattr(cmp, "COLLEGE_RIDGE_FIT_SEASONS", (2016, 2023))
    monkeypatch.setattr(cmp, "compare", lambda *a, **k: pytest.fail("the season guards did not fire"))
    with pytest.raises(SystemExit) as exc:
        cmp.main(["--league", "mbb", "--train", *train, "--holdout", *holdout])
    assert exc.value.code == 2
    assert message in capsys.readouterr().err


def test_undefined_spearman_is_null() -> None:
    # one shared team (or a constant rating) has no rank correlation: null, so the phase means skip it
    assert cmp._spearman({"a": 1.0}, {"a": 2.0}, {"a"}) is None
    assert cmp._spearman({"a": 1.0, "b": 1.0}, {"a": 2.0, "b": 3.0}, {"a", "b"}) is None
    assert cmp._spearman({"a": 1.0, "b": 2.0}, {"a": 2.0, "b": 3.0}, {"a", "b"}) == pytest.approx(1.0)


def test_a_train_phase_without_games_is_named(monkeypatch: pytest.MonkeyPatch) -> None:
    # train scores only weeks 1-4, so the 5-8 and 9+ mappings cannot be fit: a named error, not a polars one
    frame = pl.DataFrame(
        {"week": [1, 2], "margin": [3.0, -2.0], "home": [1.0, 1.0], "d_fixed": [1.0, -1.0], "d_ridge": [1.0, -1.0]}
    )
    monkeypatch.setattr(cmp, "season_frames", lambda *a, **k: (frame, pl.DataFrame()))
    with pytest.raises(ValueError, match=r"phase\(s\) \['5-8', '9-'\]"):
        cmp.compare("mbb", [2024], [2025], Path("."), 200.0)
