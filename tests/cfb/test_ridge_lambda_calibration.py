"""The CFB ridge must actually adjust for opponent, not just relabel raw EPA.

The shipped default was `_RIDGE_LAMBDA = 325`, ported from cfbfastR's glmnet
`cv$lambda[[1]]`. Because `dropped_level_ridge` forms the sklearn penalty as
`alpha = ridge_lambda * n_plays`, that landed at alpha ~ 2e7 on a real season and
crushed every team coefficient to ~0. The output still looked plausible -- it was
raw EPA/play, which is correlated with quality -- so rank-correlation checks on
the *output* passed while the adjustment did nothing. Group of 5 teams kept full
credit for weak schedules (Toledo 8th nationally, James Madison 5th) and Power
conference teams were buried (Florida 114th).

These tests are built on a synthetic league with KNOWN team strengths and an
unbalanced schedule, so "did the adjustment happen" is checkable offline with no
oracle snapshot and no network.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_adjusted_epa import _RIDGE_LAMBDA

sklearn = pytest.importorskip("sklearn", reason="dropped_level_ridge requires scikit-learn")

from sportsdataverse._common.ratings import dropped_level_ridge  # noqa: E402

# 12 teams, strength descending. The top 4 play only each other (a strong
# conference) and the bottom 4 play only each other, so RAW per-play EPA
# understates the strong group and flatters the weak one -- exactly the schedule
# imbalance an opponent adjustment exists to undo.
_N_TEAMS = 12
_TRUE = {f"T{i:02d}": 0.35 - 0.07 * i for i in range(_N_TEAMS)}


def _synthetic_league() -> pl.DataFrame:
    """Plays whose EPA is (offense strength - defense strength) plus a fixed wobble.

    The schedule is unbalanced but CONNECTED, which is what makes this a fair
    test. Each team plays its own 3 group-mates twice (so raw EPA is dominated by
    in-group opponents and therefore biased by group strength) plus one game
    against two teams from each other group (so cross-group strength is still
    identifiable). An earlier version of this fixture had only a thin bridge
    between groups; that made cross-group strength nearly unidentifiable and the
    ridge could not beat raw EPA no matter the lambda -- a badly conditioned
    fixture, not a finding.
    """
    rows: list[dict[str, object]] = []
    teams = list(_TRUE)
    groups = [teams[0:4], teams[4:8], teams[8:12]]
    pairs: list[tuple[str, str]] = []
    for grp in groups:  # in-group double round robin
        for a in grp:
            for b in grp:
                if a != b:
                    pairs.extend([(a, b)] * 2)
    for gi, grp in enumerate(groups):  # cross-group: 2 opponents from each other group
        for other in range(len(groups)):
            if other == gi:
                continue
            for k, a in enumerate(grp):
                for off in (0, 1):
                    b = groups[other][(k + off) % 4]
                    pairs.append((a, b))
    for gi, (off, dff) in enumerate(pairs):
        for k in range(24):
            # deterministic zero-mean wobble so the fit is not a perfect fit
            wobble = ((gi * 7 + k * 13) % 11 - 5) * 0.02
            rows.append(
                {
                    "game_id": f"G{gi}",
                    "pos_team": off,
                    "pos_team_id": off,
                    "def_pos_team_id": dff,
                    "home": off if k % 2 == 0 else dff,
                    "neutral_site": False,
                    "EPA": _TRUE[off] - _TRUE[dff] + wobble,
                    "pass": 1,
                    "rush": 0,
                    "wp_before": 0.5,
                }
            )
    return pl.DataFrame(rows)


def _clean() -> pl.DataFrame:
    from sportsdataverse.cfb.cfb_adjusted_epa import _REQUIRED_COLUMNS, _prepare

    return _prepare(_synthetic_league(), _REQUIRED_COLUMNS)[1]


def _spearman(xs: list[float], ys: list[float]) -> float:
    def rank(v: list[float]) -> list[float]:
        order = sorted(range(len(v)), key=lambda i: v[i])
        out = [0.0] * len(v)
        for pos, i in enumerate(order, 1):
            out[i] = float(pos)
        return out

    a, b = rank(xs), rank(ys)
    n = len(a)
    ma, mb = sum(a) / n, sum(b) / n
    num = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    da = sum((x - ma) ** 2 for x in a) ** 0.5
    db = sum((y - mb) ** 2 for y in b) ** 0.5
    return num / (da * db)


def _recovered(lam: float) -> tuple[float, float]:
    """(spearman vs true strength, coefficient spread) for one lambda."""
    offense, _defense, _ic = dropped_level_ridge(_clean(), lam)
    got = offense.filter(pl.col("team_id").is_in(list(_TRUE)))
    fitted = got["adjmodelOff"].to_list()
    truth = [_TRUE[t] for t in got["team_id"].to_list()]
    return _spearman(fitted, truth), float(got["adjmodelOff"].std())


def test_default_lambda_recovers_true_team_strength_ordering() -> None:
    rho, spread = _recovered(_RIDGE_LAMBDA)
    assert rho > 0.9, f"default lambda={_RIDGE_LAMBDA} recovers strength ordering at only rho={rho:.3f}"
    assert spread > 0.01, (
        f"default lambda={_RIDGE_LAMBDA} produced a degenerate fit (coef sd={spread:.6f}); "
        "team strengths are shrunk to zero, so the opponent adjustment is a no-op"
    )


def test_overpenalized_lambda_is_degenerate() -> None:
    """The regression itself: the old default flattens every team to the same value.

    Guards the guard -- if this ever passes, the assertion above has stopped
    discriminating and would no longer catch a re-introduction.
    """
    rho_bad, spread_bad = _recovered(325.0)
    assert spread_bad < 0.001, f"expected lambda=325 to be degenerate, got coef sd={spread_bad:.6f}"
    _rho_good, spread_good = _recovered(_RIDGE_LAMBDA)
    assert spread_good > spread_bad * 50, "the default must be far less penalized than the old 325"
    assert rho_bad < 0.9 or spread_bad < 0.001


def test_adjustment_reorders_teams_relative_to_raw_epa() -> None:
    """An adjustment that preserves the raw ordering exactly has not adjusted.

    On the unbalanced synthetic schedule, raw per-play EPA is a materially worse
    estimate of true strength than the adjusted figure. At lambda=325 the two were
    spearman 0.999982 on real 2025 data -- indistinguishable.
    """
    plays = _synthetic_league()
    raw = plays.group_by("pos_team_id").agg(raw=pl.col("EPA").mean()).sort("pos_team_id")
    raw_rho = _spearman(raw["raw"].to_list(), [_TRUE[t] for t in raw["pos_team_id"].to_list()])
    adj_rho, _spread = _recovered(_RIDGE_LAMBDA)
    assert adj_rho > raw_rho, (
        f"opponent-adjusted strength (rho={adj_rho:.3f}) should beat raw EPA/play "
        f"(rho={raw_rho:.3f}) on an unbalanced schedule"
    )


def test_reference_level_team_is_present_in_output() -> None:
    """model.matrix drops the reference level from the DESIGN, not from the OUTPUT.

    Its effect is 0 by construction and lives in the intercept, so it belongs in
    the returned table at the intercept. It used to be omitted entirely, which
    silently dropped one team per side from every fit -- and, because the season
    path fills opponent strength with None, also dropped that team's opponents'
    games from the adjusted set.
    """
    clean = _clean()
    offense, defense, intercept = dropped_level_ridge(clean, _RIDGE_LAMBDA)
    assert set(offense["team_id"].to_list()) == set(_TRUE)
    assert set(defense["team_id"].to_list()) == set(_TRUE)
    ref = sorted(clean["pos_team_id"].unique().to_list())[0]
    got = offense.filter(pl.col("team_id") == ref)["adjmodelOff"][0]
    assert got == pytest.approx(intercept), "the reference team must be emitted at the intercept"
