"""Lane 10: the rank sample gates re-fit offline from the committed reliability curves.

``tests/fixtures/bb_rate_gates/{lg}_fits.csv`` are the split-half curves ``tools/fit_rate_gates.py
--write`` saved from the real ESPN team box scores and the BB-D6 game impact box (README there).
The ``M`` fit (``r(n) = n / (n + M)``) is re-run here on each TRAIN curve and must land on the
frozen :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N` / ``ON_OFF_MIN_N`` gate (``M`` rounded
up to two significant figures); the HOLDOUT seasons' own ``M`` then scores the frozen gate.

Holdout reliability at the frozen gate (``g / (g + M_holdout)``), the median and the worst rate of
each league (gates = observed, rounded down at the second decimal; never loosen a gate to pass --
refit and re-derive it): team rates NBA 0.479 / 0.39 (opp_orb_pct), WNBA 0.489 / 0.33 (pace), MBB
0.492 / 0.43 (opp_ft_rate), WBB 0.503 / 0.41 (opp_efg_pct); on/off NBA 0.544 / 0.50 (def_rtg_diff), WNBA
0.436 / 0.26 (off_rtg_on), MBB 0.513 / 0.50, WBB 0.542 / 0.53. Per-game player box NBA 0.603 / 0.58
(reb_pg), WNBA 0.588 / 0.55 (ast_pg), MBB 0.623 / 0.50 (pts_pg), WBB 0.607 / 0.49 (pts_pg). A gate fitted
on the train seasons is about half signal on seasons it never saw, the WNBA on/off gates less so; single rates
scatter (the WNBA has 36 holdout teams).

Two WNBA rates sit under a third signal on the holdout and stay ranked as owner-accepted exceptions
(2026-11-09): team ``pace`` 0.331 and on/off ``off_rtg_on`` 0.263 (0.267 before release drift). Re-measured season by
season on the current releases, no data or source defect drives them (``rate_gates``, model-lineage). On/off
``off_rtg_diff`` (0.314) left the list when the 2026-11-17 refit counted the off side over the player's span (0.351;
0.370 at the 2026-11-18 listed-games refit, #1240).
That refit's WNBA ``net_rtg_diff`` never reaches half signal on the holdout, so the owner ruled it never ranked
(``OWNER_NEVER_RANKED``).
These figures use the fitter's inputs: box possessions per game for pace, and on/off halves that include
playoffs; the published tables rank possession-table pace and regular-season on/off.
``test_low_holdout_rates_are_the_accepted_exceptions`` pins exactly that set.
"""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.rate_gates import (
    LEVELS,
    ON_OFF_MIN_N,
    PLAYER_BOX_MIN_N,
    TEAM_MIN_N,
    on_off_gates,
    player_box_gates,
    team_gates,
)

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_rate_gates", _ROOT / "tools" / "fit_rate_gates.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)

GATES = _ROOT / "tests" / "fixtures" / "bb_rate_gates"
FROZEN = {"team": TEAM_MIN_N, "on_off": ON_OFF_MIN_N, "player_box": PLAYER_BOX_MIN_N}
#: (league, kind) -> (median, worst) holdout reliability at the frozen gate, observed, rounded down.
HOLDOUT = {
    ("nba", "team"): (0.47, 0.39), ("wnba", "team"): (0.48, 0.33), ("mbb", "team"): (0.49, 0.43),
    ("wbb", "team"): (0.50, 0.41), ("nba", "on_off"): (0.54, 0.50), ("wnba", "on_off"): (0.43, 0.26),
    ("mbb", "on_off"): (0.51, 0.50), ("wbb", "on_off"): (0.54, 0.53),
    ("nba", "player_box"): (0.60, 0.58), ("wnba", "player_box"): (0.58, 0.55), ("mbb", "player_box"): (0.62, 0.50),
    ("wbb", "player_box"): (0.60, 0.49),
}  # fmt: skip
#: Ranked rates under a third signal on the holdout, kept ranked by the owner's decision (2026-11-09; no data or
#: source defect found, rate_gates and docs/architecture/model-lineage.md). Value: the observed holdout r at the
#: frozen gate, rounded down at the third decimal; it may only rise on a refit. Any other ranked rate under 1/3,
#: or one of these reaching 1/3, fails: change this only with the owner's call.
ACCEPTED_LOW_HOLDOUT = {
    # off_rtg_on was 0.266 on the 2026-11-06 fixture; 0.263 is release drift (#1142 re-measured it), not the span
    ("wnba", "team", "pace"): 0.331, ("wnba", "on_off", "off_rtg_on"): 0.263,
}  # fmt: skip
#: Rates the owner ruled never ranked although their train fit gives a finite gate (the ruling, not the fit, sets
#: math.inf): WNBA on/off net_rtg_diff (2026-11-17), train M 2333 but no half signal on the holdout seasons.
OWNER_NEVER_RANKED = {("wnba", "on_off", "net_rtg_diff")}


def _fits(league: str) -> pl.DataFrame:
    return pl.read_csv(GATES / f"{league}_fits.csv")


@pytest.mark.parametrize("league", LEVELS)
def test_refit_lands_on_the_frozen_gates(league: str) -> None:
    fits = _fits(league)
    for kind, table in FROZEN.items():
        train = fits.filter(kind=kind, span="train")
        assert sorted(train["rate"]) == sorted(table[league]), f"{league} {kind}: a gate without a fit, or the reverse"
        train_span, hold_span = fit.SPANS[league][kind]
        hold = fits.filter(kind=kind, span="holdout")
        # the committed curves are the tool's current spans: a moved span without a refit goes red
        assert set(train["seasons"]) == {f"{train_span.start}-{train_span.stop - 1}"}
        assert set(hold["seasons"]) == {f"{hold_span.start}-{hold_span.stop - 1}"}
        assert not set(train_span) & set(hold_span) and max(train_span) < min(hold_span)
        for r in train.iter_rows(named=True):
            m = fit.fit_m(fit.parse_curve(r["curve"]))
            # the grid search is exact up to the platform's float summation (Windows vs Linux numpy)
            assert m == pytest.approx(r["m"], rel=1e-9) or (math.isinf(m) and math.isinf(r["m"])), (
                f"{league} {r['rate']}"
            )
            frozen = table[league][r["rate"]]
            if (league, kind, r["rate"]) in OWNER_NEVER_RANKED:
                assert frozen == math.inf and math.isfinite(m), f"{league} {kind} {r['rate']}: not a ruling any more"
                continue
            assert frozen == (fit.gate(m) if math.isfinite(m) else math.inf), f"{league} {kind} {r['rate']}"
            if math.isfinite(m):
                assert m <= frozen < 1.11 * m + 1  # rounded up, never down: every ranked row has r >= 0.5


@pytest.mark.parametrize(("league", "kind"), sorted(HOLDOUT))
def test_frozen_gates_hold_out(league: str, kind: str) -> None:
    fits = _fits(league).filter(kind=kind)
    held = {}
    for r in fits.filter(span="holdout").iter_rows(named=True):
        # the holdout M is re-fit from its curve too, so a hand-edited m column cannot pass
        m = fit.fit_m(fit.parse_curve(r["curve"]))
        assert m == pytest.approx(r["m"], rel=1e-9) or (math.isinf(m) and math.isinf(r["m"])), f"{league} {r['rate']}"
        held[r["rate"]] = m
    # a ranked rate whose holdout curve never reaches half signal scores 0: it is not skipped
    rel = [
        g / (g + held[m]) if math.isfinite(held[m]) else 0.0
        for m, g in FROZEN[kind][league].items()
        if math.isfinite(g)
    ]
    assert len(rel) >= 3
    median, worst = HOLDOUT[(league, kind)]
    assert float(np.median(rel)) >= median and min(rel) >= worst


def test_low_holdout_rates_are_the_accepted_exceptions() -> None:
    low = {}
    for league, kind in HOLDOUT:
        fits = _fits(league).filter(kind=kind, span="holdout")
        held = {r["rate"]: fit.fit_m(fit.parse_curve(r["curve"])) for r in fits.iter_rows(named=True)}
        for rate, g in FROZEN[kind][league].items():
            if math.isfinite(g):
                rel = g / (g + held[rate]) if math.isfinite(held[rate]) else 0.0
                if rel < 1 / 3:
                    low[(league, kind, rate)] = rel
    assert set(low) == set(ACCEPTED_LOW_HOLDOUT)
    for key, floor in ACCEPTED_LOW_HOLDOUT.items():
        assert floor <= low[key] < floor + 0.001, key


def test_on_off_halves_count_the_off_side_over_player_on_offs_tenure_span(tmp_path: Path) -> None:
    """The fit's NBA off-side base is ``player_on_off(tenure=True)``'s: the real 2024-25 fixture of three
    teams plus the other-team games that bound three traded players' spans (``bb_lineup_seasons``)."""
    from sportsdataverse._common.game_impact import game_player_impact
    from sportsdataverse._common.player_on_off import player_on_off
    from sportsdataverse.nba.nba_game_impact import nba_impact_rows
    from sportsdataverse.nba.nba_player_on_off import _game_dates

    fx = _ROOT / "tests" / "fixtures" / "bb_lineup_seasons"
    names = ("pbp", "game_lineups", "possessions", "schedule")
    frames = [
        pl.concat([pl.read_parquet(fx / f"nba_{n}_2025.parquet"), pl.read_parquet(fx / f"nba_{n}_2025_trades.parquet")])
        for n in names
    ]
    rows, schedule = nba_impact_rows(*frames, league_id="00"), frames[3]
    fit._box(game_player_impact(rows, player_dtype=pl.Int64())).write_parquet(tmp_path / "nba_impact_2025.parquet")
    fit.span_games(rows, schedule).write_parquet(tmp_path / f"nba_span_v{fit.SPAN_VERSION}_2025.parquet")
    tot = ("poss", "opp_poss", "pts", "opp_pts")
    fitted = (
        fit.on_off_halves("nba", range(2025, 2026), tmp_path)
        .group_by("season", "team_id", "player_id")
        .agg(pl.col(*tot, *(f"t_{c}" for c in tot)).sum())
    )

    def table(tenure: bool) -> pl.DataFrame:
        out = player_on_off(rows, player_dtype=pl.Int64(), min_poss=0, game_dates=_game_dates(schedule), tenure=tenure)
        return out.select(
            "season", "team_id", pl.col("player_id").cast(pl.Utf8), *[pl.col(f"{c}_on") for c in tot],
            *[(pl.col(f"{c}_on") + pl.col(f"{c}_off")).alias(f"w_{c}") for c in tot],
        )  # fmt: skip

    both = fitted.join(table(True), on=["season", "team_id", "player_id"], how="inner", validate="1:1")
    assert both.height == fitted.height > 50
    for c in tot:
        assert (both[c] == both[f"{c}_on"]).all(), c  # the on side is the player's own
        # the team's on-court sum / 5 is its total, a little short where the source lineup has a gap (at most
        # 1.2% on this fixture, never over); one game too many or too few moves a row by 15% or more
        d = (both[f"t_{c}"] - both[f"w_{c}"]) / both[f"w_{c}"]
        assert d.max() <= 1e-12 and d.min() > -0.015, c
    # the span cuts the traded players' windows: over the whole season the fit's base would be larger
    season = both.join(
        table(False).select("season", "team_id", "player_id", "w_poss"),
        on=["season", "team_id", "player_id"],
        suffix="_season",
    )
    cut = season.filter(pl.col("t_poss") < pl.col("w_poss_season") - 50)
    assert cut.height >= 3


def test_gate_rounds_up_to_two_significant_figures() -> None:
    assert [fit.gate(x) for x in (1253.0, 1805.0, 24.65, 4.8, 130.0, 999.1)] == [1300, 1900, 25, 5, 130, 1000]
    with pytest.raises(ValueError, match="no gate"):
        fit.gate(math.inf)


def test_gate_lookups() -> None:
    assert team_gates(None, ["off_rtg"]) == {}
    assert team_gates("nba", ["off_rtg", "rim_pct"]) == {"off_rtg": TEAM_MIN_N["nba"]["off_rtg"]}  # rim: no gate
    assert on_off_gates("mbb", ["net_rtg_diff"]) == {"net_rtg_diff": math.inf}  # never half signal in college
    assert player_box_gates("wbb", ["pts_pg", "war"]) == {"pts_pg": 1}  # war: a count, no gate
    assert fit.fit_m([(10.0, 0.05, 50), (20.0, 0.08, 50), (40.0, 0.1, 50)]) == math.inf  # no extrapolated gate
    with pytest.raises(KeyError, match="no sample gates"):
        team_gates("nbagl", ["off_rtg"])
