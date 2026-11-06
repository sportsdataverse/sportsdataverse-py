"""BB-D10: the four-factor RAPM split, on committed real captures.

Fixtures (``tests/fixtures/bb_factor_rapm``, README there; none synthetic): the whole 2024 and
2025 WNBA ``wnba_stats_possessions`` and ``wnba_stats_schedules`` releases, and the regular-season
plain RAPM of the ``wnba_player_impact`` release for both seasons (the oracle).

The acceptance is the identity of the split: the five offensive components (four factors and the
remainder) add up to ``o_rapm`` and the five defensive ones to ``d_rapm``, and ``o_rapm`` /
``d_rapm`` are the published plain RAPM. The gates are the values observed on these seasons; never lower one to make a change pass.
The ``bb-factor-rapm`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from sklearn.linear_model import Ridge

from sportsdataverse._common.factor_rapm import FACTORS, factor_rapm, factor_rapm_schema, factor_targets
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_rapm import build_rapm_design
from tests.conftest import skip_if_no_live
from sportsdataverse.nba.nba_player_factor_rapm import nba_factor_rapm_rows, nba_player_factor_rapm
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_player_factor_rapm import wnba_player_factor_rapm

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_factor_rapm"
_OFF = [f"o_{f}_rapm" for f in FACTORS]
_DEF = ["d_efg_rapm", "d_tov_rapm", "d_drb_rapm", "d_ftr_rapm", "d_other_rapm"]
_COMPONENTS = ["rapm", "o_rapm", "d_rapm", *_OFF, *_DEF]


def _read(name: str, season: int) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"wnba_{name}_{season}.parquet")


@cache
def _rows(season: int, season_type: str = "regular-season") -> pl.DataFrame:
    return nba_factor_rapm_rows(_read("possessions", season), _read("schedule", season), season_type)


@cache
def _split(season: int) -> pl.DataFrame:
    return factor_rapm(_rows(season), player_dtype=pl.Int64(), min_poss=200)


def test_components_add_up_to_the_totals() -> None:
    for season in (2024, 2025):
        out = _split(season)
        off = out.select((pl.sum_horizontal(_OFF) - pl.col("o_rapm")).abs().max()).item()
        deff = out.select((pl.sum_horizontal(_DEF) - pl.col("d_rapm")).abs().max()).item()
        # observed 4.6e-12 / 7.0e-12 (floating point); a mis-fit part misses by whole tenths
        assert off < 1e-9 and deff < 1e-9, (season, off, deff)
        assert out.select((pl.col("o_rapm") + pl.col("d_rapm") - pl.col("rapm")).abs().max()).item() < 1e-12


def test_parts_add_up_to_points_on_every_possession() -> None:
    rows = _rows(2025)
    v = 0.9999480870061777  # the 2025 regular season's points per field-goal attempt, as fit
    parts = factor_targets(rows, v)
    gap = parts.select((pl.sum_horizontal(FACTORS) - (pl.col("points") - v)).abs().max()).item()
    assert gap < 1e-9
    assert _split(2025)["pts_per_fga"].unique().to_list() == [pytest.approx(v, abs=1e-12)]
    assert parts.columns == ["points", *FACTORS]


#: 2024 fixture rows (raw release order) -> (what happened, efg, tov, orb, ftr, other), v the season's value.
_V24 = 0.9891586501755993
_ROWS = {
    0: ("a turnover, nothing else", 0.0, -_V24, 0.0, 0.0, 0.0),
    5: ("a made three", 3 - _V24, 0.0, 0.0, 0.0, 0.0),
    36: ("a missed two, then a turnover with no rebound recorded", -_V24, -_V24, 0.0, 0.0, _V24),
    39: ("a missed two, an offensive rebound, then nothing (the period ran out)", -_V24, 0.0, _V24, 0.0, -_V24),
    48: ("a two-shot trip, both made, no field-goal attempt", 0.0, 0.0, 0.0, 2 - _V24, 0.0),
    83: ("an and-one: a made two and the free throw", 2 - _V24, 0.0, 0.0, 1.0, 0.0),
    166: ("a miss, an offensive rebound and the putback", 2 - 2 * _V24, 0.0, _V24, 0.0, 0.0),
    8435: ("a made three, a free throw and a turnover, no rebound recorded", 3 - _V24, -_V24, 0.0, 1.0, _V24),
}


def test_each_part_on_real_possessions() -> None:
    raw = _read("possessions", 2024)
    assert _split(2024)["pts_per_fga"][0] == pytest.approx(_V24, abs=1e-15)
    parts = factor_targets(raw[list(_ROWS)], _V24)
    for got, (what, *want) in zip(parts.iter_rows(), _ROWS.values()):
        assert got[1:] == pytest.approx(tuple(want), abs=1e-12), what


def test_each_component_is_its_own_ridge() -> None:
    # Each part refit on its own with sklearn's Ridge at the season's penalty (an independent solver):
    # pins the value v and the column each part lands in, which the sum identity alone cannot.
    rows, out = _rows(2024), _split(2024).sort("player_id")
    X, _, player_ids = build_rapm_design(rows)
    assert out["player_id"].to_list() == player_ids
    parts = factor_targets(rows, _V24)
    P = len(player_ids)
    for f, d in zip(FACTORS, _DEF, strict=True):
        fit = Ridge(alpha=out["ridge_alpha"][0], solver="sparse_cg", tol=1e-12).fit(X, parts[f].to_numpy())
        # observed 1.3e-12 (o_tov) between the two solvers
        assert np.abs(100 * fit.coef_[:P] - out[f"o_{f}_rapm"].to_numpy()).max() < 1e-8, f
        assert np.abs(-100 * fit.coef_[P:] - out[d].to_numpy()).max() < 1e-8, d


def test_standard_errors_are_clustered_by_game() -> None:
    out = _split(2025).filter(pl.col("off_poss") + pl.col("def_poss") >= 1000)
    # observed 2025, 133 players: median o_rapm_se 1.0826, rapm_se 1.5388 per 100. A delete-one-game
    # jackknife of the same ridge (README) gives o_rapm/d_rapm SEs 1.01 to 1.07 times these.
    assert out["o_rapm_se"].median() == pytest.approx(1.08252, abs=1e-4)
    assert out["rapm_se"].median() == pytest.approx(1.53874, abs=1e-4)
    se = out.select(f"{c}_se" for c in _COMPONENTS)
    assert se.null_count().sum_horizontal().item() == 0 and se.min_horizontal().min() > 0
    # observed 2025 medians: o_tov 0.36 < o_efg 1.01 < o_rapm 1.08 (each part is a slice of the points)
    assert out["o_tov_rapm_se"].median() < out["o_efg_rapm_se"].median() < out["o_rapm_se"].median()


def test_the_remainder_is_rare_and_small() -> None:
    # The remainder takes only possessions the four factors cannot account for. Observed: 389 of
    # 38,037 (2024) and 456 of 44,589 (2025) regular-season possessions are non-zero (1.02%), against
    # 4,732 / 5,668 (12.4% / 12.7%) with a free-throw attempt, so the free-throw factor is not the dump.
    for season, nonzero, fta in ((2024, 389, 4732), (2025, 456, 5668)):
        rows = _rows(season)
        parts = factor_targets(rows, _split(season)["pts_per_fga"][0])
        assert parts.filter(pl.col("other").abs() > 1e-9).height == nonzero, season
        assert rows.filter(pl.col("fta") > 0).height == fta, season
        # a remainder is a few shots' value or a stray non-field-goal point: observed max |other| 2.97
        # (three empty chances, 3v, 2024) and 2.0 (2025)
        assert parts["other"].abs().max() < 3, season


def test_totals_are_the_published_plain_rapm() -> None:
    oracle = pl.read_parquet(FIX / "wnba_player_impact_2024_2025.parquet")
    for season, players in ((2024, 157), (2025, 181)):
        got = _split(season)
        want = oracle.filter(pl.col("season") == season)
        assert got.schema["player_id"] == want.schema["player_id"]
        both = got.join(want, on=["season", "player_id"], how="full", suffix="_pub", validate="1:1")
        assert both.height == got.height == want.height == players  # every player, on both sides
        assert both.filter(pl.col("off_poss") != pl.col("off_poss_pub")).height == 0
        assert both.filter(pl.col("def_poss") != pl.col("def_poss_pub")).height == 0
        for c in ("o_rapm", "d_rapm", "rapm"):
            # observed 7.2e-12 at most: the same ridge at the same RidgeCV penalty
            assert both.select((pl.col(c) - pl.col(f"{c}_pub")).abs().max()).item() < 1e-9, (season, c)


@skip_if_no_live
@pytest.mark.parametrize("season", [2025, 2026])
def test_nba_totals_track_the_published_release(season: int) -> None:
    # Live: the NBA possessions and player_impact release assets (network, about 4 GB peak). Observed
    # 2024-25: r 0.9978 / 0.9973, max |diff| 0.43 / 0.37; 2025-26: r 0.9966 / 0.9969, max 1.31 / 1.00
    # (o_rapm / d_rapm). The gap is lineups, not possessions: the possessions release (2026-08-13)
    # predates the player_impact fit (2026-10-12), whose re-compile has the same possessions but some
    # different on-floor lineups, so each season's total possessions agree exactly.
    out = nba_player_factor_rapm(season)
    pub = nba_loaders.load_nba_player_impact(season).filter(pl.col("season_type") == "Regular Season")
    assert out.schema["player_id"] == pub.schema["player_id"]
    both = out.join(pub, on="player_id", how="full", suffix="_pub", validate="1:1")
    assert both.height == out.height == pub.height  # every player on both sides
    assert out["off_poss"].sum() == pub["off_poss"].sum() and out["def_poss"].sum() == pub["def_poss"].sum()
    for c in ("o_rapm", "d_rapm"):
        r = both.select(pl.corr(c, f"{c}_pub")).item()
        gap = both.select((pl.col(c) - pl.col(f"{c}_pub")).abs().max()).item()
        assert r >= 0.995 and gap <= 1.31, (season, c, r, gap)
    worst = out.select((pl.sum_horizontal(_OFF) - pl.col("o_rapm")).abs().max()).item()
    assert worst < 1e-9  # observed 1.0e-11


def test_each_factor_carries_signal() -> None:
    out = _split(2025).filter(pl.col("off_poss") + pl.col("def_poss") >= 1000)
    sd = {c: out[c].std() for c in _OFF + _DEF}
    # observed 2025 (133 players over 1000 possessions): the factors spread by sd 0.352 (d_ftr) to
    # 1.321 (o_efg) per 100, the bookkeeping remainder by 0.114 / 0.098 (o / d_other)
    assert min(v for c, v in sd.items() if "other" not in c) > 0.3, sd
    assert sd["o_other_rapm"] < 0.15 and sd["d_other_rapm"] < 0.15, sd
    corr = np.corrcoef(out.select(_OFF + _DEF).to_numpy().T)
    off_diag = np.abs(corr[~np.eye(len(sd), dtype=bool)])
    # observed max |r| 0.323: no two components are near-duplicates. A relabel keeps every |r|, so
    # test_each_component_is_its_own_ridge is what catches one.
    assert off_diag.max() < 0.4


def test_seasons_are_fit_apart_and_repeat() -> None:
    both = factor_rapm(pl.concat([_rows(2024), _rows(2025)]), player_dtype=pl.Int64(), min_poss=200)
    for season in (2024, 2025):
        alone = _split(season)
        assert both.filter(pl.col("season") == season).equals(alone)  # no possession of the other season
    gate = (pl.col("off_poss") + pl.col("def_poss")) >= 1000
    pairs = _split(2024).filter(gate).join(_split(2025).filter(gate), on="player_id", suffix="_next")
    assert pairs.height == 92
    rho = {c: pairs.select(pl.corr(c, f"{c}_next", method="spearman")).item() for c in _COMPONENTS}
    # observed 2024 -> 2025: rapm 0.374, o_rapm 0.352, d_rapm 0.163; components 0.128 (d_efg, d_drb)
    # to 0.391 (o_ftr); the remainder 0.157 (o_other) and 0.343 (d_other)
    assert rho["rapm"] > 0.35, rho
    assert min(rho.values()) > 0.1, rho


def test_ranks_follow_the_gate() -> None:
    out = _split(2025)
    ranked = out.filter(pl.col("rapm_rank").is_not_null())
    assert ranked.filter(pl.col("off_poss") + pl.col("def_poss") < 200).height == 0
    assert out.filter((pl.col("off_poss") + pl.col("def_poss") >= 200) & pl.col("rapm_rank").is_null()).height == 0
    best = ranked.sort("d_drb_rapm", descending=True).row(0, named=True)
    assert best["d_drb_rapm_rank"] == 1.0  # higher is better on defense too (a lower_is_better list would put it last)
    assert out.filter(pl.col("o_rapm_n") != pl.col("off_poss")).height == 0
    assert out.filter(pl.col("rapm_n") != pl.col("off_poss") + pl.col("def_poss")).height == 0


def test_public_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(name: str, want: int, season: int = 2025):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [want], f"{name}: called with {seasons}"
            return _read(name, season)

        return load

    for loader, name in (("possessions", "possessions"), ("schedules", "schedule")):
        monkeypatch.setattr(wnba_loaders, f"load_wnba_stats_{loader}", fixture(name, 2025))
        monkeypatch.setattr(nba_loaders, f"load_nba_stats_{loader}", fixture(name, 2024))
    assert wnba_player_factor_rapm(2025).equals(_split(2025))
    nba = nba_player_factor_rapm(2025)  # the NBA loaders take the start year
    assert nba.height == _split(2025).height
    assert (
        nba.filter(pl.col("rapm_rank").is_not_null()).select((pl.col("off_poss") + pl.col("def_poss")).min()).item()
        >= 400
    )
    playoffs = wnba_player_factor_rapm(2025, season_type="playoffs")
    assert 0 < playoffs.height < _split(2025).height
    with pytest.warns(UserWarning, match="none of 48401 possessions is in a 'preseason' game"):
        assert wnba_player_factor_rapm(2025, season_type="preseason").height == 0  # no preseason game


def test_empty_input_and_bad_args() -> None:
    empty = nba_factor_rapm_rows(pl.DataFrame(), pl.DataFrame(), "regular-season")
    out = factor_rapm(empty, player_dtype=pl.Int64(), min_poss=1)
    assert out.height == 0 and out.schema == pl.Schema(factor_rapm_schema(pl.Int64()))
    with pytest.raises(ValueError, match="league_id"):
        nba_player_factor_rapm(2025, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        nba_player_factor_rapm(2025, season_type="Regular Season")
