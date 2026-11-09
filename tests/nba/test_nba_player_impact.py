"""nba_player_impact_context / nba_player_impact_league_averages on the real 2025 player_impact releases.

Fixtures: tests/fixtures/bb_player_impact/ (README there): the published 2024-25 NBA and 2025 WNBA
``player_impact`` tables, both season types, columns only dropped.
"""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS
from sportsdataverse._common.rate_gates import ON_OFF_MIN_N
from sportsdataverse.nba import nba_player_impact_context, nba_player_impact_league_averages
from sportsdataverse.nba.nba_player_impact import IMPACT_RANKED

_FX = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "bb_player_impact"
_LEAGUES = [("nba", "00"), ("wnba", "10")]
_SIDE_N = {"o": "off_poss", "d": "def_poss"}
_SIDE_GATE = {"o": "off_rtg_on", "d": "def_rtg_on", "net": "net_rtg_on"}


def _impact(lg: str) -> pl.DataFrame:
    return pl.read_parquet(_FX / f"{lg}_player_impact_2025.parquet")


@pytest.fixture(scope="module", params=_LEAGUES, ids=[lg for lg, _ in _LEAGUES])
def ranked(request):
    lg, league_id = request.param
    raw = _impact(lg)
    return lg, raw, nba_player_impact_context(raw, league_id=league_id)


def test_appends_n_rank_pct_for_every_metric(ranked) -> None:
    _, raw, out = ranked
    assert out.columns[: raw.width] == raw.columns and out.height == raw.height
    for m in IMPACT_RANKED:
        assert out.schema[f"{m}_n"] == pl.Int64
        assert out.schema[f"{m}_rank"] == pl.Float64 and out.schema[f"{m}_pct"] == pl.Float64


def test_n_is_the_possessions_of_the_metric_side(ranked) -> None:
    _, _, out = ranked
    for m, side in IMPACT_RANKED.items():
        want = out[_SIDE_N[side]] if side in _SIDE_N else out["off_poss"] + out["def_poss"]
        assert (out[f"{m}_n"] == want).all(), m


def test_a_rating_is_ranked_exactly_when_its_n_reaches_the_side_gate(ranked) -> None:
    """Ranked <=> finite metric and <m>_n >= the same side's on-court gate; war (a count) is ungated.
    Observed on the 2024-25 NBA regular season: 298 o_rapm / 305 d_rapm / 306 rapm of 569 ranked;
    2025 WNBA 131 / 92 / 121 of 181. No playoff run reaches a gate."""
    lg, _, out = ranked
    rs = out.filter(pl.col("season_type") == "Regular Season")
    for m, side in IMPACT_RANKED.items():
        gate = 0 if m == "war" else ON_OFF_MIN_N[lg][_SIDE_GATE[side]]
        eligible = (pl.col(f"{m}_n") >= gate) & pl.col(m).is_finite()
        assert rs.filter(eligible).height >= MIN_COHORT_PLAYERS, m
        assert rs.filter(eligible & pl.col(f"{m}_rank").is_null()).height == 0, m
        assert rs.filter(~eligible & pl.col(f"{m}_rank").is_not_null()).height == 0, m
    counts = {m: rs[f"{m}_rank"].count() for m in ("o_rapm", "d_rapm", "rapm", "war")}
    assert counts == {"nba": {"o_rapm": 298, "d_rapm": 305, "rapm": 306, "war": 569},
                      "wnba": {"o_rapm": 131, "d_rapm": 92, "rapm": 121, "war": 181}}[lg]  # fmt: skip
    playoffs = out.filter(pl.col("season_type") == "Playoffs")
    assert playoffs["rapm_rank"].count() == 0 and playoffs["war_rank"].count() == playoffs.height


def test_rank_one_is_the_highest_value_and_pct_follows_the_weibull_formula(ranked) -> None:
    lg, _, out = ranked
    rs = out.filter(pl.col("season_type") == "Regular Season")
    for m in IMPACT_RANKED:  # every metric is higher-is-better, the defensive ones included
        ranked_rows = rs.filter(pl.col(f"{m}_rank").is_not_null())
        best = ranked_rows.filter(pl.col(f"{m}_rank") == 1.0)
        assert best[m].item() == ranked_rows[m].max(), m
        n = ranked_rows.height
        assert ((ranked_rows[f"{m}_pct"] - 100 * (n + 1 - ranked_rows[f"{m}_rank"]) / (n + 1)).abs() < 1e-9).all()
    top = rs.filter(pl.col("adj_rapm_rank") == 1.0)["player_name"].item()
    assert top == {"nba": "Giannis Antetokounmpo", "wnba": "Breanna Stewart"}[lg]


def test_idempotent(ranked) -> None:
    lg, _, out = ranked
    assert nba_player_impact_context(out, league_id=dict(_LEAGUES)[lg]).equals(out)


def test_league_averages_describe_the_ranked_cohort(ranked) -> None:
    lg, raw, out = ranked
    avg = nba_player_impact_league_averages(raw, league_id=dict(_LEAGUES)[lg])
    assert set(avg["category"]) == {"player_impact_regular_season", "player_impact_playoffs"}
    assert set(avg["level"]) == {lg} and set(avg["entity"]) == {"player"}
    rs = out.filter(pl.col("season_type") == "Regular Season")
    rows = avg.filter(pl.col("category") == "player_impact_regular_season")
    assert set(rows["metric"]) == set(IMPACT_RANKED)
    for m in IMPACT_RANKED:
        row = rows.filter(pl.col("metric") == m)
        cohort = rs.filter(pl.col(f"{m}_rank").is_not_null())[m]
        assert row["n"].item() == cohort.len(), m
        assert abs(row["mean"].item() - cohort.mean()) < 1e-9, m


def test_unknown_league_raises() -> None:
    with pytest.raises(ValueError, match="league_id"):
        nba_player_impact_context(_impact("nba"), league_id="20")
