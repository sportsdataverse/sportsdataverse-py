"""Expression-vs-scalar parity: oracle_market's polars forms must equal the
unit-tested scalar implementations in wexp.market elementwise.

Guards against the drift class where the scalar tests stay green while the
vectorized expression path diverges (sign, vig, blend weight).
"""

import polars as pl
import pytest

from sportsdataverse.wexp.market import (
    devig_multiplicative,
    logit_blend,
    prob_from_american,
    spread_to_prob,
)
from sportsdataverse.wexp.oracle_market import (
    NFL_MARGIN_SIGMA,
    nfl_market_oracle_from_schedule,
)


def test_oracle_expressions_match_scalar_functions():
    sch = pl.DataFrame(
        {
            "game_id": ["a", "b", "c"],
            "season": [2023, 2023, 2023],
            "week": [1, 1, 2],
            "game_type": ["REG"] * 3,
            "location": ["Home", "Neutral", "Home"],
            "home_team": ["X", "Y", "Z"],
            "away_team": ["P", "Q", "R"],
            "result": [7, -3, 0],
            "spread_line": [3.5, -6.0, 0.5],
            "total_line": [44.0, 41.5, 50.0],
            "home_moneyline": [-180, 240, -105],
            "away_moneyline": [160, -290, -115],
        }
    )
    out = nfl_market_oracle_from_schedule(sch)
    for row in out.iter_rows(named=True):
        spread = row["spread_close"]
        mlh, mla = row["ml_home_close"], row["ml_away_close"]
        p_spread = spread_to_prob(spread, sigma=NFL_MARGIN_SIGMA)
        p_ml = devig_multiplicative([prob_from_american(mlh), prob_from_american(mla)])[0]
        assert row["p_close_spread"] == pytest.approx(p_spread, abs=1e-12)
        assert row["p_close_ml"] == pytest.approx(p_ml, abs=1e-12)
        assert row["p_close"] == pytest.approx(logit_blend(p_spread, p_ml, weight_a=0.7), abs=1e-12)
