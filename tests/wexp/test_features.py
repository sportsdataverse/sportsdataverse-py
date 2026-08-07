"""Walk-forward feature tests (SOS / SOR, carry-forward weights).

SOS/SOR run on the real NFL fixture oracle with ratings derived from the
ridge vintages, so the as-of contract is exercised against real schedule
structure rather than a synthetic grid.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.engines import ridge_margin_vintages
from sportsdataverse.wexp.features import carry_forward_weights, sos_sor_vintages
from sportsdataverse.wexp.oracle_market import nfl_market_oracle_from_schedule

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"


@pytest.fixture(scope="module")
def oracle() -> pl.DataFrame:
    return nfl_market_oracle_from_schedule(pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet"))


@pytest.fixture(scope="module")
def ratings(oracle) -> pl.DataFrame:
    v = ridge_margin_vintages(oracle, lam=10.0)
    return v.select("season", "as_of_week", "team_id", rating=pl.col("off_coef") - pl.col("def_coef"))


def test_sos_sor_contract_and_coverage(oracle, ratings):
    sos = sos_sor_vintages(oracle, ratings)
    assert sos.height > 0
    assert set(sos.columns) == {
        "season",
        "as_of_week",
        "team_id",
        "sos_played",
        "sos_remaining",
        "sor",
        "games_played",
    }
    assert sos.schema["team_id"] == pl.Utf8 and sos.schema["as_of_week"] == pl.Int32
    # week 1 has no completed games -> no rows; games_played grows with the week
    assert sos.filter(pl.col("as_of_week") == 1).height == 0
    by_week = sos.group_by("as_of_week").agg(g=pl.col("games_played").mean()).sort("as_of_week")
    early = by_week.head(1)["g"][0]
    late = by_week.tail(1)["g"][0]
    assert late > early


def test_sor_rewards_winning_against_strength(oracle, ratings):
    """SOR = wins above what a league-average team would take from the slate.

    An undefeated team must sit above a winless one at the same week, and
    SOR must correlate positively with actual win rate.
    """
    sos = sos_sor_vintages(oracle, ratings)
    walk = oracle.drop_nulls("home_win")
    wins = pl.concat(
        [
            walk.select("season", "week", team_id="home_team_id", won=pl.col("home_win").cast(pl.Float64)),
            walk.select("season", "week", team_id="away_team_id", won=1.0 - pl.col("home_win").cast(pl.Float64)),
        ]
    )
    late = sos.filter(pl.col("as_of_week") == 15)
    if late.height == 0:
        pytest.skip("fixture has no week-15 vintage")
    record = wins.filter(pl.col("week") < 15).group_by("season", "team_id").agg(win_rate=pl.col("won").mean())
    joined = late.join(record, on=["season", "team_id"], how="inner")
    assert joined.height >= 30
    assert joined.select(pl.corr("sor", "win_rate")).item() > 0.7


def test_sos_sor_is_as_of(oracle, ratings):
    """Tampering results AFTER a week cannot change that week's SOS/SOR."""
    base = sos_sor_vintages(oracle, ratings).filter(pl.col("as_of_week") <= 8)
    tampered_oracle = oracle.with_columns(
        pl.when(pl.col("week") >= 8).then(1).otherwise(pl.col("home_win")).cast(pl.Int8).alias("home_win")
    )
    after = sos_sor_vintages(tampered_oracle, ratings).filter(pl.col("as_of_week") <= 8)
    key = ["season", "as_of_week", "team_id"]
    assert base.height == after.height > 0
    assert base.sort(key).select("sos_played", "sor").equals(after.sort(key).select("sos_played", "sor"))


def _returning(vals: dict[str, float], season: int = 2019) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [season] * len(vals),
            "team_id": [int(t) for t in vals],
            "overall_returning": list(vals.values()),
        }
    )


def test_carry_forward_ramp_hits_zero_after_last_week():
    w = carry_forward_weights(_returning({"1": 0.8, "2": 0.2}), last_week=4)
    ramp = w.filter(pl.col("team_id") == "1").sort("week").select("week", "carry_weight").to_dicts()
    assert [r["week"] for r in ramp] == [1, 2, 3, 4, 5]
    assert ramp[-1]["carry_weight"] == 0.0  # week 5: current season stands alone
    # strictly decreasing across the ramp
    vals = [r["carry_weight"] for r in ramp]
    assert all(a > b for a, b in zip(vals, vals[1:]))


def test_carry_forward_rewards_continuity():
    ret = _returning({"1": 0.8, "2": 0.8})
    qb = pl.DataFrame({"season": [2019, 2019], "team_id": ["1", "2"], "qb_continuity": [1, 0]})
    w = carry_forward_weights(ret, qb_continuity=qb).filter(pl.col("week") == 1)
    by_team = {r["team_id"]: r["carry_weight"] for r in w.iter_rows(named=True)}
    assert by_team["1"] > by_team["2"]  # returning QB -> believe last year more

    # more returning production -> more weight, holding continuity fixed
    ret2 = _returning({"1": 0.9, "2": 0.1})
    w2 = carry_forward_weights(ret2).filter(pl.col("week") == 1)
    b2 = {r["team_id"]: r["carry_weight"] for r in w2.iter_rows(named=True)}
    assert b2["1"] > b2["2"]
    assert 0.0 <= min(b2.values()) and max(b2.values()) <= 1.0


def test_carry_forward_missing_continuity_is_neutral():
    """No coach source exists in-stack; the term must default neutral, not 0."""
    ret = _returning({"1": 0.5})
    with_none = carry_forward_weights(ret).filter(pl.col("week") == 1)["carry_weight"][0]
    explicit = pl.DataFrame({"season": [2019], "team_id": ["1"], "hc_continuity": [0.5]})
    with_half = carry_forward_weights(ret, hc_continuity=explicit).filter(pl.col("week") == 1)["carry_weight"][0]
    assert abs(with_none - with_half) < 1e-12
