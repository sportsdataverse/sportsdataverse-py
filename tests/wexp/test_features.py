"""Walk-forward feature tests (SOS / SOR, carry-forward weights).

SOS/SOR run on the real NFL fixture oracle with ratings derived from the
ridge vintages, so the as-of contract is exercised against real schedule
structure rather than a synthetic grid.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.engines import ridge_margin_vintages
from sportsdataverse.wexp.features import cfb_scoring_opportunities, carry_forward_weights, sos_sor_vintages
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


def test_scoring_opportunities_splits_creation_from_finishing():
    """A drive counts as an opportunity at its CLOSEST approach, not its end."""
    plays = pl.DataFrame(
        {
            "game_id": [1] * 7,
            "pos_team": [10, 10, 10, 10, 20, 20, 20],
            "def_pos_team": [20, 20, 20, 20, 10, 10, 10],
            "drive.id": ["a", "a", "b", "b", "c", "c", "d"],
            # drive a reaches the 25 then gets sacked back to the 55 -> still an opp
            "start.yardsToEndzone": [70.0, 25.0, 80.0, 62.0, 44.0, 12.0, 90.0],
            "touchdown": [False, True, False, False, False, False, False],
            "fg_made": [False, False, False, False, False, True, False],
        }
    )
    out = cfb_scoring_opportunities(plays).sort("off_team_id")
    off10 = out.row(0, named=True)
    assert off10["drives"] == 2
    assert off10["scoring_opps"] == 1  # drive a only; drive b never got inside 40
    assert off10["opp_rate"] == 0.5
    assert off10["points_per_opp"] == 7.0

    off20 = out.row(1, named=True)
    assert off20["scoring_opps"] == 1  # drive c; drive d stalled at the 90
    assert off20["points_per_opp"] == 3.0


def test_scoring_opportunities_no_opportunity_is_null_not_zero():
    """No opportunity means finishing is UNMEASURED, never measured-as-zero."""
    plays = pl.DataFrame(
        {
            "game_id": [1, 1],
            "pos_team": [10, 10],
            "def_pos_team": [20, 20],
            "drive.id": ["a", "b"],
            "start.yardsToEndzone": [90.0, 75.0],
            "touchdown": [False, False],
            "fg_made": [False, False],
        }
    )
    out = cfb_scoring_opportunities(plays)
    assert out["scoring_opps"][0] == 0
    assert out["opp_rate"][0] == 0.0
    assert out["points_per_opp"][0] is None


def test_carry_forward_weights_accepts_string_team_ids():
    """NFL ids are abbreviations — the abbr IS the canonical id, not a number."""
    ret = pl.DataFrame({"season": [2019, 2019], "team_id": ["KC", "SF"], "overall_returning": [0.8, 0.4]})
    qb = pl.DataFrame({"season": [2019, 2019], "team_id": ["KC", "SF"], "qb_continuity": [1, 0]})
    out = carry_forward_weights(ret, qb_continuity=qb)
    wk1 = out.filter(pl.col("week") == 1).sort("team_id")
    assert wk1.height == 2
    # KC: more returning production AND continuity -> strictly higher credence
    assert wk1["carry_weight"][0] > wk1["carry_weight"][1]


def test_carry_forward_weights_rejects_non_matching_id_namespace():
    """A continuity key that matches NOTHING must raise, not return 0.5.

    The silent-degrade this guards: the left join misses on every row,
    `fill_null(0.5)` hands the neutral credence to the whole league, and
    the output looks like a plausible answer. A Float64-origin id that
    stringified to "52.0" used to land here; `_team_id_utf8` now rules
    out the dtype half, leaving a genuinely wrong id namespace.
    """
    ret = pl.DataFrame({"season": [2019], "team_id": ["KC"], "overall_returning": [0.8]})
    qb = pl.DataFrame({"season": [2019], "team_id": [52.0], "qb_continuity": [1]})
    with pytest.raises(ValueError, match="shares no"):
        carry_forward_weights(ret, qb_continuity=qb)


def test_carry_forward_weights_numeric_ids_do_not_stringify_as_float():
    """Float-origin ids must become "52", never "52.0"."""
    ret = pl.DataFrame({"season": [2019], "team_id": [52.0], "overall_returning": [0.8]})
    qb = pl.DataFrame({"season": [2019], "team_id": [52], "qb_continuity": [1]})
    out = carry_forward_weights(ret, qb_continuity=qb)
    assert out["team_id"].unique().to_list() == ["52"]
    # the qb=1 signal actually landed rather than falling back to neutral 0.5
    neutral = carry_forward_weights(ret)
    assert out.filter(pl.col("week") == 1)["carry_weight"][0] > (neutral.filter(pl.col("week") == 1)["carry_weight"][0])
