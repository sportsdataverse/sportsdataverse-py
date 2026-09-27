"""Unit tests for sportsdataverse.cfb.cfb_adjusted_epa.

Structural / contract tests on a small synthetic league (offline, deterministic).
Full byte-for-value parity against the R ``adjust_epa`` is validated in the
cfbfastR-cfb-data ``team_summaries`` integration suite; here we lock the public
contract: output schema, the net = off - def identity, the valid-games filter,
clean rankings, the pandas option, and the missing-column guard.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb import cfb_adjusted_epa, cfb_adjusted_epa_by_game

_EXPECTED_COLUMNS = {
    "team_id",
    "pos_team",
    "valid_games",
    "adj_off_epa",
    "adj_def_epa",
    "off_strength_faced",
    "def_strength_faced",
    "net_adj_epa",
    "adj_off_epa_rank",
    "adj_def_epa_rank",
    "net_adj_epa_rank",
}


def _synthetic_pbp() -> pl.DataFrame:
    """A deterministic 4-team league: every ordered pairing is a game in which
    both teams take offensive snaps, so each team has plenty of valid games."""
    teams = ["1", "2", "3", "4"]
    names = {t: f"T{t}" for t in teams}
    rng = np.random.default_rng(0)
    rows: list[dict[str, object]] = []
    gid = 0
    for home in teams:
        for away in teams:
            if home == away:
                continue
            gid += 1
            week = (gid - 1) // 2 + 1  # 2 games per week
            for off, dfn in ((home, away), (away, home)):
                for _ in range(20):
                    rows.append(
                        {
                            "game_id": gid,
                            "week": week,
                            "pos_team": names[off],
                            "pos_team_id": off,
                            "def_pos_team_id": dfn,
                            "home": names[home],
                            "neutral_site": False,
                            "EPA": float(rng.normal()),
                            "pass": 1,
                            "rush": 0,
                            "wp_before": 0.5,
                            "wp_before_naive": 0.5,
                            "seasonType": 2,
                        }
                    )
    return pl.DataFrame(rows)


def test_schema_and_net_identity() -> None:
    df = cfb_adjusted_epa(_synthetic_pbp())
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) == _EXPECTED_COLUMNS
    assert df.height == 4
    net = df["adj_off_epa"] - df["adj_def_epa"]
    assert (net - df["net_adj_epa"]).abs().max() < 1e-9
    assert df["valid_games"].min() >= 2


def test_rankings_are_permutations() -> None:
    df = cfb_adjusted_epa(_synthetic_pbp())
    for rank_col in ("adj_off_epa_rank", "adj_def_epa_rank", "net_adj_epa_rank"):
        assert sorted(df[rank_col].to_list()) == [1.0, 2.0, 3.0, 4.0]


def test_return_as_pandas() -> None:
    import pandas as pd

    out = cfb_adjusted_epa(_synthetic_pbp(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert set(out.columns) == _EXPECTED_COLUMNS


def test_accepts_pandas_input() -> None:
    out = cfb_adjusted_epa(_synthetic_pbp().to_pandas())
    assert isinstance(out, pl.DataFrame)
    assert out.height == 4


def test_missing_required_column_raises() -> None:
    with pytest.raises(KeyError):
        cfb_adjusted_epa(_synthetic_pbp().drop("EPA"))


def test_stable_across_runs() -> None:
    # Stable within float tolerance (polars' threaded group-by reductions can
    # differ by a last-ULP across runs, so exact equality is too strict).
    a = cfb_adjusted_epa(_synthetic_pbp()).sort("team_id")
    b = cfb_adjusted_epa(_synthetic_pbp()).sort("team_id")
    assert a.columns == b.columns
    for col in a.columns:
        if a.schema[col].is_numeric():
            assert (a[col] - b[col]).abs().max() < 1e-9
        else:
            assert a[col].to_list() == b[col].to_list()


_BY_GAME_COLUMNS = {
    "game_id",
    "week",
    "team_id",
    "opponent_id",
    "pos_team",
    "raw_off_epa",
    "adj_off_epa",
    "raw_def_epa",
    "adj_def_epa",
    "off_strength_faced",
    "def_strength_faced",
    "net_adj_epa",
}


def test_by_game_schema_and_grain() -> None:
    df = cfb_adjusted_epa_by_game(_synthetic_pbp())
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) == _BY_GAME_COLUMNS
    # one row per (game, team): 12 games x 2 teams
    assert df.height == 24
    assert df.select(["game_id", "team_id"]).unique().height == 24
    assert df["raw_off_epa"].is_not_null().all()


def test_by_game_is_walk_forward() -> None:
    df = cfb_adjusted_epa_by_game(_synthetic_pbp())
    # week 1 has no prior weeks -> no opponent model -> null adjustments (leak-free)
    wk1 = df.filter(pl.col("week") == 1)
    assert wk1.height > 0
    assert wk1["adj_off_epa"].is_null().all()
    assert wk1["adj_def_epa"].is_null().all()
    # later weeks have a prior fit -> adjusted values present
    assert df.filter(pl.col("week") >= 2)["adj_off_epa"].is_not_null().any()
    # net = off - def where both present
    both = df.filter(pl.col("adj_off_epa").is_not_null() & pl.col("adj_def_epa").is_not_null())
    assert ((both["adj_off_epa"] - both["adj_def_epa"]) - both["net_adj_epa"]).abs().max() < 1e-9


def test_by_game_return_as_pandas() -> None:
    import pandas as pd

    out = cfb_adjusted_epa_by_game(_synthetic_pbp(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert set(out.columns) == _BY_GAME_COLUMNS


def test_by_game_requires_week() -> None:
    with pytest.raises(KeyError):
        cfb_adjusted_epa_by_game(_synthetic_pbp().drop("week"))


# --- Real-data regressions (2026 FBS-vs-FBS, weeks 1-4; see tests/fixtures/cfb_adjusted_epa/README.md) ---

_FIXTURE_2026 = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_adjusted_epa" / "fbs_plays_2026_wk1_4.parquet"


def _plays_2026() -> pl.DataFrame:
    return pl.read_parquet(_FIXTURE_2026)


def test_real_2026_opponents_rated_from_a_handful_of_plays_do_not_dominate() -> None:
    # Live board 2026-09-26: Utah State #1 (+1.43 net adj vs -0.37 raw) and Washington
    # State #2 (+1.24 vs -0.36), each off 2 FBS games. Both had played Washington, whose
    # offense the fit rated +2.2 EPA/play above baseline from 9 competitive plays: with
    # standardized dummies and alpha = lambda * n, every team is shrunk by the same ~3%
    # however few plays it rests on. A team's average opponent can't be more extreme
    # than the most extreme team in football (full-season strengths sit within ~0.4 of
    # the baseline), so 0.5 is a generous ceiling.
    out = cfb_adjusted_epa(_plays_2026())
    for col in ("off_strength_faced", "def_strength_faced"):
        spread = (out[col] - out[col].median()).abs().max()
        assert spread < 0.5, f"{col} spans {spread:.2f} EPA/play from the median"
    top10 = out.filter(pl.col("net_adj_epa_rank") <= 10)["team_id"].to_list()
    assert "328" not in top10  # Utah State: raw net 124th of 135
    assert "265" not in top10  # Washington State: raw net 123rd of 135


def test_real_2026_result_does_not_depend_on_which_team_id_sorts_first() -> None:
    # The fit used to drop the first team id (as a STRING) as the reference level, and a
    # ridge pins that team to the intercept: Boston College ("103") was rated exactly
    # league-baseline on both sides every season, mis-adjusting every opponent it played.
    plays = _plays_2026()
    first = min(plays["pos_team_id"].cast(pl.Utf8).min(), plays["def_pos_team_id"].cast(pl.Utf8).min())
    relabelled = plays.with_columns(
        pl.col(c).cast(pl.Utf8).replace(first, "zzz_relabelled") for c in ("pos_team_id", "def_pos_team_id")
    )
    a = cfb_adjusted_epa(plays).select("team_id", "net_adj_epa")
    b = cfb_adjusted_epa(relabelled).select(
        pl.col("team_id").replace("zzz_relabelled", first), pl.col("net_adj_epa").alias("net_relabelled")
    )
    j = a.join(b, on="team_id")
    assert j.height == a.height
    assert (j["net_adj_epa"] - j["net_relabelled"]).abs().max() < 1e-9


def test_strength_fit_reads_naive_wp_not_the_spread_aware_one() -> None:
    # The spread-aware wp_before started 171 of 807 2025 FBS games outside a 10-90%
    # band at 0-0 and gave 72 of them zero fit plays; the fit uses wp_before_naive.
    plays = _plays_2026()
    assert cfb_adjusted_epa(plays.drop("wp_before")).height > 100
    with pytest.raises(KeyError, match="wp_before_naive"):
        cfb_adjusted_epa(plays.drop("wp_before_naive"))


def test_real_2026_golden_net_values() -> None:
    # Pins the shipped fit end to end (band, penalty, 577-play anchor, no reference
    # level): each of those reverted alone moves these values (see #598).
    out = cfb_adjusted_epa(_plays_2026()).filter(pl.col("team_id").is_in(["103", "254", "328"])).sort("team_id")
    assert out["team_id"].to_list() == ["103", "254", "328"]  # Boston College, Utah, Utah State
    assert out["net_adj_epa"].to_list() == pytest.approx(
        [-0.008144991395416745, 0.4916917971781684, -0.18059490002011247], abs=1e-6
    )


def test_fit_band_keeps_at_least_70_percent_of_plays() -> None:
    # Owner rule for the fit band. It is a FULL-SEASON property (70-74% in 2014-2026);
    # this 2026 weeks 1-4 fixture sits at 70.04%, and through-week snapshots can dip
    # to ~64% (2026 week 1), which is why it is not a runtime error.
    from sportsdataverse.cfb.cfb_adjusted_epa import _ADJ_REQUIRED, _FIT_WP, _prepare

    base, clean = _prepare(_plays_2026(), _ADJ_REQUIRED, _FIT_WP)
    assert clean.height / base.height >= 0.70


def test_empty_fit_band_raises_instead_of_returning_no_rows() -> None:
    plays = _plays_2026().with_columns(wp_before_naive=pl.lit(None, dtype=pl.Float64))
    with pytest.raises(ValueError, match="no plays"):
        cfb_adjusted_epa(plays)


def test_all_neutral_site_frame_fits_without_a_home_term() -> None:
    out = cfb_adjusted_epa(_synthetic_pbp().with_columns(neutral_site=pl.lit(True)))
    assert out.height == 4
    assert out["net_adj_epa"].is_finite().all()


def test_by_game_keeps_postseason_out_of_regular_season_fits() -> None:
    # Bowls restart at week 1 with seasonType 3. A walk-forward fit for week w must
    # not see them: they are played after every regular-season week.
    reg = _synthetic_pbp()
    bowl = reg.filter(pl.col("game_id") == 1).with_columns(
        game_id=pl.lit(999, dtype=pl.Int64),
        week=pl.lit(1, dtype=pl.Int64),
        seasonType=pl.lit(3, dtype=pl.Int64),
        EPA=pl.col("EPA") + 5.0,
    )
    with_bowl = cfb_adjusted_epa_by_game(pl.concat([reg, bowl]))
    without = cfb_adjusted_epa_by_game(reg)
    key = ["game_id", "team_id"]
    got = with_bowl.filter(pl.col("game_id") != "999").sort(key)
    assert got["adj_off_epa"].to_list() == pytest.approx(without.sort(key)["adj_off_epa"].to_list(), nan_ok=True)
    # ...and the bowl itself is adjusted with every regular-season week behind it.
    assert with_bowl.filter(pl.col("game_id") == "999")["adj_off_epa"].is_not_null().all()


def test_by_game_rejects_nonpositive_lambda_even_with_only_week_one() -> None:
    # Week 1 has no prior fit, so the check inside the fit never ran (CodeRabbit, #598).
    week1 = _synthetic_pbp().filter(pl.col("week") == 1)
    with pytest.raises(ValueError, match="ridge_lambda"):
        cfb_adjusted_epa_by_game(week1, ridge_lambda=0.0)


# --- method="pre598": the pre-#598 fit, kept bit for bit for nfl-data's NFL build ---

_FIXTURE_DIR = _FIXTURE_2026.parent


def _nfl_shaped_pbp() -> pl.DataFrame:
    """nfl-data's input shape (python/nfl_team_summaries/input.py): nflfastR's ``wp``
    as ``wp_before``, NO ``wp_before_naive`` and NO ``seasonType``, Int64 ESPN team
    ids (string order != numeric order), Float64 pass/rush flags, continuous weeks."""
    ids = [1, 2, 10, 12, 22, 33]
    abbr = {1: "ATL", 2: "BUF", 10: "TEN", 12: "KC", 22: "ARI", 33: "BAL"}
    rng = np.random.default_rng(598)
    rows: list[dict[str, object]] = []
    for week in range(1, 6):
        order = [int(t) for t in rng.permutation(ids)]
        for g in range(3):
            home, away = order[2 * g], order[2 * g + 1]
            for off, dfn in ((home, away), (away, home)):
                for i in range(10):
                    wp = float(rng.uniform())
                    rows.append(
                        {
                            "game_id": f"2025_{week:02d}_{abbr[away]}_{abbr[home]}",
                            "week": week,
                            "pos_team": abbr[off],
                            "pos_team_id": off,
                            "def_pos_team_id": dfn,
                            "home": abbr[home],
                            "neutral_site": week == 3 and g == 0,
                            "EPA": None if i == 9 else float(rng.normal(0.05 * (off % 7) - 0.1, 1.0)),
                            "pass": float(i % 2),
                            "rush": float(i % 2 == 0 and i != 8),
                            "wp_before": None if i == 7 else wp,
                        }
                    )
    return pl.DataFrame(rows)


def _assert_matches_pre598_golden(got: pl.DataFrame, name: str, key: list[str]) -> None:
    from polars.testing import assert_frame_equal

    exp = pl.read_parquet(_FIXTURE_DIR / f"pre598_{name}.parquet")
    assert_frame_equal(got.sort(key), exp.sort(key), check_exact=False, rel_tol=0.0, abs_tol=1e-12)


@pytest.mark.parametrize(
    ("frame", "name"), [(_plays_2026, "fbs2026"), (_nfl_shaped_pbp, "nfl_synthetic")], ids=["fbs2026", "nfl"]
)
def test_pre598_season_reproduces_the_pre_598_code(frame, name) -> None:
    # Expected output: cfb_adjusted_epa(frame) run on sdv-py bd1987493^ (see the fixture README).
    _assert_matches_pre598_golden(cfb_adjusted_epa(frame(), method="pre598"), f"season_{name}", ["team_id"])


@pytest.mark.parametrize(
    ("frame", "name"), [(_plays_2026, "fbs2026"), (_nfl_shaped_pbp, "nfl_synthetic")], ids=["fbs2026", "nfl"]
)
def test_pre598_by_game_reproduces_the_pre_598_code(frame, name) -> None:
    got = cfb_adjusted_epa_by_game(frame(), method="pre598")
    _assert_matches_pre598_golden(got, f"by_game_{name}", ["game_id", "team_id"])


def test_pre598_by_game_orders_by_week_alone_like_the_pre_598_code() -> None:
    # The #598 bowl fix is NOT applied under pre598: it would change output on any frame
    # whose postseason restarts at week 1 (ESPN seasonType 3), so seasonType goes unread.
    reg = _synthetic_pbp()
    bowl = reg.filter(pl.col("game_id") == 1).with_columns(
        game_id=pl.lit(999, dtype=pl.Int64), seasonType=pl.lit(3, dtype=pl.Int64), EPA=pl.col("EPA") + 5.0
    )
    plays = pl.concat([reg, bowl])
    a = cfb_adjusted_epa_by_game(plays, method="pre598").sort(["game_id", "team_id"])
    b = cfb_adjusted_epa_by_game(plays.drop("seasonType"), method="pre598").sort(["game_id", "team_id"])
    assert a.equals(b)


def test_default_method_still_requires_naive_wp() -> None:
    nfl = _nfl_shaped_pbp()
    with pytest.raises(KeyError, match="wp_before_naive"):
        cfb_adjusted_epa(nfl)
    with pytest.raises(KeyError, match="wp_before_naive"):
        cfb_adjusted_epa_by_game(nfl)


@pytest.mark.parametrize("fn", [cfb_adjusted_epa, cfb_adjusted_epa_by_game])
def test_unknown_method_raises(fn) -> None:
    with pytest.raises(ValueError, match="method"):
        fn(_synthetic_pbp(), method="pre-598")
