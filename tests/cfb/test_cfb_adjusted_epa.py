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
from polars.testing import assert_frame_equal

from sportsdataverse.cfb import cfb_adjusted_epa, cfb_adjusted_epa_by_game, cfb_adjusted_epa_ledger

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
                            "period": 1,
                            "pos_score_diff_start": 0.0,
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


def test_fit_reads_the_score_margin_not_any_win_probability() -> None:
    # The spread-aware wp_before started 171 of 807 2025 FBS games outside a 10-90%
    # band at 0-0; the naive band still dropped 24% of plays, many within three
    # scores. The fit now keys on the margin at the snap, so neither WP is read.
    plays = _plays_2026()
    base = cfb_adjusted_epa(plays).sort("team_id")
    assert_frame_equal(cfb_adjusted_epa(plays.drop("wp_before", "wp_before_naive")).sort("team_id"), base)
    with pytest.raises(KeyError, match="pos_score_diff_start"):
        cfb_adjusted_epa(plays.drop("pos_score_diff_start"))


def _garbage_rows() -> pl.Expr:
    """The plays flag_garbage_time calls garbage on the margin at the snap (43/37/27/21)."""
    m, q = pl.col("pos_score_diff_start").abs(), pl.col("period")
    garbage = ((q == 1) & (m > 43)) | ((q == 2) & (m > 37)) | ((q == 3) & (m > 27)) | ((q >= 4) & (m > 21))
    return garbage.fill_null(False)  # null margin = not garbage, as flag_garbage_time


@pytest.mark.parametrize("fn", [cfb_adjusted_epa, cfb_adjusted_epa_by_game])
def test_garbage_time_reaches_neither_the_fit_nor_a_teams_own_epa(fn) -> None:
    # Owner decision 2026-11-25: a team's own EPA is filtered on the same plays as
    # the opponent-strength fit. Until then the fit dropped garbage time but the
    # netting counted it, so blowing out a weak team LOWERED the rating. With both
    # on the same plays, no garbage-time EPA can move any output.
    plays = _plays_2026().with_columns(seasonType=pl.lit(2, dtype=pl.Int64))  # weeks 1-4: regular season
    garbage = _garbage_rows()
    assert plays.filter(garbage).height > 500  # the fixture really has garbage time
    shifted = plays.with_columns(EPA=pl.when(garbage).then(pl.col("EPA") + 3.0).otherwise(pl.col("EPA")))
    key = ["team_id"] if fn is cfb_adjusted_epa else ["game_id", "team_id"]
    assert_frame_equal(fn(shifted).sort(key), fn(plays).sort(key))


def test_own_epa_is_the_mean_over_the_kept_plays() -> None:
    plays = _plays_2026().with_columns(seasonType=pl.lit(2, dtype=pl.Int64))
    kept = plays.filter(_garbage_rows() == False)  # noqa: E712
    by_game = cfb_adjusted_epa_by_game(plays)
    g = plays.filter(_garbage_rows()).sort("game_id").row(0, named=True)
    team, gid = str(g["pos_team_id"]), g["game_id"]
    want = kept.filter((pl.col("game_id") == gid) & (pl.col("pos_team_id") == team))["EPA"].mean()
    got = by_game.filter((pl.col("game_id") == gid) & (pl.col("team_id") == team))["raw_off_epa"].item()
    assert got == pytest.approx(want, abs=1e-12)


def test_real_2026_golden_net_values() -> None:
    # Pins the shipped fit end to end (band, penalty, 577-play anchor, no reference
    # level): each of those reverted alone moves these values (see #598).
    out = cfb_adjusted_epa(_plays_2026()).filter(pl.col("team_id").is_in(["103", "254", "328"])).sort("team_id")
    assert out["team_id"].to_list() == ["103", "254", "328"]  # Boston College, Utah, Utah State
    # Margin rule for the fit and own EPA (2026-11-25); the naive-WP-band fit gave
    # -0.0081 / 0.4917 / -0.1806.
    assert out["net_adj_epa"].to_list() == pytest.approx(
        [-0.03785754374304334, 0.7001746895242886, -0.20214225907970906], abs=1e-6
    )


def test_fit_keeps_at_least_85_percent_of_plays() -> None:
    # The margin rule keeps 90-92% of FBS-vs-FBS pass/rush plays in every full season
    # 2014-2026; this 2026 weeks 1-4 fixture keeps 89.1% (the naive WP band it
    # replaced kept 70.0%).
    from sportsdataverse.cfb.cfb_adjusted_epa import _ADJ_REQUIRED, _prepare

    base, clean = _prepare(_plays_2026(), _ADJ_REQUIRED, None)
    assert clean.height / base.height >= 0.85


def test_empty_fit_band_raises_instead_of_returning_no_rows() -> None:
    plays = _plays_2026().with_columns(period=pl.lit(4, dtype=pl.Int64), pos_score_diff_start=pl.lit(30.0))
    with pytest.raises(ValueError, match="no non-garbage-time plays"):
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
    # abs_tol 1e-6, not 1e-12: the ridge solve differs by BLAS (macOS Accelerate vs Linux OpenBLAS)
    # in the last digits; main's macOS live job went red at 1e-12 while ubuntu passed (2026-10-01).
    assert_frame_equal(got.sort(key), exp.sort(key), check_exact=False, rel_tol=0.0, abs_tol=1e-6)


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


def test_default_method_still_requires_the_score_margin() -> None:
    nfl = _nfl_shaped_pbp()
    with pytest.raises(KeyError, match="pos_score_diff_start"):
        cfb_adjusted_epa(nfl)
    with pytest.raises(KeyError, match="pos_score_diff_start"):
        cfb_adjusted_epa_by_game(nfl)


@pytest.mark.parametrize("fn", [cfb_adjusted_epa, cfb_adjusted_epa_by_game])
def test_unknown_method_raises(fn) -> None:
    with pytest.raises(ValueError, match="method"):
        fn(_synthetic_pbp(), method="pre-598")


# --- cfb_adjusted_epa_ledger: the per-game decomposition and leave-one-game-out influence ---


@pytest.fixture(scope="module")
def ledger_2026() -> pl.DataFrame:
    return cfb_adjusted_epa_ledger(_plays_2026().with_columns(seasonType=pl.lit(2, dtype=pl.Int64)))


def test_ledger_contributions_sum_to_the_season_value(ledger_2026) -> None:
    season = cfb_adjusted_epa(_plays_2026())
    sums = ledger_2026.group_by("team_id").agg(total=pl.col("contribution").sum(), net=pl.col("net_adj_epa").first())
    joined = season.select("team_id", "net_adj_epa").join(sums, on="team_id", how="full", coalesce=True)
    assert joined.height == season.height  # the ledger rates exactly the teams the season fit rates
    assert joined.null_count().sum_horizontal().item() == 0
    assert (joined["total"] - joined["net_adj_epa"]).abs().max() < 1e-12
    assert (joined["net"] - joined["net_adj_epa"]).abs().max() < 1e-12
    parts = ledger_2026["raw_contribution"] + ledger_2026["schedule_contribution"] - ledger_2026["contribution"]
    assert parts.abs().max() < 1e-12


def test_ledger_rows_match_the_season_fit_game_by_game(ledger_2026) -> None:
    # adjusted = raw + schedule term, and the weights are 1 / the games each mean is taken over
    gap = ledger_2026["raw_net_epa"] + ledger_2026["schedule_adj"] - ledger_2026["adj_net_epa"]
    assert gap.abs().max() < 1e-12
    games = ledger_2026.group_by("team_id").agg(w=pl.col("off_weight").sum(), n=pl.len())
    assert (games["w"] - 1.0).abs().max() < 1e-12
    assert set(ledger_2026["site"].unique()) == {"home", "away", "neutral"}
    assert ledger_2026["season_type"].unique().to_list() == [2]
    assert (ledger_2026["off_plays"] > 0).all()


@pytest.mark.parametrize("n_games", [3, 4])
def test_ledger_influence_equals_refitting_without_the_game(ledger_2026, n_games) -> None:
    # The leave-one-game-out value is checked against the public season fit run on the
    # plays with that game removed: an independent, play-level refit.
    plays = _plays_2026()
    games_per_team = ledger_2026.group_by("team_id").agg(n=pl.len())
    team = games_per_team.filter(pl.col("n") == n_games).sort("team_id")["team_id"][0]
    for row in ledger_2026.filter(pl.col("team_id") == team).iter_rows(named=True):
        refit = cfb_adjusted_epa(plays.filter(pl.col("game_id").cast(pl.Utf8) != row["game_id"]))
        expected = refit.filter(pl.col("team_id") == team)["net_adj_epa"].item()
        assert row["loo_net_adj_epa"] == pytest.approx(expected, abs=1e-12)
        assert row["game_influence"] == pytest.approx(row["net_adj_epa"] - expected, abs=1e-12)


def test_ledger_influence_is_null_when_a_team_would_keep_one_game(ledger_2026) -> None:
    # A season value needs two valid games (both sides adjusted). Removing a valid game of a
    # team with exactly two leaves one, so no value; with three or more there always is one.
    # (Team 57 shows why the count is of valid games: one of its three has no defensive play.)
    valid = pl.col("adj_off_epa").is_not_null() & pl.col("adj_def_epa").is_not_null()
    rows = ledger_2026.with_columns(n_valid=valid.sum().over("team_id"), valid=valid)
    two = rows.filter((pl.col("n_valid") == 2) & (pl.col("valid") == True))  # noqa: E712
    assert two.height > 0
    assert two["loo_net_adj_epa"].is_null().all()
    assert rows.filter(pl.col("n_valid") >= 3)["loo_net_adj_epa"].is_not_null().all()


def test_ledger_without_influence_and_as_pandas() -> None:
    plays = _plays_2026()
    plain = cfb_adjusted_epa_ledger(plays, influence=False)
    assert "game_influence" not in plain.columns and "season_type" not in plain.columns
    assert plain.columns[:3] == ["game_id", "week", "team_id"]
    pdf = cfb_adjusted_epa_ledger(plays, influence=False, return_as_pandas=True)
    assert list(pdf.columns) == plain.columns and len(pdf) == plain.height


def test_ledger_influence_when_the_refit_loses_the_home_term_or_a_team() -> None:
    # Edge cases the real fixture never reaches, each checked against the independent play-level refit:
    # game 1 is the only non-neutral game, so leaving it out leaves no home/away contrast (the home term
    # is fit at 0), and team 5 plays one game (99), so leaving it out removes a team from the fit.
    base = _synthetic_pbp().with_columns(neutral_site=pl.col("game_id") != 1)
    extra = base.filter(pl.col("game_id") == 2).with_columns(
        game_id=pl.lit(99, dtype=pl.Int64),
        week=pl.lit(9, dtype=pl.Int64),
        pos_team=pl.when(pl.col("pos_team_id") == "1").then(pl.lit("T1")).otherwise(pl.lit("T5")),
        pos_team_id=pl.when(pl.col("pos_team_id") == "1").then(pl.lit("1")).otherwise(pl.lit("5")),
        def_pos_team_id=pl.when(pl.col("def_pos_team_id") == "1").then(pl.lit("1")).otherwise(pl.lit("5")),
    )
    plays = pl.concat([base, extra], how="vertical_relaxed")
    ledger = cfb_adjusted_epa_ledger(plays)
    assert "5" not in ledger["team_id"].to_list()  # one game: not rated
    for gid in (1, 99):
        refit = cfb_adjusted_epa(plays.filter(pl.col("game_id") != gid)).select("team_id", expected="net_adj_epa")
        rows = ledger.filter(pl.col("game_id") == str(gid)).join(refit, on="team_id", how="left")
        assert rows.height > 0
        for got, expected in rows.select("loo_net_adj_epa", "expected").iter_rows():
            assert got == pytest.approx(expected, abs=1e-12)
