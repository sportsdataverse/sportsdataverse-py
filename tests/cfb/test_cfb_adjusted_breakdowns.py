"""cfb_adjusted_breakdowns on a real FBS season (tests/fixtures/cfb_adjusted_breakdowns/fbs_plays_2025.parquet)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.cfb import cfb_adjusted_epa
from sportsdataverse.cfb.cfb_adjusted_breakdowns import (
    _METRICS,
    _PUBLISHED,
    _kept_plays,
    _slice_values,
    cfb_adjusted_breakdowns,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"
PLAYS_2025 = FIXTURES / "cfb_adjusted_breakdowns" / "fbs_plays_2025.parquet"
PLAYS_2026 = FIXTURES / "cfb_adjusted_epa" / "fbs_plays_2026_wk1_4.parquet"

# Each published slice written out again from the fixture's own columns, independent of the module's masks.
_SLICES = {
    "epa_all": (pl.col("EPA").is_not_null(), "EPA"),
    "success_all": (pl.col("EPA").is_not_null(), "epa_success"),
    "epa_early_down": (pl.col("down").is_in([1, 2]), "EPA"),
    "epa_standard_down": (pl.col("standard_down") == True, "EPA"),  # noqa: E712
    "epa_first_half": (pl.col("period").is_in([1, 2]), "EPA"),
}


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    return pl.read_parquet(PLAYS_2025)


@pytest.fixture(scope="module")
def board(plays: pl.DataFrame) -> pl.DataFrame:
    return cfb_adjusted_breakdowns(plays)


def _one_sided_defenders(clean: pl.DataFrame) -> set[str]:
    """Teams with a game where they defended kept plays but ran none (cfb_adjusted_epa drops that defensive game)."""
    off = clean.select(t="pos_team_id", g="game_id").unique()
    dfn = clean.select(t="def_pos_team_id", g="game_id").unique()
    return set(dfn.join(off, on=["t", "g"], how="anti")["t"].to_list())


@pytest.mark.parametrize(("path", "one_sided"), [(PLAYS_2025, set()), (PLAYS_2026, {"145"})], ids=["2025", "2026wk1-4"])
def test_epa_all_is_cfb_adjusted_epa(path: Path, one_sided: set[str]) -> None:
    """Gate (a): the EPA-all-downs breakdown is cfb_adjusted_epa's own fit, team for team.

    The one exception is a team with a game it defended but did not run a kept play in (2026 week 1-4: Ole Miss in
    401856699, a game the snapshot holds 8 Florida snaps of): cfb_adjusted_epa keys games on offensive rows and drops
    that defensive game, the breakdown keeps it.
    """
    plays = pl.read_parquet(path)
    clean = _kept_plays(plays)
    assert _one_sided_defenders(clean) == one_sided
    v = _slice_values(clean, "epa_all")
    ref = cfb_adjusted_epa(plays)
    for side in ("off", "def"):
        s = v.filter(pl.col("side") == side)
        assert s.schema["team_id"] == ref.schema["team_id"]
        assert s.height == ref.height
        j = s.join(ref, on="team_id", how="left").with_columns(
            gap=(pl.col("adj") - pl.col("m") - pl.col(f"adj_{side}_epa")).abs()
        )
        assert j["gap"].null_count() == 0
        same = j.filter(~pl.col("team_id").is_in(list(one_sided)) | (side == "off"))
        assert same["gap"].max() <= 1e-12, side
        if side == "def" and one_sided:
            assert j.filter(pl.col("team_id").is_in(list(one_sided)))["gap"].min() > 1e-3


def test_slices_counts_and_raw_values(board: pl.DataFrame) -> None:
    """Each published slice's n, raw value and league mean, recomputed from the fixture with its own filters."""
    clean = _kept_plays(pl.read_parquet(PLAYS_2025))
    assert {m for m, _ in _PUBLISHED} == set(_SLICES)
    for metric, (mask, resp) in _SLICES.items():
        sl = clean.filter(mask.fill_null(False)).select(
            "pos_team_id", "def_pos_team_id", y=pl.col(resp).cast(pl.Float64)
        )
        m = sl["y"].mean()
        assert _slice_values(clean, metric)["m"][0] == pytest.approx(m, abs=1e-12), metric
        for side, key in (("off", "pos_team_id"), ("def", "def_pos_team_id")):
            exp = sl.group_by(key).agg(n=pl.len(), raw=pl.col("y").mean()).rename({key: "team_id"})
            got = board.select("team_id", n=f"{metric}_{side}_n", raw=f"{metric}_{side}_raw").drop_nulls()
            j = got.join(exp, on="team_id", how="left", suffix="_exp")
            assert j.height > 120, (metric, side)
            assert (j["n"] == j["n_exp"]).all(), (metric, side)
            assert (j["raw"] - j["raw_exp"]).abs().max() <= 1e-12, (metric, side)


def test_shrunk_is_the_closed_form(board: pl.DataFrame) -> None:
    """Gate (c): _shrunk = m + n / (n + k) (v - m) on three named 2025 teams, with m from the fixture directly."""
    clean = _kept_plays(pl.read_parquet(PLAYS_2025))
    teams = board.filter(pl.col("pos_team").is_in(["Indiana", "Ohio State", "Kent State"]))
    assert teams.height == 3
    for (metric, side), (tier, k) in _PUBLISHED.items():
        mask, resp = _SLICES[metric]
        m = clean.filter(mask.fill_null(False))[resp].cast(pl.Float64).mean()
        p = f"{metric}_{side}"
        for row in teams.iter_rows(named=True):
            v = row[f"{p}_adj"] if tier == "adj" else row[f"{p}_raw"]
            n = row[f"{p}_n"]
            assert row[f"{p}_shrunk"] == pytest.approx(m + n / (n + k) * (v - m), abs=1e-12), (p, row["pos_team"])


def test_columns_follow_the_published_table(board: pl.DataFrame) -> None:
    expected = ["team_id", "pos_team"]
    for metric in dict.fromkeys(m for m, _ in _PUBLISHED):
        for side in ("off", "def"):
            if (metric, side) in _PUBLISHED:
                expected += [f"{metric}_{side}_{c}" for c in ("raw", "adj", "shrunk", "n", "rank")]
    assert board.columns == expected
    assert board["team_id"].n_unique() == board.height
    assert board.schema["team_id"] == pl.Utf8


def test_raw_tier_has_null_adjusted_and_adj_tier_does_not(board: pl.DataFrame) -> None:
    for (metric, side), (tier, _) in _PUBLISHED.items():
        adj = board[f"{metric}_{side}_adj"]
        if tier == "raw":
            assert adj.null_count() == board.height
        else:
            assert adj.null_count() < board.height


def test_shrinkage_pulls_toward_the_mean_and_ranks_the_display_value(board: pl.DataFrame) -> None:
    for (metric, side), (tier, _) in _PUBLISHED.items():
        p = f"{metric}_{side}"
        v = pl.col(f"{p}_adj") if tier == "adj" else pl.col(f"{p}_raw")
        rated = board.filter(pl.col(f"{p}_shrunk").is_not_null())
        spread = rated.select(display=pl.col(f"{p}_shrunk").std(), value=v.std())
        assert spread["display"][0] < spread["value"][0], p
        best = rated.sort(f"{p}_rank").row(0, named=True)
        top = rated[f"{p}_shrunk"].max() if side == "off" else rated[f"{p}_shrunk"].min()
        assert best[f"{p}_shrunk"] == top, p


def test_metrics_argument_selects_and_orders(plays: pl.DataFrame) -> None:
    names = list(dict.fromkeys(m for m, _ in _PUBLISHED))[:2][::-1]
    out = cfb_adjusted_breakdowns(plays, metrics=names)
    assert [c.rsplit("_", 2)[0] for c in out.columns[2::5]] == [
        m for m in names for s in ("off", "def") if (m, s) in _PUBLISHED
    ]


def test_unpublished_or_unknown_metric_raises(plays: pl.DataFrame) -> None:
    unpublished = [m for m in _METRICS if not any((m, s) in _PUBLISHED for s in ("off", "def"))]
    for name in ["not_a_metric", *unpublished[:1]]:
        with pytest.raises(ValueError, match="not a published breakdown"):
            cfb_adjusted_breakdowns(plays, metrics=[name])


def test_missing_column_raises(plays: pl.DataFrame) -> None:
    with pytest.raises(KeyError, match="pos_score_diff_start"):
        cfb_adjusted_breakdowns(plays.drop("pos_score_diff_start"))


def test_pandas_matches_polars(plays: pl.DataFrame, board: pl.DataFrame) -> None:
    name = next(iter(_PUBLISHED))[0]
    pdf = cfb_adjusted_breakdowns(plays, metrics=[name], return_as_pandas=True)
    assert list(pdf.columns) == cfb_adjusted_breakdowns(plays, metrics=[name]).columns
    assert len(pdf) == board.height


def test_release_column_names_work_like_the_build_input(plays: pl.DataFrame) -> None:
    """The released pbp spells two slice columns differently from the team-summaries input."""
    released = plays.rename({"epa_success": "EPA_success", "down": "start.down"}).with_columns(
        pl.col("EPA_success") == 1
    )
    a = cfb_adjusted_breakdowns(plays)
    b = cfb_adjusted_breakdowns(released)
    # polars' threaded group-by means can differ in the last ULP between two runs of the same input
    assert_frame_equal(a, b, check_exact=False, abs_tol=1e-12)
