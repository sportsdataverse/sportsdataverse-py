"""MBB parity: ``parse_ncaa_bb_box`` vs the bigballR R oracle.

Oracle CSV produced by running bigballR ``get_box_scores``
(``bigballR/R/all_functions.R:3603-3678``, ``scrape_box`` ``:3492-3601``)
over the four committed ``individual_stats_{id}.html`` fixtures. All 28
oracle columns are compared cell-for-cell: strings/ids exact, floats
abs-tol 1e-9.

Row order note: within each team block the MBB oracle rows follow the
*rendered* page's DataTables sort (minutes descending -- a client-side JS
artifact of the chromote capture), not the static markup's row order, so
document order cannot reproduce it. Both sides are therefore sorted by
``(game_id, team, player)`` before comparison (the sanctioned fallback);
game/team block structure (home rows first) is still asserted via the
unsorted ``Team``/``Game_ID`` columns.

``multi_games=True`` has NO oracle -- R's aggregation groups by the dropped
``Pos`` column and hard-errors on current markup -- so it is locked by an
invariant test instead: aggregated counters must equal the per-game sums,
``g`` the game count, and each rate must be recomputed from the summed
counters (NaN -> 0).
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_box_stats import ncaa_mbb_box_scores, parse_ncaa_bb_box
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR, load_oracle

#: Oracle (R contract) column -> module snake_case column, in oracle order.
ORACLE_TO_SNAKE: "dict[str, str]" = {
    "Game_ID": "game_id",
    "Box_ID": "box_id",
    "Player": "player",
    "CleanName": "clean_name",
    "Team": "team",
    "MP": "mp",
    "PTS": "pts",
    "ORB": "orb",
    "DRB": "drb",
    "TRB": "trb",
    "AST": "ast",
    "TO": "to",
    "STL": "stl",
    "BLK": "blk",
    "FGA": "fga",
    "FGM": "fgm",
    "FG.": "fg_pct",
    "TPA": "tpa",
    "TPM": "tpm",
    "TP.": "tp_pct",
    "FTA": "fta",
    "FTM": "ftm",
    "FT.": "ft_pct",
    "TS.": "ts_pct",
    "eFG.": "efg_pct",
    "Fouls": "fouls",
    "DQ": "dq",
    "Tech": "tech",
}

STRING_COLS = {"game_id", "box_id", "player", "clean_name", "team"}

COUNTER_COLS = [
    "mp", "pts", "orb", "drb", "trb", "ast", "to", "stl", "blk",
    "fga", "fgm", "tpa", "tpm", "fta", "ftm", "fouls", "dq", "tech",
]  # fmt: skip


def _fixture_html(game_id: str) -> str:
    return (HTML_DIR / f"individual_stats_{game_id}.html").read_text(encoding="utf-8")


class _FixtureFetcher:
    """Offline fetcher replaying the committed fixtures."""

    def fetch_game_individual_stats(self, contest_id: object) -> str:
        return _fixture_html(str(contest_id))


@pytest.fixture(scope="module")
def oracle() -> pl.DataFrame:
    return load_oracle("box_scores", "mbb")


@pytest.fixture(scope="module")
def built() -> pl.DataFrame:
    frames = [parse_ncaa_bb_box(_fixture_html(gid), gid) for gid in GAMES["mbb"]]
    return pl.concat(frames, how="diagonal_relaxed")


@pytest.fixture(scope="module")
def oracle_sorted(oracle: pl.DataFrame) -> pl.DataFrame:
    """Oracle re-keyed for order-insensitive comparison (see module docstring)."""
    return oracle.with_columns(pl.col("Game_ID").cast(pl.Utf8)).sort(["Game_ID", "Team", "Player"])


@pytest.fixture(scope="module")
def built_sorted(built: pl.DataFrame) -> pl.DataFrame:
    return built.sort(["game_id", "team", "player"])


def _assert_column_parity(got: pl.DataFrame, oracle: pl.DataFrame, r_col: str, py_col: str) -> None:
    exp = oracle.get_column(r_col)
    act = got.get_column(py_col)
    if py_col in STRING_COLS:
        if exp.dtype != pl.Utf8:
            exp = exp.cast(pl.Int64).cast(pl.Utf8)  # ids: int -> exact digits, never via float
        assert act.to_list() == exp.to_list(), f"string mismatch in {r_col}"
    else:
        exp_f = exp.cast(pl.Float64)
        act_f = act.cast(pl.Float64)
        assert act_f.is_null().to_list() == exp_f.is_null().to_list(), f"null mismatch in {r_col}"
        diff = (act_f - exp_f).abs()
        max_diff = diff.max()
        assert max_diff is None or max_diff <= 1e-9, f"{r_col}: max abs diff {max_diff}"


def test_mbb_box_scores_columns(built: pl.DataFrame) -> None:
    """Module emits exactly the 28 oracle columns, snake_cased, in oracle order."""
    assert built.columns == list(ORACLE_TO_SNAKE.values())


def test_mbb_box_scores_row_count(built: pl.DataFrame, oracle: pl.DataFrame) -> None:
    assert built.height == oracle.height


def test_mbb_box_scores_block_structure(built: pl.DataFrame, oracle: pl.DataFrame) -> None:
    """Game order and home-first team blocks match the oracle exactly (unsorted)."""
    assert built.get_column("game_id").to_list() == [str(x) for x in oracle.get_column("Game_ID").to_list()]
    assert built.get_column("team").to_list() == oracle.get_column("Team").to_list()


@pytest.mark.parametrize("r_col", list(ORACLE_TO_SNAKE))
def test_mbb_box_scores_parity(r_col: str, built_sorted: pl.DataFrame, oracle_sorted: pl.DataFrame) -> None:
    """Cell-for-cell parity, both sides sorted by (game_id, team, player) -- see module docstring."""
    _assert_column_parity(built_sorted, oracle_sorted, r_col, ORACLE_TO_SNAKE[r_col])


def test_mbb_box_scores_public_fn_matches_core(built: pl.DataFrame) -> None:
    """The get_box_scores driver (injected fetcher) reproduces the concatenated core output."""
    via_public = ncaa_mbb_box_scores(GAMES["mbb"], fetcher=_FixtureFetcher())
    assert via_public.equals(built)


def test_mbb_multi_games_invariants(built: pl.DataFrame) -> None:
    """multi_games=True == per-game sums on (player, clean_name, team); rates recomputed."""
    multi = ncaa_mbb_box_scores(GAMES["mbb"], multi_games=True, fetcher=_FixtureFetcher())
    keys = ["player", "clean_name", "team"]

    manual = (
        built.group_by(keys)
        .agg(
            *(pl.col(c).sum() for c in COUNTER_COLS),
            pl.len().cast(pl.Float64).alias("g"),
        )
        .sort(keys)
    )
    assert multi.height == manual.height
    assert multi.columns == [
        "player", "clean_name", "team", "mp", "g", "pts", "orb", "drb", "trb",
        "ast", "to", "stl", "blk", "fga", "fgm", "fg_pct", "tpa", "tpm",
        "tp_pct", "fta", "ftm", "ft_pct", "ts_pct", "efg_pct", "fouls", "dq", "tech",
    ]  # fmt: skip

    joined = multi.join(manual, on=keys, how="inner", suffix="_manual")
    assert joined.height == multi.height
    for c in [*COUNTER_COLS, "g"]:
        diff = (joined.get_column(c) - joined.get_column(f"{c}_manual")).abs()
        max_diff = diff.max()
        assert max_diff is None or max_diff <= 1e-9, f"multi_games counter {c}"

    # Rates recomputed from summed counters, NaN -> 0 (R zeroes NaN, keeps Inf).
    rates = {
        "fg_pct": pl.col("fgm") / pl.col("fga"),
        "tp_pct": pl.col("tpm") / pl.col("tpa"),
        "ft_pct": pl.col("ftm") / pl.col("fta"),
        "ts_pct": (pl.col("pts") / 2) / (pl.col("fga") + 0.475 * pl.col("fta")),
        "efg_pct": (pl.col("fgm") + 0.5 * pl.col("tpm")) / pl.col("fga"),
    }
    recomputed = multi.with_columns(expr.fill_nan(0.0).alias(f"{name}_re") for name, expr in rates.items())
    for name in rates:
        diff = (recomputed.get_column(name) - recomputed.get_column(f"{name}_re")).abs()
        max_diff = diff.max()
        assert max_diff is None or max_diff <= 1e-9, f"multi_games rate {name}"


def test_empty_html_returns_empty_schema() -> None:
    df = parse_ncaa_bb_box("<html><body>nope</body></html>", "0")
    assert df.height == 0
    assert df.columns == list(ORACLE_TO_SNAKE.values())
