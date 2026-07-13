"""WBB parity: ``parse_ncaa_bb_box`` vs the wbigballR R oracle.

Oracle CSV produced by running wbigballR ``get_box_scores``
(``wbigballR/R/all_functions.R:3387-3452``, ``scrape_box`` ``:3288-3376``)
over the four committed women's ``individual_stats_{id}.html`` fixtures.

The women's oracle carries 25 columns (in a reversed order -- wbigballR's
``select(Game_ID:MP, ...)`` is a *backwards* positional range because
``Game_ID`` sits last in the frame). The 3 men's-only columns absent here
are ``Box_ID``, ``Player``, and ``Team``; the women's page also ships the
fouls column under the ``PF`` header (men's: ``Fouls``). The shared core
emits the men's superset -- this suite selects the oracle's 25, mapping
``PF`` -> ``fouls``.

Documented divergence: wbigballR never numeric-cleans ``PF`` (its count-col
lists name ``Fouls``), leaving PF a character column; the core normalizes
``PF`` -> ``Fouls`` and cleans it, so PF is compared numerically here.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_box_stats import parse_ncaa_bb_box
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR, load_oracle

#: wbigballR oracle column -> module snake_case column, in oracle order.
ORACLE_TO_SNAKE: "dict[str, str]" = {
    "Game_ID": "game_id",
    "CleanName": "clean_name",
    "Tech": "tech",
    "DQ": "dq",
    "PF": "fouls",
    "BLK": "blk",
    "STL": "stl",
    "TO": "to",
    "AST": "ast",
    "TRB": "trb",
    "DRB": "drb",
    "ORB": "orb",
    "PTS": "pts",
    "FTA": "fta",
    "FTM": "ftm",
    "TPA": "tpa",
    "TPM": "tpm",
    "FGA": "fga",
    "FGM": "fgm",
    "MP": "mp",
    "FG.": "fg_pct",
    "TP.": "tp_pct",
    "FT.": "ft_pct",
    "TS.": "ts_pct",
    "eFG.": "efg_pct",
}

STRING_COLS = {"game_id", "clean_name"}

#: Men's-superset columns the older wbigballR fork never emitted.
M_ONLY_COLS = {"box_id", "player", "team"}


def _fixture_html(game_id: str) -> str:
    return (HTML_DIR / f"individual_stats_{game_id}.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def oracle() -> pl.DataFrame:
    return load_oracle("box_scores", "wbb")


@pytest.fixture(scope="module")
def built() -> pl.DataFrame:
    frames = [parse_ncaa_bb_box(_fixture_html(gid), gid) for gid in GAMES["wbb"]]
    return pl.concat(frames, how="diagonal_relaxed")


def test_wbb_core_emits_m_superset(built: pl.DataFrame, oracle: pl.DataFrame) -> None:
    """Core output = oracle's 25 (mapped) + exactly the 3 men's-only columns."""
    extra = set(built.columns) - set(ORACLE_TO_SNAKE.values())
    assert extra == M_ONLY_COLS


def test_wbb_box_scores_row_count(built: pl.DataFrame, oracle: pl.DataFrame) -> None:
    assert built.height == oracle.height


@pytest.mark.parametrize("r_col", list(ORACLE_TO_SNAKE))
def test_wbb_box_scores_parity(r_col: str, built: pl.DataFrame, oracle: pl.DataFrame) -> None:
    """Cell-for-cell parity in oracle row order (no re-sorting)."""
    exp = oracle.get_column(r_col)
    act = built.get_column(ORACLE_TO_SNAKE[r_col])
    if ORACLE_TO_SNAKE[r_col] in STRING_COLS:
        if exp.dtype != pl.Utf8:
            exp = exp.cast(pl.Int64).cast(pl.Utf8)  # ids: int -> exact digits, never via float
        assert act.to_list() == exp.to_list(), f"string mismatch in {r_col}"
    else:
        # PF arrives as the site's raw strings in the oracle (wbigballR never
        # cleans it); numeric cast is the documented comparison bridge.
        exp_f = exp.cast(pl.Float64, strict=False) if exp.dtype == pl.Utf8 else exp.cast(pl.Float64)
        act_f = act.cast(pl.Float64)
        assert act_f.is_null().to_list() == exp_f.is_null().to_list(), f"null mismatch in {r_col}"
        diff = (act_f - exp_f).abs()
        max_diff = diff.max()
        assert max_diff is None or max_diff <= 1e-9, f"{r_col}: max abs diff {max_diff}"
