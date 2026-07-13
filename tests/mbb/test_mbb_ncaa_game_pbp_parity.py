"""MBB parity: ``parse_ncaa_bb_game_pbp`` vs the bigballR R oracle.

Exact-equality parity against oracle CSVs produced by running bigballR's
``scrape_game`` (``bigballR/R/all_functions.R:51-1090``, ``convert_events``
``:3179-3238``) on the committed HTML fixtures. All 35 contract columns are
compared cell-for-cell, plus row count and row order, per game.

Fixture games (see ``tests/fixtures/ncaa/bigballr/README.md``):

* 6470186 — blowout / garbage-time path
* 6479639 — close regulation game
* 6479592 — 1 OT
* 1613299 — 2019-era markup (older event vocabulary, same V2 comma grammar)
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_game_pbp import (
    PBP_SCHEMA,
    ncaa_mbb_play_by_play,
    parse_ncaa_bb_game_pbp,
)
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR, load_oracle_pbp


@pytest.fixture(scope="module")
def oracle() -> pl.DataFrame:
    return load_oracle_pbp("mbb")


def _fixture_html(game_id: str) -> str:
    return (HTML_DIR / f"pbp_{game_id}.html").read_text(encoding="utf-8")


@pytest.mark.parametrize("game_id", GAMES["mbb"])
def test_mbb_pbp_parity(game_id: str, oracle: pl.DataFrame) -> None:
    got = parse_ncaa_bb_game_pbp(_fixture_html(game_id), game_id)
    exp = oracle.filter(pl.col("game_id") == game_id)

    assert got.columns == list(PBP_SCHEMA)
    assert got.schema == pl.Schema(PBP_SCHEMA)
    assert got.height == exp.height, f"row count {got.height} != oracle {exp.height}"
    for col in PBP_SCHEMA:
        assert got[col].to_list() == exp[col].to_list(), f"column {col!r} diverges"


def test_empty_html_returns_contract_schema() -> None:
    got = parse_ncaa_bb_game_pbp("<html><body></body></html>", "0")
    assert got.height == 0
    assert got.schema == pl.Schema(PBP_SCHEMA)


class _FixtureFetcher:
    """Offline stand-in for ``NcaaFetcher`` reading the committed captures."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_game_pbp(self, contest_id: object) -> str:
        self.calls.append(str(contest_id))
        path = HTML_DIR / f"pbp_{contest_id}.html"
        if not path.exists():
            return "<html></html>"
        return path.read_text(encoding="utf-8")


def test_multi_game_driver_offline(oracle: pl.DataFrame) -> None:
    """``get_play_by_play`` port (all_functions.R:1857-1897): drop NA ids,
    retry a failed game once, row-bind the survivors, drop the failures."""
    fetcher = _FixtureFetcher()
    ids: list[object] = [GAMES["mbb"][0], None, "999999999", GAMES["mbb"][1]]
    got = ncaa_mbb_play_by_play(ids, fetcher=fetcher)

    keep = [GAMES["mbb"][0], GAMES["mbb"][1]]
    exp_n = oracle.filter(pl.col("game_id").is_in(keep)).height
    assert got.height == exp_n
    assert got["game_id"].unique().sort().to_list() == sorted(keep)
    # the bogus id was fetched twice (R retries a NULL/empty scrape once) ...
    assert fetcher.calls.count("999999999") == 2
    # ... and the NA id was dropped before any fetch
    assert "None" not in fetcher.calls
