"""``parse_ncaa_bb_game_pbp`` page-format guard and the drivers' contest-id handling.

Parity with bigballR lives in ``test_mbb_ncaa_game_pbp_parity.py``; this file pins what the
parser does with a page it does not read, on a real capture (B-GB-27), and that a float
contest id never reaches a URL or the ``game_id`` column as ``"6470186.0"`` (B-L38).
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.errors import UnsupportedPageFormatWarning
from sportsdataverse.mbb.mbb_ncaa_game_pbp import PBP_SCHEMA, ncaa_mbb_game_pbp, ncaa_mbb_play_by_play
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_play_by_play
from tests.mbb._bigballr_oracle import HTML_DIR

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
#: Real 2018-format page (tests/fixtures/ncaa/README.md): line score first, headed '1st Half' ... 'Total'.
PAGE_2018 = FIXTURES / "ncaa" / "test_play_by_play.html"


def test_format_guard_2018_page_warns_and_returns_zero_row_schema() -> None:
    from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp

    html = PAGE_2018.read_text(encoding="utf-8")
    with pytest.warns(
        UnsupportedPageFormatWarning, match=r"game 123 is a 2018-format stats\.ncaa\.org play-by-play page"
    ):
        got = parse_ncaa_bb_game_pbp(html, "123")
    assert got.height == 0
    assert got.schema == pl.Schema(PBP_SCHEMA)


def test_format_guard_silent_on_supported_page() -> None:
    from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp

    html = (HTML_DIR / "pbp_6470186.html").read_text(encoding="utf-8")
    with warnings.catch_warnings():
        warnings.simplefilter("error", UnsupportedPageFormatWarning)
        got = parse_ncaa_bb_game_pbp(html, "6470186")
    assert got.height > 0


class _Fetcher:
    def __init__(self, page: Path = HTML_DIR / "pbp_6470186.html") -> None:
        self.html = page.read_text(encoding="utf-8")
        self.calls: "list[object]" = []

    def fetch_game_pbp(self, game_id: object) -> str:
        self.calls.append(game_id)
        return self.html


@pytest.mark.parametrize("driver", [ncaa_mbb_play_by_play, ncaa_wbb_play_by_play])
def test_format_guard_escalated_warning_propagates_from_driver(driver: Any) -> None:
    """A pipeline that escalates the guard gets the exception, not 0 rows after a re-fetch (R853-1)."""
    f = _Fetcher(PAGE_2018)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(UnsupportedPageFormatWarning, match=r"game 123 is a 2018-format"):
            driver(["123"], fetcher=f)
    assert f.calls == ["123"]


@pytest.mark.parametrize("driver", [ncaa_mbb_play_by_play, ncaa_wbb_play_by_play])
def test_format_guard_unsupported_page_is_not_refetched(driver: Any) -> None:
    """The guard's verdict is about the page: one fetch, one warning, no empty-result retry (R853-8)."""
    f = _Fetcher(PAGE_2018)
    with pytest.warns(UnsupportedPageFormatWarning, match=r"game 123 is a 2018-format") as record:
        got = driver(["123"], fetcher=f)
    assert f.calls == ["123"]
    assert len([w for w in record if issubclass(w.category, UnsupportedPageFormatWarning)]) == 1
    assert got.height == 0


class _FlakyFetcher(_Fetcher):
    """Raises on the first fetch, then serves the real page."""

    def fetch_game_pbp(self, game_id: object) -> str:
        self.calls.append(game_id)
        if len(self.calls) == 1:
            raise ConnectionError("stats.ncaa.org dropped the connection")
        return self.html


@pytest.mark.parametrize("driver", [ncaa_mbb_play_by_play, ncaa_wbb_play_by_play])
def test_failed_scrape_is_retried_once(driver: Any) -> None:
    """A fetch that raises is retried once, as R's tryCatch -> NULL retry does (:1870-1878)."""
    f = _FlakyFetcher()
    got = driver(["6470186"], fetcher=f)
    assert f.calls == ["6470186", "6470186"]
    assert got.height > 0


def test_float_contest_id_is_an_integer_string() -> None:
    f = _Fetcher()
    df = ncaa_mbb_game_pbp(6470186.0, fetcher=f)
    assert f.calls == ["6470186"]
    assert df["game_id"].unique().to_list() == ["6470186"]

    f = _Fetcher()
    df = ncaa_mbb_play_by_play([6470186.0, float("nan"), None], fetcher=f)
    assert f.calls == ["6470186"]
    assert df["game_id"].unique().to_list() == ["6470186"]


def test_non_integral_contest_id_raises() -> None:
    with pytest.raises(ValueError, match="not an NCAA contest id"):
        ncaa_mbb_game_pbp(6470186.5, fetcher=_Fetcher())
