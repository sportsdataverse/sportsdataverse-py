"""``parse_ncaa_bb_game_pbp`` page-format guard, the drivers' contest-id handling, and their failed-fetch contract.

Parity with bigballR lives in ``test_mbb_ncaa_game_pbp_parity.py``; this file pins what the
parser does with a page it does not read, on a real capture (B-GB-27), and that a float
contest id never reaches a URL or the ``game_id`` column as ``"6470186.0"`` (B-L38).

It also pins that the multi-game driver never reads a failed fetch as a smaller season (B-M2,
task 4.5). Fixtures are the committed bigballR stats.ncaa.org captures
(``tests/fixtures/ncaa/bigballr/html``); those tests' fetcher is an offline stub serving them,
or raising what ``NcaaFetcher`` raises.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError, UnsupportedPageFormatWarning
from sportsdataverse.mbb.mbb_ncaa_game_pbp import PBP_SCHEMA, ncaa_mbb_game_pbp, ncaa_mbb_play_by_play
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_play_by_play
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR

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


def test_format_guard_skip_labels_the_rejected_layout_not_empty() -> None:
    """errors="skip" names the 2018 layout instead of calling the page empty (R974-1)."""
    f = _Fetcher(PAGE_2018)
    with pytest.warns(UnsupportedPageFormatWarning):
        got = ncaa_mbb_play_by_play(["123"], fetcher=f, errors="skip")
    assert f.calls == ["123"]
    (status,) = got["scrape_status"].to_list()
    assert status.startswith("unsupported_layout: a 2018-format stats.ncaa.org play-by-play page")


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


GOOD = GAMES["mbb"][0]
BANNED, MISSING, BROKEN, BLANK, REJECTED = "9000001", "9000002", "9000003", "9000004", "9000005"


class _StatusFetcher:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_game_pbp(self, contest_id: object) -> str:
        gid = str(contest_id)
        self.calls.append(gid)
        if gid == BANNED:
            raise AssetFetchError("NCAA fetch failed after rotating proxies (pool exhausted): ban suspected")
        if gid == MISSING:
            raise NoDataError("stats.ncaa.org has no page (HTTP 404)")
        if gid == REJECTED:
            raise ValueError("stats.ncaa.org rejected https://stats.ncaa.org/contests/9000005/play_by_play: HTTP 400")
        if gid == BROKEN:
            return "<html>BROKEN</html>"
        if gid == BLANK:
            return "<html></html>"
        return (HTML_DIR / f"pbp_{gid}.html").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _broken_page_breaks_the_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """The BROKEN page is fetched fine and then crashes the parser (a parse failure, not a fetch one)."""
    from sportsdataverse.mbb import mbb_ncaa_game_pbp as mod

    real = mod.parse_ncaa_bb_game_pbp

    def parse(html: str, *args: object, **kwargs: object) -> pl.DataFrame:
        if "BROKEN" in html:
            raise ValueError("unreadable page")
        return real(html, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(mod, "parse_ncaa_bb_game_pbp", parse)


def test_fetch_failure_raises_by_default() -> None:
    fetcher = _StatusFetcher()
    with pytest.raises(AssetFetchError, match="ban suspected"):
        ncaa_mbb_play_by_play([GOOD, BANNED], fetcher=fetcher)
    assert fetcher.calls == [GOOD, BANNED]  # no retry of a failed fetch


def test_fetch_failure_default_still_drops_a_missing_or_unreadable_game() -> None:
    df = ncaa_mbb_play_by_play([GOOD, MISSING, BROKEN, BLANK, REJECTED], fetcher=_StatusFetcher())
    assert df["game_id"].unique().to_list() == [GOOD]
    assert "scrape_status" not in df.columns


def test_fetch_failure_skip_records_a_status_row_per_dropped_game() -> None:
    df = ncaa_mbb_play_by_play(
        [GOOD, BANNED, MISSING, BROKEN, BLANK, REJECTED], fetcher=_StatusFetcher(), errors="skip"
    )
    played = df.filter(pl.col("game_id") == GOOD)
    assert played.height > 100 and played["scrape_status"].unique().to_list() == ["ok"]
    stubs = dict(df.filter(pl.col("game_id") != GOOD).select("game_id", "scrape_status").iter_rows())
    assert stubs[BANNED].startswith("fetch_failed: NCAA fetch failed")
    assert stubs[MISSING].startswith("no_data: ")
    assert stubs[BROKEN].startswith("parse_failed: unreadable page")
    assert stubs[BLANK] == "empty"
    assert stubs[REJECTED].startswith("request_error: stats.ncaa.org rejected")  # a 400 is not a parser problem
    assert len(stubs) == 5


def test_fetch_failure_wbb_shim_passes_errors_through() -> None:
    class _W(_StatusFetcher):
        def fetch_game_pbp(self, contest_id: object) -> str:
            if str(contest_id) == BANNED:
                raise AssetFetchError("pool exhausted")
            return (HTML_DIR / f"pbp_{contest_id}.html").read_text(encoding="utf-8")

    good = GAMES["wbb"][0]
    with pytest.raises(AssetFetchError):
        ncaa_wbb_play_by_play([good, BANNED], fetcher=_W())
    df = ncaa_wbb_play_by_play([good, BANNED], fetcher=_W(), errors="skip")
    assert df.filter(pl.col("game_id") == BANNED)["scrape_status"].to_list() == ["fetch_failed: pool exhausted"]


def test_fetch_failure_errors_must_be_raise_or_skip() -> None:
    with pytest.raises(ValueError, match="errors must be"):
        ncaa_mbb_play_by_play([GOOD], fetcher=_StatusFetcher(), errors="ignore")  # type: ignore[call-overload]
