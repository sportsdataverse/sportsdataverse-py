"""The multi-game play-by-play driver never reads a failed fetch as a smaller season (B-M2, task 4.5).

Fixtures are the committed bigballR stats.ncaa.org captures (``tests/fixtures/ncaa/bigballr/html``);
the fetcher is an offline stub serving them, or raising what ``NcaaFetcher`` raises.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_play_by_play
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR

GOOD = GAMES["mbb"][0]
BANNED, MISSING, BROKEN, BLANK = "9000001", "9000002", "9000003", "9000004"


class _Fetcher:
    def __init__(self) -> None:
        self.calls: "list[str]" = []

    def fetch_game_pbp(self, contest_id: object) -> str:
        gid = str(contest_id)
        self.calls.append(gid)
        if gid == BANNED:
            raise AssetFetchError("NCAA fetch failed after rotating proxies (pool exhausted): ban suspected")
        if gid == MISSING:
            raise NoDataError("stats.ncaa.org has no page (HTTP 404)")
        if gid == BROKEN:
            raise ValueError("unreadable page")
        if gid == BLANK:
            return "<html></html>"
        return (HTML_DIR / f"pbp_{gid}.html").read_text(encoding="utf-8")


def test_fetch_failure_raises_by_default() -> None:
    fetcher = _Fetcher()
    with pytest.raises(AssetFetchError, match="ban suspected"):
        ncaa_mbb_play_by_play([GOOD, BANNED], fetcher=fetcher)
    assert fetcher.calls == [GOOD, BANNED]  # no retry of a failed fetch


def test_fetch_failure_default_still_drops_a_missing_or_unreadable_game() -> None:
    df = ncaa_mbb_play_by_play([GOOD, MISSING, BROKEN, BLANK], fetcher=_Fetcher())
    assert df["game_id"].unique().to_list() == [GOOD]
    assert "scrape_status" not in df.columns


def test_fetch_failure_skip_records_a_status_row_per_dropped_game() -> None:
    df = ncaa_mbb_play_by_play([GOOD, BANNED, MISSING, BROKEN, BLANK], fetcher=_Fetcher(), errors="skip")
    played = df.filter(pl.col("game_id") == GOOD)
    assert played.height > 100 and played["scrape_status"].unique().to_list() == ["ok"]
    stubs = dict(df.filter(pl.col("game_id") != GOOD).select("game_id", "scrape_status").iter_rows())
    assert stubs[BANNED].startswith("fetch_failed: NCAA fetch failed")
    assert stubs[MISSING].startswith("no_data: ")
    assert stubs[BROKEN].startswith("parse_failed: unreadable page")
    assert stubs[BLANK] == "empty"
    assert len(stubs) == 4


def test_fetch_failure_wbb_shim_passes_errors_through() -> None:
    class _W(_Fetcher):
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
        ncaa_mbb_play_by_play([GOOD], fetcher=_Fetcher(), errors="ignore")  # type: ignore[call-overload]
