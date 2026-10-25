"""The multi-game box-score driver never reads a failed fetch as a smaller season (B-M2, task 4.5).

Fixtures are the committed bigballR stats.ncaa.org captures (``tests/fixtures/ncaa/bigballr/html``).
``box_<id>.html`` is the real box-score LANDING page: served where the per-player
``individual_stats`` tab is expected, the parser warns and returns zero rows -- a real
unreadable page. The tests assert that warning with ``pytest.warns``, so they take the same
path under pytest.ini's ``error`` filter and under the live job's ``-W default``.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.mbb.mbb_ncaa_box_stats import ncaa_mbb_box_scores
from sportsdataverse.wbb.wbb_ncaa_box_stats import ncaa_wbb_box_scores
from tests.mbb._bigballr_oracle import GAMES, HTML_DIR

GOOD = GAMES["mbb"][0]
WRONG_TAB = GAMES["mbb"][1]
BANNED, MISSING, REJECTED = "9000001", "9000002", "9000005"
NOT_INDIVIDUAL_STATS = rf"game id {WRONG_TAB} is not an individual_stats page"


class _Fetcher:
    def fetch_game_individual_stats(self, contest_id: object) -> str:
        gid = str(contest_id)
        if gid == BANNED:
            raise AssetFetchError("NCAA fetch failed: every proxy in the pool is banned")
        if gid == MISSING:
            raise NoDataError("stats.ncaa.org has no page (HTTP 404)")
        if gid == REJECTED:
            raise ValueError(
                "stats.ncaa.org rejected https://stats.ncaa.org/contests/9000005/individual_stats: HTTP 422"
            )
        page = "box" if gid == WRONG_TAB else "individual_stats"
        return (HTML_DIR / f"{page}_{gid}.html").read_text(encoding="utf-8")


def test_fetch_failure_raises_by_default() -> None:
    with pytest.raises(AssetFetchError, match="banned"):
        ncaa_mbb_box_scores([GOOD, BANNED], fetcher=_Fetcher())


def test_fetch_failure_default_still_skips_a_missing_or_unreadable_game() -> None:
    with pytest.warns(UserWarning, match=NOT_INDIVIDUAL_STATS):
        df = ncaa_mbb_box_scores([GOOD, MISSING, WRONG_TAB], fetcher=_Fetcher())
    assert df["game_id"].unique().to_list() == [GOOD]
    assert "scrape_status" not in df.columns


def test_fetch_failure_skip_records_a_status_row_per_skipped_game() -> None:
    with pytest.warns(UserWarning, match=NOT_INDIVIDUAL_STATS):
        df = ncaa_mbb_box_scores([GOOD, BANNED, MISSING, WRONG_TAB, REJECTED], fetcher=_Fetcher(), errors="skip")
    played = df.filter(pl.col("game_id") == GOOD)
    assert played.height > 10 and played["scrape_status"].unique().to_list() == ["ok"]
    stubs = dict(df.filter(pl.col("game_id") != GOOD).select("game_id", "scrape_status").iter_rows())
    assert stubs[BANNED].startswith("fetch_failed: NCAA fetch failed")
    assert stubs[MISSING].startswith("no_data: ")
    assert stubs[WRONG_TAB] == "parse_failed: zero player rows (not an individual_stats page)"
    assert stubs[REJECTED].startswith("request_error: stats.ncaa.org rejected")  # a 422 is not a parser problem
    assert len(stubs) == 4


def test_fetch_failure_skip_with_every_game_failing_keeps_the_contract_columns() -> None:
    contract = ncaa_mbb_box_scores([GOOD], fetcher=_Fetcher()).columns
    df = ncaa_mbb_box_scores([BANNED, MISSING], fetcher=_Fetcher(), errors="skip")
    assert df.columns == [*contract, "scrape_status"]
    assert df["game_id"].to_list() == [BANNED, MISSING]


def test_fetch_failure_wbb_shim_passes_errors_through() -> None:
    good = GAMES["wbb"][0]

    class _W:
        def fetch_game_individual_stats(self, contest_id: object) -> str:
            if str(contest_id) == BANNED:
                raise AssetFetchError("pool exhausted")
            return (HTML_DIR / f"individual_stats_{contest_id}.html").read_text(encoding="utf-8")

    with pytest.raises(AssetFetchError):
        ncaa_wbb_box_scores([good, BANNED], fetcher=_W())
    df = ncaa_wbb_box_scores([good, BANNED], fetcher=_W(), errors="skip")
    assert df.filter(pl.col("game_id") == BANNED)["scrape_status"].to_list() == ["fetch_failed: pool exhausted"]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [({"errors": "ignore"}, "errors must be"), ({"errors": "skip", "multi_games": True}, "multi_games")],
)
def test_fetch_failure_bad_errors_arguments_raise(kwargs: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ncaa_mbb_box_scores([GOOD], fetcher=_Fetcher(), **kwargs)
