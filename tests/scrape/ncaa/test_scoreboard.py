"""Offline tests for sport-neutral scoreboard discovery (sportsdataverse.scrape.ncaa.scoreboard)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.scrape.ncaa import scoreboard as SB
from sportsdataverse.scrape.ncaa import seasons as S

FIX = Path(__file__).resolve().parents[2] / "fixtures"


def _page(name: str) -> str:
    return (FIX / "ncaa_scoreboard" / name).read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("name", "d", "n", "first"),
    [
        ("mbb_d1_2026-11-02.html", "2026-11-02", 184, "6624354"),
        ("mfb_d3_2026-10-31.html", "10/31/2026", 117, None),
        ("mba_d1_2026-05-16.html", date(2026, 5, 16), 138, None),
    ],
)
def test_parse_every_sport_the_same_way(name, d, n, first):
    df = SB.parse_ncaa_scoreboard_contests(_page(name), d, sport_code="X", season_division_id=1)
    assert df.schema == pl.Schema(SB.SCOREBOARD_CONTESTS_SCHEMA)
    assert df.height == n == df["contest_id"].n_unique()
    assert df["contest_id"].str.contains(r"^\d+$").all()
    if first:
        assert df["contest_id"][0] == first


def test_page_showing_another_date_yields_nothing():
    """stats.ncaa.org answers an out-of-season date with another day's board: never file it under the asked date."""
    df = SB.parse_ncaa_scoreboard_contests(_page("mbb_d1_2026-11-02.html"), "2026-11-03")
    assert df.height == 0
    assert df.schema == pl.Schema(SB.SCOREBOARD_CONTESTS_SCHEMA)


def test_empty_page_is_zero_rows():
    assert SB.parse_ncaa_scoreboard_contests("", "2026-11-02").height == 0


def test_bad_date_raises():
    with pytest.raises(ValueError, match="unrecognised date"):
        SB.parse_ncaa_scoreboard_contests("", "Nov 2")


@pytest.fixture
def _mbb_table(tmp_path, monkeypatch):
    monkeypatch.setenv("SDV_PY_CACHE_DIR", str(tmp_path))
    rows = S._parse_scoreboard_page((FIX / "ncaa_season_divisions" / "mbb_2026_27_d1.html").read_text(), "MBB")
    monkeypatch.setattr(S, "_bundled", lambda: S._with_derived(pl.DataFrame(rows, schema=S._CSV_SCHEMA)))


def test_contests_resolves_the_season_and_fetches_one_page_per_date(_mbb_table):
    calls: "list[str]" = []

    def _fetch(path: str) -> str:
        calls.append(path)
        return _page("mbb_d1_2026-11-02.html")

    df = SB.ncaa_scoreboard_contests("mbb", "2026-11-02", fetch_html=_fetch)
    assert calls == [
        "season_divisions/18983/scoreboards?utf8=%E2%9C%93&season_division_id=&game_date=11%2F02%2F2026"
        "&conference_id=0&tournament_id=&commit=Submit"
    ]
    assert df.height == 184
    assert df["sport_code"].unique().to_list() == ["MBB"]
    assert df["season_division_id"].unique().to_list() == [18983]


def test_lookback_adds_earlier_dates_and_dedups(_mbb_table):
    seen: "list[str]" = []

    def _fetch(path: str) -> str:
        seen.append(path.split("game_date=")[1][:14])
        return _page("mbb_d1_2026-11-02.html")

    df = SB.ncaa_scoreboard_contests("mbb", ["2026-11-02"], lookback_days=2, fetch_html=_fetch)
    assert seen == ["10%2F31%2F2026", "11%2F01%2F2026", "11%2F02%2F2026"]
    # the fake answers every date with the 11/02 board: only 11/02 keeps rows (the shown-date guard)
    assert df["game_date"].unique().to_list() == [date(2026, 11, 2)]
    assert df.height == 184


def test_division_label_is_passed_through(_mbb_table):
    paths: "list[str]" = []
    SB.ncaa_scoreboard_contests("mbb", "2026-11-02", division="D-II", fetch_html=lambda p: paths.append(p) or "")
    assert paths[0].startswith("season_divisions/18984/")


def test_discover_dates_merges_into_schedule_master_keeping_captured_rows(_mbb_table, tmp_path):
    from sportsdataverse.scrape.ncaa.discover import discover_dates

    master = tmp_path / "mbb" / "schedule_master.parquet"
    master.parent.mkdir(parents=True)
    pl.DataFrame(
        {"contest_id": ["6624354", "6646892"], "season": ["2027", "2027"], "captured": [True, False]}
    ).write_parquet(master)

    def _fetch(path: str) -> str:
        return _page("mbb_d1_2026-11-02.html") if "11%2F02%2F2026" in path else ""

    found = discover_dates(["2026-11-02"], league="mbb", root=tmp_path, lookback_days=1, fetch_html=_fetch)
    assert found.height == 184 and found["season"].unique().to_list() == ["2027"]
    merged = pl.read_parquet(master)
    assert merged.height == 185  # 184 from the board + the not-yet-played 6646892 kept
    assert merged.filter(pl.col("contest_id") == "6624354")["captured"].to_list() == [True]  # never reset
    assert merged.filter(pl.col("captured") == False).height == 184  # noqa: E712


class _FakeFetcher:
    """Stands in for NcaaFetcher (with_browser and vendor transports alike): serves one page, records closing."""

    def __init__(self, page: str):
        self.page = page
        self.paths: "list[str]" = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True

    def fetch_html(self, path: str, force: bool = False) -> str:
        self.paths.append(path)
        return self.page


def test_default_transport_is_one_browser_closed_after_the_call(_mbb_table, monkeypatch):
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

    fake = _FakeFetcher(_page("mbb_d1_2026-11-02.html"))
    monkeypatch.setattr(NcaaFetcher, "with_browser", classmethod(lambda cls, *a, **k: fake))
    df = SB.ncaa_scoreboard_contests("mbb", date(2026, 11, 2))
    assert df.height == 184 and len(fake.paths) == 1 and fake.closed


def test_no_dates_is_an_empty_frame_with_the_schema(_mbb_table):
    df = SB.ncaa_scoreboard_contests("mbb", [], fetch_html=lambda p: "")
    assert df.height == 0 and df.schema == pl.Schema(SB.SCOREBOARD_CONTESTS_SCHEMA)


@pytest.mark.parametrize("vendor", ["decodo_patchright", None])
def test_discover_dates_default_transport_is_the_vendor_or_the_browser(_mbb_table, tmp_path, monkeypatch, vendor):
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
    from sportsdataverse.scrape.ncaa import capture
    from sportsdataverse.scrape.ncaa.discover import discover_dates

    fake = _FakeFetcher(_page("mbb_d1_2026-11-02.html"))
    if vendor:
        monkeypatch.setenv("NCAA_VENDOR", vendor)
        monkeypatch.setattr(capture, "_vendor_fetcher", lambda name, root, **kw: fake)
    else:
        monkeypatch.delenv("NCAA_VENDOR", raising=False)
        monkeypatch.setattr(NcaaFetcher, "with_browser", classmethod(lambda cls, *a, **k: fake))
    found = discover_dates(["2026-11-02"], league="mbb", root=tmp_path, lookback_days=0)
    assert found.height == 184 and fake.closed
