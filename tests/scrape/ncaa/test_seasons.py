"""Offline tests for the stats.ncaa.org season/division resolver (sportsdataverse.scrape.ncaa.seasons)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.scrape.ncaa import seasons as S

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "ncaa_season_divisions"


def _page(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    """Point the live-resolve cache at a temp dir and drop the bundled-table memo between tests."""
    monkeypatch.setenv("SDV_PY_CACHE_DIR", str(tmp_path))
    real = S._bundled  # a test may monkeypatch it; clear the memo on the real one
    real.cache_clear()
    yield
    real.cache_clear()


def _bundle(monkeypatch, rows: "list[dict]") -> None:
    monkeypatch.setattr(S, "_bundled", lambda: S._with_derived(pl.DataFrame(rows, schema=S._CSV_SCHEMA)))


def test_parse_scoreboard_page_reads_every_division_of_the_selected_season():
    rows = S._parse_scoreboard_page(_page("mbb_2026_27_d1.html"), "MBB")
    assert [(r["division_label"], r["season_division_id"]) for r in rows] == [
        ("D-I", 18983),
        ("D-II", 18984),
        ("D-III", 18985),
    ]
    assert {r["academic_year"] for r in rows} == {2027}
    assert {r["season_label"] for r in rows} == {"2026-27"}
    assert {r["game_sport_year_ctl_id"] for r in rows} == {17240}


def test_parse_scoreboard_page_is_sport_neutral():
    rows = S._parse_scoreboard_page(_page("mba_2026_27_d1.html"), "MBA")
    assert rows and {r["sport_code"] for r in rows} == {"MBA"}
    assert {r["division_label"] for r in rows} == {"D-I", "D-II", "D-III"}
    # baseball's ids are not basketball's
    assert not {r["season_division_id"] for r in rows} & {18983, 18984, 18985}


def test_season_list_comes_from_the_page():
    years = S._select_options(_page("mbb_2026_27_d1.html"), "game_sport_year_ctl_id_select")
    labels = [label for _, label, _ in years]
    assert labels[0] == "2026-27" and "1997-98" in labels
    assert [v for v, _, sel in years if sel] == ["17240"]


def test_sport_list_covers_twenty_sports_and_matches_the_registry():
    codes = {v for v, _, _ in S._select_options(_page("mbb_2026_27_d1.html"), "sport_code_select")}
    assert codes == set(S.NCAA_SPORTS)


@pytest.mark.parametrize(
    ("sport", "code"),
    [("mbb", "MBB"), ("MBA", "MBA"), ("football", "MFB"), ("cfb", "MFB"), ("softball", "WSB"),
     ("Men's Ice Hockey", "MIH"), ("mch", "MIH"), ("womens lacrosse", "WLA")],
)  # fmt: skip
def test_sport_code_aliases(sport, code):
    assert S.ncaa_sport_code(sport) == code


def test_unknown_sport_raises():
    with pytest.raises(ValueError, match="unknown NCAA sport"):
        S.ncaa_sport_code("curling")


def test_academic_year_fall_sports_key_by_start_year():
    assert S.ncaa_academic_year("football", 2026) == 2027
    assert S.ncaa_academic_year("WVB", 2026) == 2027
    assert S.ncaa_academic_year("mbb", 2027) == 2027
    assert S.ncaa_academic_year("baseball", 2026) == 2026


def test_academic_year_default_turns_over_on_july_1():
    assert S.ncaa_academic_year("mbb", on=date(2026, 6, 30)) == 2026
    assert S.ncaa_academic_year("mbb", on=date(2026, 7, 1)) == 2027


@pytest.mark.parametrize(
    ("division", "n"), [(1, 1), ("D-I", 1), ("d-ii", 2), ("III", 3), ("3", 3), ("FBS", 1), ("fcs", 1)]
)
def test_division_spellings(division, n):
    assert S._division(division) == n


def test_bundled_hit_never_fetches(monkeypatch):
    _bundle(monkeypatch, S._parse_scoreboard_page(_page("mbb_2026_27_d1.html"), "MBB"))

    def _boom(path: str) -> str:
        raise AssertionError("fetched despite a bundled hit")

    assert S.ncaa_season_division_id("mbb", 2027, fetch_html=_boom) == 18983
    assert S.ncaa_season_division_id("mbb", 2027, "D-II", fetch_html=_boom) == 18984


def test_miss_resolves_live_once_then_serves_from_cache(monkeypatch):
    _bundle(monkeypatch, [])
    calls: "list[str]" = []

    def _fetch(path: str) -> str:
        calls.append(path)
        return _page("mbb_2026_27_d1.html")

    assert S.ncaa_season_division_id("mbb", 2027, fetch_html=_fetch) == 18983
    assert len(calls) == 1 and "sport_code=MBB" in calls[0] and "academic_year=2027" in calls[0]
    # every division of that season was cached from the one page
    assert S.ncaa_season_division_id("mbb", 2027, 3, fetch_html=_fetch) == 18985
    assert len(calls) == 1
    assert (Path(S._cache_path())).is_file()


def test_allow_fetch_false_raises_instead_of_fetching(monkeypatch):
    _bundle(monkeypatch, [])
    with pytest.raises(SeasonNotFoundError, match="Men's Basketball academic year 2026-27 top division"):
        S.ncaa_season_division_id("mbb", 2027, allow_fetch=False)


def test_live_answer_for_another_season_is_not_trusted(monkeypatch):
    """The site falls back to its current season for an unknown year: that must not be cached as the asked one."""
    _bundle(monkeypatch, [])
    with pytest.raises(SeasonNotFoundError):
        S.ncaa_season_division_id("mbb", academic_year=1990, fetch_html=lambda p: _page("mbb_2026_27_d1.html"))
    assert not Path(S._cache_path()).exists()


def test_season_divisions_frame_derives_sdv_season(monkeypatch):
    _bundle(
        monkeypatch,
        S._parse_scoreboard_page(_page("mbb_2026_27_d1.html"), "MBB")
        + [dict(r, sport_code="MFB") for r in S._parse_scoreboard_page(_page("mbb_2026_27_d1.html"), "MBB")],
    )
    df = S.ncaa_season_divisions()
    assert df.schema == pl.Schema(S.SEASON_DIVISIONS_SCHEMA)
    assert df.filter(pl.col("sport_code") == "MBB")["season"].unique().to_list() == [2027]
    assert df.filter(pl.col("sport_code") == "MFB")["season"].unique().to_list() == [2026]
    assert S.ncaa_season_divisions("football").height == 3


def _football(monkeypatch) -> None:
    _bundle(monkeypatch, S._parse_scoreboard_page(_page("mfb_2026_27_fbs.html"), "MFB"))


def test_football_top_division_is_fbs_and_labels_resolve(monkeypatch):
    _football(monkeypatch)
    assert S.ncaa_season_division_id("football", 2026, allow_fetch=False) == 18904
    assert S.ncaa_season_division_id("football", 2026, "FCS", allow_fetch=False) == 18905
    assert S.ncaa_season_division_id("football", 2026, "D-II", allow_fetch=False) == 18906
    assert S.ncaa_season_division_id("football", 2026, 3, allow_fetch=False) == 18907


def test_football_division_1_is_ambiguous(monkeypatch):
    _football(monkeypatch)
    with pytest.raises(ValueError, match=r"names 2 scoreboards: 'FBS' \(18904\), 'FCS' \(18905\)"):
        S.ncaa_season_division_id("football", 2026, 1, allow_fetch=False)


def test_division_derived_from_label_in_frame(monkeypatch):
    _football(monkeypatch)
    df = S.ncaa_season_divisions("mfb").select("division_label", "division").rows()
    assert df == [("FBS", 1), ("FCS", 1), ("D-II", 2), ("D-III", 3)]


def test_legacy_checkpoint_rows_with_extra_keys_load(monkeypatch):
    """The sweep's checkpoints carry a ``division`` key the CSV schema no longer has; it is ignored."""
    rows = [dict(r, division=99) for r in S._parse_scoreboard_page(_page("mbb_2026_27_d1.html"), "MBB")]
    _bundle(monkeypatch, rows)
    assert S.ncaa_season_divisions("mbb")["division"].to_list() == [1, 2, 3]


class _FakeResource:
    def __init__(self, body: bytes):
        self.body = body

    def __truediv__(self, other: str) -> "_FakeResource":
        return self

    def is_file(self) -> bool:
        return True

    def read_bytes(self) -> bytes:
        return self.body


def _bundled_from(monkeypatch, body: bytes) -> pl.DataFrame:
    monkeypatch.setattr(S, "files", lambda pkg: _FakeResource(body))
    S._bundled.cache_clear()
    return S._bundled()


def test_bundled_table_ignores_columns_it_does_not_know(monkeypatch):
    body = (
        b"sport_code,academic_year,season_label,division,division_label,season_division_id,game_sport_year_ctl_id,extra\n"
        b"MBB,2027,2026-27,1,D-I,18983,17240,x\n"
    )
    df = _bundled_from(monkeypatch, body)
    assert df.select("sport_code", "division", "season_division_id").rows() == [("MBB", 1, 18983)]


def test_an_unreadable_bundled_table_warns_and_falls_back_to_empty(monkeypatch):
    """Read at import: it must never take ``import sportsdataverse`` down with it."""
    with pytest.warns(UserWarning, match="cannot read the bundled stats.ncaa.org season table"):
        df = _bundled_from(monkeypatch, b"sport_code,academic_year\nMBB,not-a-year\n")
    assert df.height == 0 and df.schema == pl.Schema(S.SEASON_DIVISIONS_SCHEMA)


def _season_fetch(other: str):
    """The real 2026-27 MBB page for the season list and its own season; *other* for every other request."""

    def fetch(path: str) -> str:
        if "academic_year=&" in path or "game_sport_year_ctl_id=17240&" in path:
            return _page("mbb_2026_27_d1.html")
        ctl = path.split("game_sport_year_ctl_id=")[1].split("&")[0] if "game_sport_year_ctl_id=" in path else "1"
        return other.format(ctl=ctl)

    return fetch


def test_build_tool_refuses_to_checkpoint_a_page_that_selects_no_season():
    from tools.build_ncaa_season_divisions import sport_rows

    with pytest.raises(RuntimeError, match="selects no season"):
        sport_rows(_season_fetch(""), "MBB")  # a blank 200 for 2025-26 is not "no divisions"


def test_build_tool_keeps_a_season_the_site_lists_without_divisions():
    from tools.build_ncaa_season_divisions import sport_rows

    no_divisions = (
        '<select name="game_sport_year_ctl_id_select"><option selected="selected" value="{ctl}">x</option></select>'
    )
    rows = sport_rows(_season_fetch(no_divisions), "MBB")
    assert {r["academic_year"] for r in rows} == {2027}  # only 2026-27 has ids; the rest are skipped, not fatal


def test_live_lookup_default_transport_is_a_closed_browser(monkeypatch):
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

    _bundle(monkeypatch, [])
    closed: "list[bool]" = []

    class _Fake:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            closed.append(True)

        def fetch_html(self, path: str, force: bool = False) -> str:
            assert force  # the lookup runs only for an unknown season: a cached page would predate it
            return _page("mbb_2026_27_d1.html")

    monkeypatch.setattr(NcaaFetcher, "with_browser", classmethod(lambda cls, *a, **k: _Fake()))
    assert S.ncaa_season_division_id("mbb", 2027) == 18983
    assert closed == [True]


def test_a_damaged_cache_file_reads_as_empty(monkeypatch):
    _bundle(monkeypatch, [])
    path = Path(S._cache_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json")
    assert S._cached_rows() == []
    path.write_text('{"a": 1}')  # valid JSON, wrong shape
    assert S._cached_rows() == []
