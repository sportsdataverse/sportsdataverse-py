"""Tests for the official NBA injury report (:mod:`sportsdataverse.nba.nba_official_injury`).

Fixtures are six real league PDFs, one per layout era (fetched 2026-10-26 from
``ak-static.cms.nba.com``): 2019-01-15 (9 columns), 2019-12-15 and 2022-01-15
(7 columns, ruled grid), 2024-01-15 (hourly, unruled), 2026-01-15 05:30 PM and
2026-10-25 05:00 PM (quarter-hourly, unruled).

The ruled-grid PDFs carry an INDEPENDENT oracle: pdfplumber's own
``extract_table()`` reads their cell geometry, which the word-position parser
never uses. On those, every (matchup, team, player, status, reason) row must
agree exactly. The unruled PDFs are gated on invariants a mis-assignment breaks.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba import nba_official_injury as ir
from tests.conftest import skip_if_no_live

pdfplumber = pytest.importorskip("pdfplumber")

FIX = Path(__file__).parent / "fixtures" / "nba_injury_report"
RULED = ["2019-01-15_05PM", "2019-12-15_05PM", "2022-01-15_05PM"]
UNRULED = ["2024-01-15_05PM", "2026-01-15_05_30PM", "2026-10-25_05_00PM"]
STATUSES = {"Out", "Doubtful", "Questionable", "Probable", "Available"}


def _pdf(slot: str) -> Path:
    return FIX / f"Injury-Report_{slot}.pdf"


def _ws(s: Any) -> str:
    return " ".join((s or "").split())


def _table_oracle(path: Path) -> list[tuple]:
    """(matchup, team, player, status, reason) from the ruled grid's own cells."""
    with pdfplumber.open(path) as pdf:
        table = [r for p in pdf.pages for r in (p.extract_table() or [])]
    hdr = table[0]
    i = {h: hdr.index(h) for h in ("Matchup", "Team", "Player Name", "Current Status", "Reason")}
    last: dict = {}
    out = []
    for r in table:
        if r == hdr:
            continue
        if r[i["Matchup"]]:
            last["m"] = _ws(r[i["Matchup"]])
            if not r[i["Team"]]:
                last.pop("t", None)
        if r[i["Team"]]:
            last["t"] = _ws(r[i["Team"]])
        if r[i["Player Name"]]:
            out.append(
                (last["m"], last["t"], _ws(r[i["Player Name"]]), _ws(r[i["Current Status"]]), _ws(r[i["Reason"]]))
            )
    return sorted(out)


@pytest.mark.parametrize("slot", RULED)
def test_ruled_grid_rows_match_the_table_oracle_exactly(slot: str) -> None:
    """Observed 2026-10-26: 91 / 122 / 118 players, 0 mismatches each. Before the
    nearest-row label fix the 2019 PDFs misfiled 17 and 76 players to the previous team."""
    df = ir.parse_nba_injury_report(_pdf(slot)).filter(pl.col("player_name").is_not_null())
    got = sorted(
        (m, t, p, s, _ws(r))
        for m, t, p, s, r in df.select("matchup", "team", "player_name", "current_status", "reason").iter_rows()
    )
    assert got == _table_oracle(_pdf(slot))


@pytest.mark.parametrize("slot", RULED + UNRULED)
def test_every_row_is_well_formed(slot: str) -> None:
    df = ir.parse_nba_injury_report(_pdf(slot))
    assert list(df.columns) == list(ir._SCHEMA) and df.schema == pl.Schema(ir._SCHEMA)
    players = df.filter(pl.col("reason") != "NOT YET SUBMITTED")
    assert players.height > 50
    keys = ("game_date", "matchup", "team", "player_name", "current_status", "reason")
    assert players.select(pl.col(keys).null_count()).row(0) == (0,) * len(keys)
    assert set(players["current_status"]) <= STATUSES
    # "Last, First" keeps its space: the default x_tolerance glues "Clarke,Brandon"
    assert players["player_name"].str.contains(r"^\S.*, \S").all()
    assert df["matchup"].str.contains(r"^[A-Z]{2,3}@[A-Z]{2,3}$").all()
    nys = df.filter(pl.col("reason") == "NOT YET SUBMITTED")
    assert nys["player_name"].is_null().all() and nys["team"].is_not_null().all()


@pytest.mark.parametrize("slot", RULED + UNRULED)
def test_each_team_is_one_side_of_its_matchups(slot: str) -> None:
    """A row filed under the wrong team shows up as a team whose matchups share no abbreviation."""
    df = ir.parse_nba_injury_report(_pdf(slot))
    sides = df.group_by("team").agg(pl.col("away_team_abbr"), pl.col("home_team_abbr"))
    for team, away, home in sides.iter_rows():
        common = set.intersection(*({a, h} for a, h in zip(away, home)))
        assert common, (team, set(zip(away, home)))


@pytest.mark.parametrize(
    ("slot", "expected"),
    [
        ("2019-01-15_05PM", dt.datetime(2019, 1, 15, 17, 30)),  # HH slot = the HH:30 snapshot
        ("2024-01-15_05PM", dt.datetime(2024, 1, 15, 17, 30)),
        ("2026-01-15_05_30PM", dt.datetime(2026, 1, 15, 17, 30)),
        ("2026-10-25_05_00PM", dt.datetime(2026, 10, 25, 17, 0)),
    ],
)
def test_report_datetime_is_the_title_time(slot: str, expected: dt.datetime) -> None:
    assert set(ir.parse_nba_injury_report(_pdf(slot))["report_datetime"]) == {expected}


def test_nine_column_era_keeps_category_and_previous_status() -> None:
    df = ir.parse_nba_injury_report(_pdf("2019-01-15_05PM"))
    assert df["category"].is_not_null().sum() > 50 and df["previous_status"].is_not_null().sum() > 50
    assert ir.parse_nba_injury_report(_pdf("2026-01-15_05_30PM"))["category"].is_null().all()


def test_bytes_and_path_inputs_agree() -> None:
    path = _pdf("2026-01-15_05_30PM")
    assert ir.parse_nba_injury_report(path.read_bytes()).equals(ir.parse_nba_injury_report(path))


def test_slots_follow_the_archive_eras() -> None:
    assert ir.nba_injury_report_slots(dt.date(2018, 12, 16)) == []
    old = ir.nba_injury_report_slots(dt.date(2021, 10, 14))
    assert [t.hour for t in old] == [13, 17, 20]
    assert len(ir.nba_injury_report_slots(dt.date(2021, 10, 15))) == 24
    cutover = ir.nba_injury_report_slots(dt.date(2025, 12, 22))
    assert len(cutover) == 9 + 15 * 4  # hourly 00-08, then 09:00-23:45 every 15 min
    assert len(ir.nba_injury_report_slots(dt.date(2026, 1, 15))) == 96


@pytest.mark.parametrize(
    ("when", "suffix"),
    [
        (dt.datetime(2019, 1, 15, 17), "2019-01-15_05PM.pdf"),
        (dt.datetime(2025, 12, 21, 0), "2025-12-21_12AM.pdf"),
        (dt.datetime(2025, 12, 22, 8), "2025-12-22_08AM.pdf"),
        (dt.datetime(2025, 12, 22, 9), "2025-12-22_09_00AM.pdf"),
        (dt.datetime(2026, 1, 15, 17, 30), "2026-01-15_05_30PM.pdf"),
        (dt.datetime(2026, 1, 15, 12, 0), "2026-01-15_12_00PM.pdf"),
    ],
)
def test_url_format_per_era(when: dt.datetime, suffix: str) -> None:
    assert ir.nba_injury_report_url(when) == f"https://ak-static.cms.nba.com/referee/injury/Injury-Report_{suffix}"


# the real body of a missing slot (observed 2026-10-26: 403, Server AmazonS3, 243 bytes)
_S3_403 = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>'
)


class _Resp:
    def __init__(self, status: int, content: bytes = b"") -> None:
        self.status_code, self.content = status, content


def test_latest_slot_probes_backwards_past_403s(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _pdf("2024-01-15_05PM").read_bytes()
    seen: list[str] = []

    def fake(url: str, **kwargs: Any) -> _Resp:
        seen.append(url)
        assert 403 not in kwargs["retry_statuses"]  # a missing slot must not burn retries
        return _Resp(200, body) if url.endswith("_05PM.pdf") else _Resp(403, _S3_403)

    monkeypatch.setattr(ir, "download", fake)
    df = ir.nba_injury_report(dt.date(2024, 1, 15))
    assert df.height > 50
    assert seen[0].endswith("_11PM.pdf") and seen[-1].endswith("_05PM.pdf")


def test_missing_and_failing_slots_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ir, "download", lambda url, **kw: _Resp(403, _S3_403))
    with pytest.raises(NoDataError):
        ir.nba_injury_report(report_time=dt.datetime(2024, 1, 15, 17))
    with pytest.raises(NoDataError):
        ir.nba_injury_report(dt.date(2024, 1, 15))
    # a 403 that is NOT S3's AccessDenied (CDN / WAF block) is a failed fetch, never "no report"
    monkeypatch.setattr(
        ir, "download", lambda url, **kw: _Resp(403, b"<HTML><TITLE>Access Denied</TITLE> Reference #18.x</HTML>")
    )
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        ir.nba_injury_report(dt.date(2024, 1, 15))
    monkeypatch.setattr(ir, "download", lambda url, **kw: _Resp(500))
    with pytest.raises(AssetFetchError):
        ir.nba_injury_report(report_time=dt.datetime(2024, 1, 15, 17))
    # a 200 that is not a PDF (CDN error page) is an asset failure, not a pdfplumber crash
    monkeypatch.setattr(ir, "download", lambda url, **kw: _Resp(200, b"<html>Access Denied</html>"))
    with pytest.raises(AssetFetchError, match="without a PDF body"):
        ir.nba_injury_report(report_time=dt.datetime(2024, 1, 15, 17))


@pytest.mark.parametrize("payload", [b"", b"<html>not a pdf</html>", b"%PDF-1.4 truncated"])
def test_parser_never_raises_on_an_empty_or_malformed_payload(payload: bytes) -> None:
    df = ir.parse_nba_injury_report(payload)
    assert df.height == 0 and df.schema == pl.Schema(ir._SCHEMA)


def test_fetch_failures_are_never_an_empty_report(monkeypatch: pytest.MonkeyPatch) -> None:
    when = dt.datetime(2024, 1, 15, 17)

    def not_found(url: str, **kwargs: Any) -> _Resp:
        raise NoDataError(url)  # what dl_utils.download does on a 404

    monkeypatch.setattr(ir, "download", not_found)
    with pytest.raises(AssetFetchError, match="HTTP 404"):
        ir.nba_injury_report(report_time=when)
    monkeypatch.setattr(ir, "download", lambda url, **kw: _Resp(200, b"%PDF-1.4 truncated"))
    with pytest.raises(AssetFetchError, match="unreadable PDF"):
        ir.nba_injury_report(report_time=when)


def test_caller_kwargs_override_the_download_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def fake(url: str, **kwargs: Any) -> _Resp:
        seen.update(kwargs)
        return _Resp(200, _pdf("2024-01-15_05PM").read_bytes())

    monkeypatch.setattr(ir, "download", fake)
    ir.nba_injury_report(report_time=dt.datetime(2024, 1, 15, 17), retry_statuses=(500,), timeout=5)
    assert seen["retry_statuses"] == (500,) and seen["timeout"] == 5


@skip_if_no_live
def test_live_archive_slot() -> None:
    df = ir.nba_injury_report(report_time=dt.datetime(2026, 1, 15, 17, 30))
    assert df.height > 100 and {"player_name", "current_status", "reason"} <= set(df.columns)
