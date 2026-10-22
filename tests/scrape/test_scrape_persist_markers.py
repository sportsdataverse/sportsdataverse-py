"""Scrape stages never record a failure as a capture or as an empty family (task 4.5: B-M5, B-M4, B-L4, B-L3).

Payloads are committed real captures: an ESPN injuries payload (``tests/fixtures/espn``) and the
2015 WBB stats.ncaa.org producer bundle (``tests/fixtures/ncaa/wbb_raw``).
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.scrape.espn import persist
from sportsdataverse.scrape.espn.schedule import _captured_ids
from sportsdataverse.scrape.ncaa import espn_game_xwalk, parse
from sportsdataverse.scrape.ncaa.bundle import read_bundle
from tests.conftest import load_fixture

PAYLOAD = load_fixture("espn", "injuries_nfl")
BUNDLE_GZ = (
    Path(__file__).resolve().parents[1] / "fixtures" / "ncaa" / "wbb_raw" / "wbb" / "raw" / "2015" / "385627.json.gz"
)


# --- B-M5: write_payload is atomic ---------------------------------------------------------------


def test_atomic_write_crash_mid_write_is_never_counted_as_captured(tmp_path, monkeypatch):
    real_write_text = Path.write_text

    def crash_halfway(self, data, *args, **kwargs):
        real_write_text(self, data[: len(data) // 2], *args, **kwargs)
        raise KeyboardInterrupt("killed mid-write")

    monkeypatch.setattr(Path, "write_text", crash_halfway)
    with pytest.raises(KeyboardInterrupt):
        persist.write_payload(tmp_path / "401.json", PAYLOAD)
    monkeypatch.undo()
    assert not (tmp_path / "401.json").exists()
    assert _captured_ids(tmp_path) == set()


def test_atomic_write_success_leaves_only_the_json(tmp_path):
    (tmp_path / "401.json").write_text("{}", encoding="utf-8")  # an earlier capture is replaced whole
    assert persist.write_payload(tmp_path / "401.json", PAYLOAD) is True
    assert sorted(p.name for p in tmp_path.iterdir()) == ["401.json"]
    assert _captured_ids(tmp_path) == {401}


# --- B-L4: ESPN's own error body is recognised -------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (load_fixture("espn", "error_404_body"), True),  # ESPN's real 404 body (summary?event=1)
        ({"code": 500, "message": "Internal Server Error", "extra": 1}, True),
        ({"code": "ABC", "message": "a data field that happens to be named code"}, False),
        ({"code": True, "message": "a bool is not an HTTP status"}, False),
        ({"code": 200, "items": []}, False),  # an int code alone is not an error body: "message" is required
        (PAYLOAD, False),
    ],
)
def test_is_error_payload_code_message(payload, expected):
    assert persist.is_error_payload(payload) is expected


# --- B-M4: a crashed family carries a marker, and the stage exits non-zero ------------------------

_FAMILY_CALLS = {
    "pbp": "parse_ncaa_bb_game_pbp",
    "lineups": "_parse_lineups",
    "player_box": "ncaa_mbb_player_stats",
    "team_box": "ncaa_mbb_team_stats",
    "shots": "_parse_shots",
    "possessions": "ncaa_mbb_possessions",
    "identity": "enrich_parsed",
}


def _boom(*args, **kwargs):
    raise RuntimeError("parser bug")


def test_parse_errors_marker_absent_on_a_clean_parse():
    parsed = parse.parse_bundle(read_bundle(BUNDLE_GZ), league="wbb", root=None)
    assert "parse_errors" not in parsed
    assert parsed["pbp"] and parsed["player_box"]


@pytest.mark.parametrize("family", list(_FAMILY_CALLS))
def test_parse_errors_marker_names_the_crashed_family(family, monkeypatch):
    monkeypatch.setattr(parse, _FAMILY_CALLS[family], _boom)
    parsed = parse.parse_bundle(read_bundle(BUNDLE_GZ), league="wbb", root=None)
    assert parsed["parse_errors"] == [{"family": family, "error": "RuntimeError: parser bug"}]
    if family != "identity":
        assert parsed[family] == []


@pytest.mark.parametrize(("crash", "exit_code"), [(False, None), (True, 1)])
def test_parse_errors_marker_makes_the_stage_exit_non_zero(tmp_path, monkeypatch, crash, exit_code):
    raw = tmp_path / "wbb" / "raw" / "2015"
    raw.mkdir(parents=True)
    shutil.copy(BUNDLE_GZ, raw / BUNDLE_GZ.name)
    if crash:
        monkeypatch.setattr(parse, "_parse_shots", _boom)
    monkeypatch.setattr(sys, "argv", ["ncaa_parse", "--root", str(tmp_path), "--league", "wbb"])
    if exit_code is None:
        parse._main("wbb", str(tmp_path))
    else:
        with pytest.raises(SystemExit) as exc:
            parse._main("wbb", str(tmp_path))
        assert exc.value.code == exit_code
    written = (tmp_path / "wbb" / "json" / "385627.json").read_text(encoding="utf-8")
    assert ('"parse_errors"' in written) is crash  # the game is written either way


def test_parse_errors_marker_a_bundle_that_cannot_be_read_also_exits_non_zero(tmp_path, monkeypatch):
    raw = tmp_path / "wbb" / "raw" / "2015"
    raw.mkdir(parents=True)
    shutil.copy(BUNDLE_GZ, raw / BUNDLE_GZ.name)
    (raw / "385628.json.gz").write_bytes(BUNDLE_GZ.read_bytes()[:100])  # a truncated bundle
    monkeypatch.setattr(sys, "argv", ["ncaa_parse", "--root", str(tmp_path), "--league", "wbb"])
    with pytest.raises(SystemExit) as exc:
        parse._main("wbb", str(tmp_path))
    assert exc.value.code == 1
    assert (tmp_path / "wbb" / "json" / "385627.json").exists()  # the readable one is still parsed
    assert not (tmp_path / "wbb" / "json" / "385628.json").exists()


# --- B-L3: a crosswalk written in this process is read back -------------------------------------


def test_xwalk_written_in_process_is_read_not_the_cached_miss(tmp_path):
    espn_game_xwalk.load_espn_game_index.cache_clear()
    assert espn_game_xwalk.load_espn_game_index(str(tmp_path), "wbb", 2015) == {}
    frame = pl.DataFrame({"contest_id": ["385627"], "espn_game_id": ["400585791"]})
    espn_game_xwalk.write_season_xwalk(tmp_path, "wbb", 2015, frame)
    assert espn_game_xwalk.load_espn_game_index(str(tmp_path), "wbb", 2015) == {"385627": "400585791"}
