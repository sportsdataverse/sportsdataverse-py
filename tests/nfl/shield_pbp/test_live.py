"""Live-game (in-progress payload) behaviour of the Shield parser.

Fixtures are real captures — an INGAME Q2 snapshot with an open drive, the PREGAME
1-play shell, and the 2024 wk1 final (see ``tests/fixtures/nfl_shield/README.md``).
Every test runs offline via ``game_detail=`` and ``enrich=False``: the point here is
the parse/label/live layer, not the EP/WP models.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp import build_pbp, is_final, shield_nfl_pbp
from sportsdataverse.nfl.shield_pbp.live import DEFAULT_CONTEXT, current_situation_row, resolve_context

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield"
INGAME = FIXTURES / "2026_02_DET_BUF_ingame_q2.json.gz"
PREGAME = FIXTURES / "2026_02_DET_BUF_pregame.json.gz"
FINAL = FIXTURES / "2024_01_BAL_KC.json.gz"

pytestmark = pytest.mark.skipif(not INGAME.exists(), reason="live Shield fixtures not present")

CTX = {"roof": "outdoors", "spread_line": -1.5, "total_line": 48.5}
MARKERS = ("GAME_START", "END_QUARTER", "END_GAME", "CURRENT_SITUATION")


def _payload(path: Path) -> dict:
    return json.load(gzip.open(path, "rt", encoding="utf-8"))


@pytest.fixture(scope="module")
def ingame() -> dict:
    return _payload(INGAME)


@pytest.fixture(scope="module")
def live_df(ingame: dict) -> pl.DataFrame:
    return shield_nfl_pbp(game_detail=ingame, enrich=False, context=CTX)


# --------------------------------------------------------------------------- fix 1
def test_open_drive_plays_keep_possession(live_df: pl.DataFrame, ingame: dict):
    """The in-progress drive (``endedPlaySequenceNumber`` null) must not be dropped."""
    open_drives = [d for d in ingame["driveChart"]["drives"] if d.get("endedPlaySequenceNumber") is None]
    assert len(open_drives) == 1, "fixture must contain exactly one open drive"
    start = open_drives[0]["startedPlaySequenceNumber"]
    current = live_df.filter((pl.col("play_seq") >= start) & (pl.col("is_play") == 1))
    assert current.height >= 5
    assert current["posteam"].null_count() == 0
    assert current["defteam"].null_count() == 0
    assert current["yardline_100"].null_count() == 0
    # The whole drive belongs to one team, and it is the drive's own teamId's club.
    assert current["posteam"].unique().to_list() == ["DET"]


def test_no_null_posteam_on_any_live_scrimmage_play(live_df: pl.DataFrame):
    plays = live_df.filter(~pl.col("shield_play_type").fill_null("").is_in(MARKERS))
    assert plays.filter(pl.col("posteam").is_null()).height == 0


def test_open_drive_dropped_without_the_fix(ingame: dict):
    """Mutation guard: with the open drive's synthetic end removed (the pre-fix
    behaviour — drives lacking ``endedPlaySequenceNumber`` were skipped), the
    current drive's plays lose possession again."""
    mutated = json.loads(json.dumps(ingame))
    mutated["driveChart"]["drives"] = [
        d for d in mutated["driveChart"]["drives"] if d.get("endedPlaySequenceNumber") is not None
    ]
    df = build_pbp(mutated, **CTX)
    plays = df.filter(~pl.col("shield_play_type").fill_null("").is_in(MARKERS))
    assert plays.filter(pl.col("posteam").is_null()).height >= 5


# --------------------------------------------------------------------------- fix 2
def test_fixed_drive_advances_on_the_open_drive(live_df: pl.DataFrame, ingame: dict):
    n_drives = len(ingame["driveChart"]["drives"])
    plays = live_df.filter(pl.col("is_play") == 1)
    assert plays["fixed_drive"].max() == n_drives
    drives = plays["fixed_drive"].drop_nulls().to_list()
    assert drives == sorted(drives)


# --------------------------------------------------------------------------- fix 3
def test_game_outcome_columns_null_until_final(live_df: pl.DataFrame):
    for col in ("home_score", "away_score", "result"):
        assert live_df[col].null_count() == live_df.height, f"{col} must be null mid-game"


def test_final_payload_still_carries_the_outcome():
    df = shield_nfl_pbp(game_detail=_payload(FINAL), enrich=False, context=CTX)
    assert df["home_score"].unique().to_list() == [27]
    assert df["away_score"].unique().to_list() == [20]
    assert df["result"].unique().to_list() == [7]
    assert df["live_phase"].unique().to_list() == ["FINAL"]
    # Nothing live about a final: no situation row, nothing provisional.
    assert df.filter(pl.col("shield_play_type") == "CURRENT_SITUATION").height == 0
    assert df["provisional"].sum() == 0


def test_is_final_reads_the_phase():
    assert is_final({"summary": {"phase": "FINAL"}})
    assert is_final({"summary": {"phase": "FINAL_OVERTIME"}})
    assert not is_final({"summary": {"phase": "INGAME"}})
    assert not is_final({"summary": {"phase": "PREGAME"}})
    assert not is_final({})


# --------------------------------------------------------------------------- fix 4
def test_current_situation_row(live_df: pl.DataFrame, ingame: dict):
    situation = live_df.filter(pl.col("shield_play_type") == "CURRENT_SITUATION")
    assert situation.height == 1
    row = situation.to_dicts()[0]
    summary = ingame["summary"]
    assert row["is_play"] == 0
    assert row["down"] == summary["down"]
    assert row["ydstogo"] == summary["distance"]
    assert row["yardline_100"] == 13  # summary yardLine "BUF 13", DET has the ball
    assert row["quarter_seconds_remaining"] == 5  # clock "00:05"
    assert row["qtr"] == 2
    assert row["posteam"] == "DET"
    assert row["defteam"] == "BUF"
    # Scores + timeouts come from summary, not from the play-derived running totals.
    assert row["posteam_score"] == summary["awayTeam"]["score"]["total"]
    assert row["defteam_score"] == summary["homeTeam"]["score"]["total"]
    assert row["posteam_timeouts_remaining"] == summary["awayTeam"]["timeouts"]["remaining"]
    assert row["defteam_timeouts_remaining"] == summary["homeTeam"]["timeouts"]["remaining"]
    # It is the last row, and it invents no play outcome.
    assert live_df["play_id"].to_list()[-1] == row["play_id"]
    assert row["play_type"] is None and row["desc"] is None and row["yards_gained"] is None
    # Enrichment inputs are all present, so a live WP row is scoreable.
    for col in ("posteam", "home_team", "yardline_100", "ydstogo", "down", "half_seconds_remaining", "roof"):
        assert row[col] is not None


def test_current_situation_row_is_skipped_without_a_yard_line(ingame: dict):
    mutated = json.loads(json.dumps(ingame))
    mutated["summary"]["yardLine"] = None
    df = shield_nfl_pbp(game_detail=mutated, enrich=False, context=CTX)
    assert df.filter(pl.col("shield_play_type") == "CURRENT_SITUATION").height == 0


def test_pregame_payload_builds():
    df = shield_nfl_pbp(game_detail=_payload(PREGAME), enrich=False, context=CTX)
    assert df.height >= 1
    assert df["live_phase"].unique().to_list() == ["PREGAME"]
    # PREGAME's summary situation is a placeholder (1st-and-10 at a side-less "35"),
    # so no current-situation row is emitted.
    assert df.filter(pl.col("shield_play_type") == "CURRENT_SITUATION").height == 0


def test_current_situation_row_no_ops_on_empty_frame():
    assert current_situation_row(pl.DataFrame(), {"summary": {"phase": "INGAME"}}).height == 0


# --------------------------------------------------------------------------- fix 5
def test_provisional_tail_marked(live_df: pl.DataFrame, ingame: dict):
    plays = {p["playId"]: p for p in ingame["driveChart"]["plays"]}
    flagged = live_df.filter(pl.col("provisional") == 1)
    assert flagged.height >= 1
    # Everything flagged is a play the feed has not closed...
    for pid in flagged["play_id"].to_list():
        assert plays[pid]["playEndTime"] is None
    # ...and the flags are the tail of the frame.
    provisional = live_df.filter(pl.col("is_play") == 1)["provisional"].to_list()
    assert provisional[-1] == 1
    assert sum(provisional) == len([v for v in provisional[provisional.index(1) :] if v == 1])


# --------------------------------------------------------------------------- fix 6
def test_context_injected(live_df: pl.DataFrame):
    assert live_df["roof"].unique().to_list() == ["outdoors"]
    assert live_df["spread_line"].unique().to_list() == [-1.5]
    assert live_df["total_line"].unique().to_list() == [48.5]


def test_context_partial_falls_back_to_defaults(ingame: dict):
    roof, spread, total = resolve_context(ingame, {"roof": "dome"}, game_id=None)
    assert roof == "dome"
    assert spread == DEFAULT_CONTEXT["spread_line"]
    assert total == DEFAULT_CONTEXT["total_line"]


def test_context_defaults_when_nothing_supplied(ingame: dict):
    assert resolve_context(ingame, None, game_id=None) == (
        DEFAULT_CONTEXT["roof"],
        DEFAULT_CONTEXT["spread_line"],
        DEFAULT_CONTEXT["total_line"],
    )


def test_schedule_context_fills_the_gaps(ingame: dict, monkeypatch):
    """The schedule step only runs for fields the caller left unset."""
    import sportsdataverse.nfl.shield_pbp.live as live_mod

    monkeypatch.setattr(
        live_mod, "_schedule_context", lambda _s, _g: {"roof": "closed", "spread_line": -7.0, "total_line": 41.0}
    )
    assert resolve_context(ingame, {"total_line": 48.5}, game_id="2026_02_DET_BUF") == ("closed", -7.0, 48.5)
    # Fully-supplied context needs no lookup at all.
    monkeypatch.setattr(live_mod, "_schedule_context", lambda _s, _g: pytest.fail("must not be called"))
    assert resolve_context(ingame, CTX, game_id="2026_02_DET_BUF") == ("outdoors", -1.5, 48.5)


def test_schedule_lookup_failure_degrades_with_a_warning():
    """An unusable schedule must not fail a live build (season 1899 has none)."""
    import sportsdataverse.nfl.shield_pbp.live as live_mod

    with pytest.warns(RuntimeWarning, match="schedule context unavailable"):
        assert live_mod._schedule_context(1899, "1899_01_AAA_BBB") == {}


# --------------------------------------------------------------------------- entry point
def test_entry_point_requires_a_payload_or_an_id():
    with pytest.raises(ValueError, match="game_detail"):
        shield_nfl_pbp()


def test_entry_point_unwraps_a_data_envelope(ingame: dict):
    df = shield_nfl_pbp(game_detail={"data": ingame}, enrich=False, context=CTX)
    assert df.height > 1


def test_live_columns_on_every_row(live_df: pl.DataFrame):
    for col in ("live_phase", "is_play", "provisional"):
        assert live_df[col].null_count() == 0
    assert set(live_df["is_play"].unique().to_list()) == {0, 1}
    markers = live_df.filter(pl.col("shield_play_type").is_in(("GAME_START", "END_QUARTER")))
    assert markers.height >= 1
    assert markers["is_play"].sum() == 0
