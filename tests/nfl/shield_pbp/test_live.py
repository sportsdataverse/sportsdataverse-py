"""Live-game (in-progress payload) behaviour of the Shield parser.

Fixtures are real captures — an INGAME Q2 snapshot with an open drive, the PREGAME
1-play shell, and the 2024 wk1 final (see ``tests/fixtures/nfl_shield/README.md``).
Every test runs offline via ``game_detail=`` and ``enrich=False``: the point here is
the parse/label/live layer, not the EP/WP models.
"""

from __future__ import annotations

import gzip
import json
import warnings
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl.shield_pbp import build_pbp, is_final, shield_nfl_pbp
from sportsdataverse.nfl.shield_pbp.live import DEFAULT_CONTEXT, current_situation_row, resolve_context

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nfl_shield"
INGAME = FIXTURES / "2026_02_DET_BUF_ingame_q2.json.gz"
INGAME_Q4 = FIXTURES / "2026_02_DET_BUF_ingame_q4.json.gz"
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
    # ...and the flags are a contiguous run at the tail of the play rows.
    provisional = live_df.filter(pl.col("is_play") == 1)["provisional"].to_list()
    assert provisional[-1] == 1
    first = provisional.index(1)
    assert all(v == 1 for v in provisional[first:]), "provisional must be an unbroken tail"
    assert not any(v == 1 for v in provisional[:first])


# --------------------------------------------------------------------------- fix 6
def test_context_injected(live_df: pl.DataFrame):
    assert live_df["roof"].unique().to_list() == ["outdoors"]
    assert live_df["spread_line"].unique().to_list() == [-1.5]
    assert live_df["total_line"].unique().to_list() == [48.5]


def test_context_partial_falls_back_to_defaults(ingame: dict):
    with pytest.warns(RuntimeWarning, match="spread_line, total_line"):
        roof, spread, total = resolve_context(ingame, {"roof": "dome"}, game_id=None)
    assert roof == "dome"
    assert spread == DEFAULT_CONTEXT["spread_line"]
    assert total == DEFAULT_CONTEXT["total_line"]


def test_context_defaults_when_nothing_supplied(ingame: dict):
    """A defaulted spread/total is indistinguishable from a real one in the frame, so
    falling back has to warn — ``vegas_wp`` off 2.5 / 55.5 is not market-informed."""
    with pytest.warns(RuntimeWarning, match="not market-informed"):
        assert resolve_context(ingame, None, game_id=None) == (
            DEFAULT_CONTEXT["roof"],
            DEFAULT_CONTEXT["spread_line"],
            DEFAULT_CONTEXT["total_line"],
        )


def test_fully_supplied_context_is_silent(ingame: dict):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert resolve_context(ingame, CTX, game_id=None) == ("outdoors", -1.5, 48.5)


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


def test_schedule_lookup_failure_degrades_with_a_warning(monkeypatch):
    """An unusable schedule must not fail a live build — and the test must not
    reach the network to prove it (the previous form called ``load_nfl_schedule``
    for real and passed off whichever error the environment happened to raise)."""
    import sportsdataverse.nfl as nfl_mod
    import sportsdataverse.nfl.shield_pbp.live as live_mod

    def boom(_seasons):
        raise OSError("release asset unreachable")

    monkeypatch.setattr(nfl_mod, "load_nfl_schedule", boom)
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


# --------------------------------------------------------------------------- empty payload
def test_no_play_payload_still_carries_the_live_columns():
    """Every not-yet-played game is a payload with no drive chart, so a poller started
    before kickoff hits this first. ``build_pbp`` returns a schema-less frame there;
    the entry point must still declare the columns it documents, or a consumer's
    ``pl.col("is_play")`` filter is a ColumnNotFoundError instead of an empty result."""
    df = shield_nfl_pbp(game_detail={"driveChart": {"plays": [], "drives": []}}, enrich=False, context=CTX)
    assert df.height == 0
    for col in ("live_phase", "is_play", "provisional"):
        assert col in df.columns, f"{col} missing from an empty live frame"
    assert df.filter(pl.col("is_play") == 1).height == 0
    # Same shape for the ``{"data": null}`` envelope Shield returns for an unknown uuid.
    assert shield_nfl_pbp(game_detail={"data": None}, enrich=False, context=CTX).columns == df.columns


# --------------------------------------------------------------------------- provisional nulls
def test_provisional_is_never_null(ingame: dict):
    """``is_in`` propagates nulls, so a play with no ``playId`` would leave
    ``provisional`` null — and a downstream ``provisional == 0`` mask silently drops
    the row. An ``open_ids`` set holding only ``None`` is truthy too, which would take
    the ``is_in`` branch with a needle that can never match."""
    mutated = json.loads(json.dumps(ingame))
    mutated["driveChart"]["plays"][0]["playId"] = None
    mutated["driveChart"]["plays"][-1]["playId"] = None
    df = shield_nfl_pbp(game_detail=mutated, enrich=False, context=CTX)
    assert df["provisional"].null_count() == 0
    assert set(df["provisional"].unique().to_list()) <= {0, 1}


# --------------------------------------------------------------------------- prefix invariant
# Columns that are EXPECTED to differ between two snapshots of the same game: the
# game-outcome columns (null until FINAL by design), the two snapshot-describing
# columns, and every aggregate that summarises a drive or series which had not
# finished on the earlier snapshot. ``fixed_drive`` and ``series`` themselves are NOT
# excluded — they are asserted stable.
_SNAPSHOT_SCOPED = {
    "home_score",
    "away_score",
    "result",
    "live_phase",
    "provisional",
    "fixed_drive_result",
    "series_result",
    "series_success",
} | {
    "drive_play_count",
    "drive_first_downs",
    "drive_inside20",
    "drive_ended_with_score",
    "drive_quarter_start",
    "drive_quarter_end",
    "drive_yards_penalized",
    "drive_start_transition",
    "drive_end_transition",
    "drive_game_clock_start",
    "drive_game_clock_end",
    "drive_start_yard_line",
    "drive_end_yard_line",
    "drive_play_id_started",
    "drive_play_id_ended",
    "drive_time_of_possession",
}


def _raw_plays(payload: dict) -> dict:
    plays = (payload.get("driveChart") or {}).get("plays") or []
    return {p["playId"]: p for p in plays if not p.get("playDeleted") and p.get("playId") is not None}


def test_prefix_invariant_across_two_snapshots():
    """The parser must be a pure function of the payload prefix: a later poll may not
    move a row whose raw payload object has not changed.

    This is the property the live path actually rests on — Shield revises a play's
    text / stats / yardage after the snap (up to ~11 plays back), so a consumer has to
    re-derive the whole frame every poll, and the only thing that makes that safe is
    that the parser invents no differences of its own. The 223-snapshot sweep that
    measured it (0 mismatches over 19,820 rows) lives in ``background-research`` and
    CI never runs it; this pins the same invariant on two committed snapshots.
    """
    early, late = _payload(INGAME), _payload(INGAME_Q4)
    early_raw, late_raw = _raw_plays(early), _raw_plays(late)
    unchanged = [pid for pid, p in early_raw.items() if pid in late_raw and late_raw[pid] == p]
    assert len(unchanged) >= 90, f"fixtures must share many unrevised plays, got {len(unchanged)}"

    a = shield_nfl_pbp(game_detail=early, enrich=False, context=CTX)
    b = shield_nfl_pbp(game_detail=late, enrich=False, context=CTX)
    cols = [c for c in a.columns if c not in _SNAPSHOT_SCOPED]
    assert a.columns == b.columns, "two snapshots of one game must build the same schema"

    rows_a = {r["play_id"]: r for r in a.filter(pl.col("play_id").is_in(unchanged)).select(cols).iter_rows(named=True)}
    rows_b = {r["play_id"]: r for r in b.filter(pl.col("play_id").is_in(unchanged)).select(cols).iter_rows(named=True)}
    # Fewer rows than playIds: ``build_pbp`` drops TIMEOUT rows, which have playIds.
    assert rows_a.keys() == rows_b.keys()
    assert len(rows_a) >= 80, f"too few comparable rows ({len(rows_a)}) to mean anything"

    mismatches = {
        pid: {c: (rows_a[pid][c], rows_b[pid][c]) for c in cols if rows_a[pid][c] != rows_b[pid][c]}
        for pid in rows_a
        if any(rows_a[pid][c] != rows_b[pid][c] for c in cols)
    }
    assert not mismatches, f"prefix invariant broken on {len(mismatches)} play(s): {list(mismatches.items())[:3]}"
