"""A row the id sort files inside a later drive goes back to the end of its own drive.

sportsdataverse-py#637: ESPN's ids and sequence numbers are not always chronological across
drives, and its clock stamps repeat, so a drive-ending play can carry an id past the next
drive's plays. ``_reunite_drive_rows`` moves it back only when its start down-and-distance
text continues the end text of its own drive's last earlier row.

Fixtures, trimmed from ``cfbfastR-cfb-raw/cfb/json/raw/{game_id}.json`` to the keys the
processor reads (see ``test_cfb_pbp_offline._trimmed``):

* ``summary_400548134_trimmed.json.gz`` -- Army's "4th & 0 at KENT 14" field goal (2014).
* ``summary_400787459_trimmed.json.gz`` -- Texas State's "4th & 0 at GASO 11" field goal (2015).
* ``summary_400763571_trimmed.json.gz`` -- Maryland's "3rd & 0 at IU 14" touchdown (2015).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.cfb.cfb_pbp as cfb_pbp_mod
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _run(monkeypatch, game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr(cfb_pbp_mod, "download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=game_id)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    return pl.from_dicts(proc.plays_json, infer_schema_length=None)


@pytest.mark.parametrize(
    ("game_id", "play_id", "prev_id", "text", "distance"),
    [
        # prev_id: the last play of the moved row's own drive
        (400548134, 400548134101886613, 400548134101849916, "4th & 0 at KENT 14", 14),
        (400787459, 400787459102977201, 400787459102975702, "4th & 0 at GASO 11", 11),
        (400763571, 400763571101946705, 400763571101936109, "3rd & 0 at IU 14", 14),
    ],
)
def test_drive_ender_rejoins_its_drive(monkeypatch, game_id, play_id, prev_id, text, distance):
    df = _run(monkeypatch, game_id).with_row_index("_pos")
    ids = df["id"].cast(pl.Int64).to_list()
    i = ids.index(play_id)
    row = df.row(i, named=True)
    assert row["start.downDistanceText"] == text
    # the previous snap is now its own drive's, so the "& 0 at" repair can read it
    assert row["start.distance"] == distance
    if prev_id is not None:
        assert ids[i - 1] == prev_id


def _frame(rows: list[tuple]) -> pl.DataFrame:
    cols = ["id", "drive.id", "type.text", "period.number", "start.downDistanceText", "end.downDistanceText"]
    return pl.DataFrame(rows, schema=cols, orient="row")


def test_reunite_moves_a_row_back_only_on_matching_state():
    # 400548134: Army's drive 1 ends "4th & Goal at KENT 14"; its field goal was filed after
    # Kent State's drive-3 rows.
    rows = [
        (1, 1, "Pass Incompletion", 1, "3rd & Goal at KENT 14", "4th & Goal at KENT 14"),
        (2, 3, "Rush", 1, "2nd & 6 at ARMY 13", "3rd & 3 at ARMY 10"),
        (3, 3, "Rush", 1, "3rd & 3 at ARMY 10", "4th & 3 at ARMY 10"),
        (4, 1, "Field Goal Good", 1, "4th & 0 at KENT 14", None),
        (5, 3, "Field Goal Good", 1, "4th & 3 at ARMY 10", None),
    ]
    out = cfb_pbp_mod._reunite_drive_rows(_frame(rows))
    assert out["id"].to_list() == [1, 4, 2, 3, 5]


@pytest.mark.parametrize(
    "rows",
    [
        # start text does not continue its drive's last row -> stays
        [
            (1, 1, "Pass Incompletion", 1, "3rd & Goal at KENT 14", "4th & Goal at KENT 14"),
            (2, 3, "Rush", 1, "3rd & 3 at ARMY 10", "4th & 3 at ARMY 10"),
            (4, 1, "Field Goal Good", 1, "4th & 0 at KENT 20", None),
        ],
        # it already continues the row it follows -> stays
        [
            (1, 1, "Pass Incompletion", 1, "3rd & Goal at KENT 14", "4th & Goal at KENT 14"),
            (2, 3, "Rush", 1, "3rd & 3 at ARMY 10", "4th & 0 at KENT 14"),
            (4, 1, "Field Goal Good", 1, "4th & 0 at KENT 14", None),
        ],
        # a catch-all first "drive" kickoff has no start text -> stays
        [
            (1, 2, "Rush", 1, "1st & 10 at TAR 25", "2nd & 3 at TAR 32"),
            (2, 3, "Punt", 1, "4th & 5 at TAR 30", None),
            (3, 0, "Kickoff", 1, None, "1st & 10 at TAR 25"),
        ],
        # admin rows (a timeout carrying its drive's stale state) never move
        [
            (1, 1, "Pass Incompletion", 1, "3rd & Goal at KENT 14", "4th & Goal at KENT 14"),
            (2, 3, "Rush", 1, "3rd & 3 at ARMY 10", "4th & 3 at ARMY 10"),
            (4, 1, "Timeout", 1, "4th & 0 at KENT 14", None),
        ],
        # overtime rows are ordered elsewhere -> stay
        [
            (1, 1, "Rush", 5, "3rd & Goal at KENT 14", "4th & Goal at KENT 14"),
            (2, 3, "Rush", 5, "3rd & 3 at ARMY 10", "4th & 3 at ARMY 10"),
            (4, 1, "Field Goal Good", 5, "4th & 0 at KENT 14", None),
        ],
    ],
)
def test_reunite_leaves_rows_without_evidence(rows):
    df = _frame(rows)
    assert cfb_pbp_mod._reunite_drive_rows(df)["id"].to_list() == df["id"].to_list()


def test_reunite_carries_a_try_row_with_its_touchdown():
    rows = [
        (1, 1, "Rush", 1, "1st & Goal at IU 4", "2nd & Goal at IU 2"),
        (2, 2, "Kickoff", 1, None, "1st & 10 at IU 25"),
        (3, 1, "Rushing Touchdown", 1, "2nd & Goal at IU 2", None),
        (4, 1, "Two Point Rush", 1, None, None),
        (5, 2, "Rush", 1, "1st & 10 at IU 25", "2nd & 4 at IU 31"),
    ]
    out = cfb_pbp_mod._reunite_drive_rows(_frame(rows))
    assert out["id"].to_list() == [1, 3, 4, 2, 5]


def test_reunite_is_a_no_op_without_the_drive_column():
    df = _frame([(1, 1, "Rush", 1, "1st & 10 at A 25", None)]).drop("drive.id")
    assert cfb_pbp_mod._reunite_drive_rows(df).equals(df)


def test_reunite_leaves_the_next_drives_try_behind():
    # 323080276 shape: id order alternates two drives. The drive-1 row moves back to
    # its drive; drive 3's PAT, filed right after it, belongs to drive 3's touchdown
    # and must stay there rather than land before it.
    rows = [
        (1, 1, "Rush", 1, "1st & Goal at IU 4", "2nd & Goal at IU 2"),
        (2, 3, "Rushing Touchdown", 1, "1st & Goal at MD 3", None),
        (3, 1, "Rushing Touchdown", 1, "2nd & Goal at IU 2", None),
        (4, 3, "Extra Point Good", 1, None, None),
    ]
    out = cfb_pbp_mod._reunite_drive_rows(_frame(rows))
    assert out["id"].to_list() == [1, 3, 2, 4]


def test_reunite_carries_a_same_id_copy():
    # a drives.current copy repeats the moved row's id; it moves too, so the later
    # same-id dedupe still sees the copies side by side
    rows = [
        (1, 1, "Pass Incompletion", 1, "3rd & Goal at KENT 14", "4th & Goal at KENT 14"),
        (2, 3, "Rush", 1, "3rd & 3 at ARMY 10", "4th & 3 at ARMY 10"),
        (4, 1, "Field Goal Good", 1, "4th & 0 at KENT 14", None),
        (4, 4, "Field Goal Good", 1, "4th & 0 at KENT 14", None),
    ]
    out = cfb_pbp_mod._reunite_drive_rows(_frame(rows))
    assert out["id"].to_list() == [1, 4, 4, 2]
