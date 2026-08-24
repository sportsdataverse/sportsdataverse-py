"""Offline tests for the NCAA -> cfbfastR column mapper (``to_cfbfastr``).

Runs on the committed real stats.ncaa.org pbp captures under
``tests/fixtures/cfb_ncaa/`` (contests 5336803, 5361446, 5362431, 5362535 --
two FBS blowouts, an FBS shootout, and an FCS-at-FBS game). The running-score
assertions pin REAL 2024 final scores (verified against the public record), so
a scoring-attribution regression (defensive TDs, XP after a pick-six,
lowercase "kick attempt good") fails loudly.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_ncaa_cfbfastr import (
    CFBFASTR_SCHEMA,
    _first_last,
    to_cfbfastr,
)
from sportsdataverse.cfb.cfb_ncaa_pbp import parse_cfb_ncaa_pbp

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_ncaa"

#: fixture -> (last-row pos_team, its score, def_pos_team, its score); real finals.
FINALS = {
    "5336803": ("Ohio St.", 52, "Akron", 6),
    "5361446": ("Boise St.", 56, "Ga. Southern", 45),
    "5362431": ("Cincinnati", 38, "Towson", 20),
    "5362535": ("Merrimack", 6, "Air Force", 21),
}


def _frame(cid: str) -> "pl.DataFrame":
    """The mapped frame for one committed pbp fixture (either vendored name)."""
    path = FIX / f"mfb_pbp_{cid}.html"
    if not path.exists():
        path = FIX / f"cfb_ncaa_pbp_{cid}.html"
    html = path.read_text(encoding="utf-8")
    df = to_cfbfastr(parse_cfb_ncaa_pbp(html, contest_id=cid), season=2024)
    assert isinstance(df, pl.DataFrame)
    return df


def test_returns_documented_schema() -> None:
    df = _frame("5336803")
    assert df.columns == list(CFBFASTR_SCHEMA.keys())
    assert df.schema == pl.Schema(CFBFASTR_SCHEMA)


def test_empty_input_zero_row_with_schema() -> None:
    df = to_cfbfastr(parse_cfb_ncaa_pbp("", contest_id=None))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert df.columns == list(CFBFASTR_SCHEMA.keys())
    assert df.schema == pl.Schema(CFBFASTR_SCHEMA)


def test_return_as_pandas() -> None:
    pdf = to_cfbfastr(parse_cfb_ncaa_pbp("", contest_id=None), return_as_pandas=True)
    assert not isinstance(pdf, pl.DataFrame)
    assert list(pdf.columns) == list(CFBFASTR_SCHEMA.keys())


@pytest.mark.parametrize("cid", sorted(FINALS))
def test_running_score_matches_real_final(cid: str) -> None:
    df = _frame(cid)
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {
        FINALS[cid][0]: FINALS[cid][1],
        FINALS[cid][2]: FINALS[cid][3],
    }


def test_structure_and_columns() -> None:
    df = _frame("5336803")
    assert df.height > 100
    assert df.get_column("period").max() >= 4
    assert df.get_column("id_play").n_unique() == df.height
    # cfbfastR flag family present and boolean
    for c in (
        "rush",
        "pass",
        "completion",
        "sack",
        "int",
        "touchdown",
        "punt",
        "kickoff_play",
    ):
        assert df.schema[c] == pl.Boolean, c
    # participant naming converted to "First Last"
    rushers = df.filter(pl.col("rusher_player_name").is_not_null())
    assert rushers.height > 0
    assert not rushers.get_column("rusher_player_name").str.contains(",").any()


def test_first_last_suffixes() -> None:
    assert _first_last("Wilborn Jr.,James") == "James Wilborn Jr."
    assert _first_last("Jordan III,Tre") == "Tre Jordan III"
    assert _first_last("Anthony,Malakai") == "Malakai Anthony"
    assert _first_last(None) is None
