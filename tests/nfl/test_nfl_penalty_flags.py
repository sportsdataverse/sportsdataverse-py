"""NFL penalty_detail labeler regressions — port of the CFB twin's fixes.

The NFL chain shared the CFB labeler's structure (same 0.36-live lineage), so
the gaps the 2025 CFB taxonomy measured are locked in here too: foul names
must win over disposition labels, and the case/hyphen/typo vendor variants
must classify.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

_SETUP = NFLPlayProcess._NFLPlayProcess__setup_penalty_data


def _details(rows: list[tuple[str, str]]) -> list:
    df = pl.DataFrame({"type.text": [r[0] for r in rows], "text": [r[1] for r in rows]})
    return _SETUP(NFLPlayProcess.__new__(NFLPlayProcess), df)["penalty_detail"].to_list()


def test_detail_roughing_the_passer_with_the() -> None:
    assert _details(
        [
            ("Penalty", "PENALTY BUF Roughing The Passer (Ed Oliver) 15 yards to the BUF 40"),
            ("Penalty", "PENALTY MIA roughing passer 15 yards"),
        ],
    ) == ["Roughing the Passer", "Roughing the Passer"]


def test_detail_declined_keeps_foul_name() -> None:
    df = pl.DataFrame(
        {
            "type.text": ["Pass Incompletion"],
            "text": ["Allen pass incomplete Penalty, Holding declined"],
        },
    )
    out = _SETUP(NFLPlayProcess.__new__(NFLPlayProcess), df)
    assert out["penalty_detail"][0] == "Holding"
    assert out["penalty_declined"][0] is True


def test_detail_disposition_fires_only_without_foul_name() -> None:
    assert _details([("Penalty", "PENALTY declined")]) == ["Declined"]


def test_detail_vendor_variants() -> None:
    assert _details(
        [
            ("Penalty", "PENALTY NYJ off-side 5 yards to the NYJ 35"),
            ("Penalty", "DAL Penalty, Sideline Inteference (15 Yards) to the PHI 38"),
            ("Penalty", "PENALTY NE Illegal Substitution 5 yards"),
            ("Rush", "Judkins rush for 2 yards PENALTY CLE Chop Block (15 yards)"),
            ("Punt", "Bailey punt for 44 yds PENALTY TEN Running Into The Kicker 5 yards"),
        ],
    ) == [
        "Offside",
        "Sideline Interference",
        "Substitution Infraction",
        "Chop Block",
        "Running Into Kicker",
    ]


def test_flags_declined_and_offset_off_normally_typed_plays() -> None:
    df = pl.DataFrame(
        {
            "type.text": ["Pass Incompletion", "Rush", "Rushing Touchdown"],
            "text": [
                "Allen pass incomplete Penalty, Holding declined",
                "PENALTY KC Holding offsetting NYJ Holding offsetting. NO PLAY.",
                "Henry rush for 8 yards TOUCHDOWN nullified by penalty PENALTY BAL Holding",
            ],
        },
    )
    out = _SETUP(NFLPlayProcess.__new__(NFLPlayProcess), df)
    assert out["penalty_declined"].to_list() == [True, False, False]
    assert out["penalty_offset"].to_list() == [False, True, False]
    assert out["penalty_no_play"].to_list() == [False, True, True]
