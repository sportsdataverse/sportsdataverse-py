"""Penalty flag + enforcement classification (cfbfastR-cfb-data#32).

Every case here is a real play text pattern taken from the release, not an
invented one -- the bugs these lock in were all found by measuring real seasons
(`sdv-py/dev/penalty-analysis/`), and a synthetic fixture would not have shown
any of them.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

# name-mangled private
_SETUP = CFBPlayProcess._CFBPlayProcess__setup_penalty_data


def _run(rows: list[tuple[str, str]]) -> pl.DataFrame:
    """rows = [(type.text, text)] -> the penalty columns."""
    df = pl.DataFrame({"type.text": [r[0] for r in rows], "text": [r[1] for r in rows]})
    return _SETUP(CFBPlayProcess.__new__(CFBPlayProcess), df)


def test_offsetting_matches_both_spellings() -> None:
    """ESPN writes `offsetting` 44x vs `off-setting` 1x in 2025; the old
    hyphen-only pattern flagged 0 of the 44."""
    out = _run(
        [
            ("Penalty", "PENALTY WKU Holding offsetting MSU Holding offsetting. NO PLAY."),
            ("Penalty", "PENALTY off-setting penalties. NO PLAY."),
        ]
    )
    assert out["penalty_offset"].to_list() == [True, True]


def test_declined_is_flagged_off_a_normally_typed_play() -> None:
    """The old gate required `type.text == 'Penalty'`, missing 576 of 894
    declined texts in 2025."""
    out = _run(
        [
            ("Pass Incompletion", "Smith pass incomplete Penalty, Holding declined"),
            ("Penalty", "PENALTY Holding declined"),
        ]
    )
    assert out["penalty_declined"].to_list() == [True, True]


def test_nullified_by_penalty_is_a_no_play() -> None:
    """ESPN's explicit verdict. 179 plays in 2025; the old rule caught none of
    the ones that never say 'no play'."""
    out = _run(
        [
            ("Rushing Touchdown", "Lawrence rush left for 8 yards TOUCHDOWN nullified by penalty PENALTY MOST Holding"),
        ]
    )
    assert out["penalty_no_play"][0] is True
    assert out["penalty_enforcement"][0] == "no_play"
    assert out["penalty_negated_play"][0] is True


def test_multi_penalty_counts_and_all_declined() -> None:
    """A play can carry two fouls where only one is declined, so `declined` in
    the text does NOT mean the play stood."""
    out = _run(
        [
            ("Rush", "run for 4 yds Penalty, Holding declined PENALTY Face Mask"),  # 1 of 2 declined
            ("Rush", "run for 4 yds Penalty, Holding declined"),  # all declined
        ]
    )
    assert out["penalty_count"].to_list() == [2, 1]
    assert out["penalty_declined_count"].to_list() == [1, 1]
    assert out["penalty_all_declined"].to_list() == [False, True]
    # only the all-declined play is classified as having stood
    assert out["penalty_enforcement"][1] == "declined"
    assert out["penalty_negated_play"][1] is False


def test_negating_and_standing_fouls_are_classified() -> None:
    out = _run(
        [
            ("Rush", "rush for 3 yards Penalty, Offensive Holding (-10 Yards)"),
            ("Sack", "sacked for -7 yards Penalty, Intentional Grounding"),
        ]
    )
    assert out["penalty_enforcement"].to_list() == ["negating_foul", "play_stands"]
    assert out["penalty_negated_play"].to_list() == [True, False]


def test_auto_first_down_fouls_stay_unknown() -> None:
    """These CANNOT be classified from the available signals -- a negated play
    carrying an automatic first down resets the down to 1 instead of repeating
    it, so the replay signal cannot separate it from a dead-ball foul. Guessing
    here is how cfbfastR-cfb-data#30 shipped."""
    out = _run(
        [
            ("Pass Incompletion", "pass incomplete for a 1ST down Penalty, Defensive pass interference"),
            ("Pass Reception", "pass complete for 12 yards Penalty, Personal Foul (15 Yards)"),
            ("Pass Reception", "pass complete Penalty, Roughing Passer"),
        ]
    )
    assert out["penalty_enforcement"].to_list() == ["unknown"] * 3
    # null, NOT false -- a consumer must not read "unknown" as "the play counted"
    assert out["penalty_negated_play"].to_list() == [None, None, None]


def test_no_penalty_leaves_enforcement_null() -> None:
    out = _run([("Rush", "Jones run for 5 yards to the OSU 30")])
    assert out["penalty_flag"][0] is False
    assert out["penalty_enforcement"][0] is None
    assert out["penalty_negated_play"][0] is False


@pytest.mark.parametrize(
    "text,expected",
    [
        ("TOUCHDOWN nullified by penalty PENALTY Holding", False),
        ("rush for 8 yards TOUCHDOWN", True),
        ("pass complete for a TD", True),
        ("TOUCHDOWN PENALTY Holding. NO PLAY.", False),
    ],
)
def test_td_play_excludes_negated_plays(text: str, expected: bool) -> None:
    """30 nullified touchdowns across 2015/2021/2025 were still counted."""
    from sportsdataverse.cfb.cfb_pbp import _PENALTY_NEGATED_TEXT

    df = pl.DataFrame({"text": [text]}).with_columns(
        td_play=pl.col("text").str.contains("(?i)touchdown|(?i)for a TD")
        & ~pl.col("text").str.contains(_PENALTY_NEGATED_TEXT)
    )
    assert df["td_play"][0] is expected
