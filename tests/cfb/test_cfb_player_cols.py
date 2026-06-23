"""Regression tests for play-text player-name extraction in ``__add_player_cols``.

These guard the multi-alternative ``str.extract`` group-index fix: polars
``str.extract`` defaults to capture group 1, but ESPN play text matches
non-first alternatives of the extraction patterns (e.g. "rush" is the 3rd
alternative of the rusher pattern, so its name lands in group 3). The default
group-1 extract therefore returned ``None`` for every ESPN "rush for" play --
masked for 2014+ games (``__join_participants`` overwrites with structured
participant names) but null for pre-2014 games, which have no participant array.

The fix coalesces across every capture group so the matched alternative's name
is returned regardless of its position. See ``_extract_player_name`` in
``cfb_pbp.py``.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess


def _run_player_cols(rows: list[dict]) -> pl.DataFrame:
    """Drive the (name-mangled) private ``__add_player_cols`` on synthetic rows."""
    proc = CFBPlayProcess(gameId=1)
    df = pl.DataFrame(rows)
    return proc._CFBPlayProcess__add_player_cols(df)


def _row(text: str, *, rush=False, pass_=False, sack_vec=False, sack=False, fumble_vec=False, type_text="Rush"):
    return {
        "text": text,
        "rush": rush,
        "pass": pass_,
        "sack_vec": sack_vec,
        "sack": sack,
        "fumble_vec": fumble_vec,
        "type.text": type_text,
    }


def test_rusher_extracted_from_espn_rush_text():
    """ESPN uses '{Name} rush for {N} yards' (3rd pattern alternative). The
    group-index bug returned None here; the fix must extract the rusher."""
    out = _run_player_cols(
        [
            _row("Bryant Moniz rush for 22 yards to the Hawa 39 for a 1ST down.", rush=True, type_text="Rush"),
            _row("Joey Iosefa rush for 1 yard to the Hawa 17.", rush=True, type_text="Rush"),
            _row("John Smith run for 5 yards", rush=True, type_text="Rush"),  # CFBD-style 'run' (group 1)
        ]
    )
    assert out["rusher_player_name"].to_list() == ["Bryant Moniz", "Joey Iosefa", "John Smith"]


def test_passer_and_receiver_extracted():
    out = _run_player_cols(
        [
            _row(
                "Tom Brady pass complete to Randy Moss for 15 yards to the NE 40.",
                pass_=True,
                type_text="Pass Reception",
            )
        ]
    )
    assert out["passer_player_name"][0] == "Tom Brady"
    assert out["receiver_player_name"][0] == "Randy Moss"


def test_punter_extracted_from_both_phrasings():
    """'{Name} punt' (group 1) AND 'Punt by {Name}' (group 2) must both resolve."""
    out = _run_player_cols(
        [
            _row("Scott Harding punt for 41 yards, John Doe returns for 5 yards.", type_text="Punt"),
            _row("Punt by Scott Harding for 41 yards", type_text="Punt"),
        ]
    )
    assert out["punter_player_name"].to_list() == ["Scott Harding", "Scott Harding"]


def test_kickoff_specialist_extracted():
    out = _run_player_cols(
        [
            _row("Joe Kicker kickoff for 65 yards", type_text="Kickoff"),
            _row("Joe Kicker on-side kick to the 45", type_text="Kickoff"),  # 'on-side' = group 2
        ]
    )
    assert out["kickoff_player_name"].to_list() == ["Joe Kicker", "Joe Kicker"]


def test_punt_returner_extracted_from_returns_and_returned_by():
    """'..., {Name} returns' (group 1) AND '..., returned by {Name}' (group 3)."""
    out = _run_player_cols(
        [
            _row("Scott Harding punt for 41 yards, John Doe returns for 5 yards to the 30.", type_text="Punt"),
            _row("Scott Harding punt for 41 yards, returned by John Doe for 5 yards.", type_text="Punt"),
        ]
    )
    assert out["punt_return_player_name"].to_list() == ["John Doe", "John Doe"]


def test_fg_kicker_extracted_after_digit_escape_fix():
    """The fg-kicker pattern used ``\\\\d`` (literal backslash) instead of ``\\d``,
    so it matched nothing. Fixing the escape lets the kicker name extract."""
    out = _run_player_cols([_row("Brett Baer 47 Yd Field Goal Good", type_text="Field Goal Good")])
    assert out["fg_kicker_player_name"][0] == "Brett Baer"


def test_kickoff_returner_extracted_without_yardage_tail():
    """'returned by {Name} for N yards' must not leak the yardage tail into the name."""
    out = _run_player_cols(
        [
            _row(
                "Tyler Hadden kickoff for 64 yards returned by Arthur Jaffee for 4 yards to the 30.",
                type_text="Kickoff Return",
            ),
            _row("Joe Kicker kickoff, John Doe return for 22 yards.", type_text="Kickoff Return"),
        ]
    )
    assert out["kickoff_return_player_name"].to_list() == ["Arthur Jaffee", "John Doe"]


def test_fumble_forced_player_extracted():
    out = _run_player_cols(
        [
            _row(
                "Joe Back rush for 5 yards, fumbled, forced by Bill Smith, recovered by Tom Jones at the 40.",
                rush=True,
                fumble_vec=True,
                type_text="Rush",
            )
        ]
    )
    assert out["fumble_forced_player_name"][0] == "Bill Smith"
