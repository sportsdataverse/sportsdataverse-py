"""Parity tests for ``__add_air_yards_cols`` -- the air-yards / yards-after-catch
derivation ported from the cfbfastR-cfb-data air-yards model (R/pandas 0.36-live).

ESPN annotates pass plays with the on-field catch/target point as "caught at
OU35" (completions) or "thrown to TEX42" (targets). The stated yardline is
relative to whichever team owns that side of the field, not the offense, so the
abbreviation must be resolved to the possessing-vs-defending team before it can
be turned into yards-to-endzone. The original R/pandas code disambiguated with a
character-count cosine similarity against the teams-table abbreviations; this
polars port reuses the codebase's prefix-tolerant ``_abbr_compat`` matcher (the
same one that resolves recovery/penalty teams), which also handles ESPN's
BUF/BUFF two-abbreviation-form inconsistency.

Field geometry under test:
  * catch abbrev on the POSSESSING team's side -> air_yardsToEndzone = 100 - yardline
  * catch abbrev on the DEFENDING team's side  -> air_yardsToEndzone = yardline
  * no air-yards text / unresolved abbreviation -> null (all three outputs)

air_yards          = start.yardsToEndzone - air_yardsToEndzone
yards_after_catch  = yds_receiving - air_yards   (completed passes only)
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"

# Game frame constants: home = OU (id 201), away = TEX (id 251).
_HOME_ID, _AWAY_ID = 201, 251


def _row(
    text: str,
    *,
    pos_team: int,
    def_pos_team: int,
    start_ytg: int,
    stat_yardage: int | None = None,
    completion: bool = False,
    home_abbr: str = "OU",
    away_abbr: str = "TEX",
) -> dict:
    return {
        "text": text,
        "pos_team": pos_team,
        "def_pos_team": def_pos_team,
        "homeTeamId": _HOME_ID,
        "awayTeamId": _AWAY_ID,
        "homeTeamAbbrev": home_abbr,
        "awayTeamAbbrev": away_abbr,
        "start.yardsToEndzone": start_ytg,
        "statYardage": stat_yardage,
        "completion": completion,
    }


def _run_air_yards(rows: list[dict]) -> pl.DataFrame:
    """Drive the (name-mangled) private ``__add_air_yards_cols`` on synthetic rows."""
    proc = CFBPlayProcess(gameId=1)
    return proc._CFBPlayProcess__add_air_yards_cols(pl.DataFrame(rows))


def test_caught_on_possessing_team_side():
    """OU (offense) catches at its own 40: yardline is on the possessing side,
    so air_yardsToEndzone = 100 - 40 = 60. Ball started at OU 25 (ytg 75), so
    air_yards = 75 - 60 = 15 and YAC = 18 - 15 = 3."""
    out = _run_air_yards(
        [
            _row(
                "Jackson Arnold pass complete to Deion Burks caught at OU40 for 18 yds",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=75,
                stat_yardage=18,
                completion=True,
            )
        ]
    )
    assert out["air_yardsToEndzone"][0] == 60
    assert out["air_yards"][0] == 15
    assert out["yards_after_catch"][0] == 3


def test_thrown_to_defending_team_side_incompletion():
    """OU (offense) targets the TEX 30: yardline is already the distance to the
    endzone, so air_yardsToEndzone = 30. Ball at OU 35 (ytg 65) -> air_yards = 35
    (a deep target). Incompletion -> YAC is null."""
    out = _run_air_yards(
        [
            _row(
                "Jackson Arnold pass incomplete thrown to TEX30, broken up by John Doe",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=65,
                completion=False,
            )
        ]
    )
    assert out["air_yardsToEndzone"][0] == 30
    assert out["air_yards"][0] == 35
    assert out["yards_after_catch"][0] is None


def test_prefix_tolerant_abbreviation_match():
    """ESPN's two-form inconsistency: the header abbrev is 'BUFF' but the play
    text says 'BUF'. The prefix-tolerant matcher resolves it (BUFF startswith
    BUF). Offense on its own 40 -> air_yardsToEndzone = 60."""
    out = _run_air_yards(
        [
            _row(
                "QB pass complete to WR caught at BUF40 for 9 yds",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=70,
                stat_yardage=12,
                completion=True,
                home_abbr="BUFF",
            )
        ]
    )
    assert out["air_yardsToEndzone"][0] == 60
    assert out["air_yards"][0] == 10  # 70 - 60
    assert out["yards_after_catch"][0] == 2  # 12 - 10


def test_no_air_yards_text_is_null():
    """A rush has no 'caught at'/'thrown to' token -> all three outputs null."""
    out = _run_air_yards(
        [
            _row(
                "Tawee Walker rush for 5 yards to the OU30",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=35,
            )
        ]
    )
    assert out["air_yardsToEndzone"][0] is None
    assert out["air_yards"][0] is None
    assert out["yards_after_catch"][0] is None


def test_air_yards_populated_on_real_fixture(monkeypatch):
    """End-to-end: a real captured game (NCSU @ FSU, 401754598) must yield
    non-null ``air_yards`` on completed passes after the FULL pipeline. Guards
    against the parser silently no-op'ing if the ESPN text format drifts, a
    source column is renamed, or a final column-selection step drops the new
    columns. The fixture's catch abbrevs (NCSU/FSU) resolve against the header
    abbrevs, so air-yards must be present, not all-null."""
    summary = json.loads((FIX / "summary_401754598.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598)
    proc.join_participants = False  # offline: skip the participants/roster network fetch
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)

    assert {"air_yardsToEndzone", "air_yards", "yards_after_catch"} <= set(df.columns), (
        "air-yards columns dropped from the final pipeline output"
    )
    completed = df.filter(pl.col("completion") == True)
    assert completed.height > 0, "fixture has no completed passes"
    assert completed["air_yards"].drop_nulls().len() > 0, "air_yards all-null on completed passes"
    # YAC must populate for completed passes and must decompose the play exactly:
    # air_yards + yards_after_catch == statYardage (statYardage = total play yards).
    yac = df.filter(pl.col("yards_after_catch").is_not_null())
    assert yac.height > 0, "yards_after_catch all-null on completed passes"
    assert (yac["air_yards"] + yac["yards_after_catch"] == yac["statYardage"]).all()
    # YAC is never emitted on incompletions.
    assert df.filter((pl.col("completion") == False).and_(pl.col("yards_after_catch").is_not_null())).height == 0


def test_qb_hurry_flag_on_real_fixture(monkeypatch):
    """``qb_hurry`` is a populated boolean: the fixture contains 'QB hurried by'
    plays, so the flag must be True on at least one and never null."""
    summary = json.loads((FIX / "summary_401754598.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)

    assert "qb_hurry" in df.columns
    assert df.schema["qb_hurry"] == pl.Boolean
    assert df["qb_hurry"].null_count() == 0, "qb_hurry should be a clean boolean (no nulls)"
    assert df["qb_hurry"].sum() > 0, "qb_hurry never True despite 'QB hurried by' plays in fixture"


def test_unresolved_abbreviation_is_null():
    """An abbreviation matching neither team (bad parse) -> null, never a guess."""
    out = _run_air_yards(
        [
            _row(
                "QB pass complete to WR caught at XYZ20",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=50,
                stat_yardage=8,
                completion=True,
            )
        ]
    )
    assert out["air_yardsToEndzone"][0] is None
    assert out["air_yards"][0] is None
    assert out["yards_after_catch"][0] is None


# --- 2025+ vendor text: the school's own abbreviation, not ESPN's -------------

_JSU_ID, _NDSU_ID = 55, 2449  # Jacksonville St (ESPN "JVST", text "JSU") at North Dakota St


def _vendor_row(text: str, *, pos_team: int, start_ytg: int, end_ytg: int | None, **kw) -> dict:
    """A 2025+-template row: real ESPN abbreviations, vendor text, ESPN end spot."""
    def_team = _JSU_ID if pos_team == _NDSU_ID else _NDSU_ID
    row = _row(
        text,
        pos_team=pos_team,
        def_pos_team=def_team,
        start_ytg=start_ytg,
        home_abbr="NDSU",
        away_abbr="JVST",
        **kw,
    )
    row.update({"homeTeamId": _NDSU_ID, "awayTeamId": _JSU_ID, "end.yardsToEndzone": end_ytg})
    return row


def _vendor_game(extra: list[dict]) -> list[dict]:
    """Enough end spots for the vote: NDSU rushes ending on both sides of the field."""
    return [
        # NDSU ball, ends at the JSU 30 -> ESPN yards-to-endzone 30 -> JSU is the defending side
        _vendor_row("#3 C.Miller rush for 5 yards to the JSU30", pos_team=_NDSU_ID, start_ytg=35, end_ytg=30),
        _vendor_row("#3 C.Miller rush for 4 yards to the JSU26", pos_team=_NDSU_ID, start_ytg=30, end_ytg=26),
        # NDSU ball, ends at its own 40 -> yards-to-endzone 60 -> NDSU is the possessing side
        _vendor_row("#3 C.Miller rush for 2 yards to the NDSU40", pos_team=_NDSU_ID, start_ytg=62, end_ytg=60),
        _vendor_row("#3 C.Miller rush for 3 yards to the NDSU43", pos_team=_NDSU_ID, start_ytg=60, end_ytg=57),
    ] + extra


def test_vendor_abbreviation_learned_from_end_spots():
    """'JSU' is not a prefix of ESPN's 'JVST'; the game's own end spots side it."""
    out = _run_air_yards(
        _vendor_game(
            [
                # JSU ball at its own 35 (65 to go); caught at the JSU 47 -> 53 to go -> 12 air yards
                _vendor_row(
                    "Shotgun #2 N.Hayes pass complete short left to #18 J.Williams caught at JSU47, for 14 yards",
                    pos_team=_JSU_ID,
                    start_ytg=65,
                    end_ytg=51,
                    stat_yardage=14,
                    completion=True,
                ),
                # NDSU ball at the JSU 40; thrown to the JSU 33 -> 33 to go -> 7 air yards, no YAC
                _vendor_row(
                    "#12 C.Creel pass incomplete short right to #9 R.Johnson thrown to JSU33",
                    pos_team=_NDSU_ID,
                    start_ytg=40,
                    end_ytg=40,
                ),
            ]
        )
    )
    assert out["air_yardsToEndzone"].to_list()[-2:] == [53, 33]
    assert out["air_yards"].to_list()[-2:] == [12, 7]
    assert out["yards_after_catch"].to_list()[-2:] == [2, None]
    assert "_catch_team" not in out.columns and "_catch_abbr" not in out.columns


def test_vendor_token_shapes_space_dot_and_two_words():
    """'UA 10', 'BC.41', 'Sac St10' and 'NC ST19' all parse as an abbreviation + yardline."""
    rows = [
        _row(
            "pass complete to X caught at OU 10, for 2 yards",
            pos_team=_HOME_ID,
            def_pos_team=_AWAY_ID,
            start_ytg=20,
            stat_yardage=2,
            completion=True,
        ),
        _row(
            "pass complete to X caught at TEX.41, for 9 yards",
            pos_team=_HOME_ID,
            def_pos_team=_AWAY_ID,
            start_ytg=50,
            stat_yardage=9,
            completion=True,
        ),
        _row(
            "pass incomplete to X thrown to Sac St10",
            pos_team=_HOME_ID,
            def_pos_team=_AWAY_ID,
            start_ytg=30,
            home_abbr="SACST",
            away_abbr="TEX",
        ),
        _row(
            "pass incomplete to X thrown to NC ST19",
            pos_team=_HOME_ID,
            def_pos_team=_AWAY_ID,
            start_ytg=30,
            home_abbr="NCST",
            away_abbr="TEX",
        ),
    ]
    out = _run_air_yards(rows)
    # OU (possessing) 10 -> 90 to go; TEX (defending) 41 -> 41; Sac St (possessing) 10 -> 90; NC ST (possessing) 19 -> 81
    assert out["air_yardsToEndzone"].to_list() == [90, 41, 90, 81]
    assert out["air_yards"].to_list() == [-70, 9, -60, -51]


def test_midfield_spot_needs_no_abbreviation():
    out = _run_air_yards(
        [
            _row(
                "pass complete to X caught at 50, for 3 yards",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=60,
                stat_yardage=3,
                completion=True,
            ),
            _row(
                "pass complete to X caught at the 50, for 3 yards",
                pos_team=_HOME_ID,
                def_pos_team=_AWAY_ID,
                start_ytg=60,
                stat_yardage=3,
                completion=True,
            ),
        ]
    )
    assert out["air_yardsToEndzone"].to_list() == [50, 50]
    assert out["air_yards"].to_list() == [10, 10]
    assert out["yards_after_catch"].to_list() == [-7, -7]


def test_side_vote_needs_a_majority_and_two_votes():
    """One contradictory end spot cannot flip a side; a lone spot cannot establish one."""
    rows = _vendor_game(
        [
            # a mis-stated spot that says JSU is the possessing side (end 70 at 'JSU30')
            _vendor_row("#3 C.Miller rush for 1 yard to the JSU30", pos_team=_NDSU_ID, start_ytg=71, end_ytg=70),
            # a single 'XYZ' end spot: one vote only -> no mapping -> the catch below stays null
            _vendor_row("#3 C.Miller rush for 1 yard to the XYZ30", pos_team=_NDSU_ID, start_ytg=31, end_ytg=30),
            _vendor_row(
                "pass complete to Y caught at JSU20, for 5 yards",
                pos_team=_NDSU_ID,
                start_ytg=25,
                end_ytg=20,
                stat_yardage=5,
                completion=True,
            ),
            _vendor_row(
                "pass complete to Y caught at XYZ20, for 5 yards",
                pos_team=_NDSU_ID,
                start_ytg=25,
                end_ytg=20,
                stat_yardage=5,
                completion=True,
            ),
        ]
    )
    out = _run_air_yards(rows)
    assert out["air_yardsToEndzone"].to_list()[-2:] == [20, None]  # JSU still defending side (3 of 4 votes)


def test_side_vote_uses_the_last_spot_of_a_multi_spot_play():
    """A fumble return names two spots; only the final one matches ESPN's end yardline."""
    rows = [
        # NDSU rush to the JSU 30, fumble, returned by JSU to the NDSU 45: ball ends 55 from the
        # NDSU goal, i.e. ESPN end.yardsToEndzone (NDSU still charted as pos_team) = 55.
        # First spot would vote JSU = defending (30 == ... no: 30 != 55 and 30 != 45), so it must
        # not be used; the LAST spot NDSU45 -> 100 - 45 == 55 -> NDSU is the possessing side.
        _vendor_row(
            "#3 C.Miller rush for 5 yards to the JSU30, fumble forced, recovered by JSU #9 returned to the NDSU45",
            pos_team=_NDSU_ID,
            start_ytg=35,
            end_ytg=55,
        ),
        _vendor_row("#3 C.Miller rush for 3 yards to the NDSU43", pos_team=_NDSU_ID, start_ytg=60, end_ytg=57),
        # no direct JSU end spots: JSU's side must come only from the fumble play NOT mis-voting
        _vendor_row(
            "pass complete to Y caught at NDSU40, for 5 yards",
            pos_team=_JSU_ID,
            start_ytg=45,
            end_ytg=35,
            stat_yardage=5,
            completion=True,
        ),
    ]
    out = _run_air_yards(rows)
    # NDSU learned as itself from two clean last-spot votes; JSU throwing to the NDSU 40 -> 40 to go -> 5 air yards
    assert out["air_yardsToEndzone"][-1] == 40 and out["air_yards"][-1] == 5
