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
