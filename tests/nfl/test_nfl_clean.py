"""Tests for the nflfastR ``clean_pbp`` port (:mod:`sportsdataverse.nfl.nfl_clean`)."""

from __future__ import annotations

from typing import Any, Dict

import polars as pl

from sportsdataverse.nfl.nfl_clean import _ADDED_SCHEMA, TEAM_COLUMNS, clean_nfl_pbp, team_name_fn


def _row(**overrides: Any) -> Dict[str, Any]:
    """One minimal nflverse-shape pbp row, with sensible pass-play defaults."""
    row: Dict[str, Any] = {
        "game_id": "2023_01_JAX_IND",
        "play_id": 100,
        "season": 2023,
        "desc": "(15:00) 6-G.Minshew pass incomplete short right to 15-D.Parker",
        "epa": 0.5,
        "posteam": "JAX",
        "defteam": "IND",
        "home_team": "JAX",
        "away_team": "IND",
        "qb_scramble": 0,
        "qb_kneel": 0,
        "kickoff_attempt": 0,
        "first_down_rush": 0,
        "first_down_pass": 0,
        "first_down_penalty": 0,
        "play_type": "pass",
        "passer_player_name": None,
        "passer_player_id": None,
        "rusher_player_name": None,
        "rusher_player_id": None,
        "receiver_player_name": None,
        "receiver_player_id": None,
        "fumbled_1_player_name": None,
        "fumbled_1_player_id": None,
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Name cleaning -- suffix-case hardcoded fix table
# ---------------------------------------------------------------------------


def test_passer_name_fix_minshew_suffix() -> None:
    """ "G.Minshew" extracted from desc is patched to "G.Minshew II" (verbatim
    §6 hardcoded fix table -- not something the extraction regex itself
    produces). A resolvable ``passer_player_id`` is required: the second
    custom_mode pass re-derives the name FROM the id and nulls it out when
    the id is unresolved (verbatim R semantics -- see
    ``test_name_without_resolvable_id_is_nulled``)."""
    df = pl.DataFrame([_row(passer_player_id="00-0035228")])
    out = clean_nfl_pbp(df)
    assert out["passer"][0] == "G.Minshew II"


def test_rusher_name_fix_griffin_suffix() -> None:
    """ "R.Griffin" -> "R.Griffin III" on the rusher side of the same table."""
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 10-R.Griffin left tackle for 4 yards",
                play_type="run",
                passer_player_id=None,
                rusher_player_id="00-0029263",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["rusher"][0] == "R.Griffin III"
    assert out["passer"][0] is None


def test_name_without_resolvable_id_is_nulled() -> None:
    """Verbatim (if surprising) R semantics: if a name is extracted from
    ``desc`` but no non-null player id exists in its ``(name, posteam,
    season)`` group, the second custom_mode pass explicitly nulls the name
    back out (``passer = if_else(is.na(passer_id), NA, custom_mode(passer))``
    in the R source)."""
    df = pl.DataFrame([_row()])  # no passer_player_id supplied
    out = clean_nfl_pbp(df)
    assert out["passer_id"][0] is None
    assert out["passer"][0] is None


def test_receiver_name_fix_no_op_id_guarded() -> None:
    """The D.Wells / D.Hayes receiver fixups only fire when both the raw name
    AND raw id match -- verbatim guard, not a plain name check."""
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 6-G.Minshew pass short right to 88-D.Wells for 9 yards",
                receiver_player_name="D.Wells",
                receiver_player_id="00-0017421",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["receiver"][0] == "D.Wells"


# ---------------------------------------------------------------------------
# name / id fallback (passer-else-rusher)
# ---------------------------------------------------------------------------


def test_name_id_fallback_pass_row_uses_passer() -> None:
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 6-T.Brady pass short right to 15-D.Parker for 9 yards",
                passer_player_name="T.Brady",
                passer_player_id="00-0019596",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["name"][0] == out["passer"][0] == "T.Brady"
    assert out["id"][0] == out["passer_id"][0] == "00-0019596"


def test_name_id_fallback_rush_row_uses_rusher() -> None:
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 22-D.Henry left tackle for 5 yards",
                play_type="run",
                rusher_player_name="D.Henry",
                rusher_player_id="00-0033118",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["passer"][0] is None
    assert out["name"][0] == out["rusher"][0] == "D.Henry"
    assert out["id"][0] == out["rusher_id"][0] == "00-0033118"


# ---------------------------------------------------------------------------
# fantasy fallback (rusher else receiver, else passer on a scramble)
# ---------------------------------------------------------------------------


def test_fantasy_fallback_receiver_on_reception() -> None:
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 6-T.Brady pass short right to 15-D.Parker for 9 yards",
                passer_player_name="T.Brady",
                passer_player_id="00-0019596",
                receiver_player_name="D.Parker",
                receiver_player_id="00-0034348",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["fantasy"][0] == "D.Parker"
    assert out["fantasy_id"][0] == "00-0034348"
    assert out["fantasy_player_name"][0] == "D.Parker"
    assert out["fantasy_player_id"][0] == "00-0034348"


def test_fantasy_fallback_rusher_beats_receiver() -> None:
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 22-D.Henry left tackle for 5 yards",
                play_type="run",
                rusher_player_name="D.Henry",
                rusher_player_id="00-0033118",
                receiver_player_name="D.Parker",
                receiver_player_id="00-0034348",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["fantasy"][0] == "D.Henry"
    assert out["fantasy_id"][0] == "00-0033118"
    # fantasy_player_name/_id mirror the raw rusher/receiver fallback too.
    assert out["fantasy_player_name"][0] == "D.Henry"


def test_fantasy_fallback_scramble_passer() -> None:
    """No rusher, no receiver, but a QB scramble -> fantasy falls back to the
    (cleaned) passer."""
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) 6-T.Brady scramble up the middle for 4 yards",
                qb_scramble=1,
                passer_player_name="T.Brady",
                passer_player_id="00-0019596",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["fantasy"][0] == out["passer"][0] == "T.Brady"
    assert out["fantasy_id"][0] == "00-0019596"


# ---------------------------------------------------------------------------
# Team normalization -- every touched column, historical codes
# ---------------------------------------------------------------------------


def test_team_normalization_sd_to_lac_and_oak_to_lv() -> None:
    df = pl.DataFrame(
        [
            _row(
                posteam="SD",
                defteam="OAK",
                home_team="SD",
                away_team="OAK",
                yrdln="SD 49",
                td_team="OAK",
                return_team="SD",
                penalty_team="OAK",
                side_of_field="SD",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["posteam"][0] == "LAC"
    assert out["defteam"][0] == "LV"
    assert out["home_team"][0] == "LAC"
    assert out["away_team"][0] == "LV"
    assert out["yrdln"][0] == "LAC 49"
    assert out["td_team"][0] == "LV"
    assert out["return_team"][0] == "LAC"
    assert out["penalty_team"][0] == "LV"
    assert out["side_of_field"][0] == "LAC"


def test_team_name_fn_helper_direct() -> None:
    """``team_name_fn`` itself, applied directly to an expression."""
    df = pl.DataFrame({"col": ["JAC 20", "STL", "SL", "LAR", "ARZ", "BLT", "CLV", "HST", "SD", "OAK"]})
    out = df.select(team_name_fn(pl.col("col")).alias("col"))
    assert out["col"].to_list() == ["JAX 20", "LA", "LA", "LA", "ARI", "BAL", "CLE", "HOU", "LAC", "LV"]


def test_team_columns_all_present_in_team_abbr_mapping_source() -> None:
    """Sanity check on the port decision to source team_name_fn's 10 codes
    from datasets.py::team_abbr_mapping rather than a re-hardcoded dict."""
    from sportsdataverse.nfl.datasets import team_abbr_mapping

    expected = {
        "JAC": "JAX",
        "STL": "LA",
        "SL": "LA",
        "LAR": "LA",
        "ARZ": "ARI",
        "BLT": "BAL",
        "CLV": "CLE",
        "HST": "HOU",
        "SD": "LAC",
        "OAK": "LV",
    }
    for code, target in expected.items():
        assert team_abbr_mapping[code] == target


def test_team_columns_list_has_27_entries() -> None:
    assert len(TEAM_COLUMNS) == 27


# ---------------------------------------------------------------------------
# aborted_play
# ---------------------------------------------------------------------------


def test_aborted_play_detected_from_desc() -> None:
    df = pl.DataFrame(
        [
            _row(desc="(15:00) Aborted. G.Minshew FUMBLES at JAX 20", epa=None),
            _row(desc="(15:00) 6-G.Minshew pass incomplete short right to 15-D.Parker"),
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["aborted_play"].to_list() == [1, 0]


def test_aborted_play_rusher_fumble_override() -> None:
    """On an aborted snap with no passer, rusher/rusher_id fall back to
    fumbled_1_player_name/_id -- applied AFTER the custom_mode resolution."""
    df = pl.DataFrame(
        [
            _row(
                desc="(15:00) Aborted. Snap mishandled, RECOVERED by JAX-99",
                play_type="no_play",
                fumbled_1_player_name="C.Cominsky",
                fumbled_1_player_id="00-0099999",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["aborted_play"][0] == 1
    assert out["rusher"][0] == "C.Cominsky"
    assert out["rusher_id"][0] == "00-0099999"


# ---------------------------------------------------------------------------
# empty-frame schema stability
# ---------------------------------------------------------------------------


def test_empty_frame_returns_documented_schema() -> None:
    df = pl.DataFrame(schema={"desc": pl.Utf8, "epa": pl.Float64, "game_id": pl.Utf8})
    out = clean_nfl_pbp(df)
    assert out.height == 0
    for name in _ADDED_SCHEMA:
        assert name in out.columns


def test_empty_frame_return_as_pandas() -> None:
    df = pl.DataFrame(schema={"desc": pl.Utf8, "epa": pl.Float64})
    out = clean_nfl_pbp(df, return_as_pandas=True)
    assert len(out) == 0
    assert "passer" in out.columns


# ---------------------------------------------------------------------------
# pass/rush compute-if-absent scope decision
# ---------------------------------------------------------------------------


def test_pass_rush_computed_when_absent() -> None:
    df = pl.DataFrame([_row()])
    assert "pass" not in df.columns
    out = clean_nfl_pbp(df)
    assert out["pass"][0] == 1
    assert out["rush"][0] == 0


def test_pass_rush_left_untouched_when_present() -> None:
    """A pre-existing pass/rush value is NOT recomputed (scope decision:
    nflverse frames already carry authoritative pass/rush)."""
    df = pl.DataFrame([_row(pass_=0, rush=1)]).rename({"pass_": "pass"})
    out = clean_nfl_pbp(df)
    assert out["pass"][0] == 0
    assert out["rush"][0] == 1


def test_return_as_pandas_flag() -> None:
    df = pl.DataFrame([_row()])
    out = clean_nfl_pbp(df, return_as_pandas=True)
    assert out.__class__.__name__ == "DataFrame"
    assert "passer" in out.columns
    assert out.shape[0] == 1


# ---------------------------------------------------------------------------
# fix_weird_pass_plays override
# ---------------------------------------------------------------------------


def test_fix_weird_pass_plays_forces_zero() -> None:
    df = pl.DataFrame(
        [
            _row(
                game_id="1999_01_ARI_PHI",
                play_id=1611,
                desc="(15:00) 6-G.Minshew pass incomplete short right to 15-D.Parker",
            )
        ]
    )
    out = clean_nfl_pbp(df)
    assert out["pass"][0] == 0
