"""Offline structural tests for the stats.ncaa.org college-football pbp parser.

Parses committed real-game fixtures (never synthetic) and asserts the structured
frame: schema stability, empty-input contract, down/distance/yard-line extraction,
play-type classification (0 unknowns), player/direction/yardage lifts, flags, and
the frame-wide ``qb_scramble`` derivation. The full 6-game capture corpus lives in
the ``ncaa-mfb-hoops-raw`` producer repo; three games are vendored here to prove
the parser generalizes across games.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_ncaa_pbp import (
    DRIVE_TITLES_SCHEMA,
    PBP_SCHEMA,
    parse_cfb_ncaa_drive_titles,
    parse_cfb_ncaa_pbp,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_ncaa"
GAME = FIX / "cfb_ncaa_pbp_5362535.html"


def _df() -> pl.DataFrame:
    return parse_cfb_ncaa_pbp(GAME.read_text(encoding="utf-8"), contest_id="5362535")


def test_returns_documented_schema() -> None:
    df = _df()
    assert df.columns == list(PBP_SCHEMA.keys())
    assert df.height > 0


def test_empty_input_is_zero_row_with_schema() -> None:
    df = parse_cfb_ncaa_pbp("")
    assert df.height == 0
    assert df.columns == list(PBP_SCHEMA.keys())


def test_down_distance_yardline_extracted() -> None:
    df = _df()
    dd = df.filter(pl.col("down").is_not_null())
    assert dd.height > 0
    assert dd.get_column("down").is_between(1, 4).all()
    assert dd.get_column("yard_line").str.contains(r"^[A-Z]{1,4}\d+$").all()


def test_scoring_drive_flagged() -> None:
    df = _df()
    af = df.filter(pl.col("offense") == "Air Force")
    assert af.height > 0
    assert af.get_column("drive_scored").any()
    assert "TD" in af.get_column("drive_result").to_list()


def test_play_text_preserved() -> None:
    df = _df()
    hit = df.filter(pl.col("play_text").str.contains("Anthony,Malakai rush left"))
    assert hit.height >= 1
    assert hit.get_column("down").to_list()[0] == 1


def test_contest_id_stamped() -> None:
    df = _df()
    assert (df.get_column("contest_id") == "5362535").all()


# --- play_text decomposition ----------------------------------------------


def test_every_play_is_classified() -> None:
    df = _df()
    assert df.filter(pl.col("play_type").is_null()).height == 0
    assert df.filter(pl.col("play_type") == "unknown").height == 0
    kinds = set(df.get_column("play_type").unique().to_list())
    assert {"rush", "pass", "punt", "kickoff", "field_goal"} <= kinds


def test_rush_fields_extracted() -> None:
    df = _df().filter(pl.col("play_type") == "rush")
    assert df.get_column("rusher").is_not_null().all()
    assert df.get_column("yards_gained").is_not_null().all()
    assert df.filter(pl.col("rusher") == "Corbett,Jermaine").get_column("yards_gained").min() < 0


def test_pass_fields_extracted() -> None:
    df = _df().filter(pl.col("play_type") == "pass")
    assert df.get_column("passer").is_not_null().all()
    comp = df.filter(pl.col("pass_complete") == True)  # noqa: E712
    assert comp.get_column("receiver").is_not_null().all()
    assert comp.get_column("yards_gained").is_not_null().all()
    inc = df.filter(pl.col("pass_complete") == False)  # noqa: E712
    assert (inc.get_column("yards_gained") == 0).all()


def test_special_teams_players() -> None:
    df = _df()
    assert df.filter(pl.col("play_type") == "punt").get_column("punter").is_not_null().all()
    assert df.filter(pl.col("play_type") == "kickoff").get_column("kicker").is_not_null().all()


def test_markers_flagged_not_plays() -> None:
    df = _df()
    markers = {"drive_start", "timeout", "period_marker", "coin_toss"}
    assert df.filter(pl.col("play_type").is_in(list(markers))).height > 0


# --- comprehensive field extraction ---------------------------------------


def test_yard_line_split_full_coverage() -> None:
    df = _df()
    yl = df.filter(pl.col("yard_line").is_not_null())
    assert yl.get_column("yard_line_side").is_not_null().all()
    assert yl.get_column("yard_line_number").is_not_null().all()


def test_directions_extracted() -> None:
    df = _df()
    assert df.filter(pl.col("play_type") == "rush").get_column("run_direction").is_not_null().all()
    assert df.filter(pl.col("play_type") == "pass").get_column("pass_direction").is_not_null().all()


def test_flags_present_and_true_somewhere() -> None:
    df = _df()
    for flag in (
        "is_first_down",
        "is_touchdown",
        "is_turnover",
        "out_of_bounds",
        "no_play",
        "fair_catch",
        "penalty_flag",
    ):
        assert df.filter(pl.col(flag) == True).height > 0, flag  # noqa: E712


def test_assisted_tackle_split() -> None:
    df = _df()
    row = df.filter((pl.col("tackler_1") == "Santiago,David") & (pl.col("tackler_2") == "Zdroik,Payton"))
    assert row.height >= 1


def test_penalty_fully_parsed() -> None:
    df = _df().filter(pl.col("penalty_type").is_not_null())
    assert df.height > 0
    assert df.get_column("penalty_team").is_not_null().all()
    assert df.get_column("penalty_yards").is_not_null().all()
    assert "Wilborn Jr.,James" in df.get_column("penalty_player").drop_nulls().to_list()


def test_special_teams_yardage() -> None:
    df = _df()
    assert df.filter(pl.col("play_type") == "kickoff").get_column("kick_yards").is_not_null().all()
    assert df.filter(pl.col("play_type") == "punt").get_column("punt_yards").is_not_null().all()
    fg = df.filter(pl.col("play_type") == "field_goal")
    assert fg.get_column("fg_distance").is_not_null().all()
    assert fg.get_column("fg_made").is_not_null().all()


def test_touchdown_runs_flagged() -> None:
    df = _df()
    td = df.filter(pl.col("is_touchdown") == True)  # noqa: E712
    assert td.height >= 1
    assert td.get_column("end_yard_line").str.contains("00").any()


def test_qb_scramble_derived() -> None:
    df = _df()
    assert df.filter(pl.col("play_type") != "rush").get_column("qb_scramble").is_null().all()
    assert df.filter(pl.col("play_type") == "rush").get_column("qb_scramble").is_not_null().all()
    qbs = set(df.filter(pl.col("passer").is_not_null()).get_column("passer").to_list())
    flagged = df.filter(pl.col("qb_scramble") == True)  # noqa: E712
    assert set(flagged.get_column("rusher").to_list()) <= qbs


def test_return_as_pandas() -> None:
    import pandas as pd

    df = parse_cfb_ncaa_pbp(GAME.read_text(encoding="utf-8"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(PBP_SCHEMA.keys())


def test_parser_generalizes_across_fixtures() -> None:
    files = sorted(FIX.glob("cfb_ncaa_pbp_*.html"))
    assert len(files) >= 3, "need multiple captured games to test generalization"
    for f in files:
        d = parse_cfb_ncaa_pbp(f.read_text(encoding="utf-8"))
        assert d.height > 50, f.name  # a full game
        assert d.filter(pl.col("play_type") == "unknown").height == 0, f.name
        # a rush naming an individual carrier must resolve a rusher; team-credited
        # rushes ("Akron rush ... End Of Play") legitimately don't.
        bad = d.filter(
            (pl.col("play_type") == "rush")
            & pl.col("rusher").is_null()
            & pl.col("play_text").str.contains(r",[A-Z][\w.'\-]+ rush")
        )
        assert bad.height == 0, f.name


# --- 2025-season page variants (FCS/G5 captures, 2026-08-19) ---------------
# Regression fixtures from the 1,685-game 2025 sweep in ncaa-mfb-football-raw:
#   6386335 Tulsa @ East Carolina -- multi-word team + a drive title with NO result token
#   6386574 Rice @ South Fla.     -- mixed-case yard-line side code ("Ric25")


def _variant(cid: str) -> str:
    return (FIX / f"mfb_play_by_play_{cid}.html").read_text(encoding="utf-8")


def test_multiword_team_not_truncated_when_result_missing() -> None:
    # lazy `.+?` + mandatory result used to donate "Carolina" to result -> offense "East"
    df = parse_cfb_ncaa_pbp(_variant("6386335"), contest_id="6386335")
    assert set(df.get_column("offense").unique().to_list()) == {"Tulsa", "East Carolina"}


def test_mixed_case_side_code_parses() -> None:
    # [A-Z]-only side codes nulled every Rice drive + yard line
    df = parse_cfb_ncaa_pbp(_variant("6386574"), contest_id="6386574")
    assert set(df.get_column("offense").unique().to_list()) == {"Rice", "South Fla."}
    assert df.get_column("yard_line_side").null_count() == 0
    assert "Ric" in df.get_column("yard_line_side").unique().to_list()
    assert df.filter(pl.col("end_yard_line").str.starts_with("Ric")).height > 0


def test_drive_titles_schema_and_checkpoints() -> None:
    df = parse_cfb_ncaa_drive_titles(_variant("6386335"), contest_id="6386335")
    assert df.columns == list(DRIVE_TITLES_SCHEMA.keys())
    assert df.get_column("drive_number").to_list() == list(range(1, df.height + 1))
    assert set(df.get_column("team").unique().to_list()) == {"Tulsa", "East Carolina"}
    assert df.get_column("contest_id").unique().to_list() == ["6386335"]
    # every title parsed: checkpoints + drive stats populated on every row
    for c in ("start_clock", "start_yard_line", "n_plays", "yards", "top", "score_away", "score_home"):
        assert df.get_column(c).null_count() == 0, c
    # the missing-result variant: one drive has result null but team intact
    no_result = df.filter(pl.col("result").is_null())
    assert no_result.height == 1 and no_result.item(0, "team") == "East Carolina"
    # running score is a monotone checkpoint ending at the final (27-41)
    total = df.get_column("score_away") + df.get_column("score_home")
    assert (total.diff().fill_null(0) >= 0).all()
    assert (df.item(-1, "score_away"), df.item(-1, "score_home")) == (27, 41)
    # drive numbering aligns with the play-level frame
    pbp = parse_cfb_ncaa_pbp(_variant("6386335"))
    assert df.height == pbp.get_column("drive_number").max()


def test_drive_titles_mixed_case_side_and_pandas() -> None:
    import pandas as pd

    df = parse_cfb_ncaa_drive_titles(_variant("6386574"))
    assert df.filter(pl.col("start_yard_line").str.starts_with("Ric")).height > 0
    pdf = parse_cfb_ncaa_drive_titles(_variant("6386574"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and list(pdf.columns) == list(DRIVE_TITLES_SCHEMA.keys())


def test_drive_titles_empty() -> None:
    df = parse_cfb_ncaa_drive_titles("")
    assert df.height == 0 and df.columns == list(DRIVE_TITLES_SCHEMA.keys())
