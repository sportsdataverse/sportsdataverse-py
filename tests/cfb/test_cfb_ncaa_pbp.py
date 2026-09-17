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


def test_drive_titles_nickname_side_codes() -> None:
    """Some pages use team NICKNAMES as yard-line side codes (up to 8 letters)."""
    html = (
        '<div class="drives">'
        '<h5 class="non_scoring_play">Morgan St. FUMB 15:00,BEARS38, 2 plays, -7 yards, 1:29 0 - 0</h5>'
        '<h5 class="non_scoring_play">Norfolk St. FG 13:31,SPARTANS25, 16 plays, 3 yards, 8:10 3 - 3</h5>'
        "</div>"
    )
    df = parse_cfb_ncaa_drive_titles(html)
    assert df.get_column("team").to_list() == ["Morgan St.", "Norfolk St."]
    assert df.get_column("start_yard_line").to_list() == ["BEARS38", "SPARTANS25"]
    assert df.get_column("yards").to_list() == [-7, 3]


# --- yardage in 2019-era text ---------------------------------------------
# 1735106 Villanova @ Colgate (2019) -- "for loss of N yards", a fumble advance
#   whose later "for 1 yard" clause is not the play's gain, "to the 50 yardline"
# 6386303 New Haven @ WSTCNN (2025) -- sacks + kneels also read "for loss of N yards"


def test_loss_of_yardage_is_negative() -> None:
    """'rush/sacked/kneel ... for loss of N yards' is -N (2019 text and 2025 sacks/kneels)."""
    for cid in ("1735106", "6386303"):
        df = parse_cfb_ncaa_pbp(_variant(cid), contest_id=cid)
        loss = df.filter(
            pl.col("play_type").is_in(["rush", "sack", "kneel"])
            & pl.col("play_text").str.contains(r"for loss of \d+ yard")
        )
        assert loss.height >= 5, cid
        want = (-loss.get_column("play_text").str.extract(r"for loss of (\d+) yard", 1).cast(pl.Int64)).to_list()
        assert loss.get_column("yards_gained").to_list() == want, cid


def test_fumble_advance_keeps_the_play_clause_yardage() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735106"))
    row = df.filter(pl.col("play_text").str.starts_with("Smith, Danny rush for loss of 4 yards to the VU34, fumble"))
    assert row.height == 1
    assert row.item(0, "yards_gained") == -4


def test_midfield_end_yard_line() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735106"))
    mid = df.filter(pl.col("play_text").str.contains("to the 50 yardline"))
    assert mid.height == 3
    assert mid.get_column("end_yard_line").to_list() == ["50", "50", "50"]


# --- side codes with digits / hyphens -------------------------------------
# 1736435 SFA @ Lamar University (2019) -- side code "SFA2": "SFA225" is SFA2 + 25
# 1735539 Shorter @ ETSU (2019)         -- side code "SU-ETSU" (hyphen)


def test_digit_side_code_split_from_yard_number() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1736435"))
    yl = df.filter(pl.col("yard_line").is_not_null())
    assert set(yl.get_column("yard_line_side").unique().to_list()) == {"SFA2", "LU"}
    assert yl.get_column("yard_line_number").is_between(0, 50).all()
    row = df.filter(pl.col("yard_line") == "SFA225").row(0, named=True)
    assert (row["yard_line_side"], row["yard_line_number"]) == ("SFA2", 25)


def test_hyphenated_side_code_parses() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735539"))
    assert set(df.get_column("offense").unique().to_list()) == {"Shorter", "ETSU"}
    snaps = df.filter(pl.col("play_type").is_in(["rush", "pass", "sack"]))
    assert snaps.get_column("yard_line").null_count() == 0
    assert set(snaps.get_column("yard_line_side").unique().to_list()) == {"SU-ETSU", "ETSU"}
    titles = parse_cfb_ncaa_drive_titles(_variant("1735539"))
    assert titles.get_column("team").null_count() == 0
    assert titles.filter(pl.col("start_yard_line") == "SU-ETSU25").height > 0


def test_pass_result_does_not_depend_on_the_passer_name_format() -> None:
    """2019 pages print "First Last" names the "Last,First" pattern cannot match; a completion is still one."""
    df = parse_cfb_ncaa_pbp(_variant("1735539"))
    passes = df.filter(pl.col("play_type") == "pass")
    comp = passes.filter(pl.col("play_text").str.contains(" pass complete"))
    assert comp.height >= 10
    assert comp.get_column("pass_complete").to_list() == [True] * comp.height
    assert comp.get_column("yards_gained").null_count() == 0
    inc = passes.filter(pl.col("play_text").str.contains(" pass incomplete"))
    assert inc.height > 0 and inc.get_column("pass_complete").to_list() == [False] * inc.height


# --- NC5: replay reviews reprint the ORIGINAL call ------------------------
# 6389205 Minnesota @ Ohio St. (2025): "PLAY OVERTURNED. (Original Play: ... TOUCHDOWN ...)"
# 6386512 Houston @ Oregon St. (2025): "PLAY STANDS." after an own recovery


def test_review_reprint_feeds_no_flag_but_play_text_keeps_it() -> None:
    df = parse_cfb_ncaa_pbp(_variant("6389205"))
    reviewed = df.filter(pl.col("play_text").str.contains("Original Play:"))
    assert reviewed.height >= 3
    # the overturned call carried TOUCHDOWN / a goal-line spot / a tackle; the ruling did not
    short = reviewed.filter(
        pl.col("play_text").str.starts_with("Donaldson Jr,CJ rush left for 1 yard gain to the MINN01")
    )
    assert short.height == 1
    r = short.row(0, named=True)
    assert (r["is_touchdown"], r["yards_gained"], r["end_yard_line"], r["tackler_1"]) == (
        False,
        1,
        "MINN01",
        "Roberson,Jeff",
    )
    assert "Original Play:" in r["play_text"]
    over = reviewed.filter(
        pl.col("play_text").str.starts_with("Shotgun Jackson,Bo rush middle for 5 yards gain to the MINN00 TOUCHDOWN")
    )
    assert over.row(0, named=True)["tackler_1"] is None  # the tackle belongs to the overturned call
    assert over.row(0, named=True)["is_touchdown"] is True
    stands = parse_cfb_ncaa_pbp(_variant("6386512")).filter(pl.col("play_text").str.contains("PLAY STANDS"))
    assert stands.height >= 1 and stands.get_column("is_first_down").all()


# --- NC3 / NC4: 2019-era "First Last" names and damaged tackler separators ----
# 1735890 LSU @ Vanderbilt (2019): "SH," formation prefix, "First Last" names everywhere,
#   "C. Ed.-Helaire", tacklers glued with no separator ("Kristian FultonJaCoby Stevens")
# 1735120 Wagner @ UConn (2019): "LAST, First" with a space, tacklers joined by a mangled
#   ":" ("MORGAN, D.J.3aPAUL, Keyshawn")


def test_first_last_names_are_extracted() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735890"))
    rush = df.filter(pl.col("play_type") == "rush")
    assert rush.height > 40
    assert rush.get_column("rusher").null_count() == 0
    assert "KeShawn Vaughn" in rush.get_column("rusher").to_list()
    assert "C. Ed.-Helaire" in rush.get_column("rusher").to_list()
    assert rush.filter(pl.col("play_text").str.starts_with("SH, ")).get_column("formation").unique().to_list() == [
        "SH,"
    ]
    passes = df.filter(pl.col("play_type") == "pass")
    assert passes.get_column("passer").null_count() == 0
    comp = passes.filter(pl.col("pass_complete") == True)  # noqa: E712
    assert comp.get_column("receiver").null_count() == 0
    assert {"Joe Burrow", "Riley Neal"} <= set(passes.get_column("passer").unique().to_list())
    ko = df.filter(pl.col("play_type") == "kickoff")
    assert ko.get_column("kicker").null_count() == 0
    assert "C. Ed.-Helaire" in ko.get_column("returner").to_list()
    punts = df.filter(pl.col("play_type") == "punt")
    assert punts.filter(pl.col("punter").is_null()).get_column("play_text").str.contains("punt BLOCKED").all()
    assert punts.get_column("punter").null_count() < punts.height
    sacks = df.filter(pl.col("play_type") == "sack")
    assert sacks.height > 0 and sacks.get_column("passer").null_count() == 0


def test_last_first_with_space_and_initials() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735120"))
    assert "BEAUDRY, Mike" in df.get_column("passer").to_list()
    assert "DRAYTON, Matt" in df.get_column("receiver").to_list()  # sentence period dropped
    assert "McKENZIE, D" in df.get_column("rusher").to_list()  # a bare initial
    assert (
        df.filter(pl.col("play_type").is_in(["rush", "pass"])).get_column("rusher").null_count()
        < df.filter(pl.col("play_type") == "pass").height + 3
    )


def test_tacklers_split_on_the_mangled_separator_or_left_null() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735120"))
    row = df.filter(pl.col("play_text").str.contains("MORGAN, D.J.3aPAUL, Keyshawn")).row(0, named=True)
    assert (row["tackler_1"], row["tackler_2"]) == ("MORGAN, D.J.", "PAUL, Keyshawn")
    assert not df.get_column("tackler_1").fill_null("").str.contains("3a").any()
    df = parse_cfb_ncaa_pbp(_variant("1735890"))
    single = df.filter(pl.col("play_text").str.ends_with("(Kristian Fulton).")).row(0, named=True)
    assert (single["tackler_1"], single["tackler_2"]) == ("Kristian Fulton", None)
    glued = df.filter(pl.col("play_text").str.contains("Kristian FultonJaCoby Stevens")).row(0, named=True)
    assert (glued["tackler_1"], glued["tackler_2"]) == (None, None)  # separator lost: not recoverable


# --- NC2: touchdown and PAT printed in one row (1735120) -----------------------


def test_td_and_pat_in_one_row_is_the_scoring_play() -> None:
    df = parse_cfb_ncaa_pbp(_variant("1735120"))
    both = df.filter(pl.col("play_text").str.contains("TOUCHDOWN") & pl.col("play_text").str.contains("kick attempt"))
    assert both.height == 6
    assert set(both.get_column("play_type").to_list()) == {"rush", "pass"}
    assert both.get_column("is_touchdown").all()
    row = both.filter(pl.col("play_text").str.starts_with("BEAUDRY, Mike rush for 2 yards")).row(0, named=True)
    assert (row["rusher"], row["yards_gained"], row["end_yard_line"]) == ("BEAUDRY, Mike", 2, "WAGNER0")


# --- NC1: a recovered fumble is a turnover only when the OTHER team recovered ---
# 6386512 Houston @ Oregon St. (2025): own recoveries ("... recovered by OSU ..." in an
#   Oregon St. drive); 6386493 Western Ky. @ LSU (2025): one own, one by the defense


def test_own_recovery_is_not_a_turnover() -> None:
    df = parse_cfb_ncaa_pbp(_variant("6386512"))
    own = df.filter(
        pl.col("play_text").str.contains("fumbled by")
        & pl.col("play_text").str.contains("recovered by OSU")
        & (pl.col("offense") == "Oregon St.")
    )
    assert own.height >= 2
    assert own.get_column("is_fumble").all()
    assert not own.get_column("is_turnover").any()
    assert own.get_column("turnover_type").null_count() == own.height
    df = parse_cfb_ncaa_pbp(_variant("6386493"))
    lsu = df.filter(pl.col("play_text").str.contains("recovered by LSU Van Buren")).row(0, named=True)
    wku = df.filter(pl.col("play_text").str.contains("recovered by WKU Flowers")).row(0, named=True)
    assert (lsu["offense"], lsu["is_turnover"], lsu["turnover_type"]) == ("LSU", False, None)
    assert (wku["offense"], wku["is_turnover"], wku["turnover_type"]) == ("LSU", True, "fumble")
