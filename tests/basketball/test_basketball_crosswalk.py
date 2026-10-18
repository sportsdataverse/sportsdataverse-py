"""Id crosswalks between the basketball families, proven on committed fixtures only (no network, no refs)."""

from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.basketball import basketball_crosswalk as X
from sportsdataverse.basketball.bnxt import bnxt_parsers as BNXT
from sportsdataverse.basketball.cebl import cebl_parsers as CEBL
from sportsdataverse.basketball.dbl import sportality_parsers as SP
from sportsdataverse.basketball.genius import genius_parsers as GEN
from sportsdataverse.basketball.ibba import ibba_parsers as IBBA
from sportsdataverse.basketball.ibba import ibba_segev_parsers as SEGEV
from sportsdataverse.basketball.kzs import kzs_parsers as KZS
from sportsdataverse.basketball.plg import pleague_parsers as PLG
from sportsdataverse.basketball.plg import pleague_preciser_parsers as PRE
from sportsdataverse.basketball.tbf import tbf_parsers as TBF
from sportsdataverse.basketball.tbf import tbf_runtime as TBF_R

ROOT = Path(__file__).parents[2]
FIX = ROOT / "tests" / "fixtures"
sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_pleague  # noqa: E402


def _j(rel: str) -> Any:
    p = FIX / rel
    text = gzip.open(p, "rt", encoding="utf-8").read() if rel.endswith(".gz") else p.read_text(encoding="utf-8")
    return json.loads(text)


def _is_1to1(xw: pl.DataFrame, a: str, b: str) -> bool:
    return xw[a].n_unique() == xw[b].n_unique() == xw.height


# --- Genius match id ------------------------------------------------------------------------------------


def test_bnxt_slider_game_id_to_genius_id():
    slider = BNXT.parse_sportpress_game_slider(_j("sportpress/full/schedule__game-slider__competitionId.json"))
    xw = X.genius_game_crosswalk(slider, "sportpress")
    assert xw.columns == ["game_key", "genius_match_id"] and dict(xw.schema) == {
        "game_key": pl.Int64,
        "genius_match_id": pl.Int64,
    }
    assert xw.height == slider.drop_nulls("genius_id").height == 29 and _is_1to1(xw, "game_key", "genius_match_id")


def test_cebl_games_and_gamelog():
    body = _j("cebl/full/games__season__2025.json")
    games = CEBL.parse_cebl_games({"rows": body, "_request": {"kind": "games", "season": 2025}})
    xw = X.genius_game_crosswalk(games, "cebl")
    assert xw.height == 128 and _is_1to1(xw, "game_key", "genius_match_id")
    assert xw.filter(pl.col("genius_match_id") == 2600563)["game_key"].to_list() == [14708701]  # as in test_cebl


def test_cebl_capture_game_is_the_genius_schedule_game():
    """The one real overlap between a provider fixture and a Genius fixture: CEBL 2026 game 2798717."""
    games = CEBL.parse_cebl_games({"rows": _j("cebl/captures/games__season__2026.json"), "_request": {"season": 2026}})
    sched = GEN.parse_genius_schedule(_j("genius/captures/hosted_schedule_CEBL_47852.json"))
    xw = X.genius_game_crosswalk(games, "cebl")
    assert xw.schema["genius_match_id"] == sched.schema["match_id"] == pl.Int64
    both = xw.join(sched.select("match_id", "home_team_name"), left_on="genius_match_id", right_on="match_id")
    assert both["genius_match_id"].to_list() == [2798717]


def test_tbf_header_genius_id():
    body = _j("tbf/full/header.json")
    body[TBF_R.REQUEST_KEY] = {"match_id": 346279, "home_team_process_id": 265995, "away_team_process_id": 265993}
    xw = X.genius_game_crosswalk(TBF.parse_tbf_game_header(body), "tbf")
    assert xw.to_dicts() == [{"game_key": 346279, "genius_match_id": 2913588}]


def test_kzs_matches_genius_id_and_old_seasons_drop():
    data = _j("kzs/full/matches__579_2025-10.json")["data"]
    data = dict(data) if isinstance(data, dict) else {"items": data}
    data["_request"] = {}
    matches = KZS.parse_kzs_matches(data)
    xw = X.genius_game_crosswalk(matches, "kzs")
    assert xw.height == matches.drop_nulls("genius_match_id").height > 0 and _is_1to1(xw, "game_key", "genius_match_id")


def test_sportality_game_uuid_to_genius_match_id_from_pbp_and_team_stats():
    pbp = SP.parse_sportality_pbp(_j("sportality/full/gameday__play-by-play__c4hh5nig6t.json"))
    xw = X.genius_game_crosswalk(pbp, "sportality")
    assert xw.schema == pl.Schema({"game_key": pl.Utf8, "genius_match_id": pl.Int64}) and xw.height == 1
    ts = SP.parse_sportality_team_stats(_j("sportality/full/gameday__team-stats__c4hh5nig6t.json"))
    assert X.genius_game_crosswalk(ts, "sportality").to_dicts() == xw.to_dicts()  # the two routes agree


def test_dtype_and_shape_guards():
    games = pl.DataFrame({"game_id": [1, 2], "genius_match_id": [10, 10]})
    with pytest.raises(ValueError, match="more than one"):
        X.genius_game_crosswalk(games, "cebl")
    with pytest.raises(TypeError, match="no float or string"):
        X.genius_game_crosswalk(pl.DataFrame({"game_id": [1.0], "genius_match_id": [10]}), "cebl")
    with pytest.raises(TypeError, match="no float or string"):
        X.genius_game_crosswalk(pl.DataFrame({"game_id": [1], "genius_match_id": ["10"]}), "cebl")
    with pytest.raises(ValueError, match="unknown source"):
        X.genius_game_crosswalk(games, "nope")
    with pytest.raises(ValueError, match="no column"):
        X.genius_game_crosswalk(games, "tbf")
    assert X.genius_game_crosswalk(games.head(1), "cebl", return_as_pandas=True).shape == (1, 2)


# --- Genius team id <-> Sportality team uuid -------------------------------------------------------------


def test_sportality_team_uuid_to_genius_team_id():
    fx = FIX / "sportality"
    stats = lambda n: SP.parse_sportality_stats_info(json.loads((fx / "full" / n).read_text(encoding="utf-8")))  # noqa: E731
    teams = X.genius_team_crosswalk(
        stats("statistics-v2__stats-info__teams_summary__qzw34da6jg.json"), "sportality_teams"
    )
    players = X.genius_team_crosswalk(
        stats("statistics-v2__stats-info__players_summary__qzw34da6jg.json"), "sportality_players"
    )
    assert teams.schema == pl.Schema({"team_uuid": pl.Utf8, "genius_team_id": pl.Int64}) and teams.height >= 12
    assert _is_1to1(teams, "team_uuid", "genius_team_id")
    assert set(players["team_uuid"]) <= set(teams["team_uuid"])
    assert set(map(tuple, players.rows())) <= set(map(tuple, teams.rows()))  # both routes name the same pairs
    standings = SP.parse_sportality_standings(json.loads((fx / "gameday__team-standings__ssgt_uuid.json").read_text()))
    st = X.genius_team_crosswalk(standings, "sportality_standings")
    assert st.height == standings.height and (22255 in st["genius_team_id"].to_list())


# --- IBBA <-> Segev ------------------------------------------------------------------------------------


def test_ibba_game_id_to_segev_game_id():
    games = IBBA.parse_ibba_games(_j("ibba/captures/asmx/Games@5_0_0_0_26392_2026.json"))
    xw = X.ibba_segev_game_crosswalk(games)
    assert xw.height >= 1 and xw.filter(pl.col("segev_game_id") == 50).height == 1
    # the Segev side is the game the ibba_segev header answers to: same tip-off, same game id
    info = SEGEV.parse_ibba_segev_game_info(_j("ibba/full/getGameInfo.json")).row(0, named=True)
    row = games.filter(pl.col("segev_game_id") == info["game_id"]).row(0, named=True)
    assert (row["game_id"], info["game_id"], row["tip_off"]) == (26392, 50, info["start_time"].strftime("%H:%M"))
    old = IBBA.parse_ibba_games(_j("ibba/captures/asmx/Games@5_0_0_0_0_2005.json"))
    assert X.ibba_segev_game_crosswalk(old).height == 0  # 2004-05 carries no Segev id


# --- P. LEAGUE+ <-> Preciser -----------------------------------------------------------------------------


def test_pleague_team_table_matches_the_generator_clubs_and_the_standings_logo_uuids():
    xw = X.pleague_team_crosswalk()
    assert xw.schema == pl.Schema({"team_id": pl.Int64, "team_uuid": pl.Utf8}) and xw.height == 8
    assert set(xw.rows()) == {(t, u) for t, u, _zh, _en in gen_pleague.CLUBS}
    html = (FIX / "pleague" / "standings_2022-23.html").read_text(encoding="utf-8")
    st = PLG.parse_pleague_standings(PLG.record_for("standings", html, season="2022-23"))
    assert set(st.select("team_id", "team_uuid").rows()) <= set(xw.rows())


def test_pleague_shot_team_uuids_resolve_to_the_game_headers_site_teams():
    html = (FIX / "pleague" / "game_197.html").read_text(encoding="utf-8")
    game = PLG.parse_pleague_game(PLG.record_for("game", html, gid=197)).row(0, named=True)
    body = _j("pleague_preciser/full/chart_data_197.json")
    shots = PRE.parse_pleague_preciser_shots(
        {"data": body[0], "_request": {"kind": "x", "uuid": game["uuid"], "verified": False, "gid": None}}
    )
    xw = X.pleague_team_crosswalk()
    # the game page names clubs (no ids): gid 197 is Hsinchu (site team 3, away) at Taipei Fubon (team 1, home)
    assert game["away_name_en"] == "Hsinchu Jko Lioneers" and "Fubon" in game["home_name_en"]
    home = xw.filter(pl.col("team_id") == 1)["team_uuid"].item()
    away = xw.filter(pl.col("team_id") == 3)["team_uuid"].item()
    assert shots["home_team_uuid"].unique().to_list() == [home] and shots["away_team_uuid"].unique().to_list() == [away]
    joined = shots.join(xw, on="team_uuid", how="left")
    assert joined["team_id"].null_count() == 0  # every shot's team resolves


def test_an_empty_or_zero_column_frame_gives_an_empty_typed_crosswalk():
    for empty in (pl.DataFrame(), pl.DataFrame(schema={"game_id": pl.Int64, "genius_match_id": pl.Int64})):
        xw = X.genius_game_crosswalk(empty, "cebl")
        assert xw.height == 0 and xw.schema == {"game_key": pl.Int64, "genius_match_id": pl.Int64}
    assert X.ibba_segev_game_crosswalk(pl.DataFrame()).schema == {"game_id": pl.Int64, "segev_game_id": pl.Int64}
    assert X.genius_team_crosswalk(pl.DataFrame(), "sportality_standings").schema == {
        "team_uuid": pl.Utf8,
        "genius_team_id": pl.Int64,
    }


def test_the_helpers_are_exported_from_their_homes():
    from sportsdataverse.basketball import genius, ibba, plg

    assert (
        genius.genius_game_crosswalk is X.genius_game_crosswalk
        and genius.genius_team_crosswalk is X.genius_team_crosswalk
    )
    assert ibba.ibba_segev_game_crosswalk is X.ibba_segev_game_crosswalk
    assert plg.pleague_team_crosswalk is X.pleague_team_crosswalk
