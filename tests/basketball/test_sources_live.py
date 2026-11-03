"""cdn.nba.com / cdn.wnba.com live adapter (BB-D13) against ESPN on the same real games.

Fixtures (provenance: tests/basketball/fixtures/live_adapter/README.md): the liveData captures already in
tests/{nba,wnba}/fixtures/*_live/ plus the NBA Cup final, ESPN's ``espn_<lg>_pbp(raw=True)`` dict for each
of the three games captured the same day, and the BB-D14 id-map rows those games need. Every check runs
the adapted summary through the unmodified ``helper_<lg>_pbp`` + ``game_page_frames`` and compares with
ESPN's own summary of the game.
"""

from __future__ import annotations

import copy
import fnmatch
import gzip
import json
import sys
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.basketball.sources import dispatch
from sportsdataverse.basketball.sources.contract import KNOWN_LOSSY, _validate_summary
from sportsdataverse.basketball.sources.dispatch import _process_game
from sportsdataverse.basketball.sources.idmap import IDMAP_SCHEMA
from sportsdataverse.nba.live_pbp import to_espn_summary as live
from sportsdataverse.nba.nba_game_page import game_page_frames, league_pbp_functions
from tests.conftest import skip_if_no_live

TESTS = Path(__file__).resolve().parents[1]
FIX = TESTS / "basketball" / "fixtures" / "live_adapter"

#: (league, ESPN id, liveData id, liveData playbyplay path, liveData boxscore path)
GAMES = (
    (
        "nba",
        401809243,
        "0022500001",
        TESTS / "nba/fixtures/nba_live/playbyplay_0022500001.json",
        TESTS / "nba/fixtures/nba_live/boxscore_0022500001.json",
    ),
    (
        "wnba",
        401856986,
        "1022600097",
        TESTS / "wnba/fixtures/wnba_live/playbyplay_1022600097.json",
        TESTS / "wnba/fixtures/wnba_live/boxscore_1022600097.json",
    ),
    ("nba", 401809839, "0062500001", FIX / "playbyplay_0062500001.json.gz", FIX / "boxscore_0062500001.json.gz"),
)
IDS = [f"{g[0]}_{g[1]}" for g in GAMES]

#: Shot coordinates are not lost: liveData is finer (tenths of a foot) than ESPN's whole feet.
COORDINATE_FEET = {"coordinate_x_raw": 1.0, "coordinate_y_raw": 1.0, "coordinate_x": 1.0, "coordinate_y": 1.0}
COORDINATE_TENTHS = {"x_legacy": 10.0, "y_legacy": 10.0}


def _json(path: Path) -> dict:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _idmap() -> pl.DataFrame:
    return pl.DataFrame(_json(FIX / "idmap_rows.json"), schema=IDMAP_SCHEMA)


def _payload(game: tuple) -> dict:
    return {"playbyplay": _json(game[3]), "boxscore": _json(game[4]), "idmap": _idmap()}


def _espn(game: tuple) -> dict:
    return _json(FIX / f"espn_{game[0]}_{game[1]}.json.gz")


def _adapt(game: tuple) -> dispatch.AdaptedGame:
    return live._live_adapter(game[0], game[1], dispatch.SourceContext(payload=_payload(game)))


@lru_cache(maxsize=None)
def _frames(i: int) -> tuple[dict, dict, dict, dict]:
    """(live final, live frames, ESPN final, ESPN frames) for GAMES[i]."""
    game = GAMES[i]
    _, parse = league_pbp_functions(game[0])
    lf = parse(game[1], copy.deepcopy(_adapt(game).summary))
    ef = parse(game[1], copy.deepcopy(_espn(game)))
    return lf, game_page_frames(lf, league=game[0]), ef, game_page_frames(ef, league=game[0])


def _pair_frames(lfr: dict, efr: dict, key: str, on: str) -> pl.DataFrame:
    left, right = lfr[key], efr[key]
    if on == "athlete_id":
        left, right = left.drop_nulls(on), right.drop_nulls(on)
    assert left.schema[on] == right.schema[on]
    return left.join(right, on=on, how="inner", suffix="_espn")


def _joined(i: int, key: str = "pbp", on: str = "id") -> pl.DataFrame:
    _, lfr, _, efr = _frames(i)
    return _pair_frames(lfr, efr, key, on)


@pytest.mark.parametrize("game", GAMES, ids=IDS)
def test_adapted_summary_passes_the_contract(game: tuple) -> None:
    adapted = _adapt(game)
    report = _validate_summary(adapted.summary, game[0])
    assert report.ok and report.gop_ok, report.summary()
    assert report.n_plays > 350
    assert all("late inserts" in w for w in report.warnings)  # ESPN's own feed warns the same way
    assert adapted.native_ids == {"espn_event_id": str(game[1]), "live_game_id": game[2]}
    # every liveData play type in these games has a measured ESPN type
    assert not [n for n in adapted.notes if "play types" in n]


def test_every_type_the_tables_emit_has_an_espn_id() -> None:
    targets = {*live._SHOT_TYPES.values(), *live._TYPES.values(), *live._FAMILY.values()}
    assert targets <= set(live.ESPN_TYPE_IDS)


@pytest.mark.parametrize("i", range(len(GAMES)), ids=IDS)
def test_play_ids_are_espns(i: int) -> None:
    """ESPN's sequenceNumber IS liveData's actionNumber: the adapter emits ESPN's own play ids."""
    _, lfr, _, efr = _frames(i)
    ours, theirs = set(lfr["pbp"]["id"]), set(efr["pbp"]["id"])
    assert len(ours & theirs) / len(theirs) > 0.98, (len(ours), len(theirs), len(ours & theirs))


@pytest.mark.parametrize("i", range(len(GAMES)), ids=IDS)
def test_score_after_every_period_and_the_final_match_espn(i: int) -> None:
    lf, lfr, ef, efr = _frames(i)

    def by_period(pbp: pl.DataFrame) -> list[tuple]:
        return pbp.group_by("period_number", maintain_order=True).agg(pl.col("home_score", "away_score").last()).rows()

    assert by_period(lfr["pbp"]) == by_period(efr["pbp"])
    assert lfr["periods"] == efr["periods"]

    def final(f: dict) -> dict:
        return {c["homeAway"]: (c["team"]["id"], c["score"]) for c in f["header"]["competitions"][0]["competitors"]}

    assert final(lf) == final(ef)


@pytest.mark.parametrize("i", range(len(GAMES)), ids=IDS)
def test_scoring_and_clock_columns_match_espn_row_for_row(i: int) -> None:
    """Clock ("MM:SS" / "SS.t", the opening tip at the period start), period, scores and scoring flags."""
    j = _joined(i)
    for col in ("clock_display_value", "period_number", "home_score", "away_score", "scoring_play", "score_value"):
        bad = j.filter(pl.col(col).ne_missing(pl.col(f"{col}_espn")))
        assert bad.is_empty(), (col, bad.select("id", col, f"{col}_espn").head(3).rows())


@pytest.mark.parametrize("i", range(len(GAMES)), ids=IDS)
def test_shot_coordinates_share_espns_sign_and_frame(i: int) -> None:
    """``x = 25 + xLegacy / 10``, ``y = yLegacy / 10``: same side of the rim on every shot, ESPN's whole-foot
    rounding on all but the NBA's re-located ones (401809243 #209: liveData moved a layup 4 ft, ESPN did not)."""
    j = _joined(i).filter(pl.col("x_legacy").is_not_null() & pl.col("x_legacy_espn").is_not_null())
    assert j.height > 120
    for col, tol in {**COORDINATE_FEET, **COORDINATE_TENTHS}.items():
        near = j.filter((pl.col(col) - pl.col(f"{col}_espn")).abs() <= tol).height
        assert near >= j.height - 1, (col, j.height - near)
    # the sign: every shot more than 2 ft off the rim's centre line is on the same side in both feeds
    off = j.filter((pl.col("x_legacy").abs() > 20) & (pl.col("x_legacy_espn").abs() > 20))
    assert off.height > 60
    assert off.filter(pl.col("x_legacy").sign() != pl.col("x_legacy_espn").sign()).is_empty()
    # the same shots are located in both feeds (free throws and non-shots stay unlocated), except the
    # end-of-period heave: liveData logs it to the team with no shooter and no location
    _, lfr, _, efr = _frames(i)
    located = [f["pbp"].filter(pl.col("x_legacy").is_not_null()) for f in (lfr, efr)]
    assert set(located[0]["id"]) <= set(located[1]["id"])
    missed = located[1].filter(~pl.col("id").is_in(located[0]["id"].implode()))
    assert set(missed["type_text"]) <= {"Heave Jump Shot"}


def test_free_throw_and_technical_order_matches_espn() -> None:
    """WNBA 401856986: three technicals, each followed by its free throw and then the substitutions."""
    i = IDS.index("wnba_401856986")
    _, lfr, _, efr = _frames(i)

    def sequence(pbp: pl.DataFrame) -> list[tuple]:
        ft_or_tech = pl.col("type_text").str.contains("(?i)free throw|technical")
        return pbp.filter(ft_or_tech).select("id", "type_text").rows()

    ours, theirs = sequence(lfr["pbp"]), sequence(efr["pbp"])
    assert sum("Technical" in t for _, t in ours) == 6  # three technical fouls + three technical free throws
    assert ours == theirs


@pytest.mark.parametrize("i", range(len(GAMES)), ids=IDS)
def test_team_and_player_ids_are_espns(i: int) -> None:
    lf, lfr, ef, efr = _frames(i)
    teams = {c["team"]["id"] for c in ef["header"]["competitions"][0]["competitors"]}
    assert {c["team"]["id"] for c in lf["header"]["competitions"][0]["competitors"]} == teams
    assert set(lfr["pbp"]["team_id"].drop_nulls().cast(pl.Utf8)) <= teams
    # every mapped liveData player is an ESPN athlete of this game, with ESPN's points and minutes
    ours, theirs = (f["player_box"].drop_nulls("athlete_id") for f in (lfr, efr))
    assert set(ours["athlete_id"]) <= set(theirs["athlete_id"])
    j = _joined(i, "player_box", "athlete_id")
    assert j.height >= 20
    for col in ("points", "minutes", "team_id"):
        assert j.filter(pl.col(col).ne_missing(pl.col(f"{col}_espn"))).is_empty(), col
    shooters = _joined(i).filter(pl.col("shooting_play") == True)
    agree = shooters.filter(pl.col("athlete_id_1") == pl.col("athlete_id_1_espn")).height
    assert agree / shooters.height > 0.98
    # the folded rows: the assister / blocker / stealer / incoming substitute is ESPN's second participant
    second = _joined(i).filter(pl.col("athlete_id_2_espn").is_not_null())
    assert second.height > 100
    assert second.filter(pl.col("athlete_id_2") == pl.col("athlete_id_2_espn")).height / second.height > 0.95


@pytest.mark.parametrize("i", range(len(GAMES)), ids=IDS)
def test_team_box_totals_match_espn(i: int) -> None:
    j = _joined(i, "team_box", "team_id")
    assert j.height == 2
    cols = [c for c in j.columns if c.endswith(("_made", "_attempted", "_pct", "rebounds", "assists", "turnovers"))]
    cols = [c for c in cols if f"{c}_espn" in j.columns and not c.endswith("_espn")]
    assert len(cols) >= 12
    for col in cols + ["team_score", "steals", "blocks", "fouls", "technical_fouls", "turnover_points"]:
        assert j.filter(pl.col(col).ne_missing(pl.col(f"{col}_espn"))).is_empty(), col


def _measured_lossy(games: Iterable[tuple[dict, dict]]) -> set[str]:
    """Every game-page column or key whose live value differs from ESPN's on any ``(live, ESPN)`` frames pair."""
    lossy: set[str] = set()
    for lfr, efr in games:
        for key, on in (("pbp", "id"), ("team_box", "team_id"), ("player_box", "athlete_id")):
            j = _pair_frames(lfr, efr, key, on)
            lossy |= set(efr[key].columns) - set(lfr[key].columns)
            for col in lfr[key].columns:
                if col == on or f"{col}_espn" not in j.columns or col in {**COORDINATE_FEET, **COORDINATE_TENTHS}:
                    continue
                if j.select(pl.col(col).cast(pl.Utf8).ne_missing(pl.col(f"{col}_espn").cast(pl.Utf8)).any()).item():
                    lossy.add(col)
        lossy |= {k for k in ("prior", "neutral_site", "regulation_periods", "periods") if lfr[k] != efr[k]}
    return lossy


def test_known_lossy_covers_every_gap_on_these_games() -> None:
    """Every gap measured on these three games is in KNOWN_LOSSY; ``test_sources_parity`` checks the converse
    (each pattern matches a gap) on the 27-game BB-D15 corpus, which these games are part of."""
    measured = _measured_lossy((_frames(i)[1], _frames(i)[3]) for i in range(len(GAMES)))
    for lg in ("nba", "wnba"):
        patterns = KNOWN_LOSSY[(lg, "live")]
        assert {c for c in measured if not any(fnmatch.fnmatch(c, p) for p in patterns)} == set(), lg


def test_neutral_site_is_the_measured_gap() -> None:
    """The NBA Cup final (Las Vegas): ESPN says neutral, liveData cannot, so the prior keeps home court."""
    _, lfr, _, efr = _frames(IDS.index("nba_401809839"))
    assert efr["neutral_site"] is True and lfr["neutral_site"] is False


@pytest.mark.parametrize("game", GAMES[:2], ids=IDS[:2])
def test_live_through_dispatch_once_it_is_selectable(monkeypatch: pytest.MonkeyPatch, game: tuple) -> None:
    lg = game[0]
    monkeypatch.setitem(dispatch.SOURCE_ORDER, lg, ("espn", "live"))
    out = _process_game(lg, game[1], source="live", payloads={"live": _payload(game)})
    prov = out.provenance
    assert prov["served"] == "live" and prov["fallback"] is False and out.health == {"live": "ok"}
    assert prov["lossy_columns"] == list(KNOWN_LOSSY[(lg, "live")])
    assert prov["native_ids"]["live_game_id"] == game[2] and prov["contract"]["ok"]
    assert out.frames["pbp"].height > 350 and out.frames["player_box"].height >= 20


@pytest.mark.parametrize("lg", ["nba", "wnba"])
def test_live_is_registered_and_selectable(monkeypatch: pytest.MonkeyPatch, lg: str) -> None:
    """Dispatch imports the adapter on first use; "live" joined SOURCE_ORDER after BB-D15's parity gates."""
    monkeypatch.setattr(dispatch, "_ADAPTERS", {})
    monkeypatch.delitem(sys.modules, live.__name__)  # a fresh import registers into the fresh registry
    adapter = dispatch._adapter_for(lg, "live")
    assert adapter is not None and adapter.__qualname__ == "_live_adapter"
    assert dispatch.SOURCE_ORDER[lg] == ("espn", "live")
    assert dispatch._adapter_for("mbb", "live") is None


def test_no_id_map_falls_through_to_espn(monkeypatch: pytest.MonkeyPatch) -> None:
    game = GAMES[0]
    monkeypatch.setitem(dispatch.SOURCE_ORDER, "nba", ("espn", "live"))
    monkeypatch.delenv("SDV_DATA_API_URL", raising=False)
    payloads = {"live": {k: v for k, v in _payload(game).items() if k != "idmap"}, "espn": _espn(game)}
    out = _process_game("nba", game[1], source="live", payloads=payloads)
    assert out.provenance["served"] == "espn"
    assert "no id map" in out.health["live"]


def test_a_game_or_team_missing_from_the_id_map_falls_through(monkeypatch: pytest.MonkeyPatch) -> None:
    game = GAMES[0]
    monkeypatch.setitem(dispatch.SOURCE_ORDER, "nba", ("espn", "live"))
    no_game = _idmap().filter(pl.col("kind") != "game")
    out = _process_game(
        "nba", game[1], source="live", payloads={"live": {**_payload(game), "idmap": no_game}, "espn": _espn(game)}
    )
    assert out.provenance["served"] == "espn" and "not in the id map" in out.health["live"]
    no_team = _idmap().filter(~((pl.col("kind") == "team") & (pl.col("source_id") == "1610612760")))
    out = _process_game(
        "nba", game[1], source="live", payloads={"live": {**_payload(game), "idmap": no_team}, "espn": _espn(game)}
    )
    assert "team 1610612760 is not in the id map" in out.health["live"]


def test_a_live_payload_of_another_game_is_not_served_under_this_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """The NBA payloads injected under the WNBA game's ESPN id: the id map says 1022600097, not 0022500001."""
    monkeypatch.setitem(dispatch.SOURCE_ORDER, "wnba", ("espn", "live"))
    wrong = {**_payload(GAMES[0]), "idmap": _idmap()}
    out = _process_game("wnba", GAMES[1][1], source="live", payloads={"live": wrong, "espn": _espn(GAMES[1])})
    assert out.provenance["served"] == "espn"
    assert "playbyplay payload is game '0022500001', the id map says 1022600097" in out.health["live"]


def test_a_final_box_over_a_short_play_list_falls_through(monkeypatch: pytest.MonkeyPatch) -> None:
    """The two payloads are fetched apart: a final box with a play list cut before the last score is refused."""
    game = GAMES[0]
    monkeypatch.setitem(dispatch.SOURCE_ORDER, "nba", ("espn", "live"))
    payload = _payload(game)
    actions = payload["playbyplay"]["game"]["actions"]
    last_score = max(i for i, a in enumerate(actions) if a.get("shotResult") == "Made")
    del actions[last_score:]
    out = _process_game("nba", game[1], source="live", payloads={"live": payload, "espn": _espn(game)})
    assert out.provenance["served"] == "espn"
    assert "final box score (125, 124) but the play-by-play ends at" in out.health["live"]
    # mid-game the same lag is expected and served
    payload["boxscore"]["game"]["gameStatus"] = 2
    assert _process_game("nba", game[1], source="live", payloads={"live": payload}).provenance["served"] == "live"


def test_an_unmapped_person_is_a_null_athlete_and_a_note() -> None:
    """The Cup final: liveData charges a technical to San Antonio's coach (1628188), whom no crosswalk pairs."""
    adapted = _adapt(GAMES[2])
    assert any("1628188" in n for n in adapted.notes)
    tech = next(p for p in adapted.summary["plays"] if p["id"] == "401809839606")
    assert tech["type"]["text"] == "Technical Foul" and tech["participants"] == [{"athlete": {"id": None}}]
    assert tech["team"] == {"id": "24"}  # still charged to the team, as ESPN has it


def test_substitutions_pair_in_and_out_whichever_comes_first() -> None:
    """ESPN's one row per substitution, ``[in, out]``, keyed at the out action, also when the in is logged first."""
    game = GAMES[1]
    payload = _payload(game)
    base, _ = live._live_to_espn_summary(
        payload["playbyplay"],
        payload["boxscore"],
        espn_id=game[1],
        season=2026,
        lookup=live._idmap_lookup("wnba", _idmap()),
    )
    espn = {p["id"]: p for p in _espn(game)["plays"] if p["type"]["text"] == "Substitution"}
    subs = [p for p in base["plays"] if p["type"]["text"] == "Substitution"]
    same = [p for p in subs if p["id"] in espn and p["participants"] == espn[p["id"]]["participants"]]
    assert len(subs) > 40 and len(same) / len(subs) > 0.95
    # swap every out/in pair's order: the pairing must not change
    actions = payload["playbyplay"]["game"]["actions"]
    for a in actions:
        if a["actionType"] == "substitution":
            a["orderNumber"] += 1 if a["subType"] == "out" else -1
    swapped, notes = live._live_to_espn_summary(
        payload["playbyplay"],
        payload["boxscore"],
        espn_id=game[1],
        season=2026,
        lookup=live._idmap_lookup("wnba", _idmap()),
    )
    pairs = {p["id"]: p["participants"] for p in swapped["plays"] if p["type"]["text"] == "Substitution"}
    assert pairs == {p["id"]: p["participants"] for p in subs} and not [n for n in notes if "unpaired" in n]


@pytest.mark.parametrize(
    ("game_id", "season_type"),
    [("0022500001", 2), ("0042500405", 3), ("0052500211", 5), ("0062500001", 2), ("0012500001", None)],
)
def test_season_type_comes_from_the_game_id(game_id: str, season_type: int | None) -> None:
    game = GAMES[0]
    payload = _payload(game)
    payload["boxscore"]["game"]["gameId"] = game_id
    summary, _ = live._live_to_espn_summary(
        payload["playbyplay"],
        payload["boxscore"],
        espn_id=game[1],
        season=2026,
        lookup=live._idmap_lookup("nba", _idmap()),
    )
    assert summary["header"]["season"]["type"] == season_type
    # an unknown code (preseason) is null, which the contract fails, so dispatch falls through to ESPN
    assert _validate_summary(summary, "nba").ok is (season_type is not None)


@pytest.mark.parametrize(
    ("iso", "espn"),
    [("PT11M36.00S", "11:36"), ("PT01M00.00S", "1:00"), ("PT00M23.10S", "23.1"), ("PT00M00.00S", "0.0"), ("x", None)],
)
def test_clock(iso: str, espn: str | None) -> None:
    assert live._clock(iso) == espn


@skip_if_no_live
@pytest.mark.parametrize(("lg", "espn_id"), [("nba", 401809243), ("wnba", 401856986)])
def test_live_fetch_through_dispatch(monkeypatch: pytest.MonkeyPatch, lg: str, espn_id: int) -> None:
    """Fetches liveData from cdn.nba.com / cdn.wnba.com (curl_cffi Chrome impersonation) and the id map release."""
    from sportsdataverse.basketball.sources.idmap import _build_idmap_from_release
    from tests.conftest import skip_on_transient_network_error

    monkeypatch.setitem(dispatch.SOURCE_ORDER, lg, ("espn", "live"))
    try:
        idmap = _build_idmap_from_release(lg, [2026])
    except Exception as exc:  # noqa: BLE001
        skip_on_transient_network_error(exc)
    try:
        out = _process_game(lg, espn_id, source="live", fallthrough=False, payloads={"live": {"idmap": idmap}})
    except dispatch.AllSourcesFailed as exc:
        # a blocked fetch, or a game the CDN has since retired, is a skip, never a pass
        if "fetch failed" in str(exc) or "NoDataError" in str(exc):
            pytest.skip(f"cdn liveData unavailable from this IP: {exc}")
        raise
    assert out.provenance["served"] == "live" and out.provenance["contract"]["ok"]
    assert out.frames["pbp"].height > 350
