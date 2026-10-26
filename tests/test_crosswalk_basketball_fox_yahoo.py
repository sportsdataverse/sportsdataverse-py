"""Offline gates for the Fox game and Yahoo team / game id legs of the basketball crosswalks.

Every payload here is a real capture (see ``tests/fixtures/crosswalk_basketball/README.md``):
Thanksgiving 2025 men's college slate, where 19 of 20 games were neutral-site and three
late West-coast tips fall on different ET dates in ESPN (23:59) and Fox (00:00).
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest

from sportsdataverse import _crosswalk_basketball_fox_yahoo as fy
from sportsdataverse._crosswalk_basketball_sources import CrosswalkSourceError
from sportsdataverse.mbb.mbb_crosswalk import FOX_DISPLAY_ALIAS
from sportsdataverse.mbb.mbb_crosswalk import SCHEDULE_COLUMNS as MBB_SCHEDULE_COLUMNS

F = Path(__file__).parent / "fixtures" / "crosswalk_basketball"
THANKSGIVING = date(2025, 11, 27)
_LOG = "sportsdataverse._crosswalk_basketball_fox_yahoo"


def _json(name: str) -> Any:
    return json.loads((F / name).read_text())


def _schedule() -> pl.DataFrame:
    """The ESPN slate as an MBB schedule crosswalk (Fox / Yahoo columns null)."""
    s = pl.read_parquet(F / "espn_mbb_schedule_20251127.parquet")
    return s.select(
        pl.lit(2026, dtype=pl.Int32).alias("season"),
        pl.col("game_date"),
        pl.col("home_id").cast(pl.Int32).alias("home_espn_team_id"),
        pl.col("away_id").cast(pl.Int32).alias("away_espn_team_id"),
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_game_id"),
        *[pl.lit(None, dtype=pl.Utf8).alias(c) for c in ("bart_muid", "bart_team1", "bart_team2", "bart_winner")],
        *[pl.lit(None, dtype=pl.Utf8).alias(c) for c in ("kp_game_id", "fox_game_id", "yahoo_game_id")],
        pl.lit("espn_only").alias("match_method"),
        pl.lit(None, dtype=pl.Float64).alias("match_confidence"),
    ).select(MBB_SCHEDULE_COLUMNS)


def _teams() -> pl.DataFrame:
    """ESPN team rows shaped like the MBB team crosswalk's ESPN and Yahoo columns."""
    d = pl.read_parquet(F / "espn_mbb_teams_20251127.parquet")
    return d.select(
        pl.col("team_id").cast(pl.Int32).alias("espn_team_id"),
        pl.col("display_name").cast(pl.Utf8).alias("espn_display_name"),
        pl.col("team").cast(pl.Utf8).alias("espn_location"),
        pl.col("mascot").cast(pl.Utf8).alias("espn_mascot"),
        pl.lit(None, dtype=pl.Utf8).alias("fox_team_id"),
        pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_id"),
        pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_name"),
    )


@pytest.fixture
def fox_calls(monkeypatch: pytest.MonkeyPatch) -> List[tuple]:
    calls: List[tuple] = []

    def fox_get(path: str, params: Any = None, **_: Any) -> Dict[str, Any]:
        calls.append((path, params))
        return _json("fox_cbk_scores_segment_20251127.json")

    monkeypatch.setattr("sportsdataverse._fox_layout.fox_get", fox_get)
    return calls


@pytest.fixture
def yahoo(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    def teams(**kw: Any) -> Dict[str, Any]:
        calls.append(kw)
        return _json("yahoo_ncaab_league_teams_division1.json")

    def game_ids(**kw: Any) -> Dict[str, Any]:
        calls.append(kw)
        return _json("yahoo_ncaab_game_ids_20251126_20251127.json")

    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_teams", teams)
    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_game_ids_by_date", game_ids)
    return calls


def _by_game(df: pl.DataFrame, col: str) -> Dict[str, Any]:
    return dict(zip(df["espn_game_id"].to_list(), df[col].to_list()))


# --- Fox -------------------------------------------------------------------


def test_fox_scoreboard_games_reads_the_division_i_segment(fox_calls: List[tuple]) -> None:
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING])
    assert fox_calls == [("cbk/league/scores-segment/20251127", {"groupId": 2})]
    assert games.schema == pl.Schema(fy.FOX_GAMES_SCHEMA)
    assert games.height == 23
    # 05:00 UTC is midnight ET: Fox files the Acrisure Classic tip on the 27th.
    ole_miss = games.filter(pl.col("fox_game_id") == "263945").row(0, named=True)
    assert ole_miss["game_date"] == THANKSGIVING
    assert (ole_miss["fox_home_team"], ole_miss["fox_away_team"]) == ("Ole Miss Rebels", "Utah Runnin' Utes")


def test_fox_games_match_on_teams_in_either_order(fox_calls: List[tuple]) -> None:
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING])
    fox_to_espn = fy.fox_team_map(_teams(), games, FOX_DISPLAY_ALIAS)
    out = fy.attach_fox_games(_schedule(), games, fox_to_espn)
    assert out.columns == MBB_SCHEDULE_COLUMNS
    fox = _by_game(out, "fox_game_id")
    # ESPN's home is Arkansas; Fox's is Duke (neutral site, Chicago).
    assert fox["401817234"] == "260615"
    # ESPN 2025-11-26 23:59 ET, Fox 2025-11-27 00:00 ET: matched a day apart on both teams.
    assert fox["401830745"] == "263945"
    assert fox["401831220"] == "264002"
    # Chaminade is not Division I: only Boise State resolves, and one team is not enough across dates.
    assert fox["401831203"] is None
    assert sum(v is not None for v in fox.values()) == 22
    assert out["fox_game_id"].drop_nulls().is_unique().all()


def test_a_matchup_fox_and_espn_pair_differently_stays_unmatched(fox_calls: List[tuple]) -> None:
    # ESPN re-pairs a bracket game after Fox published it: Arkansas's row now names another
    # opponent. Fox's Duke-Arkansas game resolves both teams, so one shared team is not enough.
    sched = _schedule().with_columns(
        pl.when(pl.col("espn_game_id") == "401817234")
        .then(pl.lit(219, dtype=pl.Int32))  # Penn, idle that day
        .otherwise(pl.col("away_espn_team_id"))
        .alias("away_espn_team_id")
    )
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING])
    out = fy.attach_fox_games(sched, games, fy.fox_team_map(_teams(), games, FOX_DISPLAY_ALIAS))
    assert _by_game(out, "fox_game_id")["401817234"] is None
    assert "260615" not in out["fox_game_id"].to_list()


def test_fox_team_map_uses_the_alias_bridge(fox_calls: List[tuple]) -> None:
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING])
    # "Miami (FL) Hurricanes" only reaches ESPN's "Miami Hurricanes" through FOX_DISPLAY_ALIAS.
    miami_fox = games.filter(pl.col("fox_home_team") == "Miami (FL) Hurricanes")["fox_home_team_id"].item()
    assert fy.fox_team_map(_teams(), games, FOX_DISPLAY_ALIAS)[miami_fox] == 2390
    assert miami_fox not in fy.fox_team_map(_teams(), games)


def test_fox_games_resolve_through_the_team_crosswalk_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sportsdataverse._fox_layout.fox_get", lambda *a, **k: _json("fox_nba_scores_segment_20260302.json")
    )
    games = fy.fox_scoreboard_games("nba", [date(2026, 3, 2)])
    # Blank the names so only the crosswalk's fox_team_id can resolve a team.
    team = pl.read_parquet(F / "nba_team_crosswalk_2026.parquet").with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("espn_display_name")
    )
    s = pl.read_parquet(F / "espn_nba_schedule_20260302.parquet").select(
        pl.col("game_date"),
        pl.col("home_id").alias("home_espn_team_id"),
        pl.col("away_id").alias("away_espn_team_id"),
        pl.col("game_id").cast(pl.Utf8).alias("espn_game_id"),
        *[pl.lit(None, dtype=pl.Utf8).alias(c) for c in ("fox_game_id", "fox_home_team_id", "fox_away_team_id")],
    )
    out = fy.attach_fox_games(s, games, fy.fox_team_map(team, games))
    fox_of = dict(zip(team["espn_team_id"].to_list(), team["fox_team_id"].to_list()))
    assert out["fox_game_id"].null_count() == 0
    for row in out.iter_rows(named=True):
        assert (row["fox_home_team_id"], row["fox_away_team_id"]) == (
            fox_of[row["home_espn_team_id"]],
            fox_of[row["away_espn_team_id"]],
        )
    assert _by_game(out, "fox_game_id")["401810734"] == "44357"  # Houston at Washington


def _fox_with(games: pl.DataFrame, *rows: Dict[str, Any]) -> pl.DataFrame:
    return pl.concat([games, pl.DataFrame(list(rows), schema=fy.FOX_GAMES_SCHEMA)])


def test_a_one_team_fox_game_never_takes_a_row_against_a_team_fox_lists(fox_calls: List[tuple]) -> None:
    # Fox 263979 is Boise State vs Chaminade (not Division I, so only Boise resolves), Fox date
    # 11-27; ESPN files it 11-26 23:59. Give Boise a second late tip: ESPN 11-27 vs Penn, Fox 11-28.
    # Boise is the only team the 11-27 Fox game and that row share, but Fox lists Penn too, so the
    # row is not the Chaminade game: it must take its own Fox game a day later.
    games = _fox_with(
        fy.fox_scoreboard_games("mbb", [THANKSGIVING]),
        {
            "fox_game_id": "900001",
            "game_date": date(2025, 11, 28),
            "fox_home_team_id": "95",
            "fox_away_team_id": "9001",
            "fox_home_team": "Boise State Broncos",
            "fox_away_team": "Pennsylvania Quakers",
        },
    )
    sched = pl.concat(
        [
            _schedule(),
            _schedule()
            .head(1)
            .with_columns(
                pl.lit(THANKSGIVING).alias("game_date"),
                pl.lit(68, dtype=pl.Int32).alias("home_espn_team_id"),
                pl.lit(219, dtype=pl.Int32).alias("away_espn_team_id"),
                pl.lit("boise-penn").alias("espn_game_id"),
            ),
        ]
    )
    out = fy.attach_fox_games(sched, games, fy.fox_team_map(_teams(), games, FOX_DISPLAY_ALIAS))
    fox = _by_game(out, "fox_game_id")
    assert fox["boise-penn"] == "900001"
    assert fox["401831203"] is None  # the Chaminade row: one team is not enough across dates
    assert "263979" not in out["fox_game_id"].to_list()


@pytest.mark.parametrize(("fox_day", "matched"), [(date(2025, 11, 28), True), (date(2025, 11, 26), False)])
def test_fox_dates_match_a_day_late_never_a_day_early(fox_calls: List[tuple], fox_day: date, matched: bool) -> None:
    # Duke-Arkansas is ESPN 11-27. Fox a day later is ESPN's 23:59 tip; Fox a day earlier is not.
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING]).with_columns(
        pl.when(pl.col("fox_game_id") == "260615")
        .then(pl.lit(fox_day))
        .otherwise(pl.col("game_date"))
        .alias("game_date")
    )
    out = fy.attach_fox_games(_schedule(), games, fy.fox_team_map(_teams(), games, FOX_DISPLAY_ALIAS))
    assert (_by_game(out, "fox_game_id")["401817234"] == "260615") is matched


def test_fox_join_keys_are_cast_not_asserted(fox_calls: List[tuple]) -> None:
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING])
    fox_to_espn = fy.fox_team_map(_teams(), games, FOX_DISPLAY_ALIAS)
    want = fy.attach_fox_games(_schedule(), games, fox_to_espn)
    odd = _schedule().with_columns(pl.col("game_date").cast(pl.Datetime), pl.col("home_espn_team_id").cast(pl.Int64))
    got = fy.attach_fox_games(odd, games, fox_to_espn)
    assert got["fox_game_id"].to_list() == want["fox_game_id"].to_list()


def test_fox_team_map_is_one_to_one(fox_calls: List[tuple]) -> None:
    games = fy.fox_scoreboard_games("mbb", [THANKSGIVING])
    duke = games.filter(pl.col("fox_game_id") == "260615").row(0, named=True)
    # A second Fox id carrying Duke's name: neither id may claim Duke.
    twin = _fox_with(games, {**duke, "fox_game_id": "900002", "fox_home_team_id": "9002"})
    fox_to_espn = fy.fox_team_map(_teams(), twin, FOX_DISPLAY_ALIAS)
    assert duke["fox_home_team_id"] not in fox_to_espn and "9002" not in fox_to_espn
    assert len(set(fox_to_espn.values())) == len(fox_to_espn)
    # A crosswalk fox_team_id owns its ESPN team; Fox's own id for Duke then maps nowhere.
    claimed = _teams().with_columns(
        pl.when(pl.col("espn_display_name") == "Duke Blue Devils").then(pl.lit("crosswalk-duke")).alias("fox_team_id")
    )
    fox_to_espn = fy.fox_team_map(claimed, games, FOX_DISPLAY_ALIAS)
    assert fox_to_espn["crosswalk-duke"] == 150 and duke["fox_home_team_id"] not in fox_to_espn


def test_a_malformed_fox_payload_warns_and_leaves_the_schedule(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("sportsdataverse._fox_layout.fox_get", lambda *a, **k: {"sectionList": "not a list"})
    sched = _schedule()
    with caplog.at_level(logging.WARNING, logger=_LOG):
        out = fy.with_fox_games("mbb", sched, _teams(), alias=FOX_DISPLAY_ALIAS)
    assert out.equals(sched)
    assert "Fox games unavailable" in caplog.text


def test_fox_failure_warns_and_leaves_the_schedule(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def down(*_: Any, **__: Any) -> Dict[str, Any]:
        raise ConnectionError("fox down")

    monkeypatch.setattr("sportsdataverse._fox_layout.fox_get", down)
    sched = _schedule()
    with caplog.at_level(logging.WARNING, logger=_LOG):
        out = fy.with_fox_games("mbb", sched, _teams(), alias=FOX_DISPLAY_ALIAS)
    assert out.equals(sched)
    assert "Fox games unavailable" in caplog.text


# --- Yahoo -----------------------------------------------------------------


def test_yahoo_teams_asks_for_division_i_by_division(yahoo: List[Dict[str, Any]]) -> None:
    out = fy.yahoo_teams("mbb")
    assert yahoo == [{"league": "ncaab", "return_parsed": False, "division_ids": "1", "get_teams_by_division": "true"}]
    assert out.schema == pl.Schema(fy.YAHOO_TEAMS_SCHEMA)
    assert out.height == 51


def test_yahoo_teams_join_espn_by_name_tiers(yahoo: List[Dict[str, Any]]) -> None:
    matched = fy.match_yahoo_teams(_teams(), fy.yahoo_teams("mbb"))
    assert matched["espn_team_id"].is_unique().all() and matched["yahoo_team_id"].is_unique().all()
    assert dict(matched.group_by("yahoo_match_method").len().iter_rows()) == {
        "exact_name": 42,
        "location": 4,
        "nickname": 3,
    }
    got = {
        name: (yid, method)
        for name, yid, method in matched.join(_teams().select("espn_team_id", "espn_display_name"), on="espn_team_id")
        .select("espn_display_name", "yahoo_team_id", "yahoo_match_method")
        .iter_rows()
    }
    assert got["Duke Blue Devils"] == ("ncaab.t.173", "exact_name")
    assert got["Pennsylvania Quakers"] == ("ncaab.t.460", "location")  # Yahoo: "Penn Quakers"
    assert got["UNLV Rebels"] == ("ncaab.t.401", "location")  # Yahoo: "UNLV Runnin' Rebels"
    assert got["Miami Hurricanes"] == ("ncaab.t.355", "nickname")  # Yahoo: "Miami (FL) Hurricanes"
    assert got["Long Island University Sharks"] == ("ncaab.t.309", "nickname")  # Yahoo: "LIU Sharks"
    # Memphis is not in the trimmed Yahoo listing, so two ESPN "Tigers" are left for Yahoo's one
    # "Grambling State Tigers": an ambiguous key matches neither.
    assert "Grambling Tigers" not in got and "Memphis Tigers" not in got


def test_with_yahoo_teams_fills_the_columns_and_logs_the_unmatched(
    yahoo: List[Dict[str, Any]], caplog: pytest.LogCaptureFixture
) -> None:
    team = _teams()
    with caplog.at_level(logging.WARNING, logger=_LOG):
        out = fy.with_yahoo_teams("mbb", team)
    assert out.columns == team.columns and out.height == team.height
    assert out["yahoo_team_id"].null_count() == 2  # Memphis and Grambling (see the tier test)
    assert out.filter(pl.col("espn_display_name") == "Utah Utes")["yahoo_team_name"].item() == "Utah Runnin' Utes"
    assert (
        "2 Yahoo teams matched no ESPN team: ncaab.t.234 Grambling State Tigers, ncaab.t.1237 Queens University Royals"
        in caplog.text
    )


def test_yahoo_game_ids_take_yahoos_home_side(yahoo: List[Dict[str, Any]]) -> None:
    team = fy.with_yahoo_teams("mbb", _teams())
    known = fy.yahoo_game_ids("mbb", 2026, [date(2025, 11, 26), THANKSGIVING])
    assert yahoo[-1] == {
        "leagues": "ncaab",
        "start_range": "2025-11-26",
        "end_range": "2025-11-27",
        "season": "2025",
        "return_parsed": False,
    }
    out = fy.assign_yahoo_game_ids("mbb", _schedule(), team, known)
    ids = _by_game(out, "yahoo_game_id")
    assert ids["401817234"] == "ncaab.g.202511270173"  # Duke (Yahoo's home), not Arkansas
    assert ids["401812276"] == "ncaab.g.202511270421"  # Northwestern at home, not neutral
    assert ids["401830745"] == "ncaab.g.202511260365"  # the midnight tip keeps ESPN's ET date
    assert ids["401831203"] is None  # Chaminade (not Division I) is the home side
    assert set(out["yahoo_game_id"].drop_nulls()) <= known
    assert out["yahoo_game_id"].null_count() == 1


def test_yahoo_game_ids_without_the_list_keep_the_home_side(yahoo: List[Dict[str, Any]]) -> None:
    team = fy.with_yahoo_teams("mbb", _teams())
    ids = _by_game(fy.assign_yahoo_game_ids("mbb", _schedule(), team, None), "yahoo_game_id")
    assert ids["401817234"] == "ncaab.g.202511270019"  # Arkansas, zero-padded to 4 digits


def test_yahoo_id_list_failure_keeps_unverified_ids(
    monkeypatch: pytest.MonkeyPatch, yahoo: List[Dict[str, Any]], caplog: pytest.LogCaptureFixture
) -> None:
    team = fy.with_yahoo_teams("mbb", _teams())

    def down(**_: Any) -> Dict[str, Any]:
        raise ConnectionError("yahoo down")

    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_game_ids_by_date", down)
    with caplog.at_level(logging.WARNING, logger=_LOG):
        out = fy.with_yahoo_games("mbb", 2026, _schedule(), team)
    assert _by_game(out, "yahoo_game_id")["401817234"] == "ncaab.g.202511270019"
    assert "keeping unverified home-side ids" in caplog.text


def test_yahoo_teams_failure_leaves_the_columns_null(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def down(**_: Any) -> Dict[str, Any]:
        raise ConnectionError("yahoo down")

    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_teams", down)
    team = _teams()
    with caplog.at_level(logging.WARNING, logger=_LOG):
        out = fy.with_yahoo_teams("mbb", team)
    assert out.equals(team)
    assert "Yahoo teams unavailable" in caplog.text


def test_a_nickname_alone_never_pairs_two_schools(yahoo: List[Dict[str, Any]]) -> None:
    yahoo_frame = fy.yahoo_teams("mbb")
    # Miami pairs with "Miami (FL) Hurricanes" on the nickname tier because the schools agree;
    # move ESPN's Miami to another school and the shared "Hurricanes" alone must not pair them.
    moved = _teams().with_columns(
        pl.when(pl.col("espn_display_name") == "Miami Hurricanes")
        .then(pl.lit("Tulsa"))
        .otherwise(pl.col("espn_location"))
        .alias("espn_location")
    )
    matched = fy.match_yahoo_teams(moved, yahoo_frame)
    assert 2390 not in matched["espn_team_id"].to_list()
    assert "ncaab.t.355" not in matched["yahoo_team_id"].to_list()


def test_a_malformed_yahoo_id_list_is_a_source_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_game_ids_by_date", lambda **_: [])
    with pytest.raises(CrosswalkSourceError, match="AttributeError"):
        fy.yahoo_game_ids("mbb", 2026, [THANKSGIVING])


def test_a_capped_yahoo_week_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    # A week at the 1000-row cap is truncated; trusting it would null real games.
    games = _json("yahoo_ncaab_game_ids_20251126_20251127.json")["data"]["leagues"][0]["games"]
    capped = {"data": {"leagues": [{"games": (games * 11)[:1000]}]}}
    monkeypatch.setattr(
        "sportsdataverse.providers.yahoo.yahoo_shangrila.yahoo_league_game_ids_by_date", lambda **_: capped
    )
    with pytest.raises(CrosswalkSourceError, match="1000-row cap"):
        fy.yahoo_game_ids("mbb", 2026, [THANKSGIVING])


# --- builder flags ---------------------------------------------------------


@pytest.mark.parametrize("league", ["nba", "wnba", "mbb", "wbb"])
def test_schedule_builders_only_fetch_fox_and_yahoo_when_asked(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    import importlib

    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    calls: List[str] = []
    monkeypatch.setattr(mod, "with_fox_games", lambda lg, out, team, **k: calls.append(f"fox:{lg}") or out)
    if hasattr(mod, "with_yahoo_games"):
        monkeypatch.setattr(
            mod, "with_yahoo_games", lambda lg, season, out, team, **k: calls.append(f"yahoo:{lg}") or out
        )
    monkeypatch.setattr(mod, "espn_scoreboard_games", lambda *a, **k: pl.DataFrame())
    monkeypatch.setattr(mod, "_assemble_schedule_crosswalk", lambda *a: pl.DataFrame({"game_date": []}))
    seen: Dict[str, Any] = {}

    def team_xwalk(**k: Any) -> pl.DataFrame:
        seen.update(k)
        return pl.DataFrame()

    monkeypatch.setattr(mod, f"{league}_team_crosswalk", team_xwalk)
    games = pl.DataFrame({"game_date": [THANKSGIVING]})
    if league in ("nba", "wnba"):
        monkeypatch.setattr("sportsdataverse._crosswalk_basketball_sources.stats_schedule_games", lambda *a, **k: games)
    else:
        monkeypatch.setattr(mod, "bart_super_sked", lambda *a, **k: games)
    build = getattr(mod, f"{league}_schedule_crosswalk")

    build(season=2026)
    assert calls == [] and not seen.get("yahoo")
    if league == "nba":
        build(season=2026, fox=True)
        assert calls == ["fox:nba"]
    else:
        build(season=2026, fox=True, yahoo=True)
        assert calls == [f"fox:{league}", f"yahoo:{league}"] and seen["yahoo"] is True
