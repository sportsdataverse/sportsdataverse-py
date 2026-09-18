"""Offline gates for the basketball crosswalk source adapters.

Two defect classes are locked in here, both found when the live Track X-B flip
was attempted and aborted:

1. **Silent-empty contract.** ``stats_schedule_games`` / ``bart_super_sked``
   used to swallow every fetch failure and return a well-formed *empty* frame,
   so a dead source degraded the whole crosswalk to ``unmatched`` instead of
   failing. The tests below assert the **raise**, and separately assert that a
   provably-empty-but-valid payload still returns the typed empty frame.
   ``stats_rosters`` had the same swallow per team and is held to the same
   contract. The ESPN/Fox per-item adapters (``espn_rosters``, ``fox_rosters``,
   ``espn_scoreboard_games``, ``espn_conference_map``) keep skipping an
   *isolated* failure (R parity) but tally them through ``FetchTally``: a
   provider that failed every item and answered none raises after the loop,
   and ``strict=True`` raises on the first failure.
2. **Wrong envelope for ``scheduleleaguev2``.** The payload is
   ``{"meta":…, "leagueSchedule": {"gameDates": [...]}}``, not the
   ``resultSets`` envelope, so it parsed to zero rows against a healthy API.
   Exercised against **real captured bodies** (see the fixture READMEs) --
   never a hand-written one.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Callable, List, NamedTuple, Optional

import polars as pl
import pytest

from sportsdataverse._crosswalk_basketball_sources import (
    CrosswalkSourceError,
    FetchTally,
    bart_super_sked,
    espn_conference_map,
    espn_rosters,
    espn_scoreboard_games,
    espn_team_directory,
    fox_rosters,
    require_source,
    stats_rosters,
    stats_schedule_games,
)
from sportsdataverse.errors import NoDataError
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

FIXTURES = {
    "nba": Path(__file__).parent / "fixtures" / "nba_stats" / "scheduleleaguev2_2025_26.json",
    "wnba": Path(__file__).parent / "fixtures" / "wnba_stats" / "scheduleleaguev2_2026.json",
}


def _payload(league: str) -> dict:
    with FIXTURES[league].open(encoding="utf-8") as handle:
        return json.load(handle)


def _patch_fetch(monkeypatch: pytest.MonkeyPatch, league: str, fake: Any) -> None:
    """Point the league's ``scheduleleaguev2`` wrapper at ``fake``."""
    module = "sportsdataverse.nba.nba_stats" if league == "nba" else "sportsdataverse.wnba.wnba_stats"
    name = f"{league}_stats_scheduleleaguev2"
    monkeypatch.setattr(f"{module}.{name}", fake, raising=True)


# --------------------------------------------------------------------------
# Defect 2 -- leagueSchedule envelope
# --------------------------------------------------------------------------


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_parser_renders_league_schedule(league: str) -> None:
    """The real captured body parses to one row per game, not zero."""
    df = parse_nba_stats_result_sets(_payload(league))
    assert isinstance(df, pl.DataFrame)
    expected = sum(len(d["games"]) for d in _payload(league)["leagueSchedule"]["gameDates"])
    assert expected > 0
    assert df.height == expected


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_parser_column_contract_matches_r(league: str) -> None:
    """Columns match ``hoopR::nba_schedule()`` / ``wehoop::wnba_schedule()``.

    In particular the nested ``homeTeam``/``awayTeam`` objects flatten to
    ``home_team_id`` (not ``home_team_team_id`` and not ``hometeam_teamid``),
    which is what ``stats_schedule_games`` selects on.
    """
    df = parse_nba_stats_result_sets(_payload(league))
    assert isinstance(df, pl.DataFrame)
    for column in (
        "game_id",
        "game_code",
        "game_date",
        "game_date_time_utc",
        "home_team_id",
        "away_team_id",
        "home_team_tricode",
        "away_team_tricode",
        "home_team_name",
        "home_team_city",
        "home_team_slug",
        "week_name",
        "season",
        "league_id",
        "season_type_id",
        "season_type_description",
    ):
        assert column in df.columns, column
    # list-valued members are dropped, as the R readers do
    assert not any(c.startswith(("broadcasters", "points_leaders")) for c in df.columns)


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_parser_derives_season_type_from_game_id(league: str) -> None:
    """``season_type_description`` comes from the 3rd char of ``game_id`` (R rule)."""
    df = parse_nba_stats_result_sets(_payload(league))
    assert isinstance(df, pl.DataFrame)
    labels = {
        "1": "Pre-Season",
        "2": "Regular Season",
        "3": "All-Star",
        "4": "Playoffs",
        "5": "Play-In Game",
    }
    for game_id, described in zip(df["game_id"].to_list(), df["season_type_description"].to_list()):
        assert described == labels.get(str(game_id)[2:3])
    assert df["season_type_description"].null_count() < df.height


def test_parser_returns_zero_row_frame_on_malformed_payload() -> None:
    """Parser contract: malformed input is a zero-row frame, never a raise."""
    for payload in ({}, {"leagueSchedule": {}}, {"leagueSchedule": {"gameDates": []}}):
        out = parse_nba_stats_result_sets(payload)
        assert isinstance(out, pl.DataFrame)
        assert out.height == 0


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_schedule_games_projects_the_real_capture(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    """End to end: real body -> parser -> adapter mini-schema, fully populated."""
    _patch_fetch(monkeypatch, league, lambda **kw: _payload(league))
    games = stats_schedule_games(league, 2026)
    assert games.height == sum(len(d["games"]) for d in _payload(league)["leagueSchedule"]["gameDates"])
    for column in ("game_date", "season_type", f"{league}_game_id", f"{league}_home_team_id"):
        assert games[column].null_count() == 0, column
    assert games["game_date"].dtype == pl.Date

    teams = stats_schedule_games(league, 2026, teams=True)
    assert teams.height > 0
    assert teams[f"{league}_team_id"].null_count() == 0
    assert teams[f"{league}_team_tricode"].null_count() == 0


# --------------------------------------------------------------------------
# Defect 1 -- a source that cannot produce data must fail loudly
# --------------------------------------------------------------------------


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_schedule_games_raises_when_the_fetch_fails(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    def boom(**kwargs: Any) -> dict:
        raise TimeoutError("connection reset")

    _patch_fetch(monkeypatch, league, boom)
    with pytest.raises(CrosswalkSourceError, match="scheduleleaguev2"):
        stats_schedule_games(league, 2026)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"meta": {"code": 200}},
        {"leagueSchedule": None},
        {"leagueSchedule": {"gameDates": None}},
        {"resultSets": []},
    ],
    ids=["none", "empty", "meta-only", "null-schedule", "null-gamedates", "wrong-envelope"],
)
def test_stats_schedule_games_raises_on_unproduced_payload(monkeypatch: pytest.MonkeyPatch, payload: Any) -> None:
    """An unrenderable payload is *unproduced*, and must not pass for empty."""
    _patch_fetch(monkeypatch, "wnba", lambda **kw: payload)
    with pytest.raises(CrosswalkSourceError):
        stats_schedule_games("wnba", 2026)


@pytest.mark.parametrize("teams", [False, True])
def test_stats_schedule_games_allows_a_provably_empty_season(monkeypatch: pytest.MonkeyPatch, teams: bool) -> None:
    """``gameDates: []`` is a real answer -- typed empty frame, no raise."""
    _patch_fetch(monkeypatch, "wnba", lambda **kw: {"leagueSchedule": {"seasonYear": "2026", "gameDates": []}})
    out = stats_schedule_games("wnba", 2026, teams=teams)
    assert out.height == 0
    expected = (
        ["wnba_team_id", "wnba_team_tricode", "wnba_team_name", "wnba_team_city", "wnba_team_slug"]
        if teams
        else ["game_date", "season_type", "wnba_game_id", "wnba_game_code", "wnba_home_team_id", "wnba_away_team_id"]
    )
    assert out.columns == expected


@pytest.mark.parametrize(
    ("league", "expected"),
    [("wbb", 50), ("mbb", 50), ("nba", None), ("wnba", None)],
)
def test_espn_scoreboard_games_sends_the_ncaa_root_group(
    monkeypatch: pytest.MonkeyPatch, league: str, expected: Optional[int]
) -> None:
    """College scoreboards without ``groups`` return a featured subset, not the slate.

    ESPN answered 2026-01-15 with 10 WBB events bare and 82 with ``groups=50``,
    which is why the ESPN side of the college crosswalks collapsed. Pro leagues
    have no group axis, so they must stay bare.
    """
    seen: List[dict] = []

    def fake_scoreboard(**kwargs: Any) -> pl.DataFrame:
        seen.append(kwargs)
        return pl.DataFrame()

    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda lg: {"scoreboard": fake_scoreboard},
    )
    espn_scoreboard_games(league, [date(2026, 1, 15)])
    assert seen and seen[0].get("groups") == expected


def test_espn_scoreboard_games_lets_the_caller_override_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[dict] = []

    def fake_scoreboard(**kwargs: Any) -> pl.DataFrame:
        seen.append(kwargs)
        return pl.DataFrame()

    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda lg: {"scoreboard": fake_scoreboard},
    )
    espn_scoreboard_games("wbb", [date(2026, 1, 15)], groups=100)
    assert seen[0]["groups"] == 100


@pytest.mark.parametrize("athlete_key", ["athlete_id", "id"])
def test_espn_rosters_resolves_either_athlete_id_column(monkeypatch: pytest.MonkeyPatch, athlete_key: str) -> None:
    """wbb/wnba name the key ``athlete_id``; mbb/nba name it ``id`` -- both resolve.

    Looking for only ``athlete_id`` left every NBA and MBB roster with a null
    join key, so each player row silently failed to rejoin its own match.
    """
    roster = pl.DataFrame(
        {
            athlete_key: [4278039, 4433188],
            "full_name": ["Nickeil Alexander-Walker", "Devin Carter"],
            "jersey": ["7", "22"],
            "position_abbreviation": ["G", "G"],
            "date_of_birth": ["1998-09-02T07:00Z", "2002-03-18T08:00Z"],
        }
    )
    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda league: {"roster": lambda **kw: roster},
    )
    out = espn_rosters("nba", 1, "ATL", 2026)
    assert out["espn_athlete_id"].to_list() == ["4278039", "4433188"]


def test_espn_rosters_raises_when_the_athlete_id_column_is_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    """A populated roster with no athlete id is a rename upstream, not anonymity."""
    roster = pl.DataFrame({"full_name": ["A B"], "jersey": ["1"]})
    monkeypatch.setattr(
        "sportsdataverse._crosswalk_basketball_sources._espn_accessors",
        lambda league: {"roster": lambda **kw: roster},
    )
    with pytest.raises(CrosswalkSourceError, match="no resolvable athlete id"):
        espn_rosters("nba", 1, "ATL", 2026)


# --------------------------------------------------------------------------
# Per-item fetches (one call per team / date / conference): an isolated failure
# is skipped and logged, as in the R producers' tryCatch-to-NULL; a provider
# that failed EVERY item and answered none raises, because one unreachable or
# rate-limited host fails every item identically and the crosswalk would
# otherwise ship well-formed with that provider's columns all null. A 404
# (NoDataError) is an answered item and never counts as a failure. strict=True
# turns any non-404 failure into an immediate raise.
#
# Each adapter is driven over the same three items (1, 2, 3); ``outcome`` maps
# an item to the exception it should raise, healthy otherwise.
# --------------------------------------------------------------------------

_ITEMS = (1, 2, 3)
_DATES = {1: date(2026, 1, 15), 2: date(2026, 1, 16), 3: date(2026, 1, 17)}
_SRC = "sportsdataverse._crosswalk_basketball_sources"


def _outcome(failures: dict[int, Exception], healthy: Callable[[int], pl.DataFrame]) -> Callable[[int], pl.DataFrame]:
    def call(item: int) -> pl.DataFrame:
        if item in failures:
            raise failures[item]
        return healthy(item)

    return call


def _espn_roster(item: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "athlete_id": [item * 10],
            "full_name": [f"Player {item}"],
            "jersey": ["1"],
            "position_abbreviation": ["G"],
            "date_of_birth": ["2000-01-01T08:00Z"],
        }
    )


def _fox_roster(item: int) -> pl.DataFrame:
    return pl.DataFrame(
        {"athlete_id": [item * 10], "player": [f"Player {item}"], "jersey": ["1"], "position_group": ["G"]}
    )


def _scoreboard(item: int) -> pl.DataFrame:
    return pl.DataFrame(
        {"game_id": [item], "date": [f"{_DATES[item].isoformat()}T23:00Z"], "home_team_id": [1], "away_team_id": [2]}
    )


class _Adapter(NamedTuple):
    endpoint: str
    patch: Callable[[pytest.MonkeyPatch, Callable[[int], pl.DataFrame]], None]
    run: Callable[[bool], pl.DataFrame]
    survivors: Callable[[pl.DataFrame], set[int]]
    label: Callable[[int], str]


def _patch_espn_roster(mp: pytest.MonkeyPatch, call: Callable[[int], pl.DataFrame]) -> None:
    mp.setattr(f"{_SRC}._espn_accessors", lambda league: {"roster": lambda team_id, **kw: call(int(team_id))})


def _run_espn_rosters(strict: bool) -> pl.DataFrame:
    tally = FetchTally("espn_nba_team_roster", strict=strict)
    frames = [espn_rosters("nba", item, "T", 2026, tally=tally) for item in _ITEMS]
    tally.finish()
    return pl.concat(frames)


def _patch_fox_roster(mp: pytest.MonkeyPatch, call: Callable[[int], pl.DataFrame]) -> None:
    mp.setattr("sportsdataverse.nba.nba_fox_ext.fox_nba_team_roster", lambda fox_team_id, **kw: call(int(fox_team_id)))


def _run_fox_rosters(strict: bool) -> pl.DataFrame:
    tally = FetchTally("fox_nba_team_roster", strict=strict)
    frames = [fox_rosters("nba", item, str(item), tally=tally) for item in _ITEMS]
    tally.finish()
    return pl.concat(frames)


def _patch_scoreboard(mp: pytest.MonkeyPatch, call: Callable[[int], pl.DataFrame]) -> None:
    by_stamp = {int(d.strftime("%Y%m%d")): item for item, d in _DATES.items()}
    mp.setattr(f"{_SRC}._espn_accessors", lambda league: {"scoreboard": lambda dates, **kw: call(by_stamp[dates])})


def _patch_conferences(mp: pytest.MonkeyPatch, call: Callable[[int], pl.DataFrame]) -> None:
    stubs = _group_stubs({item: (f"Conference {item}", [item * 11]) for item in _ITEMS})
    healthy_teams = stubs["teams"]

    def teams(season: Any, stype: Any, gid: Any, **kw: Any) -> pl.DataFrame:
        call(int(gid))  # raises for the failing items
        return healthy_teams(season, stype, gid, **kw)

    stubs["teams"] = teams
    mp.setattr(f"{_SRC}._ncaa_group_accessors", lambda league: stubs)


_PER_ITEM = {
    "espn_rosters": _Adapter(
        "espn_nba_team_roster",
        _patch_espn_roster,
        _run_espn_rosters,
        lambda out: set(out["espn_team_id"].to_list()),
        lambda item: f"team_id={item}",
    ),
    "fox_rosters": _Adapter(
        "fox_nba_team_roster",
        _patch_fox_roster,
        _run_fox_rosters,
        lambda out: set(out["espn_team_id"].to_list()),
        lambda item: f"fox_team_id={item}",
    ),
    "espn_scoreboard_games": _Adapter(
        "espn_nba_scoreboard",
        _patch_scoreboard,
        lambda strict: espn_scoreboard_games("nba", [_DATES[i] for i in _ITEMS], strict=strict),
        lambda out: {int(v) for v in out["espn_game_id"].to_list()},
        lambda item: f"dates={_DATES[item].strftime('%Y%m%d')}",
    ),
    "espn_conference_map": _Adapter(
        "espn_wbb_season_group",
        _patch_conferences,
        lambda strict: espn_conference_map("wbb", 2026, strict=strict),
        lambda out: {int(v) // 11 for v in out["team_id"].to_list()},
        lambda item: f"group_id={item}",
    ),
}
_HEALTHY = {
    "espn_rosters": _espn_roster,
    "fox_rosters": _fox_roster,
    "espn_scoreboard_games": _scoreboard,
    "espn_conference_map": _espn_roster,  # unused: the conference stub builds its own frames
}


def _install(monkeypatch: pytest.MonkeyPatch, adapter: str, failures: dict[int, Exception]) -> _Adapter:
    spec = _PER_ITEM[adapter]
    spec.patch(monkeypatch, _outcome(failures, _HEALTHY[adapter]))
    return spec


@pytest.mark.parametrize("adapter", sorted(_PER_ITEM))
def test_per_item_adapters_skip_and_warn_on_an_isolated_failure(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, adapter: str
) -> None:
    """One dead item among three is skipped (R parity) and logged, the rest survive."""
    spec = _install(monkeypatch, adapter, {2: TimeoutError("one item is down")})
    with caplog.at_level(logging.WARNING, logger=_SRC):
        out = spec.run(False)
    assert spec.survivors(out) == {1, 3}
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert spec.endpoint in warnings[0].getMessage()
    assert "skipped 1 of 3" in warnings[0].getMessage()
    assert spec.label(2) in warnings[0].getMessage()
    assert "TimeoutError" in warnings[0].getMessage()


@pytest.mark.parametrize("adapter", sorted(_PER_ITEM))
def test_per_item_adapters_raise_when_every_item_fails(monkeypatch: pytest.MonkeyPatch, adapter: str) -> None:
    """Every item failing identically is one unreachable host, not three empty items."""
    spec = _install(monkeypatch, adapter, {item: TimeoutError("host unreachable") for item in _ITEMS})
    with pytest.raises(CrosswalkSourceError, match=rf"{spec.endpoint}: all 3 per-item fetches failed") as exc:
        spec.run(False)
    assert "TimeoutError: host unreachable" in str(exc.value)
    assert isinstance(exc.value.__cause__, TimeoutError)


@pytest.mark.parametrize("adapter", sorted(_PER_ITEM))
def test_per_item_adapters_treat_404_on_every_item_as_empty(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, adapter: str
) -> None:
    """A 404 is an answer -- three of them are an empty provider, not a dead one."""
    spec = _install(monkeypatch, adapter, {item: NoDataError("404") for item in _ITEMS})
    if adapter == "espn_conference_map":
        # Every conference 404ing still resolves no teams, which the walk refuses on its own.
        with pytest.raises(CrosswalkSourceError, match="resolved no teams"):
            spec.run(False)
        return
    with caplog.at_level(logging.WARNING, logger=_SRC):
        out = spec.run(False)
    assert out.height == 0
    assert out.width > 0, "an empty answer still carries the documented schema"
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize("adapter", sorted(_PER_ITEM))
def test_per_item_adapters_raise_on_the_first_failure_when_strict(
    monkeypatch: pytest.MonkeyPatch, adapter: str
) -> None:
    spec = _install(monkeypatch, adapter, {2: TimeoutError("one item is down")})
    with pytest.raises(CrosswalkSourceError, match=rf"{spec.endpoint}\({spec.label(2)}\) failed: TimeoutError"):
        spec.run(True)


@pytest.mark.parametrize("adapter", ["espn_rosters", "fox_rosters"])
def test_lone_roster_call_is_a_loop_of_one(monkeypatch: pytest.MonkeyPatch, adapter: str) -> None:
    """Without a shared tally a failed roster fetch raises; a 404 is the typed empty frame."""
    spec = _install(monkeypatch, adapter, {1: TimeoutError("down"), 2: NoDataError("404")})
    call = espn_rosters if adapter == "espn_rosters" else (lambda lg, item, *_: fox_rosters(lg, item, str(item)))
    with pytest.raises(CrosswalkSourceError, match="all 1 per-item fetches failed"):
        call("nba", 1, "T", 2026)
    out = call("nba", 2, "T", 2026)
    assert out.height == 0 and out.width > 0


# The builders own the per-team roster loop, so the tally wiring lives there:
# a builder that forgot ``finish()`` would pass every adapter test above.
_BUILDERS = {
    "nba": ("sportsdataverse.nba.nba_crosswalk", "nba_team_crosswalk", "nba_player_crosswalk", {"nba_team_id": None}),
    "wnba": (
        "sportsdataverse.wnba.wnba_crosswalk",
        "wnba_team_crosswalk",
        "wnba_player_crosswalk",
        {"wnba_team_id": None},
    ),
    "wbb": ("sportsdataverse.wbb.wbb_crosswalk", "wbb_team_crosswalk", "wbb_player_crosswalk", {}),
    "mbb": ("sportsdataverse.mbb.mbb_crosswalk", "mbb_team_crosswalk", "mbb_player_crosswalk", {}),
}


def _player_builder(monkeypatch: pytest.MonkeyPatch, league: str, failures: dict[int, Exception]) -> Callable[..., Any]:
    module, team_fn, player_fn, extra = _BUILDERS[league]
    teams = pl.DataFrame(
        {
            "espn_team_id": pl.Series(list(_ITEMS), dtype=pl.Int32),
            "espn_abbreviation": [f"T{i}" for i in _ITEMS],
            "fox_team_id": pl.Series([None] * len(_ITEMS), dtype=pl.Utf8),
            **{k: pl.Series([v] * len(_ITEMS), dtype=pl.Utf8) for k, v in extra.items()},
        }
    )
    monkeypatch.setattr(f"{module}.{team_fn}", lambda **kw: teams)
    monkeypatch.setattr(
        f"{_SRC}._espn_accessors",
        lambda lg: {"roster": lambda team_id, **kw: _outcome(failures, _espn_roster)(int(team_id))},
    )
    import importlib

    return getattr(importlib.import_module(module), player_fn)


@pytest.mark.parametrize("league", sorted(_BUILDERS))
def test_player_crosswalk_skips_one_dead_team_and_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, league: str
) -> None:
    build = _player_builder(monkeypatch, league, {2: TimeoutError("one team is down")})
    with caplog.at_level(logging.WARNING, logger=_SRC):
        out = build(season=2026)
    assert set(out["espn_team_id"].to_list()) == {1, 3}
    assert any(f"espn_{league}_team_roster: skipped 1 of 3" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("league", sorted(_BUILDERS))
def test_player_crosswalk_raises_when_every_team_roster_fails(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    build = _player_builder(monkeypatch, league, {item: TimeoutError("host unreachable") for item in _ITEMS})
    with pytest.raises(CrosswalkSourceError, match=f"espn_{league}_team_roster: all 3 per-item fetches failed"):
        build(season=2026)


@pytest.mark.parametrize("league", sorted(_BUILDERS))
def test_player_crosswalk_strict_raises_on_the_first_dead_team(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    build = _player_builder(monkeypatch, league, {2: TimeoutError("one team is down")})
    with pytest.raises(CrosswalkSourceError, match=rf"espn_{league}_team_roster\(team_id=2\) failed: TimeoutError"):
        build(season=2026, strict=True)


# --------------------------------------------------------------------------
# stats_rosters: a failed Stats roster fetch is not an empty roster.
#
# It wrapped the commonteamroster call in `except Exception: return empty`, and
# the Stats runtime answers a 403 / rate limit / blank body with `{}` rather
# than raising -- which parses to the same zero-row frame as a real empty
# roster. From a host that cannot reach stats.nba.com the player crosswalk
# therefore came back with every nba_* / wnba_* column null and no error.
# --------------------------------------------------------------------------


def _raising(exc: Exception) -> Any:
    def fetch(*args: Any, **kwargs: Any) -> Any:
        raise exc

    return fetch


_ROSTER_FIXTURES = {
    "nba": ("nba_stats/commonteamroster_1610612747_2023_24.json", "1610612747", "2023-24"),
    "wnba": ("wnba_stats/commonteamroster_1611661319_2023.json", "1611661319", "2023"),
}


def _roster_body(league: str) -> str:
    return (Path(__file__).parent / "fixtures" / _ROSTER_FIXTURES[league][0]).read_text(encoding="utf-8")


def _stats_rosters(league: str, transport: Any) -> pl.DataFrame:
    """Drive the REAL wrapper + runtime; only the HTTP transport is replaced."""
    _, team_id, season = _ROSTER_FIXTURES[league]
    return stats_rosters(league, 13, team_id, season, transport=transport)


@pytest.fixture
def _no_stats_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDV_PY_NBA_STATS_RETRIES", raising=False)


@pytest.mark.usefixtures("_no_stats_retries")
@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_rosters_projects_the_real_capture(league: str) -> None:
    body = _roster_body(league)
    rows = next(rs for rs in json.loads(body)["resultSets"] if rs["name"] == "CommonTeamRoster")["rowSet"]
    out = _stats_rosters(league, lambda *a: (200, body))
    assert out.height == len(rows) > 0
    assert out.schema[f"{league}_player_id"] == pl.Utf8
    assert out[f"{league}_player_id"].to_list() == [str(r[14]) for r in rows]
    assert out[f"{league}_player_name"].null_count() == 0


@pytest.mark.usefixtures("_no_stats_retries")
@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_rosters_raises_when_the_transport_fails(league: str) -> None:
    """A timeout (the observed droplet failure) must not read as an empty roster."""
    with pytest.raises(CrosswalkSourceError, match="commonteamroster") as exc:
        _stats_rosters(league, _raising(TimeoutError("stats.nba.com timed out")))
    assert isinstance(exc.value.__cause__, TimeoutError)


@pytest.mark.usefixtures("_no_stats_retries")
@pytest.mark.parametrize("league", ["nba", "wnba"])
@pytest.mark.parametrize(("status", "body"), [(403, ""), (429, ""), (500, "<html>"), (200, "")])
def test_stats_rosters_raises_when_the_request_is_refused(league: str, status: int, body: str) -> None:
    """The runtime answers a refused request with ``{}``, not an exception."""
    _, team_id, season = _ROSTER_FIXTURES[league]
    with pytest.raises(CrosswalkSourceError, match="no CommonTeamRoster result set") as exc:
        _stats_rosters(league, lambda *a: (status, body))
    message = str(exc.value)
    assert f"{league}_stats_commonteamroster" in message and team_id in message and season in message
    assert "SDV_PY_NBA_STATS_RETRIES" in message, "the error must say how to ride out a throttle"


@pytest.mark.usefixtures("_no_stats_retries")
@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_rosters_raises_when_rows_do_not_parse(league: str) -> None:
    """Rows present but dropped by the parser (ragged) must not read as an empty roster."""
    payload = json.loads(_roster_body(league))
    for rs in payload["resultSets"]:
        rs["rowSet"] = [row[:-1] for row in rs["rowSet"]]
    with pytest.raises(CrosswalkSourceError, match="rows"):
        _stats_rosters(league, lambda *a: (200, json.dumps(payload)))


def _sequence(*responses: tuple) -> Any:
    """A transport that answers each call with the next ``(status, body)``."""
    queue = list(responses)

    def transport(*args: Any) -> tuple:
        return queue.pop(0)

    return transport


def test_stats_rosters_retries_a_transient_throttle_when_retries_are_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "1")
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")
    out = _stats_rosters("nba", _sequence((429, ""), (200, _roster_body("nba"))))
    assert out.height > 0


def test_stats_rosters_raises_once_retries_are_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "1")
    monkeypatch.setenv("SDV_PY_NBA_STATS_BACKOFF", "0")
    with pytest.raises(CrosswalkSourceError, match="no CommonTeamRoster result set"):
        _stats_rosters("nba", _sequence((429, ""), (429, "")))


@pytest.mark.usefixtures("_no_stats_retries")
@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_rosters_allows_a_provably_empty_roster(league: str) -> None:
    """The envelope answered with no players: the real capture, rowSet emptied."""
    payload = json.loads(_roster_body(league))
    for rs in payload["resultSets"]:
        rs["rowSet"] = []
    out = _stats_rosters(league, lambda *a: (200, json.dumps(payload)))
    assert out.height == 0
    assert dict(out.schema) == {
        "espn_team_id": pl.Int32,
        f"{league}_player_id": pl.Utf8,
        f"{league}_player_name": pl.Utf8,
        f"{league}_jersey_num": pl.Utf8,
        f"{league}_position": pl.Utf8,
        f"{league}_birth_date": pl.Utf8,
    }


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_stats_rosters_without_a_stats_team_id_is_empty(league: str) -> None:
    out = stats_rosters(league, 13, None, "2023", transport=_raising(AssertionError("must not fetch")))
    assert out.height == 0
    assert f"{league}_player_id" in out.columns


def test_bart_super_sked_raises_when_the_fetch_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(url: str, **kwargs: Any) -> str:
        raise ConnectionError("torvik down")

    monkeypatch.setattr("sportsdataverse.mbb.torvik_runtime._get", boom, raising=True)
    with pytest.raises(CrosswalkSourceError, match="super_sked"):
        bart_super_sked("wbb", 2026)


def test_bart_super_sked_raises_when_a_payload_parses_to_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-empty blob that yields zero games is unproduced, not empty."""
    monkeypatch.setattr("sportsdataverse.mbb.torvik_runtime._get", lambda url, **kw: "<html>404</html>", raising=True)
    with pytest.raises(CrosswalkSourceError, match="zero games"):
        bart_super_sked("mbb", 2026)


def test_bart_super_sked_allows_a_provably_empty_season(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sportsdataverse.mbb.torvik_runtime._get", lambda url, **kw: "[]", raising=True)
    assert bart_super_sked("mbb", 2026).height == 0


# ---------------------------------------------------------------------------
# require_source -- the guard that replaced `except Exception: source = None`.
#
# The swallow it replaces is what hid a *missing provider module*: with no
# `fox_nba_teams` in the package at all, the ImportError was caught and the
# crosswalk built cleanly with silently-null fox_* columns for every team.
# ---------------------------------------------------------------------------


def test_require_source_passes_a_frame_through_including_an_empty_one() -> None:
    """A source that answered with no rows is legitimate -- no raise."""
    empty = pl.DataFrame(schema={"fox_team_id": pl.Utf8})
    assert require_source("provider()", lambda: empty).height == 0
    assert require_source("provider()", lambda: pl.DataFrame({"a": [1]})).height == 1


def test_require_source_raises_on_a_missing_provider_module() -> None:
    """The exact defect: an absent module read as a clean build."""

    def _fetch() -> Any:
        from sportsdataverse.nba.nba_fox_ext import fox_nba_teams_that_do_not_exist  # type: ignore[attr-defined]

        return fox_nba_teams_that_do_not_exist()

    with pytest.raises(CrosswalkSourceError, match="fox_nba_teams") as exc:
        require_source("fox_nba_teams()", _fetch)
    assert "ImportError" in str(exc.value), "the error must name why the source was missing"


@pytest.mark.parametrize("boom", [TimeoutError("down"), ValueError("bad payload")])
def test_require_source_raises_on_any_fetch_failure(boom: Exception) -> None:
    def _fetch() -> Any:
        raise boom

    with pytest.raises(CrosswalkSourceError, match=type(boom).__name__):
        require_source("provider()", _fetch)


@pytest.mark.parametrize("bad", [None, {}, [], "not a frame"])
def test_require_source_raises_when_the_result_is_not_a_frame(bad: Any) -> None:
    with pytest.raises(CrosswalkSourceError, match="expected a polars DataFrame"):
        require_source("provider()", lambda: bad)


def test_require_source_propagates_an_inner_source_error_unwrapped() -> None:
    """An adapter that already reported precisely must not be re-wrapped."""
    inner = CrosswalkSourceError("scheduleleaguev2 returned no leagueSchedule envelope")

    def _fetch() -> Any:
        raise inner

    with pytest.raises(CrosswalkSourceError) as exc:
        require_source("provider()", _fetch)
    assert exc.value is inner


_ESPN_DIR_COLS = ["team_id", "abbreviation", "display_name", "short_name", "team", "mascot"]


# ---------------------------------------------------------------------------
# NBA Stats team directory (standings + game log) behind nba_team_crosswalk.
#
# Both calls went through the parsed wrapper, so a refused request (`{}`)
# rendered as zero rows and took the "standings not open yet" branch: every
# nba_team_id came back null, and nba_player_crosswalk then handed stats_rosters
# a None team id -- its legitimate "no team id -> empty" path -- so the
# blocked-host scenario never reached a raise.
# ---------------------------------------------------------------------------

_NBA_STATS_FIXTURES = Path(__file__).parent / "fixtures" / "nba_stats"


def _nba_stats_transport(standings: tuple, gamelog: tuple) -> Any:
    def transport(url: str, *args: Any) -> tuple:
        return standings if "leaguestandingsv3" in url else gamelog

    return transport


def _real(name: str) -> tuple:
    return 200, (_NBA_STATS_FIXTURES / name).read_text(encoding="utf-8")


def _nba_team_crosswalk(monkeypatch: pytest.MonkeyPatch, transport: Any) -> pl.DataFrame:
    """Real Stats wrappers + runtime on a stubbed transport; ESPN stubbed, Fox pre-fetched."""
    from sportsdataverse.nba import nba_crosswalk

    monkeypatch.delenv("SDV_PY_NBA_STATS_RETRIES", raising=False)
    espn = pl.DataFrame(
        {
            "team_id": ["13", "2", "99"],
            "abbreviation": ["LAL", "BOS", "XXX"],
            "display_name": ["Los Angeles Lakers", "Boston Celtics", "Nowhere Nobodies"],
            "short_name": ["Lakers", "Celtics", "Nobodies"],
            "team": ["Los Angeles", "Boston", "Nowhere"],
            "mascot": ["Lakers", "Celtics", "Nobodies"],
        }
    )
    monkeypatch.setattr(nba_crosswalk, "espn_team_directory", lambda *a, **k: espn)
    return nba_crosswalk.nba_team_crosswalk(season=2024, fox=pl.DataFrame(), transport=transport)


def test_nba_team_crosswalk_joins_the_real_stats_captures(monkeypatch: pytest.MonkeyPatch) -> None:
    """Healthy path; an ESPN team with no Stats match stays graceful (``unmatched``)."""
    out = _nba_team_crosswalk(
        monkeypatch,
        _nba_stats_transport(_real("leaguestandingsv3_2023_24.json"), _real("leaguegamelog_team_2023_24.json")),
    )
    by_espn = {r["espn_team_id"]: r for r in out.iter_rows(named=True)}
    assert by_espn[13]["nba_team_id"] == "1610612747" and by_espn[13]["nba_team_abbreviation"] == "LAL"
    assert by_espn[2]["nba_team_id"] == "1610612738" and by_espn[2]["nba_team_abbreviation"] == "BOS"
    assert by_espn[99]["nba_team_id"] is None and by_espn[99]["match_method"] == "unmatched"


@pytest.mark.parametrize(("status", "body"), [(403, ""), (429, ""), (200, "")])
def test_nba_team_crosswalk_raises_when_standings_are_refused(
    monkeypatch: pytest.MonkeyPatch, status: int, body: str
) -> None:
    transport = _nba_stats_transport((status, body), _real("leaguegamelog_team_2023_24.json"))
    with pytest.raises(CrosswalkSourceError, match="leaguestandingsv3.*SDV_PY_NBA_STATS_RETRIES"):
        _nba_team_crosswalk(monkeypatch, transport)


def test_nba_team_crosswalk_raises_when_the_game_log_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refused tricode source: every nba_team_abbreviation would silently be null."""
    transport = _nba_stats_transport(_real("leaguestandingsv3_2023_24.json"), (403, ""))
    with pytest.raises(CrosswalkSourceError, match="leaguegamelog.*SDV_PY_NBA_STATS_RETRIES"):
        _nba_team_crosswalk(monkeypatch, transport)


def test_nba_team_crosswalk_allows_standings_that_have_not_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    """A real Standings set with no rows is provably empty: every team unmatched, no raise."""
    payload = json.loads(_real("leaguestandingsv3_2023_24.json")[1])
    payload["resultSets"][0]["rowSet"] = []
    transport = _nba_stats_transport((200, json.dumps(payload)), (403, ""))
    out = _nba_team_crosswalk(monkeypatch, transport)
    assert out.height == 3
    assert out["match_method"].to_list() == ["unmatched"] * 3


def _boom(*args: Any, **kwargs: Any) -> Any:
    raise TimeoutError("fox is down")


@pytest.mark.parametrize(
    ("league", "target", "extra"),
    [
        # nba/wnba resolve Stats before Fox, so hand those in pre-fetched: only
        # the Fox leg is under test here.
        ("nba", "fox_nba_teams", {"stats": pl.DataFrame()}),
        ("wnba", "fox_wnba_teams", {"stats": pl.DataFrame()}),
        ("mbb", "fox_mbb_teams_all", {"bart": pl.DataFrame()}),
        ("wbb", "fox_wbb_teams_all", {"bart": pl.DataFrame()}),
    ],
)
def test_team_crosswalk_raises_when_fox_cannot_be_produced(
    monkeypatch: pytest.MonkeyPatch, league: str, target: str, extra: dict
) -> None:
    """A dead Fox source must fail the build, not ship all-null fox_* columns."""
    import importlib

    monkeypatch.setattr(importlib.import_module(f"sportsdataverse.{league}.{league}_fox_ext"), target, _boom)
    crosswalk = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    # Bind the ESPN stub in the crosswalk module's own namespace -- it imported
    # espn_team_directory by value, so patching the source module is a no-op.
    monkeypatch.setattr(
        crosswalk,
        "espn_team_directory",
        lambda *a, **k: pl.DataFrame(schema={c: pl.Utf8 for c in _ESPN_DIR_COLS}),
    )
    with pytest.raises(CrosswalkSourceError, match=target):
        getattr(crosswalk, f"{league}_team_crosswalk")(season=2026, **extra)


# ---------------------------------------------------------------------------
# Provider imports must live INSIDE the guarded callable.
#
# Hoisting `from ...torvik import torvik_ratings` to the top of the builder
# defeated the guard twice over: a missing provider module raised a raw
# ImportError instead of CrosswalkSourceError, and a caller who supplied a
# pre-fetched `bart` frame still had to have the module installed.
# ---------------------------------------------------------------------------

_RATINGS_PROVIDER = {
    "mbb": ("sportsdataverse.mbb.torvik", "torvik_ratings"),
    "wbb": ("sportsdataverse.wbb.bart_wbb", "bart_wbb_ratings"),
}


def _stub_espn(monkeypatch: pytest.MonkeyPatch, crosswalk: Any) -> None:
    monkeypatch.setattr(
        crosswalk,
        "espn_team_directory",
        lambda *a, **k: pl.DataFrame(schema={c: pl.Utf8 for c in _ESPN_DIR_COLS}),
    )


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_team_crosswalk_raises_when_the_ratings_module_is_missing(monkeypatch: pytest.MonkeyPatch, league: str) -> None:
    """An absent Torvik provider must name itself as a CrosswalkSourceError."""
    import importlib
    import sys

    module_path, provider = _RATINGS_PROVIDER[league]
    monkeypatch.setitem(sys.modules, module_path, None)  # import of this module now raises
    # Drop any cached crosswalk module: import_module would otherwise hand back one
    # imported before the poison, which could still hold a top-level provider import
    # and let this test pass without proving the import happens inside the guard.
    monkeypatch.delitem(sys.modules, f"sportsdataverse.{league}.{league}_crosswalk", raising=False)
    crosswalk = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    _stub_espn(monkeypatch, crosswalk)
    with pytest.raises(CrosswalkSourceError, match=provider) as exc:
        getattr(crosswalk, f"{league}_team_crosswalk")(season=2026, fox=pl.DataFrame())
    assert module_path in str(exc.value), "the error must name the module that was missing"


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_team_crosswalk_supplied_ratings_frame_needs_no_provider_module(
    monkeypatch: pytest.MonkeyPatch, league: str
) -> None:
    """A pre-fetched `bart` frame must bypass the provider import entirely."""
    import importlib
    import sys

    module_path, _ = _RATINGS_PROVIDER[league]
    monkeypatch.setitem(sys.modules, module_path, None)
    # Same reason as above: a cached crosswalk module would not exercise the
    # supplied-frame short-circuit against a genuinely absent provider.
    monkeypatch.delitem(sys.modules, f"sportsdataverse.{league}.{league}_crosswalk", raising=False)
    crosswalk = importlib.import_module(f"sportsdataverse.{league}.{league}_crosswalk")
    _stub_espn(monkeypatch, crosswalk)
    out = getattr(crosswalk, f"{league}_team_crosswalk")(season=2026, fox=pl.DataFrame(), bart=pl.DataFrame())
    assert isinstance(out, pl.DataFrame)
    assert out.height == 0


# ---------------------------------------------------------------------------
# ESPN conference labels (the Track X-B blocker).
#
# The Site v2 `teams` directory carries no conference, so `espn_conference`
# shipped 0/362 where the R producer ships 361/361. It is reconstructed from
# the Core v2 season group tree instead. Locked in here: the $ref id parsing,
# the raise on an empty walk, and -- the latent one -- that the join key is a
# clean Utf8 id, since a float-origin id stringifies as "123.0" and would match
# nothing.
# ---------------------------------------------------------------------------

_CORE = "http://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons/2026"


def _group_stubs(tree: dict[int, tuple[str, list[int]]]) -> dict[str, Any]:
    """Stub the three Core v2 group wrappers from a {gid: (name, [team ids])}."""

    def children(season: Any, stype: Any, gid: Any, **kw: Any) -> pl.DataFrame:
        return pl.DataFrame({"$ref": [f"{_CORE}/types/2/groups/{g}?lang=en" for g in tree]})

    def group(season: Any, stype: Any, gid: Any, **kw: Any) -> pl.DataFrame:
        return pl.DataFrame({"id": [str(gid)], "name": [tree[int(gid)][0]]})

    def teams(season: Any, stype: Any, gid: Any, **kw: Any) -> pl.DataFrame:
        return pl.DataFrame({"$ref": [f"{_CORE}/teams/{t}?lang=en" for t in tree[int(gid)][1]]})

    return {"children": children, "group": group, "teams": teams}


def _patch_tree(monkeypatch: pytest.MonkeyPatch, tree: dict[int, tuple[str, list[int]]]) -> None:
    import sportsdataverse._crosswalk_basketball_sources as src

    monkeypatch.setattr(src, "_ncaa_group_accessors", lambda league: _group_stubs(tree))


def test_espn_conference_map_walks_the_group_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every conference's members carry that conference's name, keyed on Utf8 ids."""
    _patch_tree(monkeypatch, {2: ("Atlantic Coast Conference", [52, 152]), 8: ("Southeastern Conference", [2579])})

    out = espn_conference_map("wbb", 2026)

    assert out.schema == {"team_id": pl.Utf8, "conference_name": pl.Utf8}
    assert dict(zip(out["team_id"], out["conference_name"])) == {
        "52": "Atlantic Coast Conference",
        "152": "Atlantic Coast Conference",
        "2579": "Southeastern Conference",
    }


def test_espn_conference_map_raises_when_the_walk_resolves_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """An all-null espn_conference column must fail the build, not ship."""
    _patch_tree(monkeypatch, {})
    with pytest.raises(CrosswalkSourceError, match="resolved no teams"):
        espn_conference_map("wbb", 2026)


def test_espn_conference_map_is_empty_for_non_ncaa_leagues(monkeypatch: pytest.MonkeyPatch) -> None:
    """NBA/WNBA have no group-50 tree; their crosswalks carry no conference."""
    for league in ("nba", "wnba"):
        out = espn_conference_map(league, 2026)
        assert out.height == 0
        # Full schema, not just names: a Null-dtyped ``team_id`` is an unusable
        # join key that a columns-only assertion would wave through.
        assert dict(out.schema) == {"team_id": pl.Utf8, "conference_name": pl.Utf8}


def test_espn_team_directory_joins_conference_on_a_clean_utf8_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """An Int64 team_id must join as "52", never the float-origin "52.0"."""
    import sportsdataverse._crosswalk_basketball_sources as src

    monkeypatch.setattr(
        src,
        "_espn_accessors",
        lambda league: {
            "teams": lambda **kw: pl.DataFrame(
                {
                    "team_id": pl.Series([52, 2579], dtype=pl.Int64),
                    "team_abbreviation": ["ND", "UT"],
                    "team_display_name": ["Notre Dame Fighting Irish", "Texas Longhorns"],
                    "team_short_display_name": ["Notre Dame", "Texas"],
                    "team_location": ["Notre Dame", "Texas"],
                    "team_name": ["Fighting Irish", "Longhorns"],
                }
            )
        },
    )
    _patch_tree(monkeypatch, {2: ("Atlantic Coast Conference", [52]), 8: ("Southeastern Conference", [2579])})

    out = espn_team_directory("wbb", season=2026)

    assert out["team_id"].to_list() == ["52", "2579"], "a float-origin id would read '52.0'"
    assert out["conference_name"].to_list() == ["Atlantic Coast Conference", "Southeastern Conference"]


def test_espn_team_directory_prefers_an_upstream_conference_column(monkeypatch: pytest.MonkeyPatch) -> None:
    """A directory that already carries conference is not re-walked."""
    import sportsdataverse._crosswalk_basketball_sources as src

    monkeypatch.setattr(
        src,
        "_espn_accessors",
        lambda league: {
            "teams": lambda **kw: pl.DataFrame({"team_id": ["52"], "conference_name": ["Big Ten Conference"]})
        },
    )

    def _boom(league: str) -> Any:  # pragma: no cover - must not be reached
        raise AssertionError("the group tree must not be walked when upstream ships conference")

    monkeypatch.setattr(src, "_ncaa_group_accessors", _boom)

    assert espn_team_directory("wbb", season=2026)["conference_name"].to_list() == ["Big Ten Conference"]
