"""Shield player/team statistics -> ESPN ``summary['boxscore']`` shape.

Phase 5 of the Shield plan. The adapter's summary shipped an **empty** ``boxscore``
(``to_espn_summary``'s divergence 3), which cost two things downstream:

* ``NFLPlayProcess.__attach_player_ids`` reads ``boxscore.players`` to map every
  text-derived ``*_player_name`` onto an ESPN athlete id. With no box, no id is
  attached and every player-keyed ``advBoxScore`` / usage section falls back to
  name matching alone.
* ``advBoxScore["espn_players"]`` / ``["espn_team"]`` are ESPN's own boxes,
  surfaced verbatim, and the team box is the authoritative source of the
  countable turnover totals (``create_box_score``).

Data path: the **documented Shield endpoints**
``/football/v2/stats/live/player-statistics/{gameId}`` and
``/football/v2/stats/live/team-statistics/{gameId}``
(``sdv-internal-refs/nfl/nfl-com-api.openapi.yaml``, ``getLivePlayerStatistics`` /
``getLiveTeamStatistics``). Despite the ``live`` path segment both answer for
**finals in every era** the Shield corpus covers (probed 2002/2005/2010/2014/2019/2024
/2025: 25-34 player rows per side each), so they are the whole box for a settled game
as well as a live one. They carry every counting stat ESPN's ten player categories
need, already aggregated by the league -- which is why nothing here re-derives a box
from the per-play ``stats`` arrays :mod:`...shield_pbp.stat_ids` decodes.

ESPN athlete ids come from :func:`sportsdataverse.nfl.nfl_players_crosswalk`
(``gsis_id`` -> ``espn_id``). A gsis id the crosswalk does not carry gets a **null**
athlete id and keeps its Shield ``X.Surname`` name -- never a guessed id.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

__all__: List[str] = []  # private module: nothing here is part of the flat API


def _n(value: Any) -> float:
    """A Shield stat as a number (a missing / null field counts as 0)."""
    return float(value) if isinstance(value, (int, float)) else 0.0


def _i(value: Any) -> str:
    """ESPN's integer rendering of a Shield counting stat."""
    return str(int(round(_n(value))))


def _d(value: Any) -> str:
    """ESPN's one-decimal rendering of a Shield rate/average stat."""
    return f"{_n(value):.1f}"


def _half(value: Any) -> str:
    """Sacks: ESPN writes a whole number bare and a half-sack with one decimal."""
    v = _n(value)
    return str(int(v)) if v == int(v) else f"{v:.1f}"


def _solo(p: Mapping[str, Any]) -> float:
    """ESPN ``soloTackles``: defensive + special-teams + miscellaneous solo tackles.

    ESPN lists a player in ``defensive`` on a special-teams or miscellaneous tackle
    alone (measured: 34 of 189 ESPN defensive rows over five 2025 games are players
    with no *defensive* tackle at all), so all three families are summed.
    """
    return (
        _n(p.get("defensiveTackles"))
        + _n(p.get("defensiveSpecialTeamsTackles"))
        + _n(p.get("defensiveMiscellaneousTackles"))
    )


def _assists(p: Mapping[str, Any]) -> float:
    return (
        _n(p.get("defensiveTacklesAssists"))
        + _n(p.get("defensiveSpecialTeamsTacklesAssists"))
        + _n(p.get("defensiveMiscellaneousTacklesAssists"))
    )


def _pct(made: Any, att: Any) -> str:
    a = _n(att)
    return f"{(100.0 * _n(made) / a):.1f}" if a else "0.0"


#: ESPN NFL player-box categories, in ESPN's own order, each as
#: ``(category name, ESPN keys, renderers, activity predicate fields)``.
#: A renderer takes the Shield player row and returns ESPN's string value.
#: ``adjQBR`` is ESPN-proprietary and has no Shield counterpart -- it is emitted as
#: ESPN's own "no value" token rather than invented.
_CATEGORIES: Tuple[
    Tuple[str, Tuple[str, ...], Tuple[Callable[[Mapping[str, Any]], str], ...], Tuple[str, ...]], ...
] = (
    (
        "passing",
        (
            "completions/passingAttempts",
            "passingYards",
            "yardsPerPassAttempt",
            "passingTouchdowns",
            "interceptions",
            "sacks-sackYardsLost",
            "adjQBR",
            "QBRating",
        ),
        (
            lambda p: f"{_i(p.get('passingCompletions'))}/{_i(p.get('passingAttempts'))}",
            lambda p: _i(p.get("passingYards")),
            lambda p: _d(p.get("passingYardsPerAttempt")),
            lambda p: _i(p.get("passingTouchdowns")),
            lambda p: _i(p.get("passingInterceptions")),
            lambda p: f"{_i(p.get('passingTimesSacked'))}-{_i(p.get('passingSackYardsLost'))}",
            lambda p: "--",
            lambda p: _d(p.get("passingRating")),
        ),
        ("passingAttempts", "passingTimesSacked"),
    ),
    (
        "rushing",
        ("rushingAttempts", "rushingYards", "yardsPerRushAttempt", "rushingTouchdowns", "longRushing"),
        (
            lambda p: _i(p.get("rushingAttempts")),
            lambda p: _i(p.get("rushingYards")),
            lambda p: _d(p.get("rushingAverage")),
            lambda p: _i(p.get("rushingTouchdowns")),
            lambda p: _i(p.get("rushingLong")),
        ),
        ("rushingAttempts",),
    ),
    (
        "receiving",
        (
            "receptions",
            "receivingYards",
            "yardsPerReception",
            "receivingTouchdowns",
            "longReception",
            "receivingTargets",
        ),
        (
            lambda p: _i(p.get("receptions")),
            lambda p: _i(p.get("receptionsYards")),
            lambda p: _d(p.get("receptionsAverage")),
            lambda p: _i(p.get("receptionsTouchdowns")),
            lambda p: _i(p.get("receptionsLong")),
            lambda p: _i(p.get("receptionsPassTarget")),
        ),
        ("receptions", "receptionsPassTarget"),
    ),
    (
        "fumbles",
        ("fumbles", "fumblesLost", "fumblesRecovered"),
        (
            lambda p: _i(p.get("fumbles")),
            lambda p: _i(p.get("fumblesLost")),
            lambda p: _i(_n(p.get("fumblesOwnRecoveries")) + _n(p.get("fumblesOpponentRecoveries"))),
        ),
        ("fumbles", "fumblesLost", "fumblesOwnRecoveries", "fumblesOpponentRecoveries"),
    ),
    (
        "defensive",
        (
            "totalTackles",
            "soloTackles",
            "sacks",
            "tacklesForLoss",
            "passesDefended",
            "QBHits",
            "defensiveTouchdowns",
        ),
        (
            lambda p: _i(_solo(p) + _assists(p)),
            lambda p: _i(_solo(p)),
            lambda p: _half(p.get("defensiveSacks")),
            lambda p: _half(p.get("defensiveTacklesForLoss")),
            lambda p: _i(p.get("defensivePassesDefended")),
            lambda p: _i(p.get("defensiveQuarterbackHits")),
            lambda p: _i(_n(p.get("interceptionsTouchdowns")) + _n(p.get("fumblesOpponentRecoveryTouchdowns"))),
        ),
        (
            "defensiveTackles",
            "defensiveTacklesAssists",
            "defensiveSpecialTeamsTackles",
            "defensiveSpecialTeamsTacklesAssists",
            "defensiveMiscellaneousTackles",
            "defensiveMiscellaneousTacklesAssists",
            "defensiveSacks",
            "defensiveTacklesForLoss",
            "defensivePassesDefended",
            "defensiveQuarterbackHits",
        ),
    ),
    (
        "interceptions",
        ("interceptions", "interceptionYards", "interceptionTouchdowns"),
        (
            lambda p: _i(p.get("defensiveInterceptions")),
            lambda p: _i(p.get("interceptionsYards")),
            lambda p: _i(p.get("interceptionsTouchdowns")),
        ),
        ("defensiveInterceptions",),
    ),
    (
        "kickReturns",
        ("kickReturns", "kickReturnYards", "yardsPerKickReturn", "longKickReturn", "kickReturnTouchdowns"),
        (
            lambda p: _i(p.get("kickReturns")),
            lambda p: _i(p.get("kickReturnsYards")),
            lambda p: _d(p.get("kickReturnsYardsAverage")),
            lambda p: _i(p.get("kickReturnsLongest")),
            lambda p: _i(p.get("kickReturnsTouchdowns")),
        ),
        ("kickReturns",),
    ),
    (
        "puntReturns",
        ("puntReturns", "puntReturnYards", "yardsPerPuntReturn", "longPuntReturn", "puntReturnTouchdowns"),
        (
            lambda p: _i(p.get("puntReturns")),
            lambda p: _i(p.get("puntReturnsYards")),
            lambda p: _d(p.get("puntReturnsYardsAverage")),
            lambda p: _i(p.get("puntReturnsLongest")),
            lambda p: _i(p.get("puntReturnsTouchdowns")),
        ),
        ("puntReturns",),
    ),
    (
        "kicking",
        (
            "fieldGoalsMade/fieldGoalAttempts",
            "fieldGoalPct",
            "longFieldGoalMade",
            "extraPointsMade/extraPointAttempts",
            "totalKickingPoints",
        ),
        (
            lambda p: f"{_i(p.get('fieldGoalsMade'))}/{_i(p.get('fieldGoalsAttempted'))}",
            lambda p: _pct(p.get("fieldGoalsMade"), p.get("fieldGoalsAttempted")),
            lambda p: _i(p.get("fieldGoalsLongestMade")),
            lambda p: f"{_i(p.get('extraPointsMade'))}/{_i(p.get('extraPointsAttempted'))}",
            lambda p: _i(3 * _n(p.get("fieldGoalsMade")) + _n(p.get("extraPointsMade"))),
        ),
        ("fieldGoalsAttempted", "extraPointsAttempted"),
    ),
    (
        "punting",
        ("punts", "puntYards", "grossAvgPuntYards", "touchbacks", "puntsInside20", "longPunt"),
        (
            lambda p: _i(p.get("punts")),
            lambda p: _i(p.get("puntsYards")),
            lambda p: _d(p.get("puntsYardsAverageGross")),
            lambda p: _i(p.get("puntsTouchbacks")),
            lambda p: _i(p.get("puntsInside20")),
            lambda p: _i(p.get("puntsLongest")),
        ),
        ("punts",),
    ),
)

#: ESPN team-box ``name`` -> renderer over one side of the Shield team-statistics payload,
#: in ESPN's own emission order. ``totalDrives`` has no Shield counterpart and is omitted
#: rather than guessed (``parse_espn_team_box`` copies whatever names are present).
_TEAM_STATS: Tuple[Tuple[str, str, Callable[[Mapping[str, Any]], str]], ...] = (
    ("firstDowns", "1st Downs", lambda t: _i(t.get("firstDownsTotal"))),
    ("firstDownsPassing", "Passing 1st downs", lambda t: _i(t.get("firstDownsPassing"))),
    ("firstDownsRushing", "Rushing 1st downs", lambda t: _i(t.get("firstDownsRushing"))),
    ("firstDownsPenalty", "1st downs from penalties", lambda t: _i(t.get("firstDownsPenalty"))),
    (
        "thirdDownEff",
        "3rd down efficiency",
        lambda t: f"{_i(t.get('thirdDownConversions'))}-{_i(t.get('thirdDownAttempts'))}",
    ),
    (
        "fourthDownEff",
        "4th down efficiency",
        lambda t: f"{_i(t.get('fourthDownConversions'))}-{_i(t.get('fourthDownAttempts'))}",
    ),
    ("totalOffensivePlays", "Total Plays", lambda t: _i(t.get("totalPlays"))),
    # Shield's team statistics carry no drive count; it is counted off the adapted drive
    # chart instead (``drive_counts``) and omitted when the caller has none.
    ("totalDrives", "Total Drives", lambda t: _i(t.get("_totalDrives"))),
    ("totalYards", "Total Yards", lambda t: _i(t.get("totalYards"))),
    (
        "yardsPerPlay",
        "Yards per Play",
        lambda t: f"{(_n(t.get('totalYards')) / _n(t.get('totalPlays'))):.1f}" if _n(t.get("totalPlays")) else "0.0",
    ),
    ("netPassingYards", "Passing", lambda t: _i(t.get("passingYards"))),
    (
        "completionAttempts",
        "Comp/Att",
        lambda t: f"{_i(t.get('passingCompletions'))}/{_i(t.get('passingAttempts'))}",
    ),
    ("yardsPerPass", "Yards per pass", lambda t: _d(t.get("passingYardsPerAttempt"))),
    ("interceptions", "Interceptions thrown", lambda t: _i(t.get("passingInterceptions"))),
    (
        "sacksYardsLost",
        "Sacks-Yards Lost",
        lambda t: f"{_i(t.get('passingSacks'))}-{_i(t.get('passingSackYardsLost'))}",
    ),
    ("rushingYards", "Rushing", lambda t: _i(t.get("rushingYards"))),
    ("rushingAttempts", "Rushing Attempts", lambda t: _i(t.get("rushingPlays"))),
    ("yardsPerRushAttempt", "Yards per rush", lambda t: _d(t.get("rushingYardsAverage"))),
    (
        "redZoneAttempts",
        "Red Zone (Made-Att)",
        lambda t: f"{_i(t.get('redZoneSuccesses'))}-{_i(t.get('redZoneAttempts'))}",
    ),
    (
        "totalPenaltiesYards",
        "Penalties",
        lambda t: f"{_i(t.get('penaltiesMade'))}-{_i(t.get('penaltiesYards'))}",
    ),
    (
        "turnovers",
        "Turnovers",
        lambda t: _i(
            _n(t.get("turnovers"))
            if t.get("turnovers") is not None
            else _n(t.get("fumblesLost")) + _n(t.get("passingInterceptions"))
        ),
    ),
    ("fumblesLost", "Fumbles lost", lambda t: _i(t.get("fumblesLost"))),
    ("defensiveTouchdowns", "Defensive / Special Teams TDs", lambda t: _i(t.get("defensiveTouchdowns"))),
    ("possessionTime", "Possession", lambda t: str(t.get("timeOfPossession") or "0:00")),
)


@lru_cache(maxsize=1)
def _gsis_to_espn() -> Dict[str, Tuple[str, str]]:
    """``gsis_id -> (espn_id, full_name)`` from the nflverse players master.

    Cached for the life of the process: this runs on Game on Paper's request path and
    the crosswalk is a whole-league parquet read. A failed load degrades to an empty
    map (every athlete id then null), never a raise.
    """
    try:
        from sportsdataverse.nfl.nfl_players import nfl_players_crosswalk

        xwalk = nfl_players_crosswalk()
    except Exception as exc:  # noqa: BLE001 -- identity is an enrichment, never the game
        logging.debug(f"shield box: players crosswalk unavailable -- {exc}")
        return {}
    out: Dict[str, Tuple[str, str]] = {}
    for row in xwalk.select(["gsis_id", "espn_id", "full_name"]).iter_rows():
        gsis, espn, name = row
        if gsis and espn:
            out[str(gsis)] = (str(espn), str(name) if name else "")
    return out


def _athlete(player: Mapping[str, Any], ids: Mapping[str, Tuple[str, str]]) -> Dict[str, Any]:
    """The ESPN ``athlete`` block for one Shield player row.

    An unmapped ``gsisPlayerId`` keeps a **null** ``id`` and the Shield short name --
    ``NFLPlayProcess.__attach_player_ids`` skips a row with no id, so an unmapped
    player degrades to the pre-Phase-5 behaviour instead of poisoning the name->id map
    with a wrong athlete.
    """
    gsis = player.get("gsisPlayerId")
    espn_id, full_name = ids.get(str(gsis), (None, ""))
    short = player.get("gsisPlayerName") or ""
    return {
        "id": espn_id,
        "displayName": full_name or short,
        "shortName": short,
        "jersey": (player.get("gsisPlayerJerseyNumber") or "").lstrip("0") or None,
        "guid": player.get("personId"),
        "gsisId": str(gsis) if gsis else None,
    }


def _side_players(
    side: Mapping[str, Any],
    team: Mapping[str, Any],
    ids: Mapping[str, Tuple[str, str]],
) -> Dict[str, Any]:
    """One team's ``boxscore.players`` entry: ten categories of athletes with stats."""
    players = side.get("players") or []
    statistics = []
    for name, keys, renderers, predicate in _CATEGORIES:
        athletes = [
            {"athlete": _athlete(p, ids), "stats": [render(p) for render in renderers]}
            for p in players
            if any(_n(p.get(f)) for f in predicate)
        ]
        statistics.append({"name": name, "keys": list(keys), "athletes": athletes})
    return {"team": dict(team), "statistics": statistics}


def shield_boxscore(
    player_stats: Optional[Mapping[str, Any]],
    team_stats: Optional[Mapping[str, Any]],
    teams: Mapping[str, Mapping[str, Any]],
    drive_counts: Optional[Mapping[str, int]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Project the Shield player/team statistics payloads onto ESPN's ``boxscore``.

    Args:
        player_stats: A ``/stats/live/player-statistics/{gameId}`` body
            (``{gameId, offset, awayTeam: {teamId, players[]}, homeTeam: {...}}``),
            or None to leave ``players`` empty.
        team_stats: A ``/stats/live/team-statistics/{gameId}`` body, or None to leave
            ``teams`` empty.
        teams: ``{"homeTeam": <espn team block>, "awayTeam": <espn team block>}`` -- the
            ``{id, abbreviation, displayName, ...}`` dicts the adapter already builds for
            the header. ``id`` must be the **ESPN** team id: it is what
            ``NFLPlayProcess.__attach_player_ids`` keys every name on.
        drive_counts: ``{espn team id: drives}`` off the adapted drive chart -- Shield's team
            statistics carry no drive count. Omitted -> ``totalDrives`` is left out of the
            team box rather than emitted as a 0.

    Returns:
        ``{"teams": [...], "players": [...]}`` in ESPN's own shape and away-then-home
        order, ready to drop into the adapted summary. Either half is an empty list when
        its payload is None.
    """
    ids = _gsis_to_espn()
    out: Dict[str, List[Dict[str, Any]]] = {"teams": [], "players": []}
    sides: Sequence[Tuple[str, str]] = (("awayTeam", "away"), ("homeTeam", "home"))
    for key, home_away in sides:
        team = teams.get(key) or {}
        if not team.get("id"):
            continue
        side = (player_stats or {}).get(key)
        if isinstance(side, Mapping):
            out["players"].append(_side_players(side, team, ids))
        tside = (team_stats or {}).get(key)
        if isinstance(tside, Mapping):
            drives = (drive_counts or {}).get(str(team.get("id")))
            tside = {**tside, "_totalDrives": drives}
            out["teams"].append(
                {
                    "team": dict(team),
                    "homeAway": home_away,
                    "statistics": [
                        {"name": name, "label": label, "displayValue": render(tside)}
                        for name, label, render in _TEAM_STATS
                        if not (name == "totalDrives" and drives is None)
                    ],
                }
            )
    return out


def fetch_shield_box(
    shield_game_id: str,
    *,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Fetch ``(player_statistics, team_statistics)`` for one Shield game, fail-open.

    Two GETs against ``api.nfl.com`` through the shared bearer-token runtime. Each is
    independently fail-open: a box must never cost the game, so a failure returns None
    for that half and the summary simply ships without it (the pre-Phase-5 behaviour).

    Args:
        shield_game_id: The Shield game uuid.
        headers: A :func:`sportsdataverse.nfl.nfl_games.nfl_headers_gen` dict to reuse;
            minted once here and shared across both calls when None.

    Returns:
        ``(player_stats, team_stats)``, either of which may be None.
    """
    from sportsdataverse.nfl.nfl_api_runtime import _get
    from sportsdataverse.nfl.nfl_games import nfl_headers_gen

    base = "https://api.nfl.com/football/v2/stats/live"
    try:
        headers = headers or nfl_headers_gen()
    except Exception as exc:  # noqa: BLE001
        logging.debug(f"shield box: token mint failed -- {exc}")
        return None, None
    fetched: List[Optional[Dict[str, Any]]] = []
    for route in ("player-statistics", "team-statistics"):
        try:
            body = _get(f"{base}/{route}/{shield_game_id}", headers=headers)
            fetched.append(body if isinstance(body, dict) else None)
        except Exception as exc:  # noqa: BLE001 -- the box is an enrichment
            logging.debug(f"shield box: {route} fetch failed -- {exc}")
            fetched.append(None)
    return fetched[0], fetched[1]
