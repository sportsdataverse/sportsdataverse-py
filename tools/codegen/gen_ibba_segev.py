"""Generate the ``ibba_segev`` flat-API endpoint YAML + returns-schemas (Segev Stats, Israeli league play-by-play).

Idempotent: same spec + same fixtures -> byte-identical output. Shares :func:`gen_ibba.emit` (the YAML,
schema and description-snippet writer) with ``gen_ibba.py``. Reads the frozen ``ibba-segev.openapi.yaml`` from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/ibba_segev.yaml`` -- 6 wrappers over the spec's 3 methods (``getGameInfo`` feeds
  ``game_info`` and ``rosters``; ``getBoxScore`` feeds ``player_box``, ``team_box`` and ``period_scores``;
  ``getActions`` feeds ``pbp``). The one spec path takes ``method`` (sent as a fixed parameter per wrapper) and
  ``game_id``; there is no season.
* ``tools/codegen/schemas/native/ibba_segev/<short>.yaml`` -- one returns-schema per wrapper: the columns of its
  parser run over the FULL bodies of Segev game 50 in ``tests/fixtures/ibba/full/`` (dtypes are pinned by name in
  ``ibba_segev_parsers.SPECS`` regardless). Descriptions stay blank here.
* ``tools/codegen/ibba_segev_column_descriptions.snippet.yaml`` -- the block for ``manual_column_descriptions.yaml``
  (gitignored).
* ``tests/fixtures/ibba/spec_paths_segev.json`` -- the spec's path and ``method`` enum, for the tests.

Run: ``python tools/codegen/gen_ibba_segev.py``
"""

from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from gen_ibba import FIXTURES, ROOT, emit
from gen_ibba import LOCAL as ASMX_LOCAL

from sportsdataverse.basketball.ibba import ibba_segev_parsers as P

HOST = "https://stats.segevstats.com"
PATH = "/realtimestat_heb/api/"
SPEC_PATH = "/api/"  # the spec path under its server ``https://stats.segevstats.com/realtimestat_heb``
SPEC = "ibba-segev.openapi.yaml"
SNIPPET = ROOT / "tools" / "codegen" / "ibba_segev_column_descriptions.snippet.yaml"
SPEC_PATHS = FIXTURES / "spec_paths_segev.json"
GAME = 50

ACCESS = (
    "Access: Segev Stats is the live-stats vendor behind basket.co.il's game "
    "pages; its JSON-RPC API is keyless, unofficial and not documented for outside use. The wrappers space requests "
    "at least one second apart and read only finished games; payloads are not redistributed as release assets. "
    "The host serves JSON as text/html."
)
IDS = (
    "Games and ids. ``game_id`` is one flat Segev id space across seasons AND competitions: men's Winner League, "
    "Winner Cup, women's, Leumit, youth, school and pre-season games share it, so read ``competition`` from "
    "``ibba_segev_game_info`` (team names do not separate men from women reliably). There is no game list: league "
    "game ids come from ``ibba_games.segev_game_id`` (2025-26 on), the women's league and Leumit from a scan or "
    "Genius Sports org IBBA. Segev team and player ids are stable across seasons (Jaylen Hoard = player 1000 in "
    "games 50 and 419), unlike the IBBA season registrations, and are not IBBA ids."
)
CAVEATS = (
    "Play-by-play: actions name the player by Segev id and by JERSEY number, ``score`` reads away-home, and shot "
    "``x`` / ``y`` are one half-court frame (x 0-1470 across, basket near 735; y out from the baseline, rim near "
    "112-140); which sideline is x = 0 and the metric scale are unverified. Segev renamed its team game stats "
    "between 2025-26 and 2026-27; the parsers unify them. Unfinished games and ids of another game are refused."
)
GAME_ID = "Required. Segev game id (e.g. 50 = Hapoel Be'er Sheva vs Maccabi Tel Aviv, 2025-26 round 1): ``segev_game_id`` in ibba_games."

Entry = Tuple[str, Any, str, list, Dict[str, Any]]
_GAME_PARAM = [{"name": "game_id", "query_key": "game_id", "type": "int", "required": True, "description": GAME_ID}]


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


#: short -> (spec path, fixture loader, summary, extra_params, example_args) plus the method in METHOD
ENDPOINTS: Dict[str, Entry] = {
    "game_info": (
        PATH,
        lambda: _full("getGameInfo"),
        "Game header: tip-off, competition, both teams and the game state (scores are in the box score).",
        _GAME_PARAM,
        {"game_id": GAME},
    ),
    "rosters": (
        PATH,
        lambda: _full("getGameInfo"),
        "Both teams' game rosters with Segev player ids and names in English and Hebrew.",
        _GAME_PARAM,
        {"game_id": GAME},
    ),
    "player_box": (
        PATH,
        lambda: _full("getBoxScore"),
        "Per-player box score of a finished game, plus a team-credited line per side.",
        _GAME_PARAM,
        {"game_id": GAME},
    ),
    "team_box": (
        PATH,
        lambda: _full("getBoxScore"),
        "Team totals of both sides: final score, timeouts, lead stats, shooting, rebounds and team game stats.",
        _GAME_PARAM,
        {"game_id": GAME},
    ),
    "period_scores": (
        PATH,
        lambda: _full("getBoxScore"),
        "Points and team fouls in each period of a finished game.",
        _GAME_PARAM,
        {"game_id": GAME},
    ),
    "pbp": (
        PATH,
        lambda: _full("getActions"),
        "Play-by-play of a finished game: every action with shot coordinates, running scores and jersey numbers.",
        _GAME_PARAM,
        {"game_id": GAME},
    ),
}
#: short -> the JSON-RPC method the wrapper sends as a fixed parameter.
METHOD = {
    "game_info": "getGameInfo",
    "rosters": "getGameInfo",
    "player_box": "getBoxScore",
    "team_box": "getBoxScore",
    "period_scores": "getBoxScore",
    "pbp": "getActions",
}

_SIDE = (
    "Side of the game: home or away (the feed's homeTeam / awayTeam; an action's parameters.team 1 = home, 2 = away)."
)
LOCAL: Dict[str, str] = {
    "game_id": "Segev game id (key space: one flat sequence across seasons and competitions); equals the IBBA "
    "ibba_games segev_game_id from 2025-26. Not an IBBA game id.",
    "start_time": "Scheduled tip-off in Israel local time as a naive datetime (the feed has no offset; it can differ "
    "from the IBBA tip_off by minutes).",
    "competition": "Competition name ('Winner League', 'Winner Cup', 'Women', 'Leumit', 'Noar', 'School', 'Preparation "
    "Games'): the reliable men's / women's discriminator.",
    "home_team_id": "Home Segev team id (key space: Segev teams, stable across seasons and competitions; Maccabi Tel "
    "Aviv = 2). Not an IBBA team-season id.",
    "home_team": "Home team name in English, upper case; some women's teams end in ' (W)', not all.",
    "home_team_local": "Home team name in Hebrew (Latin sponsor words kept).",
    "home_short_name": "Home team short name in English, upper case.",
    "away_team_id": "Away Segev team id (key space: Segev teams, stable across seasons and competitions).",
    "away_team": "Away team name in English, upper case.",
    "away_team_local": "Away team name in Hebrew (Latin sponsor words kept).",
    "away_short_name": "Away team short name in English, upper case.",
    "periods": "Regulation periods in the game (4).",
    "finished": "True once the game is final (the wrappers read finished games only).",
    "current_period": "Current period, the last one played for a finished game (5+ means overtime).",
    "clock": "Game clock remaining as 'mm:ss' ('00:00' for a finished game's header; a play's clock in pbp).",
    "team_side": _SIDE,
    "team_id": "Segev team id of the side (key space: Segev teams, stable across seasons). Not an IBBA team-season id; "
    "null on game-level actions.",
    "player_id": "Segev player id (key space: Segev players, stable across seasons: Jaylen Hoard = 1000 in games 50 "
    "and 419); null on team lines and on actions without a player. Not an IBBA player-season id.",
    "jersey_number": "Jersey number as an integer.",
    "first_name": "First name in English, upper case.",
    "last_name": "Last name in English, upper case.",
    "first_name_local": "First name in Hebrew.",
    "last_name_local": "Last name in Hebrew.",
    "in_roster": "Roster flag, 0 or 2 in the samples; meaning unverified.",
    "api_id": "External player id as an integer: null or equal to the Segev player_id (both occur within one game); its "
    "source is unverified.",
    "row_type": "player (one player's line) or team (actions credited to the team and no player, such as team "
    "rebounds and turnovers).",
    "starter": "True when the player was in the starting five.",
    "on_court": "True when the player was on court at the final buzzer or the feed's last state.",
    "minutes": "Time played as 'mm:ss'.",
    "seconds_played": "Time played in seconds, derived from minutes.",
    "plus_minus": "Plus-minus while the player was on court, signed.",
    "fg2m": "Two-point field goals made.",
    "fg2_missed": "Two-point field goals MISSED (not attempted; attempts = made + missed, verified against the IBBA "
    "box score).",
    "fg3m": "Three-point field goals made.",
    "fg3_missed": "Three-point field goals MISSED (not attempted).",
    "ftm": "Free throws made.",
    "ft_missed": "Free throws MISSED (not attempted).",
    "dreb": "Defensive rebounds.",
    "oreb": "Offensive rebounds.",
    "fouls": "Fouls committed (the feed's f; equals the IBBA fouls).",
    "fouls_drawn": "Fouls drawn (the feed's f_drawn; equals the IBBA fouls_drawn).",
    "steals": "Steals made by the player or team.",
    "turnovers": "Turnovers committed by the player or team.",
    "assists": "Assists credited to the player or team.",
    "blocks": "Shots the player or team blocked.",
    "blocks_against": "Own shots that were blocked.",
    "fast_break_pts": "Fast-break points; for a team total it equals the P of the feed's 'P of N' fast-break stat.",
    "second_chance_pts": "Second-chance points (scored after an offensive rebound).",
    "pts": "Points scored by the player or side (free throws included).",
    "val": "Efficiency rating (the feed's rate; equals the IBBA VAL).",
    "deflections": "Deflections (count of deflection actions).",
    "recoveries": "The feed's rec count; unverified, probably loose-ball recoveries.",
    "dunks": "Dunks made by the player or side.",
    "offensive_fouls_drawn": "Offensive fouls drawn (charges taken); inferred.",
    "and_ones": "And-one plays (a made basket plus a foul); inferred.",
    "vps": "The feed's vps ratio with two decimals; its definition is unverified (it matches neither points per shot "
    "nor rate per minute).",
    "fg2a": "Two-point field goals attempted, derived as made + missed.",
    "fg3a": "Three-point field goals attempted, derived as made + missed.",
    "fta": "Free throws attempted, derived as made + missed.",
    "score": "Final score of the side (homeScore / awayScore).",
    "timeouts": "Timeouts the side took (the feed's homeTimeOut / awayTimeOut).",
    "time_leading": "Time the side led as 'mm:ss'.",
    "biggest_lead": "The side's biggest lead in points.",
    "times_tied": "Times the score was tied (same value for both sides).",
    "lead_changes": "Lead changes (same value for both sides).",
    "pts_from_turnovers": "Points scored off opponent turnovers (the feed renamed the key in 2026-27).",
    "pts_in_paint": "Points scored in the paint (the feed renamed the key in 2026-27).",
    "starting_points": "Points scored by the starting five.",
    "bench_points": "Points scored by the bench.",
    "fast_break_possessions": "The N of the feed's 'P of N' fast-break stat (fast-break possessions or attempts); "
    "unverified. Null before 2026-27 where the key differs.",
    "defensive_stops": "Defensive stops; the feed adds it in 2026-27, null before.",
    "biggest_scoring_run": "Biggest scoring run as the feed prints it ('23-0 (36-6)'); 2026-27 on, null before.",
    "period": "Period number (1-4 regulation, 5+ overtime).",
    "home_points": "Points the home side scored IN this period (not cumulative).",
    "away_points": "Points the away side scored IN this period (not cumulative).",
    "home_fouls": "Team fouls the home side committed in this period.",
    "away_fouls": "Team fouls the away side committed in this period.",
    "action_id": "Action id (key space: Segev actions; unique and increasing within one game, 500001 ... for game 50).",
    "parent_action_id": "Id of the action this one follows from (rebound -> missed shot, steal -> turnover, assist or "
    "block -> shot); null when none.",
    "user_time_utc": "Wall-clock time the scorer logged the action, 'HH:MM:SS', UTC (inferred: game 50 tipped at 18:40 "
    "Israel time and its first tick reads 15:42).",
    "type": "Action type: game, quarter, clock, substitution, shot, freeThrow, rebound, assist, steal, turnover, block, "
    "deflection, foul, foul-drawn or timeout.",
    "subtype": "Subtype by type: shot jump-shot / lay-up / dunk; rebound offensive / defensive; turnover bad-pass, "
    "ball-handling, travelling, ...; foul personal / technical / unsportsmanlike; start-of-game, end-of-quarter, ...",
    "jersey": "Jersey number (a string, not an id) of the acting player; null when none (team rebound, timeout).",
    "score_away": "Away running score after a scoring action (the feed's score reads away-home); null otherwise.",
    "score_home": "Home running score after a scoring action; null otherwise.",
    "player_points": "The scorer's running points after a scoring action; null otherwise.",
    "points": "Shot value, 2 or 3; null on other actions.",
    "made": "Shot or free-throw result: made, missed, or blocked (shots only).",
    "x": "Shot x: across the half court, sideline to sideline, 0-1470, basket near 735; which sideline is 0 and the "
    "metric scale are unverified.",
    "y": "Shot y: distance out from the baseline, rim near 112-140, deepest observed 1176; metric scale unverified.",
    "fast_break": "True when the shot or free throw came on a fast break.",
    "second_chance": "True when the attempt followed an offensive rebound.",
    "points_from_turnover": "True when the attempt came off an opponent turnover.",
    "free_throw_number": "Which free throw of the set (1, 2, 3).",
    "free_throws_awarded": "Free throws in the set.",
    "foul_kind": "Foul kind, shooting or offensive; absent on some fouls. 'shooting' also appears on technical and "
    "offensive-unsportsmanlike fouls, so it does not prove a shooting foul.",
    "fouled_on_jersey": "Jersey number (string) of the fouled player.",
    "foul_free_throws": "Free throws the foul awards (the feed sends a string when non-zero, 0 otherwise).",
    "is_coach_foul": "True on a foul charged to the coach or bench (player_id null); null otherwise.",
    "player_in_jersey": "Substitution: jersey of the player coming in; null on the row that records the player going out.",
    "player_out_jersey": "Substitution: jersey of the player going out; null on the row that records the player coming in.",
}
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "pbp": {
        "game_id": LOCAL["game_id"],
        "team_id": "Segev team id of the acting side (stable across seasons); null on game-level actions.",
        "clock": "Game clock remaining in the period as 'mm:ss', counting down from 10:00.",
    },
    "rosters": {"team_id": "Segev team id of the roster's side (stable across seasons)."},
    "game_info": {"clock": "Game clock remaining as 'mm:ss' ('00:00' for a finished game)."},
}
# the ASMX box vocabulary is reused by name where the meaning is identical
for _k in ("steals", "assists", "turnovers", "dunks"):
    assert _k in ASMX_LOCAL


def describe(short: str, col: str) -> str:
    return LOCAL_BY.get(short, {}).get(col) or LOCAL.get(col, "")


def main() -> None:
    def check(spec: Dict[str, Any]) -> None:
        op = spec["paths"][SPEC_PATH]["get"]
        method = next(p for p in op["parameters"] if p["name"] == "method")["schema"]["enum"]
        if set(method) != set(METHOD.values()):
            raise SystemExit(
                f"ibba_segev: spec methods {sorted(method)} differ from METHOD {sorted(set(METHOD.values()))}"
            )
        if set(spec["paths"]) != {SPEC_PATH}:
            raise SystemExit(f"ibba_segev: spec paths differ: {sorted(spec['paths'])}")
        SPEC_PATHS.write_text(json.dumps({SPEC_PATH: sorted(method)}, indent=1) + "\n", encoding="utf-8", newline="\n")

    doc = {
        "api": "ibba_segev",
        "host": HOST,
        "name_pattern": "ibba_segev_{short}",
        "module": "ibba_segev",
        "parser_module": "basketball.ibba.ibba_segev_parsers",
        "getter_module": "sportsdataverse.basketball.ibba.ibba_segev_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": "ibba_segev_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw JSON-RPC envelope (``dict``): ``result`` with the method's block plus ``_request`` "
            "(the method and game id sent)",
            "raises": [
                "sportsdataverse.errors.NoDataError: Segev has nothing for the request (HTTP 404, 'game not found', "
                "a finished game with no actions).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a non-200, an error or changed envelope, a body for another game, a game that is not final).",
            ],
            "notes": [ACCESS, IDS, CAVEATS],
            "see_also": [
                {
                    "name": "Segev Stats",
                    "url": "https://stats.segevstats.com",
                    "note": "the live-stats vendor behind the Israeli league game pages",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
            ],
        },
    }
    emit(
        stem="ibba_segev",
        spec_file=SPEC,
        refs_source="ibba",
        doc=doc,
        endpoints=ENDPOINTS,
        specs=P.SPECS,
        describe_fn=describe,
        parser_prefix="parse_ibba_segev_",
        snippet=SNIPPET,
        frames=lambda short, entry: getattr(P, f"parse_ibba_segev_{short}")(entry[1]()),
        check_routes=check,
        fixed={short: {"method": method} for short, method in METHOD.items()},
    )


if __name__ == "__main__":
    main()
