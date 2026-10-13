"""Generate the ``genius`` flat-API endpoint YAML + returns-schemas (Genius Sports / FIBA LiveStats).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_vhl.py``. Reads the frozen spec
and the returns md from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling
checkout) and emits:

* ``tools/codegen/endpoints/genius.yaml`` -- 11 wrappers over the spec's 7 routes. One ``data.json`` feeds five
  frames (game, team box, player box, play-by-play, shots), each with its own parser; the hosted competition
  schedule has a plain and an ``embednf`` route sharing one parser. The org and competition are parameters
  (``org``, ``comp_id``), the match is ``match_id``; the spec's ``compId`` / ``matchId`` are renamed snake_case.
  The widget key rides in the path (``/widget/?{widget_key}``): the site widget is keyed by a bare query string.
* ``tools/codegen/schemas/native/genius/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser
  over a committed fixture (``tests/fixtures/genius``): the FULL ``data.json`` body for the five game frames
  (numeric dtypes come from full bodies, never from the 3-row refs captures), the refs captures or real saved
  pages for the rest. Descriptions are blank (they never live in ``schemas/**``).
* ``tools/codegen/genius_column_descriptions.snippet.yaml`` -- ``{genius_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (gitignored; the integrator merges it).
* ``tests/fixtures/genius/spec_routes.json`` -- the spec's routes, so the tests read the route list without the
  private refs checkout.

Run: ``python tools/codegen/gen_genius.py``
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.genius import genius_parsers as P

HOST = "https://fibalivestats.dcd.shared.geniussports.com"
HOSTED = "https://hosted.dcd.shared.geniussports.com"
WIDGET = "https://widget.wh.geniussports.com"
SPEC = "genius-livestats.openapi.yaml"
SNIPPET = ROOT / "tools" / "codegen" / "genius_column_descriptions.snippet.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "genius"
ROUTES_SNAPSHOT = FIXTURES / "spec_routes.json"

NOTES = [
    "Access (sdv-internal-refs/genius-livestats/README.md): the Genius hosts publish no robots.txt and no terms. "
    "CloudFront refuses datacenter IPs on the LiveStats and hosted hosts: from a server set the optional "
    "``GENIUS_PROXY`` environment variable to an http(s) proxy URL (it is never logged or printed). The wrappers "
    "space requests at least one second apart and read only the Genius hosts; keep crawls slow and incremental. "
    "Some league sites (the NZ NBL) forbid AI crawlers in their own robots.txt, which does not cover these hosts.",
    "Org and season. Every competition is an ORG code plus a competition id (``comp_id``): ``genius_competitions`` "
    "lists an org's competitions with the sdv END-year ``season`` (a START-year org lists 2025-26 as season 2026), "
    "``genius_match_competition`` maps a game to its competition, and ``data.json`` itself names no date, "
    "competition or season. The hosted ORG can differ from the LiveStats ORG of ``/u/`` (NZNBL vs NZN). "
    "Match ids are one global sequence across every Genius customer.",
    "``data.json`` carries no global team or player id: ``tno`` (1 or 2) and ``pno`` are in-game numbers. Join "
    "players across games on name, team code and shirt number; global team ids come from the hosted schedule, the "
    "widget or ``genius_competition_games``. Only finished games are read.",
]


def _p(name: str, typ: str, desc: str) -> Dict[str, Any]:
    return {"name": name, "type": typ, "required": True, "description": desc}


MATCH_ID = (
    "Genius match id (match_id in genius_schedule, genius_widget_games or genius_competition_games), e.g. 2902690."
)
COMP_ID = "Genius competition id: one per org competition-season (comp_id in genius_competitions), e.g. 49915."
ORG = "Genius ORG code of the league (ESAKE, CEBL, BNXT, WBBL, ...); see genius_competitions."
_ROUND = {
    "name": "round_number",
    "query_key": "roundNumber",
    "type": "int",
    "default": -1,
    "description": "Round to list; -1 (default) lists every round of the competition.",
}

_DATA = {"path_params": [_p("match_id", "int", MATCH_ID)], "example_args": {"match_id": 2902690}}
_SCHEDULE = {
    "path_params": [_p("org", "str", ORG), _p("comp_id", "int", COMP_ID)],
    "extra_params": [_ROUND],
}

#: short -> (spec path, wrapper path, parser, frame key, summary, fields)
ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "game": dict(
        spec="/data/{matchId}/data.json",
        path="/data/{match_id}/data.json",
        parser=P.parse_genius_game,
        frame="game",
        summary="One row per finished game: period and clock state, both teams' names, codes and scores, officials.",
        fields=_DATA,
    ),
    "team_box": dict(
        spec="/data/{matchId}/data.json",
        path="/data/{match_id}/data.json",
        parser=P.parse_genius_team_box,
        frame="team_box",
        summary="Team box score of a finished game: score by period, fouls, timeouts and the team totals.",
        fields=_DATA,
    ),
    "player_box": dict(
        spec="/data/{matchId}/data.json",
        path="/data/{match_id}/data.json",
        parser=P.parse_genius_player_box,
        frame="player_box",
        summary="Player box score of a finished game: minutes, shooting, rebounds, assists and the efficiency formulas.",
        fields=_DATA,
    ),
    "pbp": dict(
        spec="/data/{matchId}/data.json",
        path="/data/{match_id}/data.json",
        parser=P.parse_genius_pbp,
        frame="pbp",
        summary="Play-by-play of a finished game, newest action first, with the running score and lead.",
        fields=_DATA,
    ),
    "shots": dict(
        spec="/data/{matchId}/data.json",
        path="/data/{match_id}/data.json",
        parser=P.parse_genius_shots,
        frame="shots",
        summary="Shot chart of a finished game: court x/y (percent of the full court), result and shooter.",
        fields=_DATA,
    ),
    "competition_games": dict(
        spec="/data/competition/{compId}.json",
        path="/data/competition/{comp_id}.json",
        parser=P.parse_genius_competition_games,
        frame="competition_games",
        summary="The recent-games window of a competition, with the global Genius team ids.",
        fields={"path_params": [_p("comp_id", "int", COMP_ID)], "example_args": {"comp_id": 49915}},
    ),
    "match_competition": dict(
        spec="/u/{org}/{matchId}/",
        path="/u/{org}/{match_id}/",
        parser=P.parse_genius_match_competition,
        frame="match_competition",
        summary="The competition (comp_id and name) a game belongs to, from its LiveStats HTML shell.",
        fields={
            "path_params": [
                _p("org", "str", "LiveStats ORG of the game's shell (can differ from the hosted ORG: NZN vs NZNBL)."),
                _p("match_id", "int", MATCH_ID),
            ],
            "example_args": {"org": "ESAKE", "match_id": 2902690},
        },
    ),
    "competitions": dict(
        spec="/{org}/en/schedule",
        path="/{org}/en/schedule",
        parser=P.parse_genius_competitions,
        frame="competitions",
        summary="Every competition of an org from the hosted WebStats chooser, with its END-year season.",
        host=HOSTED,
        fields={"path_params": [_p("org", "str", ORG)], "example_args": {"org": "ESAKE"}},
    ),
    "schedule": dict(
        spec="/{org}/en/competition/{compId}/schedule",
        path="/{org}/en/competition/{comp_id}/schedule",
        parser=P.parse_genius_schedule,
        frame="schedule",
        summary="Every match of a competition (hosted WebStats): ids, status, venue, teams and scores.",
        host=HOSTED,
        fields={**_SCHEDULE, "example_args": {"org": "CEBL", "comp_id": 47852}},
    ),
    "schedule_embed": dict(
        spec="/embednf/{org}/en/competition/{compId}/schedule",
        path="/embednf/{org}/en/competition/{comp_id}/schedule",
        parser=P.parse_genius_schedule,
        frame="schedule_embed",
        summary="Every match of a competition from the JSON-wrapped embed page (orgs whose plain pages answer 500).",
        host=HOSTED,
        fields={**_SCHEDULE, "example_args": {"org": "HKS", "comp_id": 49946}},
    ),
    "widget_games": dict(
        spec="/widget/",
        path="/widget/?{widget_key}",
        parser=P.parse_genius_widget_games,
        frame="widget_games",
        summary="The latest games a league site's results widget shows (needs no proxy).",
        host=WIDGET,
        fields={
            "path_params": [
                _p(
                    "widget_key",
                    "str",
                    "The league site's public widget key (the bare query string of widget.wh.geniussports.com/widget/).",
                )
            ],
            "example_args": {"widget_key": "RFJIN037I07QV520B7WEXKV1GYNA31"},
        },
    ),
}

#: Descriptions of columns the parsers derive or stamp (they win over the returns md).
LOCAL: Dict[str, str] = {
    "match_id": "Genius match id of the game. data.json does not carry it: it is the match_id asked for, stamped by the "
    "runtime (the global match sequence shared by every Genius customer).",
    "seconds_played": "Seconds played parsed from the printed mm:ss text as minutes * 60 + seconds (so '224:60' gives "
    "13500); null when the feed sent an integer, whose unit is unknown.",
    "row_index": "Position of the action in the feed order, 0 = the newest action by game time (normally the game end "
    "row). The feed is not strictly sorted by action_number, so sort on action_number for entry order.",
    "season": "sdv END-year season of the competition (2026 = 2025-26) from the label span, else the Genius year; null "
    "when no rule applies. See season_rule.",
    "season_rule": "Which rule produced season: 'label span', 'genius year + 1 (split)', 'genius year (calendar)', "
    "'label year (calendar)' or 'unknown'.",
    "selected": "True for the competition the hosted chooser pre-selects (the org's default, not always the newest).",
    "org": "ORG code the request used (the hosted ORG, or the LiveStats ORG for the /u/ shell).",
    "team1_name": "Full name of team 1, the team listed first (the home team on the hosted schedule and widget).",
    "team1_code": "Short code of team 1, in the competition's own script when the org enters one.",
    "team1_score": "Final points of team 1 (cast to an integer; the 2014-15 feed sends a string).",
    "team2_name": "Full name of team 2, the team listed second.",
    "team2_code": "Short code of team 2, in the competition's own script when the org enters one.",
    "team2_score": "Final points of team 2 (cast to an integer; the 2014-15 feed sends a string).",
    "referee1": "First referee's name, from officials.referee1 (international form) or the flat officials_referee1 text.",
    "referee2": "Second referee's name, from officials.referee2 (international form) or the flat officials_referee2 text.",
    "referee3": "Third referee's name when the feed lists one; null otherwise.",
    "commissioner": "Commissioner (game supervisor) name when the feed lists one; null otherwise.",
    "tno": "Team number within this game: 1 = the team listed first (home on the hosted schedule and widget), 2 = the "
    "other, 0 = no team. In-game only: not a team id.",
    "pno": "Player number within this game, the key of the team's player map; 0 = no player. In-game only: not a "
    "person id and not the shirt number.",
    "minutes": "Minutes as the feed prints them, an 'mm:ss' text (can exceed 59 for a team; '199:60' happens); the "
    "text of an integer when the feed sends one.",
    "ot_score": "Points the team scored in overtime; null without overtime. Whether several overtimes are summed "
    "is unverified.",
    "qualifier": "Qualifier codes of the action joined with commas (pointsinthepaint, fromturnover, 2ndchance, "
    "fastbreak, blocked, ...); null when none.",
    "score_1": "Team 1 score after the action, always an integer (the feed sends a string in most games).",
    "score_2": "Team 2 score after the action, always an integer (the feed sends a string in most games).",
    "result": "Shot result: 1 = made, 0 = missed.",
    "period": "Period number; restarts at 1 for the first overtime (read it with period_type).",
    "period_type": "Period type: REGULAR or OVERTIME.",
    "action_number": "Action sequence number, unique within the game (gaps where actions were deleted); a shot joins "
    "its play-by-play row on it. Null in the 2014-15 feed.",
    "previous_action": "action_number of the linked earlier action (the shot an assist or rebound belongs to); null "
    "when the feed sends an empty string.",
}

#: frame -> {column: path in the returns md} for the columns whose name is not the wire name snake-cased.
_TOP = {
    "period": "period",
    "period_type": "periodType",
    "in_ot": "inOT",
    "clock": "clock",
    "periods_max": "periodsMax",
    "period_length_regular": "periodLengthREGULAR",
    "period_length_overtime": "periodLengthOVERTIME",
    "attendance": "attendance",
    "disable_match": "disableMatch",
    "total_time_added": "totalTimeAdded",
}
_TEAM = {
    "name": "name",
    "name_international": "nameInternational",
    "short_name": "shortName",
    "code": "code",
    "code_international": "codeInternational",
    "coach": "coach",
    "score": "score",
    "full_score": "full_score",
    "p1_score": "p1_score",
    "p2_score": "p2_score",
    "p3_score": "p3_score",
    "p4_score": "p4_score",
    "fouls": "fouls",
    "timeouts": "timeouts",
}
_PLAYER = {
    "name": "name",
    "first_name": "firstName",
    "family_name": "familyName",
    "international_first_name": "internationalFirstName",
    "international_family_name": "internationalFamilyName",
    "scoreboard_name": "scoreboardName",
    "shirt_number": "shirtNumber",
    "playing_position": "playingPosition",
    "starter": "starter",
    "captain": "captain",
    "active": "active",
}
_PBP = {
    "gt": "gt",
    "clock": "clock",
    "player": "player",
    "shirt_number": "shirtNumber",
    "first_name": "firstName",
    "family_name": "familyName",
    "international_first_name": "internationalFirstName",
    "international_family_name": "internationalFamilyName",
    "scoreboard_name": "scoreboardName",
    "action_type": "actionType",
    "sub_type": "subType",
    "success": "success",
    "scoring": "scoring",
    "lead": "lead",
}
_SHOT = {
    "action_type": "actionType",
    "sub_type": "subType",
    "x": "x",
    "y": "y",
    "player": "player",
    "shirt_number": "shirtNumber",
    "period": "per",
    "period_type": "perType",
    "result": "r",
}
_WINDOW = {
    "league_id": "leagueId",
    "competition_id": "competitionId",
    "match_id": "matchId",
    "match_status": "matchStatus",
    "match_time": "matchTime",
    "match_time_utc": "matchTimeUTC",
    "live": "live",
    "period": "period",
    "period_type": "periodType",
    "clock": "clock",
    "home_team_id": "hometeamId",
    "home_name": "homename",
    "home_name_international": "homenameInternational",
    "home_code": "homecode",
    "home_score": "homescore",
    "away_team_id": "awayteamId",
    "away_name": "awayname",
    "away_name_international": "awaynameInternational",
    "away_code": "awaycode",
    "away_score": "awayscore",
}

_FILLER = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)
_HEADING = re.compile(r"^## `https://[^/]+(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route path: {record path: description}}`` from the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    section: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            section = h.group(1)
            continue
        if line.startswith("## "):
            section = None
        m = _ROW.match(line)
        if m and section:
            out.setdefault(section, {}).setdefault(m.group(1), m.group(2).strip())
    return out


def _wire_path(frame: str, col: str) -> Optional[str]:
    """The returns-md record path a column of ``frame`` reads, or ``None`` when it is derived."""
    if frame == "game":
        return _TOP.get(col)
    if frame == "team_box":
        if col in _TEAM:
            return f"tm.<tno>.{_TEAM[col]}"
        return P._team_wire(col) if col not in {"match_id", "tno", "ot_score", "minutes", "seconds_played"} else None
    if frame == "player_box":
        if col in _PLAYER:
            return f"tm.<tno>.pl.<pno>.{_PLAYER[col]}"
        return P._player_wire(col) if col not in {"match_id", "tno", "pno", "minutes", "seconds_played"} else None
    if frame == "pbp":
        return f"pbp.[].{_PBP[col]}" if col in _PBP else None
    if frame == "shots":
        return f"tm.<tno>.shot.[].{_SHOT[col]}" if col in _SHOT else None
    if frame == "competition_games":
        return f"[].{_WINDOW[col]}"
    if frame == "match_competition":
        return col if col in {"match_id", "comp_id", "competition_name", "org_label", "og_description"} else None
    if frame == "competitions":
        return f"competitions.[].{col}" if col in {"comp_id", "label", "league_id", "year"} else None
    if frame in ("schedule", "schedule_embed"):
        return f"matches.[].{col}"
    if frame == "widget_games":
        return f"matches.[].{col}"
    return None


#: The pinned md paths for a few columns whose record path differs from the parser column name.
_PATH_FIX = {("pbp", "score_1"): "pbp.[].s1", ("pbp", "score_2"): "pbp.[].s2", ("shots", "pno"): "tm.<tno>.shot.[].pno"}

#: frame -> returns-md route heading.
_SECTION = {
    "game": "/data/{matchId}/data.json",
    "team_box": "/data/{matchId}/data.json",
    "player_box": "/data/{matchId}/data.json",
    "pbp": "/data/{matchId}/data.json",
    "shots": "/data/{matchId}/data.json",
    "competition_games": "/data/competition/{compId}.json",
    "match_competition": "/u/{org}/{matchId}/",
    "competitions": "/{org}/en/schedule",
    "schedule": "/{org}/en/competition/{compId}/schedule",
    "schedule_embed": "/embednf/{org}/en/competition/{compId}/schedule",
    "widget_games": "/widget/",
}


#: Longer text for columns whose md row is too terse to stand alone (under 15 characters).
_SHORT: Dict[str, str] = {
    "short_name": "Short team name (shortName in the feed), in the competition's own script when the org enters one.",
    "rebounds_total": "Total rebounds (offensive plus defensive) the player collected in the game.",
    "assists": "Assists credited to the player in the game.",
    "turnovers": "Turnovers committed by the player in the game.",
    "steals": "Steals credited to the player in the game.",
    "blocks": "Shots the player blocked in the game (blocks made, not received).",
    "fouls_on": "Fouls drawn: personal fouls committed against the player in the game.",
    "points": "Points the player scored in the game, free throws included.",
    "player": "Shooter's display name as the org enters it, in the local script when it uses one.",
    "home_name": "Home team full name, in the competition's own script when the org enters one.",
    "home_code": "Home team short code, in the competition's own script when the org enters one.",
    "away_name": "Away team full name, in the competition's own script when the org enters one.",
    "away_code": "Away team short code, in the competition's own script when the org enters one.",
    "home_team_code": "Home team short code from the widget, in the local script when the org enters one.",
    "away_team_code": "Away team short code from the widget, in the local script when the org enters one.",
}
_DATA_FRAMES = frozenset({"game", "team_box", "player_box", "pbp", "shots"})
#: Columns of the game frames whose LOCAL text beats the md row (derived, stamped, or terser in the md).
_LOCAL_FIRST = frozenset(
    {
        "match_id",
        "seconds_played",
        "row_index",
        "minutes",
        "ot_score",
        "qualifier",
        "score_1",
        "score_2",
        "previous_action",
        "result",
        "tno",
        "pno",
        "action_number",
        "period",
        "period_type",
        "referee1",
        "referee2",
        "referee3",
        "commissioner",
        "team1_name",
        "team1_code",
        "team1_score",
        "team2_name",
        "team2_code",
        "team2_score",
    }
)


#: Player box-score counts whose md row only restates the column name: say whose count it is and over what.
_PLAYER_BOX_TEXT: Dict[str, str] = {
    "field_goals_made": "Field goals the player made in the game (two- and three-pointers together; a count).",
    "field_goals_attempted": "Field goals the player attempted in the game (two- and three-pointers together; a count).",
    "free_throws_made": "Free throws the player made in the game (a count).",
    "free_throws_attempted": "Free throws the player attempted in the game (a count).",
    "points_in_the_paint": "Points the player scored in the paint in the game (the feed's sPointsInThePaint).",
}


def _describe(frame: str, col: str, md: Dict[str, Dict[str, str]]) -> str:
    if frame == "player_box" and col in _PLAYER_BOX_TEXT:
        return _PLAYER_BOX_TEXT[col]
    if frame in _DATA_FRAMES and col in _LOCAL_FIRST and col in LOCAL:
        # the game header's period / period_type are top-level wire fields the md describes well
        if not (frame == "game" and col in {"period", "period_type"}):
            return LOCAL[col]
    path = _PATH_FIX.get((frame, col)) or _wire_path(frame, col)
    text = md.get(_SECTION[frame], {}).get(path or "", "")
    if len(text) >= 15 and not _FILLER.match(text):
        return text
    return _SHORT.get(col) or LOCAL.get(col, text)


def _data_body(name: str) -> Dict[str, Any]:
    path = FIXTURES / "full" / name
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
        body = json.load(fh)
    body["_request"] = {"match_id": int(re.search(r"data_(\d+)", name).group(1))}  # type: ignore[union-attr]
    return body


def _capture(name: str, **stamp: Any) -> Any:
    rec = json.loads((FIXTURES / "captures" / name).read_text(encoding="utf-8"))
    if stamp and isinstance(rec, dict):
        rec["_request"] = stamp
    return rec


def _page(kind: str, name: str, **stamp: Any) -> Dict[str, Any]:
    rec = P._page_record(kind, (FIXTURES / "html" / name).read_text(encoding="utf-8"))
    if stamp:
        rec["_request"] = stamp
    return rec


#: frame -> builder of the record its parser runs over.
SOURCES: Dict[str, Callable[[], Any]] = {
    **{k: (lambda: _data_body("data_2902690.json.gz")) for k in ("game", "team_box", "player_box", "pbp", "shots")},
    "competition_games": lambda: _capture("ESAKE__49915.json"),
    "match_competition": lambda: _capture("u_ESAKE_2902690.json", org="ESAKE"),
    "competitions": lambda: _page("chooser", "hosted_ESAKE_schedule.html", org="ESAKE"),
    "schedule": lambda: _page("schedule", "hosted_ESAKE_34541_first3.html"),
    "schedule_embed": lambda: _page("embednf", "embednf_HKS_49946_first3.json"),
    "widget_games": lambda: _page("widget", "widget_ESAKE.js"),
}


def _endpoint(short: str, e: Dict[str, Any]) -> Dict[str, Any]:
    names = [p["name"] for p in e["fields"].get("path_params", [])]
    assert not {"league", "sport"} & set(names), e["path"]  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", e["path"])) == set(names), e["path"]
    out: Dict[str, Any] = {
        "short": short,
        "summary": e["summary"],
        "path": e["path"],
        "parser": e["parser"].__name__,
        "returns_schema": f"native/genius/{short}",
        **copy.deepcopy(e["fields"]),
    }
    if "host" in e:
        out["host"] = e["host"]
    return out


def main() -> None:
    refs = refs_dir("genius-livestats", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e["spec"] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"genius: spec paths and ENDPOINTS differ: {sorted(missing)}")
    routes = {
        path: {"servers": [s["url"] for s in item.get("servers") or []], "operation": item["get"]["operationId"]}
        for path, item in sorted(spec["paths"].items())
    }
    ROUTES_SNAPSHOT.write_text(json.dumps(routes, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    doc = {
        "api": "genius",
        "host": HOST,
        "name_pattern": "genius_{short}",
        "module": "genius",
        "parser_module": "basketball.genius.genius_parsers",
        "getter_module": "sportsdataverse.basketball.genius.genius_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": "genius_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the body or page record (``dict`` / ``list``): ``data.json`` as served plus ``_request`` "
            "(the match id asked for), or the sdv-internal-refs capture shape of a page",
            "raises": [
                "sportsdataverse.errors.NoDataError: Genius has nothing for the request (an unknown match id or ORG, "
                "an org without hosted pages, a competition with no games).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed (the CloudFront block, a rate limit, a "
                "5xx), the game is not finished, or the body is for another match, competition or ORG.",
            ],
            "notes": NOTES,
            "see_also": [
                {
                    "name": "Genius Sports LiveStats",
                    "url": "https://fibalivestats.dcd.shared.geniussports.com/",
                    "note": "the platform whose public pages these wrappers read",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, e) for short, e in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/genius.yaml", doc)

    md = _md_rows(refs / "genius-livestats-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/genius"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, e in ENDPOINTS.items():
        frame = e["frame"]
        df = e["parser"](SOURCES[frame]())
        want = {c: t for c, t in P.SCHEMAS[frame if frame != "schedule_embed" else "schedule"].items()}
        assert dict(df.schema) == want, (short, "parser frame differs from the pinned schema")
        cols: List[Dict[str, str]] = columns_from_frame(df, [], leaf_fallback=False)
        assert df.height > 0, f"genius_{short}: the fixture yields no rows, so the schema would be unverified"
        block = snippet.setdefault(f"genius_{short}", {})
        for col in cols:
            text = _describe(frame, col["name"], md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _FILLER.match(text):
                bad.append(f"genius_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"genius_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"genius: empty or filler descriptions (add them to LOCAL): {bad}")
    write_yaml(SNIPPET, snippet)
    print(f"genius: {len(ENDPOINTS)} endpoints, {sum(map(len, snippet.values()))} descriptions -> {SNIPPET.name}")


if __name__ == "__main__":
    main()
