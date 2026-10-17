"""Generate the ``vhl`` flat-API endpoint YAML + returns-schemas (VHL, ``www.vhlru.ru`` HTML pages).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_liiga.py``
(itself ``gen_asa.py`` with the shared plumbing in ``gen_soccer_common.py``). Reads the frozen
spec and the returns md from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``,
else the sibling checkout) and emits:

* ``tools/codegen/endpoints/vhl.yaml`` -- 13 wrappers over the spec's 7 HTML routes. One page
  feeds several frames (the report: game info, goals, penalties, skater and goalie lines; the
  stats page: skaters and goalies; the player page: card and career), each with its own
  parser; :data:`ENDPOINTS` is the source of truth. The spec keys a season by tournament id
  (``tn``); the wrappers take ``season`` (END year) + ``stage`` instead, put the season in the
  tn path segment, and ``vhl_runtime`` maps it to the tn. The team page id is named
  ``team_page_id`` (khl/ terminology), not the spec's ``team_id``.
* ``tools/codegen/schemas/native/vhl/<short>.yaml`` -- one returns-schema per wrapper: the
  columns of its parser run over its committed fixture. The report frames read the FULL page
  ``tests/fixtures/vhl/full/report_899223.html``; every other wrapper its cut-down page. Each
  parser emits a fixed column list with dtypes pinned by name, so a fixture cannot mistype a
  rate. The schemas carry ``description: ''`` (descriptions never live in ``schemas/**``).
* ``tools/codegen/vhl_column_descriptions.snippet.yaml`` -- ``{vhl_<short>: {column: text}}``,
  the block for ``manual_column_descriptions.yaml``. Text comes from :data:`LOCAL` (the columns
  the parsers derive, and the ones whose md row is too terse), then the ``vhl-returns.md`` row
  of the same record path, then any row with that leaf. An empty or filler text fails the run.

Run: ``python tools/codegen/gen_vhl.py``
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.hockey.vhl import vhl_parsers as P
from sportsdataverse.hockey.vhl.vhl_runtime import ONLINE, REQUEST_KEY, STAGES

HOST = "https://www.vhlru.ru"
SPEC = "vhl.openapi.yaml"
SNIPPET = ROOT / "tools" / "codegen" / "vhl_column_descriptions.snippet.yaml"
#: Owner flag (sdv-internal-refs/vhl/README.md), rendered as a Notes item on every wrapper.
SANCTIONS = (
    "Owner flag (sdv-internal-refs/vhl/README.md): the VHL is run by Russian league operators, and "
    "several clubs are owned or sponsored by sanctioned state companies. Fetching on your own machine "
    "is fine; a published data release or a hosted mirror needs the owner's sanctions / legal sign-off first."
)
FIXTURES = ROOT / "tests" / "fixtures" / "vhl"
REPORT_FULL = "full/report_899223.html"

SEASON = (
    "Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with "
    "``stage``, to the site's tournament id (tn) in this path segment."
)
STAGE = "Season stage: regular (default) or playoff."
GAME_ID = "Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223."


def _p(name: str, desc: str) -> Dict[str, Any]:
    return {"name": name, "type": "int", "required": True, "description": desc}


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


_STAGE_Q = _q("stage", "stage", STAGE, choices=list(STAGES), default="regular")
_TN_ROUTE = {"path_params": [_p("season", SEASON)], "extra_params": [_STAGE_Q], "example_args": {"season": 2026}}
_REPORT = {
    "path_params": [_p("season", SEASON)],
    "extra_params": [_q("game_id", "idgame", f"Required. {GAME_ID}", typ="int", required=True), _STAGE_Q],
    "example_args": {"season": 2026, "game_id": 899223},
}
_PLAYER = {
    "path_params": [_p("player_id", "Player id (player_id in any VHL frame), e.g. 34526.")],
    "example_args": {"player_id": 34526},
}

_REPORT_REQ = {"season": 2026, "stage": "regular", "tn": 1374, "game_id": 899223}

# short -> (spec path, wrapper path, parser, fixture, request stamp, md section, record prefixes, summary, fields)
Entry = Tuple[str, str, Callable[..., Any], str, Dict[str, Any], str, Tuple[str, ...], str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "schedule": (
        "/en/calendar/{tn}/0/",
        "/en/calendar/{season}/0/",
        P.parse_vhl_schedule,
        "calendar_1374.html",
        {"season": 2026, "stage": "regular", "tn": 1374},
        "calendar",
        ("games.[].", "tournament."),
        "Schedule and results of one season stage: every game with its club ids, score and decision.",
        _TN_ROUTE,
    ),
    "game_info": (
        "/en/report/{tn}/",
        "/en/report/{season}/",
        P.parse_vhl_game_info,
        REPORT_FULL,
        _REPORT_REQ,
        "report",
        ("game.",),
        "Game header of a report: date, attendance, score by period, teams, coaches and officials.",
        _REPORT,
    ),
    "goals": (
        "/en/report/{tn}/",
        "/en/report/{season}/",
        P.parse_vhl_goals,
        REPORT_FULL,
        _REPORT_REQ,
        "report",
        ("goals.[].", "game."),
        "Goals of a game with scorer, assists, strength and the jerseys on ice for both teams.",
        _REPORT,
    ),
    "penalties": (
        "/en/report/{tn}/",
        "/en/report/{season}/",
        P.parse_vhl_penalties,
        REPORT_FULL,
        _REPORT_REQ,
        "report",
        ("penalties.[].", "game."),
        "Penalties of a game: team, time, player, minutes and infraction.",
        _REPORT,
    ),
    "player_box": (
        "/en/report/{tn}/",
        "/en/report/{season}/",
        P.parse_vhl_player_box,
        REPORT_FULL,
        _REPORT_REQ,
        "report",
        ("skaters.[].", "game."),
        "Skater box score of a game: scoring, shots, faceoffs, time on ice, hits and blocks.",
        _REPORT,
    ),
    "goalie_box": (
        "/en/report/{tn}/",
        "/en/report/{season}/",
        P.parse_vhl_goalie_box,
        REPORT_FULL,
        _REPORT_REQ,
        "report",
        ("goalies.[].", "game."),
        "Goalie box score of a game: shots against, saves, save percentage and time on ice.",
        _REPORT,
    ),
    "pbp": (
        "/online/{idgame}.html",
        "/online/{game_id}.html",
        P.parse_vhl_pbp,
        "online_899223.html",
        _REPORT_REQ,
        "online",
        ("events.[].", "game."),
        "Text broadcast of a game (online.vhlru.ru, Russian text): goals, penalties, shootout attempts, "
        "goalie changes and comments. No shot coordinates.",
        {
            "host": ONLINE,
            "path_params": [_p("game_id", GAME_ID)],
            "extra_params": [
                _q(
                    "season",
                    "season",
                    "Required. Season END year of the game; with ``stage`` it locates the game in the calendar, "
                    "whose game number the broadcast page must carry (the page names no game id).",
                    typ="int",
                    required=True,
                ),
                _STAGE_Q,
            ],
            "example_args": {"game_id": 899223, "season": 2026},
        },
    ),
    "standings": (
        "/en/standings/league/{tn}/",
        "/en/standings/league/{season}/",
        P.parse_vhl_standings,
        "standings_1374.html",
        {"season": 2026, "stage": "regular", "tn": 1374},
        "standings",
        ("rows.[].", "tournament."),
        "League standings of one regular season (/standings/league/; /standings/regular/ ignores the season).",
        {"path_params": [_p("season", SEASON)], "example_args": {"season": 2026}},
    ),
    "skater_stats": (
        "/en/stats/players/{tn}/",
        "/en/stats/players/{season}/",
        P.parse_vhl_skater_stats,
        "stats_players_1374.html",
        {"season": 2026, "stage": "regular", "tn": 1374},
        "stats",
        ("forwards.[].", "defensemen.[].", "tournament."),
        "Season stats of every defenseman and forward of one season stage.",
        _TN_ROUTE,
    ),
    "goalie_stats": (
        "/en/stats/players/{tn}/",
        "/en/stats/players/{season}/",
        P.parse_vhl_goalie_stats,
        "stats_players_1374.html",
        {"season": 2026, "stage": "regular", "tn": 1374},
        "stats",
        ("goalies.[].", "tournament."),
        "Season stats of every goalie of one season stage.",
        _TN_ROUTE,
    ),
    "player": (
        "/en/players/{player_id}/",
        "/en/players/{player_id}/",
        P.parse_vhl_player,
        "player_34418.html",
        {"player_id": 34418},
        "players",
        ("",),
        "Player card: position, height, weight, birth date, age, country and photo.",
        _PLAYER,
    ),
    "player_career": (
        "/en/players/{player_id}/",
        "/en/players/{player_id}/",
        P.parse_vhl_player_career,
        "player_34418.html",
        {"player_id": 34418},
        "players",
        ("seasons.[].", ""),
        "Player career by season stage and team (skater and goalie columns in one frame).",
        _PLAYER,
    ),
    "roster": (
        "/en/teams/{team_id}/",
        "/en/teams/{team_page_id}/",
        P.parse_vhl_roster,
        "team_460279.html",
        {"team_page_id": 460279},
        "teams",
        ("roster.[].", ""),
        "CURRENT roster of a team page (player names in Cyrillic even on /en/).",
        {
            "path_params": [
                _p(
                    "team_page_id",
                    "Team page id (home_team_page_id / away_team_page_id in vhl_game_info), e.g. 390. "
                    "Neither a club id nor a team-season id.",
                )
            ],
            "example_args": {"team_page_id": 390},
        },
    ),
}

#: Descriptions of the columns the parsers add or re-key (they win over the returns md).
LOCAL: Dict[str, str] = {
    "season": "Season END year (2026 = 2025/26).",
    "stage": "Season stage: regular or playoff.",
    "tn": "Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps "
    "season + stage to it.",
    "game_id": "Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and "
    "broadcast pages do not print it: it comes from the request.",
    "game_date": "Game date, ISO 'YYYY-MM-DD': the calendar day heading plus the season (August-December = START year).",
    "home_club_id": "Home club id: stable across seasons (the img.khl.ru logo path id). Neither a team-season "
    "id nor a team page id.",
    "away_club_id": "Away club id: stable across seasons (the img.khl.ru logo path id).",
    "team_side": "Side of the game the row belongs to: home or away.",
    "team_page_id": "Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows "
    "the page id of the row's side.",
    "team": "Team short name (Latin; the report tables' form).",
    "home_short": "Home team short name (the report tables' form).",
    "away_short": "Away team short name (the report tables' form).",
    "position_group": "Position group: defense or forward.",
    "event_index": "Order of the event on the page, 0 = the first listed (the broadcast lists the latest event first).",
    "referee1_number": "First referee's sweater number.",
    "referee1": "First referee, 'Surname Given'.",
    "referee2_number": "Second referee's sweater number.",
    "referee2": "Second referee, 'Surname Given'.",
    "linesman1_number": "First linesman's sweater number.",
    "linesman1": "First linesman, 'Surname Given'.",
    "linesman2_number": "Second linesman's sweater number.",
    "linesman2": "Second linesman, 'Surname Given'.",
    "calendar_tn": "Tournament id of the team's CURRENT season stage (the page's 'To the calendar' link).",
    "calendar_team_season_id": "The team's team-season id in calendar_tn.",
    "roster_as_of": "Date the roster is valid for, as printed ('12.10.2026').",
    "born": "Birth date 'DD.MM.YYYY' as the public page prints it.",
    # the md rows of these say only "Goals (G)." and the like
    "scorer": "Goal scorer's name, 'Surname Given' as the report prints it.",
    "goals": "Goals (G) the player scored in the row's scope: the game in a box score, the season stage "
    "in stats and career rows.",
    "assists": "Assists (A) credited to the player in the row's scope: the game in a box score, the season "
    "stage in stats and career rows.",
    "saves": "Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage "
    "in stats and career rows).",
    "shutouts": "Shutouts (SO): games in the row's scope in which the goalie allowed no goal.",
}

#: The manual-description quality rule (tests/codegen/test_manual_descriptions.py).
_FILLER = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)

_HEADING = re.compile(r"^## `https://[^/]+(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
#: Section of the returns md -> the path fragment that names it.
_SECTIONS = {
    "calendar": "/en/calendar/",
    "report": "/en/report/",
    "online": "/online/",
    "standings": "/en/standings/",
    "stats": "/en/stats/",
    "players": "/en/players/",
    "teams": "/en/teams/",
}


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section: {record path: description}}`` from the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    section: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            section = next((k for k, frag in _SECTIONS.items() if h.group(1).startswith(frag)), None)
            continue
        if line.startswith("## "):
            section = None
        m = _ROW.match(line)
        if m and section:
            out.setdefault(section, {}).setdefault(m.group(1), m.group(2).strip().replace("&lt;", "<"))
    return out


def _describe(name: str, entry: Entry, md: Dict[str, Dict[str, str]]) -> str:
    if name in LOCAL:
        return LOCAL[name]
    section, prefixes = entry[5], entry[6]
    rows = md.get(section, {})
    for prefix in prefixes:
        if prefix + name in rows:
            return rows[prefix + name]
    leaf = next((d for k, d in rows.items() if k.rsplit(".", 1)[-1] == name), None)
    return leaf or ""


def _frame(entry: Entry) -> Any:
    """The parser's frame over the entry's fixture, stamped like a live record."""
    _sp, _path, parser, fixture, stamp, _section, _prefixes, _summary, _fields = entry
    kind = {
        "calendar": "calendar",
        "report": "report",
        "online": "online",
        "standings": "standings",
        "stats": "stats_players",
        "players": "player",
        "teams": "team",
    }[entry[5]]
    rec = P._page_record(kind, (FIXTURES / fixture).read_text(encoding="utf-8"))
    rec[REQUEST_KEY] = dict(stamp)
    return parser(rec)


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, parser, _fixture, _stamp, _section, _prefixes, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/vhl/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("vhl", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"vhl: spec paths and ENDPOINTS differ: {sorted(missing)}")
    doc = {
        "api": "vhl",
        "host": HOST,
        "name_pattern": "vhl_{short}",
        "module": "vhl",
        "parser_module": "hockey.vhl.vhl_parsers",
        "getter_module": "sportsdataverse.hockey.vhl.vhl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (standings, schedule, goals) name other families
        "schema_prefix": "vhl_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the page's parsed record (``dict``): the sdv-internal-refs capture shape, plus "
            "``_request`` (season, stage, tn and the ids asked for)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the VHL has nothing for the request (a season without "
                "that stage, a game not in the season's calendar, an unknown game, player or team page).",
                "sportsdataverse.errors.AssetFetchError: the page could not be fetched, or it is not the page "
                "asked for (another season, game or player; a missing table or header drift).",
            ],
            "notes": [SANCTIONS],
            "see_also": [
                {
                    "name": "VHL",
                    "url": "https://www.vhlru.ru/en/",
                    "note": "the league site whose pages these wrappers read",
                },
                {
                    "name": "fastRhockey",
                    "url": "https://fastRhockey.sportsdataverse.org",
                    "note": "SportsDataverse hockey package (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/vhl.yaml", doc)

    md = _md_rows(refs / "vhl-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/vhl"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = snippet.setdefault(f"vhl_{short}", {})
        for col in cols:
            text = _describe(col["name"], entry, md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _FILLER.match(text):
                bad.append(f"vhl_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"vhl_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"vhl: empty or filler descriptions (add them to LOCAL): {bad}")
    write_yaml(SNIPPET, snippet)
    print(f"vhl: {len(ENDPOINTS)} endpoints, {sum(map(len, snippet.values()))} descriptions -> {SNIPPET.name}")


if __name__ == "__main__":
    main()
