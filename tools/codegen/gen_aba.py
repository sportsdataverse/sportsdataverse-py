"""Generate the ``aba`` flat-API endpoint YAML + returns-schemas (ABA League and ABA League 2, the Adriatic leagues).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_feb.py`` (itself ``gen_asa.py``
with the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and the returns md from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/aba.yaml`` -- 12 wrappers over the spec's 7 routes. One route feeds several frames
  (the match page feeds ``game``, ``periods``, ``box_players``, ``box_teams`` and ``pbp``; the player page feeds
  ``player`` and ``player_games``), each with its own parser; :data:`ENDPOINTS` is the source of truth. The wrappers
  take ``season`` (END year) and ``league`` (``aba`` / ``aba2``); ``aba_runtime`` maps them to the site's
  ``sez`` / ``lea`` and host.
* ``tools/codegen/schemas/native/aba/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixture (the FULL match record in ``tests/fixtures/aba/full/``, the refs captures for the
  rest). Each parser emits a fixed column list with dtypes pinned by name (``aba_parsers.SPECS``), so a fixture
  cannot mistype a rate. The schemas carry ``description: ''`` (descriptions never live in ``schemas/**``).
* ``tools/codegen/notes/aba_descriptions.yaml`` -- ``{aba_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it). A filler or missing description stops the run.
* ``tests/fixtures/aba/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_aba.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.aba import aba_parsers as P
from sportsdataverse.basketball.aba.aba_runtime import HOSTS, REQUEST_KEY
from sportsdataverse.dl_utils import underscore

HOST = HOSTS["aba"]
SPEC = "aba-liga.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "aba"
#: Column descriptions, keyed ``aba_<short>`` -> column; merged into manual_column_descriptions.yaml.
NOTES = ROOT / "tools" / "codegen" / "notes" / "aba_descriptions.yaml"
#: The spec's route list, committed so the tests check ENDPOINTS without the refs checkout.
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)
LIVE = "/live-match/rezultati-1718"
GAME_NO_EXAMPLE = 6
SEASON_EXAMPLE = 2026

SEASON = (
    "Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key "
    "``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``)."
)
GAME_NO = (
    "Required. Game number within the season AND league (it restarts at 1 for every season and league), "
    "e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps."
)
LEAGUE = {
    "name": "league",
    "query_key": "league",
    "type": "str",
    "default": "aba",
    "choices": ["aba", "aba2"],
    "description": (
        "League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, "
        "druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host."
    ),
}
_SEASON_P = {"name": "season", "type": "int", "required": True, "description": SEASON}
_GAME_P = {"name": "game_no", "type": "int", "required": True, "description": GAME_NO}


def _match_fields() -> Dict[str, Any]:
    return {
        "path_params": [dict(_GAME_P), dict(_SEASON_P)],
        "extra_params": [dict(LEAGUE)],
        "example_args": {"game_no": GAME_NO_EXAMPLE, "season": SEASON_EXAMPLE},
    }


def _helper_fields() -> Dict[str, Any]:
    return {
        "extra_params": [
            {"name": "game_no", "query_key": "game_no", "type": "int", "required": True, "description": GAME_NO},
            {"name": "season", "query_key": "season", "type": "int", "required": True, "description": SEASON},
            dict(LEAGUE),
        ],
        "example_args": {"game_no": GAME_NO_EXAMPLE, "season": SEASON_EXAMPLE},
    }


def _season_fields() -> Dict[str, Any]:
    return {
        "path_params": [dict(_SEASON_P)],
        "extra_params": [dict(LEAGUE)],
        "example_args": {"season": SEASON_EXAMPLE},
    }


def _player_fields() -> Dict[str, Any]:
    return {
        "path_params": [
            {
                "name": "player_id",
                "type": "int",
                "required": True,
                "description": (
                    "Required. ABA player id (global: one person keeps one id across seasons and both leagues), "
                    "e.g. 4807 (Carlik Jones); ``player_id`` in aba_box_players."
                ),
            },
            dict(_SEASON_P),
        ],
        "extra_params": [dict(LEAGUE)],
        "example_args": {"player_id": 4807, "season": SEASON_EXAMPLE},
    }


def _json(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _match() -> Dict[str, Any]:
    return _json("full/match_6_25_1.json")


def _helper(name: str) -> Callable[[], Dict[str, Any]]:
    def load() -> Dict[str, Any]:
        stamp = {"kind": "x", "game_no": 6, "season": 2026, "sez": 25, "league": "aba", "league_id": 1}
        return {"rows": _json(name), REQUEST_KEY: stamp}

    return load


def _capture(name: str) -> Callable[[], Dict[str, Any]]:
    return lambda: _json(f"captures/{name}.json")


# short -> (spec path, wrapper path, parser, fixture loader, md sources, summary, fields)
Source = Tuple[str, str]  # (returns-md section, record-path prefix)
Entry = Tuple[str, str, Callable[..., Any], Callable[[], Dict[str, Any]], Tuple[Source, ...], str, Dict[str, Any]]
_MATCH = "/match/{gameNo}/{sez}/{lea}/Overview"
_MATCH_W = "/match/{game_no}/{season}/Overview"
ENDPOINTS: Dict[str, Entry] = {
    "game": (
        _MATCH,
        _MATCH_W,
        P.parse_aba_game,
        _match,
        (("match", "game."),),
        "Header of one finished game: round, date, venue, attendance, both clubs with their score, "
        "commissioner and referees.",
        _match_fields(),
    ),
    "periods": (
        _MATCH,
        _MATCH_W,
        P.parse_aba_periods,
        _match,
        (("match", "periods.[]."),),
        "Points each team scored in each quarter and overtime of one finished game.",
        _match_fields(),
    ),
    "box_players": (
        _MATCH,
        _MATCH_W,
        P.parse_aba_box_players,
        _match,
        (("match", "players.[]."),),
        "Player box score of one finished game (2001-02 on): minutes, points, shooting splits, rebounds, "
        "assists, fouls, plus/minus and valuation.",
        _match_fields(),
    ),
    "box_teams": (
        _MATCH,
        _MATCH_W,
        P.parse_aba_box_teams,
        _match,
        (("match", "teams.[]."),),
        "Team totals of one finished game: the Team Compare table, home then away.",
        _match_fields(),
    ),
    "pbp": (
        _MATCH,
        _MATCH_W,
        P.parse_aba_pbp,
        _match,
        (("match", "pbp.[]."),),
        "Play-by-play of one finished game (2017-18 on), in game order: shots, rebounds, fouls, "
        "substitutions, timeouts and period markers.",
        _match_fields(),
    ),
    "shots": (
        f"{LIVE}/create_shooting_chart.php",
        f"{LIVE}/create_shooting_chart.php",
        P.parse_aba_shots,
        _helper("create_shooting_chart_6_25_1.json"),
        (("shots", ".[]."),),
        "Shot chart of one finished game (2017-18 on): every field-goal attempt with its court position.",
        _helper_fields(),
    ),
    "score_flow": (
        f"{LIVE}/create_json.php",
        f"{LIVE}/create_json.php",
        P.parse_aba_score_flow,
        _helper("create_json_6_25_1.json"),
        (("flow", ".[]."),),
        "Score after every whole minute of one finished game (the Result Graph; 2017-18 on).",
        _helper_fields(),
    ),
    "score_difference": (
        f"{LIVE}/create_scoredifference.php",
        f"{LIVE}/create_scoredifference.php",
        P.parse_aba_score_difference,
        _helper("captures/create_scoredifference.json"),
        (("scorediff", ".[]."),),
        "Score after every whole minute of one finished game, from the Score difference chart (2017-18 on); "
        "the same rows as aba_score_flow.",
        _helper_fields(),
    ),
    "calendar": (
        "/calendar/{sez}/{lea}/",
        "/calendar/{season}/",
        P.parse_aba_calendar,
        _capture("calendar"),
        (("calendar", "games.[]."),),
        "Full schedule and results of one season of one league: every round and game.",
        _season_fields(),
    ),
    "standings": (
        "/standings/{sez}/{lea}/",
        "/standings/{season}/",
        P.parse_aba_standings,
        _capture("standings"),
        (("standings", "rows.[]."),),
        "Standings tables of one season of one league: regular season groups, Top 8 and Play-out.",
        _season_fields(),
    ),
    "player": (
        "/player/{playerId}/{sez}/{lea}/",
        "/player/{player_id}/{season}/",
        P.parse_aba_player,
        _capture("player__playerId"),
        (("player", ""),),
        "One player's bio for one season and league: club, position, height, birth date and place, "
        "nationality and the career line.",
        _player_fields(),
    ),
    "player_games": (
        "/player/{playerId}/{sez}/{lea}/",
        "/player/{player_id}/{season}/",
        P.parse_aba_player_games,
        _capture("player__playerId"),
        (("player", "games.[]."),),
        "One player's game log for one season and league: one row per game with the box-score stats.",
        _player_fields(),
    ),
}
# ``_helper("captures/...")`` above reads under tests/fixtures/aba/; the JSON helper captures are bare lists.

NOTES_TEXT = [
    "Unofficial use of a public source: these wrappers read the leagues' own PHP sites (www.aba-liga.com, "
    "druga.aba-liga.com), keyless, with no bot wall; payloads are not redistributed as release assets. "
    "www.aba-liga.com/robots.txt is the single line ``User-agent: *`` (nothing disallowed, no Crawl-delay) and "
    "druga has no robots.txt; no terms page was found. The runtime waits at least 1 s per host before every request.",
    "Coverage: box scores from 2001-02; play-by-play, the starter and on-court markers, the shot chart and the "
    "minute-by-minute score from 2017-18 (earlier games answer an empty pbp and NoDataError for the helpers). "
    "A game is keyed by (game_no, season, league_id): game_no restarts at 1 for every season and league. Team and "
    "player ids are global; the calendar carries club names only. Club names are the club's CURRENT name on every "
    "season: join on team_id.",
    "Only finished games are served: an unplayed game, or one whose period scores, team totals or play-by-play do not "
    "add up to the final score, raises AssetFetchError, as does a page for another game, season or league than "
    "asked. The three live-match helpers echo nothing about the game, so the runtime fetches the same game's match "
    "page first (one extra request) and checks the helper against it.",
    "Times: ``CET`` means Central European wall-clock time including summer time (the label never changes); ABA "
    "League 2 prints ``Local time``. Officials appear by name only; the player bio (birth date and place, "
    "nationality) is the public player profile.",
]

#: Descriptions of the columns the parsers add, re-key or translate (they win over the returns md).
LOCAL: Dict[str, str] = {
    "game_no": "Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is "
    "(game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route.",
    "season": "Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001.",
    "league_id": "League code, the site's lea: 1 = ABA League, 2 = ABA League 2.",
    "league": "League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument.",
    "season_label": "Season label as the page prints it ('2025/26' on the match page, 'Season 2025/26' on the "
    "calendar and standings pages).",
    "team_id": "ABA club id (global: the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 "
    "and 2025-26). Joins every frame that carries it; the calendar has club names only.",
    "player_id": "ABA player id (global: one person keeps one id across seasons and both leagues, e.g. Carlik Jones = "
    "4807), cast to an integer. Join on it, never on the printed name.",
    "period_no": "1-based position of the period: 1-4 for Q1-Q4, then 5 for OT1, 6 for OT2.",
    "home_points": "Home points scored IN the period (not cumulative).",
    "away_points": "Away points scored IN the period (not cumulative).",
    "seconds_left": "The clock as whole seconds REMAINING in the period (08:34 = 514), derived by the parser; null "
    "where the clock is.",
    "pts_paint": "Points in the paint (Points from Paint). Null where the season has no such column (2008-09) and "
    "on did-not-play rows.",
    "pts_second_chance": "Second-chance points (Points from 2ndCh). Null where the season has no such column "
    "(2008-09) and on did-not-play rows.",
    "pts_fast_break": "Fast-break points (Points from FstBr). Null where the season has no such column (2008-09) "
    "and on did-not-play rows.",
    "referees": "Referees' names in page order (crew chief first), joined with '; ' (2008-09 adds an IOC-style "
    "country code in parentheses). Names only.",
    "date": "Game date, from the date-and-time line (the first date of a window); null for 'TBA'.",
    "date_to": "Last date of a 'dd.mm.yyyy - dd.mm.yyyy' window printed before a game is scheduled; null on a "
    "game with a single date.",
    "local_time": "Arena-local tip-off 'hh:mm' when the arena is outside Central Europe ('(19:00 Dubai local time)'); "
    "null otherwise.",
    "local_place": "City of local_time ('Dubai', 'Cluj-Napoca'); null when the arena is in Central Europe.",
    "jersey": "Jersey number as printed; the play-by-play omits it on some rows (all starting-lineup rows and many "
    "home rows), so it is null there.",
    "assists": "Assists made (the Ass column).",
    "steals": "Steals made (the St column).",
    "wins": "Games won in this table (the W column).",
    "losses": "Games lost in this table (the L column).",
    "career": "The Clubs box, one free-text line per stint, oldest first, joined with '; ' "
    "('2021/22 - Texas Legends; 2024-27 - Partizan Mozzart Bet').",
}
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "box_players": {"jersey": "Jersey number worn in the game (the box lists players by jersey number)."},
    "shots": {
        "side": "Which team took the shot: home or away (the site's ekipa, a string '1' / '2').",
        "x": "Shot x as a PERCENTAGE (0-100) of the court LENGTH from the LEFT baseline of the site's 1000x535 px "
        "full-court image. Home shoots at x < 50 and away at x > 50. The site sends it as a string in one half of "
        "a game and a number in the other; cast to float.",
        "y": "Shot y as a PERCENTAGE (0-100) of the court WIDTH, drawn upward from the bottom edge (the line through "
        "both baskets is 50). Whether 'up' is the scorer's table side is unverified.",
        "made": "True when the attempt was made (the site's koordinata_uspeh '1'), false when missed.",
        "player_id": "Shooter's ABA player id (global), cast from the site's string; null when the shooter was not "
        "recorded (id 0). Join to the box score on it, never on the name.",
        "player_name": "Shooter's full name, 'First Last' (the box prints 'Last F.'); null when not recorded.",
    },
    "score_flow": {
        "minute": "Elapsed game minute: 0 (tip-off) to 40, then 41-45 per overtime; every minute once, in order.",
        "home_score": "Home cumulative score at the end of that minute (the site sends a numeric string). The last "
        "row equals the final score.",
        "away_score": "Away cumulative score at the end of that minute (the site sends a numeric string). The last "
        "row equals the final score.",
    },
    "game": {
        "when": "Date-and-time line verbatim (English weekday, dd.mm.yyyy, hh:mm, time-zone label).",
    },
    "calendar": {
        "when": "Date and Time cell verbatim: 'Monday, 06.10.2025 18:00 CET', with '(19:00 Dubai local time)' for "
        "arenas outside Central Europe, 'Local time' on ABA League 2, a date window or 'TBA' before scheduling.",
        "round": "Round or phase label, the accordion panel title ('ROUND 1', 'Top8 - R1', 'Quarter-finals, Round "
        "1', 'Finals, Round 4').",
        "group": "Group letter ('A', 'B') from the Group column, which exists only in seasons with groups; null "
        "after the groups (Top8, Play-out, playoffs) and where the page has no Group column.",
    },
}
LOCAL_BY["score_difference"] = LOCAL_BY["score_flow"]
for _short in ("shots", "score_flow", "score_difference"):
    LOCAL_BY[_short]["game_no"] = LOCAL["game_no"]

_HEADING = re.compile(r"^## `(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
_SECTIONS = {
    "match": "/match/",
    "shots": "create_shooting_chart",
    "flow": "create_json",
    "scorediff": "create_scoredifference",
    "calendar": "/calendar/",
    "standings": "/standings/",
    "player": "/player/",
}


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section: {record path: description}}`` from the returns md (unobserved rows skipped)."""
    out: Dict[str, Dict[str, str]] = {}
    section: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            section = next((k for k, frag in _SECTIONS.items() if frag in h.group(1)), None)
            continue
        if line.startswith("## "):
            section = None
        m = _ROW.match(line)
        if m and section and not m.group(2).strip().startswith("Unobserved"):
            out.setdefault(section, {}).setdefault(m.group(1), m.group(2).strip().replace("&lt;", "<"))
    return out


def _describe(short: str, col: str, entry: Entry, md: Dict[str, Dict[str, str]]) -> str:
    if col in LOCAL_BY.get(short, {}):
        return LOCAL_BY[short][col]
    if col in LOCAL:
        return LOCAL[col]
    for section, prefix in entry[4]:
        for path, text in md.get(section, {}).items():
            leaf = path[len(prefix) :]
            if path.startswith(prefix) and "." not in leaf and underscore(leaf) == col:
                return text
    return ""


def _frame(entry: Entry) -> pl.DataFrame:
    """The parser's frame over the entry's fixture."""
    frame = entry[2](entry[3]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, parser, _fix, _src, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/aba/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("aba-liga", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"aba: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(P.SPECS) == set(ENDPOINTS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "aba",
        "host": HOST,
        "name_pattern": "aba_{short}",
        "module": "aba",
        "parser_module": "basketball.aba.aba_parsers",
        "getter_module": "sportsdataverse.basketball.aba.aba_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (game, pbp, shots) name other families
        "schema_prefix": "aba_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw record (``dict``): the page record (the refs capture shape) or, for the three live-match "
            "helpers, ``{'rows': [...]}``, plus ``_request`` (kind, season, sez, league and the game or player asked for)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the site has nothing for the request (an unknown game or "
                "season, a season before 2001-02, a helper with no data before 2017-18).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a page for another game, season or league, an unfinished game, a helper that does not belong "
                "to the game, layout drift).",
            ],
            "notes": NOTES_TEXT,
            "see_also": [
                {
                    "name": "ABA League",
                    "url": "https://www.aba-liga.com",
                    "note": "the league site whose pages and chart helpers these wrappers read",
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
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/aba.yaml", doc)

    md = _md_rows(refs / "aba-liga-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/aba"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = notes.setdefault(f"aba_{short}", {})
        for col in cols:
            text = " ".join(_describe(short, col["name"], entry, md).split())
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"aba_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"aba_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"aba: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"aba: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
