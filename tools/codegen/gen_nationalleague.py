"""Generate the ``nationalleague`` flat-API endpoint YAML + returns-schemas from the National League
app API spec (``www.nationalleague.ch/api``, keyless).

Idempotent: same spec + same captures + same fixtures -> byte-identical output. Modeled on
``gen_asa.py``, with the shared plumbing in ``gen_soccer_common.py``.

The generator reads the frozen spec and the route-scoped ``nationalleague-returns.md`` from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/nationalleague.yaml`` -- 17 wrappers over the 12 spec routes. The game
  page (``/games/{gameId}``) feeds six (overview, pbp, lineups, player_box, team_stats, shots);
  every other route is one wrapper.
* ``tools/codegen/schemas/native/nationalleague/<short>.yaml`` -- one returns-schema per wrapper.
  Columns and dtypes come from running the wrapper's parser over a FULL body
  (``tests/fixtures/nationalleague/full/``) where one exists, else over the refs capture: the
  truncated captures hold three rows, where a rate can look like an integer. Every
  ``description`` is written empty: descriptions never live in ``schemas/**`` (a re-capture
  clobbers them).
* ``tools/codegen/nationalleague_column_descriptions.snippet.yaml`` -- the returns-table prose,
  keyed by schema key (``nl_<short>``) then column, for merging into
  ``manual_column_descriptions.yaml``. It comes from the returns md section of the wrapper's own
  route (``fo`` is faceoffs in a game and penalties in a season row, so a flat cross-route map
  would be wrong), then the curated :data:`_DERIVED` prose for the columns the parsers add or
  rename.

robots.txt disallows ``/api``; the owner approved building on it on 2026-10-12 (polite,
incremental use). The ruling is carried into every wrapper's docstring (``docstring.notes``).

Run: ``python tools/codegen/gen_nationalleague.py``
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.nl import nationalleague_parsers as P

HOST = "https://www.nationalleague.ch/api"
SPEC = "nationalleague.openapi.yaml"
STEM = "nationalleague"
FULL = ROOT / "tests" / "fixtures" / STEM / "full"
SNIPPET = ROOT / "tools" / "codegen" / f"{STEM}_column_descriptions.snippet.yaml"

# (short, spec path, parser, sample, md prefixes, summary or None for the spec's).
# ``sample`` is a refs capture slug (capture.py: the path with "/" -> "__", braces dropped) or
# ``full/<file>``. ``md prefixes`` scope the returns-md rows to the part of the body the parser reads.
_GAME = "/games/{gameId}"
_GAME_FULL = "full/game_20261105000325.json"
ENDPOINTS: List[Tuple[str, str, str, str, Tuple[str, ...], Any]] = [
    ("schedule", "/games", "parse_nationalleague_games", "games", ("[].",), None),
    ("current_games", "/games/current", "parse_nationalleague_games", "games__current", ("[].",), None),
    ("team_schedule", "/games/team/{teamId}", "parse_nationalleague_games", "games__team__teamId", ("[].",), None),
    (
        "playoffs",
        "/games/playoffs",
        "parse_nationalleague_bracket",
        "games__playoffs",
        ("[].games.[].games.[].", "[].games.[].=series_", "[]."),
        "Playoff and play-in series by round: one row per game, with its series score.",
    ),
    (
        "playouts",
        "/games/playouts",
        "parse_nationalleague_bracket",
        "games__playouts",
        ("[].games.[].games.[].", "[].games.[].=series_", "[]."),
        "Playout and league-qualification series by round: one row per game, with its series score.",
    ),
    (
        "game_overview",
        _GAME,
        "parse_nationalleague_game_overview",
        _GAME_FULL,
        ("overview.", "result."),
        "One game's header: teams, date, status, final and per-period score, coaches, referees, arena, attendance.",
    ),
    (
        "pbp",
        _GAME,
        "parse_nationalleague_pbp",
        _GAME_FULL,
        ("actions.[].actions.[].",),
        "One game's event timeline: goals with assists, penalties, goaltender changes, revoked goals.",
    ),
    (
        "lineups",
        _GAME,
        "parse_nationalleague_lineups",
        _GAME_FULL,
        ("lineupHome.[].players.[].",),
        "One game's lineups: every dressed player by unit, with licence number, position and line.",
    ),
    (
        "player_box",
        _GAME,
        "parse_nationalleague_player_box",
        _GAME_FULL,
        ("playerStatsHome.[].",),
        "One game's player box score: goals, assists, shots, faceoffs, ice time, goaltending.",
    ),
    (
        "team_stats",
        _GAME,
        "parse_nationalleague_team_stats",
        _GAME_FULL,
        ("teamStatsHome.",),
        "One game's team totals: shots by period, faceoffs by zone, power play and penalty kill.",
    ),
    (
        "shots",
        _GAME,
        "parse_nationalleague_shots",
        _GAME_FULL,
        ("shotsHome.[].",),
        "One game's shot attempts with rink location (metres) and expected goals (xG); NL games only.",
    ),
    ("player_stats", "/player", "parse_nationalleague_player_stats", "full/player_2026_4940.json", ("[].",), None),
    ("top_scorers", "/player/topscorer", "parse_nationalleague_player_stats", "player__topscorer", ("[].",), None),
    (
        "team_player_stats",
        "/player/team/{teamId}",
        "parse_nationalleague_player_stats",
        "player__team__teamId",
        ("[].",),
        None,
    ),
    ("player_season", "/player/{playerId}", "parse_nationalleague_player_stats", "player__playerId", ("",), None),
    ("standings", "/teams", "parse_nationalleague_standings", "full/teams_2026_4940.json", ("[].",), None),
    ("team_info", "/teams/{teamId}/info", "parse_nationalleague_team_info", "teams__teamId__info", ("",), None),
]

# Path tokens -> wrapper argument names (ids are Int64 in every frame, so they are ints here too).
_PATH_PARAM = {"gameId": "game_id", "teamId": "team_id", "playerId": "player_id"}

# Argument prose for the path ids (the spec describes the wire's digit string; the argument is an int).
_PATH_DOC = {
    "game_id": "SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same "
    "id as the sihf family's game_id), e.g. 20261105000325.",
    "team_id": "SIHF team id (team_id in nl_standings; the same id as the sihf family's team_id), e.g. 101144 = EV Zug.",
    "player_id": "SIHF licence number (player_id in nl_player_stats; joins the sihf family's player_id), e.g. 305519.",
}

# The game page ignores ``season`` (the game id fixes it; dropping it served the same game), so the
# six game wrappers do not take it.
_IGNORED_QUERY = {_GAME: {"season"}}

_RULING = (
    "robots.txt on www.nationalleague.ch disallows /api. The owner approved building on it on 2026-10-12 for "
    "polite, incremental use: requests are spaced at least 1 s apart, and a bulk backfill or a published mirror "
    "needs the owner's sign-off first."
)
_INCREMENTAL = (
    "Crawl incrementally: fetch a finished game once (keyed by game_id), find new games with one nl_schedule call "
    "per run, and fetch season tables at most once per game day."
)

# Upstream names the parsers rename -> their new name, per shaper (the md documents the upstream name).
_RENAMES: Dict[str, Dict[str, str]] = {
    "pbp": {"third": "period"},
    "player_box": {"fo": "faceoffs", "fo_percentage": "faceoffs_percentage"},
    "team_stats": {"fo": "faceoffs", "fo_percentage": "faceoffs_percentage"},
    "shots": {"pid": "player_id", "sec": "period", "sit": "strength", "type": "shot_type"},
    "lineups": {},
    "team_info": {"id": "cms_node_id", "fields_team_id": "team_id"},
}
_SEASON_ROW_RENAMES = {"fo": "penalties"}

_GAME_ID = (
    "SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). "
    "The same id as the sihf family's game_id; its first four digits are the season END year."
)
_TEAM_ID = "SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id."
_PLAYER_ID = (
    "SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every "
    "SIHF *LicenceNr."
)
_VIMEO = "Vimeo video id (Int64): vimeo.com/<id>."

# Curated prose: columns the parsers add, and ids whose md text describes the upstream digit string.
_DERIVED: Dict[str, str] = {
    "game_id": _GAME_ID,
    "team_id": _TEAM_ID,
    "home_team_id": "Home team. " + _TEAM_ID,
    "away_team_id": "Away team. " + _TEAM_ID,
    "series_home_team_id": "Team with home ice in the series. " + _TEAM_ID,
    "series_away_team_id": "The other team in the series. " + _TEAM_ID,
    "player_id": _PLAYER_ID,
    "vimeo_video_id": "Full-game video. " + _VIMEO,
    "highlight_vimeo_video_id": "Highlights video. " + _VIMEO,
    "video_vimeo_id": "Clip of this event. " + _VIMEO,
    "spectators": "Attendance (Int64; the API sends a digit string).",
    "team_side": "home or away: the side of the team this row belongs to.",
    "event_index": "1-based position of the event in the game's timeline.",
    "game_seconds": "Elapsed game time of the event in seconds, from the MMSS `time` (822 = 8:22 -> 502).",
    "unit": "Lineup group: goalkeeper, or the line number 1-4 as a string.",
    "player_number": "Shooter's sweater number, from the game's lineup (a string).",
    "player_first_name": "Shooter's first name, from the game's lineup.",
    "player_last_name": "Shooter's last name, from the game's lineup.",
    "cms_node_id": "Umbraco CMS node id of the club page (Int64; not a team id).",
    "penalties": "Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot.",
    "faceoffs": "Faceoffs taken (upstream `fo`: fow + fol). Not penalties.",
    "faceoffs_percentage": "Faceoff win percentage (upstream `foPercentage`); -1 without faceoffs.",
    "pos_x": "Across-ice position in metres, 0-30 (rink width). Both axes run in metres on a 30 x 60 m rink.",
    "pos_y": (
        "Distance in metres from the end boards of the attacked end, 0-60 along the rink length. Shots are mirrored "
        "to one attacking end (inferred); above 30 is a shot from beyond centre; the goal line is 4 m out."
    ),
    "pos_y_percentage": "pos_y / 24, the depth of the app's zone drawing; above 1 for long shots.",
    "xg": (
        "Expected goals of this shot, 0-1. Non-zero from the 2023 season (one sampled game per season; 0 in the "
        "2021 and 2022 games); always 0 on a BLOCK."
    ),
}


# Returns-md texts under the description floor (15 characters, tests/codegen/test_manual_descriptions.py),
# spelled out without changing their meaning.
_LONGER: Dict[str, str] = {
    "First assists": "First (primary) assists credited",
    "Second assists": "Second (secondary) assists credited",
    "Arena name": "Name of the arena the game is played in",
    "File extension": "File extension of the club logo media file",
    "Media title": "Title of the club logo media item in the club CMS",
    "First name": "First (given) name of the player",
    "Last name": "Last (family) name of the player",
    "Faceoffs lost": "Number of faceoffs lost",
    "Faceoffs won": "Number of faceoffs won",
    "Goals": "Number of goals scored",
    "Goals against": "Goals against: number of goals conceded",
    "Games played": "Number of games played",
    "Goals per game": "Goals scored per game played",
    "Overtime wins": "Number of wins in overtime",
    "Shootout wins": "Number of wins decided in a shootout",
    "Team captain": "Whether the player is the team captain",
    "Club name": "Club name from the club's CMS record",
    "Team display name": "Team name as the league app displays it",
    "Shootout goals": "Goals scored in shootouts",
    "Shootout saves": "Saves made in shootouts",
    "Standings rank": "Position of the team in the standings table",
    "Shifts": "Number of shifts played",
    "Shutouts": "Number of shutouts (games without a goal conceded)",
    "Shots on goal": "Number of shots on goal",
    "Saves": "Number of saves made by the goaltender",
    "Saves per game": "Goaltender saves per game played",
}
_FILLER = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)


def _md_sections(path: Path) -> Dict[str, List[Tuple[str, str]]]:
    """``{route: [(column path, description), ...]}`` from the ``## `/route``` tables of the returns md."""
    out: Dict[str, List[Tuple[str, str]]] = {}
    route = None
    for line in path.read_text(encoding="utf-8").splitlines():
        head = re.match(r"^## `(/[^`]*)`", line)
        if head:
            route = head.group(1)
            out[route] = []
            continue
        if line.startswith("## "):
            route = None
        cells = [c.strip() for c in line.strip().strip("|").split("|")] if line.startswith("| `") else []
        if route and len(cells) >= 3:
            out[route].append((cells[0].strip("`"), cells[-1]))
    return out


def _key(rest: str) -> str:
    """``fields.logo.urlSegment`` -> ``fields_logo_url_segment`` (the parsers' flattened name)."""
    return "_".join(underscore(seg) for seg in rest.split(".") if seg and seg != "[]")


def _descriptions(rows: List[Tuple[str, str]], prefixes: Tuple[str, ...], renames: Dict[str, str]) -> Dict[str, str]:
    """Column -> description for the md rows directly under ``prefixes``; ``prefix=label`` prepends ``label``.

    A row nested one list deeper than its prefix (``[].games.[].x`` under ``[].``) belongs to another
    shape and is skipped; the first prefix that documents a column wins.
    """
    out: Dict[str, str] = {}
    for spec in prefixes:
        prefix, _, label = spec.partition("=")
        for col, desc in rows:
            rest = col[len(prefix) :] if col.startswith(prefix) else ""
            if rest and "[]" not in rest:
                out.setdefault(label + _key(rest), desc)
    for old, new in renames.items():
        if old in out:
            out.setdefault(new, out.pop(old))
    return out


def _sample(refs: Path, sample: str) -> Any:
    path = (
        (ROOT / "tests" / "fixtures" / STEM / sample)
        if sample.startswith("full/")
        else refs / "captures" / f"{sample}.json"
    )
    return json.loads(path.read_text(encoding="utf-8"))


def _params(path: str, op: dict) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    """``(path_params, extra_params, example_args)`` for one spec operation."""
    pps: List[dict] = []
    qps: List[dict] = []
    examples: Dict[str, Any] = {}
    for prm in op.get("parameters", []):
        wire, desc = prm["name"], str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            name = _PATH_PARAM[wire]
            pps.append({"name": name, "type": "int", "required": True, "description": _PATH_DOC[name]})
        elif prm.get("in") == "query" and wire not in _IGNORED_QUERY.get(path, set()):
            name = underscore(wire)
            qps.append({"name": name, "query_key": wire, "type": "int", "description": desc})
        else:
            continue
        if "example" in prm:
            examples[name] = int(prm["example"])
    return pps, qps, examples


def _entry(short: str, path: str, op: dict, parser: str, summary: Any) -> Dict[str, Any]:
    wire_path = path
    for wire, name in _PATH_PARAM.items():
        wire_path = wire_path.replace("{" + wire + "}", "{" + name + "}")
    pps, qps, examples = _params(path, op)
    assert not {"league", "sport"} & {p["name"] for p in pps}, path  # Review Focus 4: the renderer eats these
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (summary or op.get("summary") or f"GET {path}").rstrip(".") + ".",  # prose, period-terminated
        "path": wire_path,
        "parser": parser,
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": examples,
    }
    if pps:
        entry["path_params"] = pps
    if qps:
        entry["extra_params"] = qps
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = {path: ops["get"] for path, ops in spec["paths"].items() if "get" in ops}
    covered = {path for _, path, *_ in ENDPOINTS}
    assert covered == set(ops), f"spec routes without a wrapper: {sorted(set(ops) - covered)}"
    shorts = [e[0] for e in ENDPOINTS]
    assert len(shorts) == len(set(shorts)), shorts

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "nl_{short}",
        "module": STEM,
        "parser_module": "hockey.nl.nationalleague_parsers",
        "getter_module": "sportsdataverse.hockey.nl.nationalleague_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "schema_prefix": "nl_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: a 404, or an empty answer (no games or rows for that "
                "season, team or phase).",
                "sportsdataverse.errors.AssetFetchError: a failed fetch, an error object in a 200, a body "
                "without the ids it must carry, or an answer for another season, team, game or player.",
            ],
            "see_also": [
                {
                    "name": "National League",
                    "url": "https://www.nationalleague.ch/",
                    "note": "the league site whose app serves this API",
                },
                {
                    "name": "fastRhockey",
                    "url": "https://fastRhockey.sportsdataverse.org",
                    "note": "the SportsDataverse hockey package for R",
                },
            ],
            "notes": [_RULING, _INCREMENTAL],
        },
        "endpoints": [_entry(s, p, ops[p], parser, summary) for s, p, parser, _, _, summary in ENDPOINTS],
    }
    write_yaml(ROOT / "tools/codegen/endpoints" / f"{STEM}.yaml", doc)

    md = _md_sections(refs / f"{STEM}-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native" / STEM
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    blank = 0
    for short, path, parser, sample, prefixes, _ in ENDPOINTS:
        fn: Callable[..., pl.DataFrame] = getattr(P, parser)
        frame = fn(_sample(refs, sample))
        renames = _RENAMES.get(short, _SEASON_ROW_RENAMES if parser.endswith(("player_stats", "standings")) else {})
        cols = columns_from_frame(frame, [_DERIVED, _descriptions(md[path], prefixes, renames)], leaf_fallback=False)
        for col in cols:
            if col["description"]:
                text = _LONGER.get(col["description"], col["description"])
                assert len(text) >= 15 and text.lower() != col["name"] and not _FILLER.match(text), (short, col)
                snippet.setdefault(f"nl_{short}", {})[col["name"]] = text
            else:
                blank += 1
            col["description"] = ""  # prose goes to the snippet, never into schemas/**
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"nl_{short}", "kind": "dataframe", "columns": cols})
    write_yaml(SNIPPET, snippet)
    total = sum(len(v) for v in snippet.values())
    print(f"{STEM}: {len(ENDPOINTS)} endpoints over {len(ops)} routes; {total} descriptions, {blank} undescribed")


if __name__ == "__main__":
    main()
