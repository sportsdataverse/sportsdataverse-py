"""Generate the four eSports hokejovyzapis flat-API families from one OpenAPI spec.

The eSports s.r.o. scoring system writes one key layout into four public S3 buckets, so one spec
(``sdv-internal-refs/esports-hokejovyzapis/esports-hokejovyzapis.openapi.yaml``) serves four
families that differ only in their base URL ``https://s3.eu-west-1.amazonaws.com/<bucket>``:

=================  ==========================  =======  ==================
stem               bucket                      prefix   package
=================  ==========================  =======  ==================
``del_esports``    ``de.hokejovyzapis.cz``     ``del``  ``hockey.del_``
``icehl_esports``  ``icehl.hokejovyzapis.cz``  ``icehl`` ``hockey.icehl``
``eihl_esports``   ``eihl.hokejovyzapis.cz``   ``eihl`` ``hockey.eihl``
``metal_esports``  ``den.hokejovyzapis.cz``    ``metal`` ``hockey.metal``
=================  ==========================  =======  ==================

Each family lists only the paths whose per-path ``servers`` bucket enum names its bucket (Denmark
has no shots, ``roster.json`` and ``shiftsSC`` are DEL only, EIHL's game file is ``game-info``). The
listing route ``/`` (S3 ListObjectsV2 XML) is left out: the wrappers' getter is JSON-only.

Emits, idempotently:

* ``tools/codegen/endpoints/<stem>.yaml``;
* ``tools/codegen/schemas/native/<stem>/<short>.yaml``: columns and dtypes from running the
  endpoint's parser over ``tests/fixtures/<stem>/full/<slug>.json`` when a full body is committed,
  else over the truncated refs capture; descriptions are left blank (they belong in
  ``manual_column_descriptions.yaml``);
* ``tools/codegen/notes/esports_hokejovyzapis_descriptions.yaml``: the descriptions for that file,
  keyed by schema, translated from the refs ``esports-hokejovyzapis-returns.md`` (integration merges
  them);
* ``sportsdataverse/hockey/_esports_schemas.py``: the same columns and types as package data, so the
  parsers give an empty or partial frame the dtypes of a populated one without reading ``tools/``.

Run: ``python tools/codegen/gen_esports_hokejovyzapis.py``
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.hockey import esports_parsers as P

SOURCE = "esports-hokejovyzapis"
SPEC = f"{SOURCE}.openapi.yaml"
SERVER = "https://s3.eu-west-1.amazonaws.com/{bucket}"
NOTES = ROOT / "tools/codegen/notes/esports_hokejovyzapis_descriptions.yaml"
PACKAGE_SCHEMAS = ROOT / "sportsdataverse/hockey/_esports_schemas.py"
_PACKAGE_SCHEMAS_HEAD = '''"""Returns-schema columns and types of the eSports hokejovyzapis families (package data).

``SCHEMAS[stem][short]`` is ``{column: type}`` in documented order, the R-style type of
``tools/codegen/schemas/native/<stem>/<short>.yaml`` (``integer``, ``numeric``, ``character``,
``logical``). :mod:`sportsdataverse.hockey.esports_parsers` reads it to give an empty or partial
frame the dtypes of a populated one. Written by ``tools/codegen/gen_esports_hokejovyzapis.py``;
do not edit.
"""

from typing import Dict, List

__all__: List[str] = []

# fmt: off
SCHEMAS: Dict[str, Dict[str, Dict[str, str]]] = '''


def write_package_schemas(schemas: Dict[str, Dict[str, Dict[str, str]]]) -> None:
    """Write :data:`PACKAGE_SCHEMAS` (``json.dumps`` layout, left alone by ruff format)."""
    body = json.dumps(schemas, indent=4, ensure_ascii=False)
    with PACKAGE_SCHEMAS.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(f"{_PACKAGE_SCHEMAS_HEAD}{body}\n# fmt: on\n")


@dataclass(frozen=True)
class Family:
    stem: str
    bucket: str
    prefix: str
    label: str
    site: str
    league_default: int
    league_doc: str
    example: Dict[str, int]  # a completed 2026/27 game: match_id, team_id, league_part_id (season is fixed)

    @property
    def capture_dir(self) -> str:
        return self.bucket.split(".")[0]


FAMILIES = (
    Family(
        "del_esports",
        "de.hokejovyzapis.cz",
        "del",
        "DEL",
        "https://www.penny-del.org/",
        1,
        "1 = DEL regular season, 3 = DEL playoffs (also 2 = Test-Liga pre-season, 4 = MagentaSport Cup 2020/21, "
        "5 = NHL Global Series 2022/23).",
        {"match_id": 4444, "team_id": 12},
    ),
    Family(
        "icehl_esports",
        "icehl.hokejovyzapis.cz",
        "icehl",
        "ICE Hockey League",
        "https://www.ice.hockey/",
        1,
        "1 = ICE Hockey League, regular season and playoffs in one file (split them by league_part_name); "
        "2 = pre-season (2025/26 on).",
        {"match_id": 8161, "team_id": 4, "league_part_id": 56},
    ),
    Family(
        "eihl_esports",
        "eihl.hokejovyzapis.cz",
        "eihl",
        "EIHL",
        "https://www.eliteleague.co.uk/",
        3,
        "3 = Elite League, 4 = Challenge Cup, 5 = playoffs.",
        {"match_id": 4440, "team_id": 96},
    ),
    Family(
        "metal_esports",
        "den.hokejovyzapis.cz",
        "metal",
        "Metal Ligaen",
        "https://www.metalligaen.dk/",
        1,
        "1 = Metal Ligaen, regular season and playoffs in one file (split them by league_part_name); "
        "2 = pre-season, 4 = Final-4 cup.",
        {"match_id": 575, "team_id": 8},
    ),
)

#: Example season, by its END year: 2027 = 2026/27, the bucket key ``2026``.
EXAMPLE_SEASON = 2027

Parser = Callable[..., Any]
# spec path -> (wrapper short, parser, summary). Every spec path except "/" must be listed.
ROUTES: Dict[str, Tuple[str, Parser, str]] = {
    "/league-matches/{season}/{league_id}.json": (
        "schedule",
        P.parse_esports_schedule,
        "Schedule and results of one competition season, one row per match, played or not.",
    ),
    "/matches/{match_id}/game-header.json": (
        "game_info",
        P.parse_esports_game_info,
        "Game info of one match: state, arena, attendance, officials, coaches, period scores.",
    ),
    "/matches/{match_id}/game-info.json": (
        "game_info",
        P.parse_esports_game_info,
        "Game info of one match: state, arena, attendance, officials, coaches, period scores.",
    ),
    "/matches/{match_id}/period-events.json": (
        "pbp",
        P.parse_esports_pbp,
        "Play-by-play of one match: goals, penalties, goalie changes and timeouts, one row per event.",
    ),
    "/matches/{match_id}/team-stats/{team_id}.json": (
        "player_box",
        P.parse_esports_player_stats,
        "Player boxscore of one team in one match, one row per player.",
    ),
    "/matches/{match_id}/top-goalies.json": (
        "goalie_box",
        P.parse_esports,
        "Goalie lines of one match, one row per goalie who played.",
    ),
    "/matches/{match_id}/faceoffs.json": (
        "faceoffs",
        P.parse_esports,
        "Every faceoff of one match: time, dot, winner and loser.",
    ),
    "/matches/{match_id}/shiftsSC.json": (
        "shifts",
        P.parse_esports_shifts,
        "Every shift of one match, one row per player shift.",
    ),
    "/matches/{match_id}/roster.json": (
        "roster",
        P.parse_esports_lineup,
        "Game roster of one match by line slot, one row per dressed player.",
    ),
    "/visualization/shots/{match_id}.json": (
        "shots",
        P.parse_esports_shots,
        "Shots of one match with rink coordinates and result, one row per shot.",
    ),
    "/tables/{season}/{league_id}.json": (
        "standings",
        P.parse_esports_standings,
        "Standings of one competition season, one row per team.",
    ),
    "/table/{season}/{league_id}.json": (
        "standings",
        P.parse_esports_standings,
        "Standings of one competition season, one row per team.",
    ),
    "/league-top-scorers/{season}/{league_id}.json": (
        "top_scorers",
        P.parse_esports,
        "Skater scoring leaderboard of one competition season.",
    ),
    "/league-team-stats/{season}/{league_id}/{team_id}.json": (
        "team_player_stats",
        P.parse_esports_player_stats,
        "Season player stats of one team in one competition, one row per player.",
    ),
    "/widget/esports/match/{match_id}.json": (
        "game_center",
        P.parse_esports,
        "Game-center bundle of one match as one row: game data, team stats, goal and penalty lists.",
    ),
    "/league-part-players-stats/{league_part_id}.json": (
        "league_part_player_stats",
        P.parse_esports_player_stats,
        "Every skater's stats in one league part (a competition stage), one row per player.",
    ),
}

_PARAM_DOCS = {
    "season": "Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the "
    "wrapper requests season - 1.",
    "match_id": "Match id (per-bucket match key space: match_id in {prefix}_schedule). Not the league "
    "website's game id.",
    "team_id": "Team id (per-bucket team key space: home_id / guest_id in {prefix}_schedule).",
    "league_part_id": "League part id, one stage of a competition (league_part_id in {prefix}_schedule).",
}


def slug(path: str) -> str:
    """The refs ``capture.py`` slug: ``/matches/{match_id}/faceoffs.json`` -> ``matches__match_id__faceoffs``."""
    return path.strip("/").replace("/", "__").replace("{", "").replace("}", "").removesuffix(".json")


def bucket_paths(spec: dict, bucket: str) -> List[Tuple[str, dict]]:
    """``[(path, get_op)]`` the spec's per-path ``servers`` say ``bucket`` serves, listing route excluded."""
    out = []
    for path, item in spec["paths"].items():
        enum = item["servers"][0]["variables"]["bucket"]["enum"]
        if path != "/" and bucket in enum:
            out.append((path, item["get"]))
    return out


def _path_param(fam: Family, prm: dict) -> Dict[str, Any]:
    name = prm["name"]
    entry: Dict[str, Any] = {"name": name, "type": "int", "required": True}
    if name == "league_id":
        entry.update(required=False, default=fam.league_default, description=f"Competition id. {fam.league_doc}")
    else:
        entry["description"] = _PARAM_DOCS[name].format(prefix=fam.prefix)
    if name == "season":
        entry["transform"] = "season_start_year"
    return entry


def _example(fam: Family, name: str) -> int:
    if name == "season":
        return EXAMPLE_SEASON
    return fam.league_default if name == "league_id" else fam.example[name]


def endpoint_entry(fam: Family, path: str, op: dict) -> Dict[str, Any]:
    short, parser, summary = ROUTES[path]
    params = [p for p in op.get("parameters", []) if p.get("in") == "path"]
    assert not {"league", "sport"} & {p["name"] for p in params}, path  # generate.py substitutes ESPN slugs
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "path_params": [_path_param(fam, p) for p in params],
        "example_args": {p["name"]: _example(fam, p["name"]) for p in params},
        "parser": parser.__name__,
        "returns_schema": f"native/{fam.stem}/{short}",
    }


def family_doc(fam: Family, ops: List[Tuple[str, dict]]) -> Dict[str, Any]:
    return {
        "api": fam.stem,
        "host": SERVER.format(bucket=fam.bucket),
        "name_pattern": f"{fam.prefix}_{{short}}",
        "module": fam.stem,
        "parser_module": "hockey.esports_parsers",
        "getter_module": "sportsdataverse.hockey.esports_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "schema_prefix": f"{fam.prefix}_",
        "parser_columns": True,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: the bucket has no such key (S3 404 NoSuchKey: unknown "
                "season, competition, match or team), or the file holds no rows (an unplayed game, an empty "
                "shot shell).",
            ],
            "see_also": [
                {
                    "name": fam.label,
                    "url": fam.site,
                    "note": "the league site whose widgets read this public eSports bucket",
                },
            ],
        },
        "endpoints": [endpoint_entry(fam, p, op) for p, op in ops],
    }


def source_body(refs: Path, fam: Family, path: str) -> Optional[Path]:
    """The full committed body when there is one, else the truncated refs capture."""
    full = ROOT / "tests/fixtures" / fam.stem / "full" / f"{slug(path)}.json"
    capture = refs / "captures" / fam.capture_dir / f"{slug(path)}.json"
    return full if full.exists() else capture if capture.exists() else None


# --------------------------------------------------------------------------------------------
# Descriptions (for manual_column_descriptions.yaml), from the refs returns doc
# --------------------------------------------------------------------------------------------

_SECTION = re.compile(r"^## `([^`]+)`(?: — (.+))?$")
_ROW = re.compile(r"^\| `([^`]+)` \| [^|]+ \| (.*) \|$")
_ABSENT = re.compile(r"\s*Absent in: [^.]*\.\s*$")
_ROLE = re.compile(r"^(scorer|assist\d|penalized|replaced|fouled|goalie|outgoing_goalie)_(player_id|jersey|name)$")
_ROLE_TEXT = {
    "scorer": "goal scorer",
    "assist1": "first assist",
    "assist2": "second assist",
    "penalized": "penalized player",
    "replaced": "player serving the penalty in the penalized player's place",
    "fouled": "fouled player",
    "goalie": "goalie entering the game (goalkeeperChange)",
    "outgoing_goalie": "goalie leaving the game (goalkeeperChange)",
}
_FIELD_TEXT = {"player_id": "player id (playerId key space)", "jersey": "jersey number", "name": "full name"}

#: Columns the parsers derive or rename, so the returns doc has no row for them.
DERIVED = {
    "match_id": "Match id (per-bucket match key space); joins to the {match_id} path param of the game wrappers.",
    "event_index": "Row order of the event within the game (0-based), across periods.",
    "period": "Period key as the body sends it: 1, 2, 3, OT (overtime) or SO (shootout).",
    "period_number": "Period number: 1-3, 4 = overtime, 5 = shootout.",
    "event_type": "Event type: goal, penalty, goalkeeperChange, timeout, periodStart, periodEnd, videoReview, ...",
    "event_id": "Event id within its event type.",
    "team_side": "Team of the row: home or visitor.",
    "extra_attacker": "True when the scoring team had an extra attacker on (goalie pulled).",
    "empty_net": "True when the goal went into an empty net.",
    "plus_player_ids": "Comma-separated player ids (playerId key space) of the scoring team's players on ice.",
    "minus_player_ids": "Comma-separated player ids (playerId key space) of the conceding team's players on ice.",
    "penalty_from_real_time": "Penalty start, local wall clock YYYY-MM-DD HH:MM:SS.",
    "penalty_from_scoreboard_time": "Penalty start, game second.",
    "penalty_to_real_time": "Penalty end, local wall clock YYYY-MM-DD HH:MM:SS.",
    "penalty_to_scoreboard_time": "Penalty end, game second.",
    "shot_id": "Shot id within the shot files.",
    "shot_result_id": "Shot result code, the same value as match_shot_resutl_id: 4 = goal (verified); 1, 2 and 3 "
    "(on goal / missed / blocked in the widgets) are unverified.",
    "is_goal": "True when the shot result code is 4 (goal).",
    "slot": "Line slot, three digits <group><line><position>: group 1 = goalies (101, 102), 2 = defence pairs, "
    "3 = forward lines.",
    "loser_id": "Player id (playerId key space) of the faceoff loser (DEL spells the key losser, ICEHL looser).",
    "loser_name": "Full name of the faceoff loser.",
    "overtime_score_home": "Home team goals in overtime (present once a game of the season went to overtime).",
    "overtime_score_guest": "Visiting team goals in overtime (present once a game of the season went to overtime).",
    "shootout_score_home": "Home team shootout goals (present once a game of the season went to a shootout).",
    "shootout_score_guest": "Visiting team shootout goals (present once a game of the season went to a shootout).",
}
_SHORT_DERIVED = {
    (
        "pbp",
        "time",
    ): "Game second of the event from the opening face-off (0-3600 in regulation, continuing through overtime).",
    ("shots", "xg"): "Expected-goals value of the shot (ICEHL and EIHL only; null elsewhere).",
    (
        "shots",
        "xg_type",
    ): "Shot-danger class from the xG model, e.g. LD / MD / HD (ICEHL and EIHL only; null elsewhere).",
    ("shifts", "shift_id"): "Shift id within the shift files.",
    ("shifts", "start_time"): "Shift start, game second.",
    ("shifts", "end_time"): "Shift end, game second.",
    ("shifts", "start_real_time"): "Shift start, local wall clock.",
    ("shifts", "end_real_time"): "Shift end, local wall clock.",
    ("shifts", "duration"): "Shift length in seconds (end_time - start_time).",
    ("player_box", "name"): "Player full name.",
    ("team_player_stats", "name"): "Player full name.",
    ("league_part_player_stats", "name"): "Player full name.",
    ("schedule", "season"): "Season by its END year (2027 = 2026/27); the body's START-year key plus one.",
    ("player_box", "player_id"): "Player id (playerId key space, per bucket, stable across games and seasons).",
    ("team_player_stats", "player_id"): "Player id (playerId key space, per bucket, stable across games and seasons).",
    ("league_part_player_stats", "player_id"): "Player id (playerId key space, per bucket, stable across seasons).",
    ("standings", "team_id"): "Team id (per-bucket team key space); joins to home_id / guest_id in the schedule.",
}


def section_maps(md: Path) -> Dict[Tuple[str, str], Dict[str, str]]:
    """``{(path, bucket_dir): {suffix_key: description}}`` from the returns doc's per-path tables.

    A row ``matches.[].home.id`` is reachable under every suffix (``matches_home_id``, ``home_id``,
    ``id``); the suffix that drops the fewest leading segments wins, so ``id`` resolves to the
    match id, not a team id.
    """
    ranked: Dict[Tuple[str, str], Dict[str, Tuple[int, str]]] = {}
    current: List[Dict[str, Tuple[int, str]]] = []
    for line in md.read_text(encoding="utf-8").splitlines():
        if m := _SECTION.match(line):
            buckets = [b.strip() for b in (m.group(2) or "de, icehl, eihl, den").split(",")]
            current = [ranked.setdefault((m.group(1), b), {}) for b in buckets]
        elif line.startswith("## "):
            current = []
        elif current and (r := _ROW.match(line)):
            segs = [s for s in r.group(1).split(".") if s not in ("[]", "<key>")]
            text = _ABSENT.sub("", r.group(2)).strip()
            for drop in range(len(segs)):
                name = "_".join(P._key(s) for s in segs[drop:])
                for d in current:
                    if name not in d or d[name][0] > drop:
                        d[name] = (drop, text)
    return {k: {n: t for n, (_drop, t) in v.items()} for k, v in ranked.items()}


def describe(short: str, col: str, cols: List[str], section: Dict[str, str]) -> str:
    if (short, col) in _SHORT_DERIVED:
        return _SHORT_DERIVED[(short, col)]
    if col in DERIVED:
        return DERIVED[col]
    if m := _ROLE.match(col):
        return f"{_FIELD_TEXT[m.group(2)][:1].upper()}{_FIELD_TEXT[m.group(2)][1:]} of the {_ROLE_TEXT[m.group(1)]}."
    if f"{col}_home" in cols and f"{col}_away" in cols:
        return f"{col} in total ({col}_home + {col}_away: the body splits it by home and away games)."
    return section.get(col, "")


#: Shorts whose schema is the union of every bucket's columns. One scoring system writes the same
#: period-events layout everywhere, but a capture only shows the event types its game had (the EIHL
#: one has no penalty, so no ``penalty_*`` / ``penalized_*`` columns): a column another bucket's
#: capture shows is documented for every bucket, so a game with that event never leaves the schema.
UNION_SHORTS = frozenset({"pbp"})


def main() -> None:
    refs = refs_dir(SOURCE, SPEC)
    spec = load_spec(refs, SPEC)
    unknown = sorted(set(spec["paths"]) - set(ROUTES) - {"/"})
    if unknown:
        raise SystemExit(f"{SOURCE}: spec paths without a route entry: {unknown}")
    sections = section_maps(refs / f"{SOURCE}-returns.md")
    plan = [(fam, bucket_paths(spec, fam.bucket)) for fam in FAMILIES]
    frames: Dict[Tuple[str, str], Any] = {}
    for fam, ops in plan:
        for path, _op in ops:
            body = source_body(refs, fam, path)
            if body is None:
                raise SystemExit(f"{fam.stem}: no capture for {path}")
            frames[(fam.stem, path)] = ROUTES[path][1](json.loads(body.read_text(encoding="utf-8")))
    union: Dict[str, Dict[str, Any]] = {}  # path -> {column: dtype}, first bucket that shows it wins
    for (_stem, path), df in frames.items():
        if ROUTES[path][0] in UNION_SHORTS:
            for name, dtype in df.schema.items():
                union.setdefault(path, {}).setdefault(name, dtype)
    notes: Dict[str, Dict[str, str]] = {}
    package: Dict[str, Dict[str, Dict[str, str]]] = {}
    for fam, ops in plan:
        write_yaml(ROOT / f"tools/codegen/endpoints/{fam.stem}.yaml", family_doc(fam, ops))
        schema_dir = ROOT / f"tools/codegen/schemas/native/{fam.stem}"
        rewrite_schema_dir(schema_dir)
        described = total = 0
        for path, _op in ops:
            short = ROUTES[path][0]
            df = frames[(fam.stem, path)]
            extra = {n: t for n, t in union.get(path, {}).items() if n not in df.columns}
            df = df.with_columns(pl.lit(None, dtype=t).alias(n) for n, t in extra.items()) if extra else df
            key = f"{fam.prefix}_{short}"
            columns = columns_from_frame(df, [], leaf_fallback=False)
            write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": columns})
            package.setdefault(fam.stem, {})[short] = {c["name"]: c["type"] for c in columns}
            own = sections.get((path, fam.capture_dir), {})
            others = [sections.get((path, f.capture_dir), {}) for f in FAMILIES if f is not fam]
            notes[key] = {}
            for c in df.columns:
                text = describe(short, c, df.columns, own)
                if not text and c in extra:  # documented from the bucket whose capture shows it
                    text = next((t for o in others if (t := describe(short, c, df.columns, o))), "")
                notes[key][c] = text.replace("{prefix}", fam.prefix)
            total += len(df.columns)
            described += sum(1 for v in notes[key].values() if v)
        print(f"{fam.stem}: {len(ops)} endpoints, {described}/{total} columns described")
    write_yaml(NOTES, notes)
    write_package_schemas({stem: dict(sorted(shorts.items())) for stem, shorts in sorted(package.items())})


if __name__ == "__main__":
    main()
