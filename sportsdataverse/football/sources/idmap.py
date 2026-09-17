"""Pre-kickoff id map: ESPN event / team ids <-> every alternate source's ids (private, experimental).

The map is built **before kickoff from schedules** (a nightly job) and stored as two small
parquets GOP reads in O(1) at request time. It is never built on ESPN at request time --
that is the case it exists for. Today only the NFL ESPN <-> Shield <-> nflverse leg is
materialised, from nfl-raw's ``nfl/espn/crosswalk/{games,teams}.json``; the Yahoo NFL ids are
pure functions of that row (``nfl.g.{ET date}{ESPN home id:03d}``, ``nfl.t.{ESPN team id}``)
and are filled in the same pass. CBS / Fox / NCAA columns exist in the schema and stay null
until their builders land (CBS ids come from the week scoreboard page; Fox and Yahoo CFB from
``cfb_schedule_crosswalk``; NCAA from ``ncaa-mfb-football-raw`` stage 06 restricted to its
pre-game tiers).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

GAME_SCHEMA: dict[str, pl.DataType] = {
    "league": pl.Utf8,
    "season": pl.Int32,
    "season_type": pl.Int32,
    "week": pl.Int32,
    "espn_event_id": pl.Utf8,
    "kickoff_utc": pl.Utf8,
    "neutral_site": pl.Boolean,
    "home_espn_team_id": pl.Utf8,
    "away_espn_team_id": pl.Utf8,
    "shield_game_id": pl.Utf8,
    "nflverse_game_id": pl.Utf8,
    "cbs_game_id": pl.Utf8,
    "yahoo_game_id": pl.Utf8,
    "fox_game_id": pl.Utf8,
    "ncaa_game_id": pl.Utf8,
}
"""One row per game. Every id is Utf8: ids are labels, never arithmetic (join-key dtype discipline)."""

TEAM_SCHEMA: dict[str, pl.DataType] = {
    "league": pl.Utf8,
    "espn_team_id": pl.Utf8,
    "espn_abbr": pl.Utf8,
    "shield_team_id": pl.Utf8,
    "nflverse_abbr": pl.Utf8,
    "cbs_team_id": pl.Utf8,
    "yahoo_team_id": pl.Utf8,
    "fox_team_id": pl.Utf8,
    "ncaa_team_id": pl.Utf8,
}
"""One row per team (per league); the adapter's team-id rewrite table."""

_ET = ZoneInfo("America/New_York")
_GAMES_FILE, _TEAMS_FILE = "games.parquet", "teams.parquet"


def _yahoo_nfl_game_id(kickoff_utc: str, home_espn_team_id: str | int) -> str:
    """Yahoo NFL game id = ``nfl.g.{YYYYMMDD in US-Eastern}{ESPN home id, zero-padded to 3}``.

    Verified on all 16 2026 week-1 games and Super Bowl LX (``B_yahoo_nfl.md`` §4), including
    the Wednesday-opener date roll (``2026-09-10T00:20Z`` -> ``20260909``).
    """
    dt = datetime.strptime(kickoff_utc, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
    return f"nfl.g.{dt.astimezone(_ET):%Y%m%d}{int(home_espn_team_id):03d}"


def _build_nfl_idmap(games_json: str | Path, teams_json: str | Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build the NFL id map from nfl-raw's ESPN crosswalk (offline, no network).

    Args:
        games_json: ``nfl-raw/nfl/espn/crosswalk/games.json`` (list of rows with ``espn_event_id``,
            ``shield_game_id``, ``game_id`` (nflverse), ``kickoff_utc``, home/away ESPN team ids).
        teams_json: ``nfl-raw/nfl/espn/crosswalk/teams.json``.

    Returns:
        ``(games, teams)`` frames in :data:`GAME_SCHEMA` / :data:`TEAM_SCHEMA`.

        | column | filled from |
        |---|---|
        | shield_game_id, nflverse_game_id | crosswalk verbatim |
        | yahoo_game_id, yahoo_team_id | computed (:func:`_yahoo_nfl_game_id`; ``nfl.t.{espn id}``) |
        | cbs_*, fox_*, ncaa_* | null until their builders land |
    """
    rows = json.loads(Path(games_json).read_text(encoding="utf-8"))
    games = pl.DataFrame(
        [
            {
                "league": "nfl",
                "season": r["season"],
                "season_type": r["season_type"],
                "week": r["week"],
                "espn_event_id": str(r["espn_event_id"]),
                "kickoff_utc": r["kickoff_utc"],
                "neutral_site": bool(r.get("neutral_site", False)),
                "home_espn_team_id": str(r["home_espn_team_id"]),
                "away_espn_team_id": str(r["away_espn_team_id"]),
                "shield_game_id": r.get("shield_game_id"),
                "nflverse_game_id": r.get("game_id"),
                "cbs_game_id": None,
                "yahoo_game_id": _yahoo_nfl_game_id(r["kickoff_utc"], r["home_espn_team_id"]),
                "fox_game_id": None,
                "ncaa_game_id": None,
            }
            for r in rows
        ],
        schema=GAME_SCHEMA,
    )
    trows = json.loads(Path(teams_json).read_text(encoding="utf-8"))
    teams = pl.DataFrame(
        [
            {
                "league": "nfl",
                "espn_team_id": str(t["espn_team_id"]),
                "espn_abbr": t.get("espn_abbr"),
                "shield_team_id": t.get("shield_team_id"),
                "nflverse_abbr": t.get("nflverse_abbr"),
                "cbs_team_id": None,
                "yahoo_team_id": f"nfl.t.{int(t['espn_team_id'])}",
                "fox_team_id": None,
                "ncaa_team_id": None,
            }
            for t in trows
        ],
        schema=TEAM_SCHEMA,
    )
    return games, teams


def _write_idmap(games: pl.DataFrame, teams: pl.DataFrame, directory: str | Path) -> Path:
    """Write ``games.parquet`` + ``teams.parquet`` into ``directory`` (created if needed); returns it."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    games.select(*GAME_SCHEMA).write_parquet(directory / _GAMES_FILE)
    teams.select(*TEAM_SCHEMA).write_parquet(directory / _TEAMS_FILE)
    return directory


def _load_idmap(directory: str | Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read the two parquets back; dtypes are asserted against the schemas."""
    directory = Path(directory)
    games = pl.read_parquet(directory / _GAMES_FILE)
    teams = pl.read_parquet(directory / _TEAMS_FILE)
    for frame, schema, name in ((games, GAME_SCHEMA, "games"), (teams, TEAM_SCHEMA, "teams")):
        if dict(frame.schema) != schema:
            raise ValueError(f"{name}.parquet schema drift: {dict(frame.schema)} != {schema}")
    return games, teams


def _lookup(games: pl.DataFrame, espn_event_id: str | int, teams: pl.DataFrame | None = None) -> dict | None:
    """The id-map row for one ESPN event id (plus ``home_team`` / ``away_team`` rows when ``teams`` is given).

    Returns:
        The game row as a dict, or None when the game is not mapped. With ``teams``, the dict also
        carries ``home_team`` and ``away_team`` (their :data:`TEAM_SCHEMA` rows, or None).
    """
    hit = games.filter(pl.col("espn_event_id") == str(espn_event_id))
    if hit.is_empty():
        return None
    row = hit.row(0, named=True)
    if teams is not None:
        for side in ("home", "away"):
            t = teams.filter(
                (pl.col("league") == row["league"]) & (pl.col("espn_team_id") == row[f"{side}_espn_team_id"])
            )
            row[f"{side}_team"] = t.row(0, named=True) if not t.is_empty() else None
    return row
