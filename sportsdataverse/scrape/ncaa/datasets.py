"""Committed dataset trees: schedules, teams, rosters (html + json + parquet).

**Zero extra HTTP.** Every page these trees are built from is already fetched by
an existing stage, and this module only owns the persist/enrich/aggregate side:

- ``schedules`` piggybacks on :mod:`ncaa_discover`'s team-page sweep, which
  fetches ``teams/{team_id}`` for every team in the season and (before this
  module) parsed out the contest ids and threw the HTML away.
- ``rosters`` piggybacks on :mod:`ncaa_rosters`' sweep of
  ``teams/{team_id}/roster``.
- ``teams`` needs no fetch at all -- it is the bundled sdv-py ``(team, season)
  -> id`` crosswalk, reshaped.

Layout (``{lg}`` = ``mbb``/``wbb``, ``{season}`` = ending year, e.g. 2026)::

    {lg}/schedules/html/{season}/{team_id}.html   <- per-team source page
    {lg}/schedules/json/{season}/{team_id}.json   <- per-team parsed schedule
    {lg}/schedules/parquet/{season}.parquet       <- ONE compiled season dataset
    {lg}/rosters/html/{season}/{team_id}.html     <- per-team source page
    {lg}/rosters/json/{season}/{team_id}.json     <- per-team parsed roster
    {lg}/rosters/parquet/{season}.parquet         <- ONE compiled season dataset
    {lg}/teams/{html,json,parquet}/{season}.*     <- one file per season

**html + json are per team; parquet is the compiled season dataset.** There is
no per-team parquet and no json aggregate -- the season view is parquet-only,
exactly one file per season per kind, so the tree never accumulates hundreds of
tiny parquet fragments.

**The committed HTML is the durable checkpoint.** Both sweeps skip a team whose
HTML already exists and re-derive its json from it offline, so a re-run never
re-fetches a page this repo already holds, and a parser fix can be re-applied to
every season with :func:`rebuild_missing` -- no network.

**Shard safety.** Per-team html/json are disjoint per shard, so ``--shard i/N``
workers never collide. The season parquet is NOT built by the shards (each sees
only its slice, and concurrent writers would race one file into a truncated
mess); it is compiled by a separate non-sharded pass over the per-team json --
:func:`build_season_aggregate`, driven by ``scripts/run_datasets.sh``.

Every frame carries BOTH machine ids and human-readable names: schedules carry
``team``/``opponent`` next to ``team_id``/``opponent_id``, rosters carry
``clean_name`` (properly-cased display name) and ``player`` (the ALL-CAPS
``FIRST.LAST`` key the play-by-play stream uses) next to ``player_id``.

**These three trees are where the ESPN identity lives.** All of them are
reference data, so each carries the ESPN team id off the same
``ncaa_espn_team_crosswalk`` row -- schedules for BOTH sides
(``espn_team_id`` / ``opponent_espn_team_id``), rosters for the roster's team,
``teams`` for the team plus ESPN's display name and mascot. The per-game
families deliberately do NOT: they carry ``ncaa_team_id`` and join here.
"""

from __future__ import annotations

import argparse
import html as _html
import importlib
import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_schedule import parse_ncaa_bb_team_roster, parse_ncaa_bb_team_schedule
from sportsdataverse.mbb.mbb_ncaa_team_ids import ncaa_mbb_team_ids
from sportsdataverse.wbb.wbb_ncaa_team_ids import ncaa_wbb_team_ids

logger = logging.getLogger(__name__)

#: This repo's league token -- the only line that differs from the MBB sibling.
# NOTE: the per-repo league token lives in each -raw repo's shim, not here --
# a shared engine that defaults to one league is how a wbb run silently
# reads mbb data (see capture.py's pre-extraction hardcode).

#: league -> bundled ``(team, season) -> id`` crosswalk getter. Two entries, and
#: the leaf module owns it so ncaa_discover/ncaa_rosters can import it without a
#: cycle. ponytail: a dict, not a plugin registry.
TEAM_ID_CROSSWALKS: Dict[str, Callable[[], pl.DataFrame]] = {
    "mbb": ncaa_mbb_team_ids,
    "wbb": ncaa_wbb_team_ids,
}

#: The bundled crosswalk is scoped to the Division-I ``season_divisions`` id
#: (that is the only division the scoreboard sweep in ``refresh_ncaa_team_ids``
#: walks), so every row here is D-I by construction. Stamped as a column rather
#: than left implicit so a future D-II/D-III tree can union in without a schema
#: change.
DIVISION = "I"

_KINDS = ("schedules", "rosters", "teams")
_EXT = {"html": "html", "json": "json", "parquet": "parquet"}


def _is_per_team(kind: str, fmt: str) -> bool:
    """``html``/``json`` are per team; ``parquet`` is the compiled season file.

    ``teams`` is one row per team but one FILE per season in every format, so it
    is never per-team on disk.
    """
    if kind not in _KINDS:
        raise ValueError(f"unrecognized kind={kind!r}; expected one of {list(_KINDS)}")
    return kind != "teams" and fmt != "parquet"


SCHEDULE_COLUMNS: List[str] = [
    "season",
    "league",
    "team_id",
    "team",
    "espn_team_id",
    "contest_id",
    "game_date",
    "home",
    "home_score",
    "away",
    "away_score",
    "opponent",
    "opponent_id",
    "opponent_espn_team_id",
    "is_neutral",
    "detail",
    "attendance",
]

ROSTER_COLUMNS: List[str] = [
    "season",
    "league",
    "team_id",
    "team",
    "espn_team_id",
    "player_id",
    "clean_name",
    "player",
    "jersey",
    "class",
    "position",
    "height",
    "ht_inches",
    "hometown",
    "high_school",
    "gp",
    "gs",
]

TEAMS_COLUMNS: List[str] = [
    "season",
    "season_ncaa",
    "league",
    "ncaa_team_id",
    "team",
    "conference",
    "division",
    "espn_team_id",
    "espn_display_name",
    "espn_mascot",
]

#: ESPN identifiers are filled from sdv-py's ``ncaa_espn_team_crosswalk``, which
#: may not be published yet; absent it, these ship as all-null Utf8 columns.
_ESPN_COLUMNS: List[str] = ["espn_team_id", "espn_display_name", "espn_mascot"]
#: That loader's join keys: ``ncaa_team_id`` is Int64 (ours is Utf8 -- cast at
#: the boundary) and ``season`` is the NCAA ``"YYYY-YY"`` form, so it joins to
#: our ``season_ncaa``, NOT to our ending-year ``season``.
_ESPN_ID_COLUMN = "ncaa_team_id"
_ESPN_SEASON_COLUMN = "season"

__all__ = [
    "DIVISION",
    "ROSTER_COLUMNS",
    "SCHEDULE_COLUMNS",
    "TEAMS_COLUMNS",
    "TEAM_ID_CROSSWALKS",
    "build_season_aggregate",
    "build_teams",
    "dataset_path",
    "persist_roster",
    "persist_schedule",
    "read_html",
    "rebuild_missing",
    "season_ncaa",
    "season_teams",
]


def season_ncaa(season: int) -> str:
    """Ending-year int -> crosswalk ``"YYYY-YY"`` (2026 -> ``"2025-26"``)."""
    return f"{season - 1}-{str(season)[-2:]}"


def dataset_path(
    root: Union[str, Path],
    league: str,
    kind: str,
    fmt: str,
    season: int,
    team_id: Optional[Union[int, str]] = None,
) -> Path:
    """Canonical path for one dataset artifact.

    Args:
        root: Repo root (the data tree lives at ``{root}/{league}/``).
        league: ``"mbb"`` or ``"wbb"``.
        kind: ``"schedules"``, ``"rosters"``, or ``"teams"``.
        fmt: ``"html"``, ``"json"``, or ``"parquet"``.
        season: Ending year, e.g. ``2026``.
        team_id: Required for per-team artifacts (``html``/``json`` of
            ``schedules``/``rosters``); ignored for the compiled season
            ``parquet`` and for ``teams`` in every format.

    Returns:
        The absolute path. Parent directories are NOT created.
    """
    base = Path(root) / league / kind / fmt
    ext = _EXT[fmt]
    if not _is_per_team(kind, fmt):
        return base / f"{season}.{ext}"
    if team_id is None:
        raise ValueError(f"{kind}/{fmt} is per-team; team_id is required")
    return base / str(season) / f"{team_id}.{ext}"


def read_html(root: Union[str, Path], league: str, kind: str, season: int, team_id: Union[int, str]) -> Optional[str]:
    """The committed HTML for one team, or ``None`` when it was never captured.

    The sweeps call this BEFORE fetching: a hit means the page is already in the
    repo and the network round trip is skipped entirely.
    """
    path = dataset_path(root, league, kind, "html", season, team_id)
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def season_teams(league: str, season: int) -> pl.DataFrame:
    """The season's D-I teams from the bundled crosswalk.

    Returns:
        ``season`` (Utf8 ending year), ``season_ncaa`` (``"YYYY-YY"``),
        ``league``, ``ncaa_team_id`` (Utf8 -- NCAA ids stay strings),
        ``team``, ``conference``, ``division``.

    Raises:
        ValueError: *league* is unknown, or the crosswalk has no rows for
            *season* (a season-format drift, raised rather than silently
            returning an empty, complete-looking team list).
    """
    try:
        crosswalk_fn = TEAM_ID_CROSSWALKS[league]
    except KeyError:
        raise ValueError(f"unrecognized league={league!r}; expected one of {sorted(TEAM_ID_CROSSWALKS)}") from None
    season_str = season_ncaa(season)
    rows = crosswalk_fn().filter(pl.col("season") == season_str)
    if rows.height == 0:
        raise ValueError(f"no {league!r} crosswalk teams for season={season} ({season_str})")
    return rows.select(
        pl.lit(str(season)).alias("season"),
        pl.col("season").alias("season_ncaa"),
        pl.lit(league).alias("league"),
        # Int64 -> Utf8 straight off the integer id: never via Float64, which
        # would stringify as "609554.0" and break every downstream join.
        pl.col("id").cast(pl.Int64).cast(pl.Utf8).alias("ncaa_team_id"),
        pl.col("team"),
        pl.col("conference"),
        pl.lit(DIVISION).alias("division"),
    ).sort("ncaa_team_id")


@lru_cache(maxsize=8)
def _teams_with_espn(league: str, season: int) -> pl.DataFrame:
    """:func:`season_teams` plus the ESPN identifier columns.

    The one source of team identity for all three trees, so schedules, rosters
    and ``teams`` can never disagree about a team's ESPN id. Cached because the
    schedules/rosters sweeps ask for the same season once per team -- tests that
    monkeypatch :func:`_espn_crosswalk` must call ``cache_clear()``.
    """
    return _with_espn(season_teams(league, season), league, season)


def _team_identity(league: str, season: int, team_id: Union[int, str]) -> Dict[str, Optional[str]]:
    """``{"team": name, "espn_team_id": id}`` for one team; nulls when unknown."""
    teams = _teams_with_espn(league, season)
    hit = teams.filter(pl.col("ncaa_team_id") == str(team_id))
    if hit.height == 0:
        return {"team": None, "espn_team_id": None}
    return {"team": hit.get_column("team")[0], "espn_team_id": hit.get_column("espn_team_id")[0]}


def _empty(columns: List[str], schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema={c: schema.get(c, pl.Utf8) for c in columns})


_SCHEDULE_TYPES: Dict[str, pl.DataType] = {
    "home_score": pl.Int64,
    "away_score": pl.Int64,
    "attendance": pl.Int64,
    "is_neutral": pl.Boolean,
}
_ROSTER_TYPES: Dict[str, pl.DataType] = {"ht_inches": pl.Int64}


def _conform(df: pl.DataFrame, columns: List[str], types: Dict[str, pl.DataType]) -> pl.DataFrame:
    """Project onto the documented column set and pin every dtype.

    A source frame can legitimately be missing a column (a season whose roster
    table omits ``GS``, or a legacy payload whose ``ht_inches`` was all-null and
    inferred as Null) -- those become typed nulls rather than a schema that
    diverges per team and blows up the ``_season`` concatenation.
    """
    have = set(df.columns)
    return df.select(
        [pl.col(c) if c in have else pl.lit(None, dtype=types.get(c, pl.Utf8)).alias(c) for c in columns]
    ).cast(types)  # type: ignore[arg-type]


def _enrich_schedule(df: pl.DataFrame, team_id: Union[int, str], season: int, league: str) -> pl.DataFrame:
    """Stamp identity columns and derive ``opponent`` / ``opponent_id``.

    ``parse_ncaa_bb_team_schedule`` already emits both sides by NAME (``home`` /
    ``away``); the swept team is whichever side matches its own crosswalk name,
    so the opponent is the other one. ``opponent_id`` comes from an exact join
    against the same season's crosswalk -- the ids the parser matched names
    against in the first place. No fuzzy matching, deliberately: an unmatched
    opponent (a non-D-I exhibition foe) stays null rather than guessing.

    BOTH sides also carry the ESPN team id (``espn_team_id`` for the swept team,
    ``opponent_espn_team_id`` for the other), off the same crosswalk row -- so a
    schedule joins to ESPN without a second lookup. An opponent with no
    crosswalk row keeps its name and gets nulls for both ids.
    """
    teams = _teams_with_espn(league, season)
    tid = str(team_id)
    identity = _team_identity(league, season, tid)
    team_name = identity["team"]
    if team_name is None:
        logger.warning(
            "team_id=%s is not in the %s %s crosswalk; team/opponent names will be null", tid, season, league
        )

    if df.height == 0:
        return _empty(SCHEDULE_COLUMNS, _SCHEDULE_TYPES)

    out = df.rename({"game_id": "contest_id"}).with_columns(
        pl.lit(str(season)).alias("season"),
        pl.lit(league).alias("league"),
        pl.lit(tid).alias("team_id"),
        pl.lit(team_name, dtype=pl.Utf8).alias("team"),
        pl.lit(identity["espn_team_id"], dtype=pl.Utf8).alias("espn_team_id"),
    )
    # box_id is the identical value the parser assigns to game_id (one shared
    # positional fill), so it is dropped rather than shipped as a duplicate.
    opponent = (
        pl.when(pl.col("home") == pl.col("team"))
        .then(pl.col("away"))
        .when(pl.col("away") == pl.col("team"))
        .then(pl.col("home"))
        .otherwise(None)
    )
    out = out.with_columns(opponent.alias("opponent")).join(
        teams.select(
            pl.col("team").alias("opponent"),
            pl.col("ncaa_team_id").alias("opponent_id"),
            pl.col("espn_team_id").alias("opponent_espn_team_id"),
        ),
        on="opponent",
        how="left",
    )
    return _conform(out, SCHEDULE_COLUMNS, _SCHEDULE_TYPES)


def _enrich_roster(df: pl.DataFrame, team_id: Union[int, str], season: int, league: str) -> pl.DataFrame:
    """Stamp identity columns onto a parsed roster frame.

    The parser's ``name`` column is byte-identical to ``clean_name``; only
    ``clean_name`` is kept. ``player`` (ALL-CAPS ``FIRST.LAST``) is the key the
    play-by-play stream uses, ``clean_name`` the properly-cased display form --
    both ship, which is the whole point of the roster tree.

    The team's ESPN id rides along too -- a roster is reference data, so the
    ESPN identity belongs on it (unlike the per-game families, which carry the
    NCAA id only and join ``teams`` for ESPN).
    """
    tid = str(team_id)
    identity = _team_identity(league, season, tid)

    if df.height == 0:
        return _empty(ROSTER_COLUMNS, _ROSTER_TYPES)

    stamped = df.with_columns(
        pl.lit(str(season)).alias("season"),
        pl.lit(league).alias("league"),
        pl.lit(tid).alias("team_id"),
        pl.lit(identity["team"], dtype=pl.Utf8).alias("team"),
        pl.lit(identity["espn_team_id"], dtype=pl.Utf8).alias("espn_team_id"),
    )
    return _conform(stamped, ROSTER_COLUMNS, _ROSTER_TYPES)


def _write_json(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(df.write_json(), encoding="utf-8")


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _persist(
    kind: str,
    df: pl.DataFrame,
    raw_html: Optional[str],
    team_id: Union[int, str],
    season: int,
    league: str,
    root: Union[str, Path],
    overwrite: bool,
) -> pl.DataFrame:
    html_path = dataset_path(root, league, kind, "html", season, team_id)
    if raw_html is not None and (overwrite or not html_path.exists()):
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(raw_html, encoding="utf-8")
    json_path = dataset_path(root, league, kind, "json", season, team_id)
    if overwrite or not json_path.exists():
        _write_json(df, json_path)
    # No per-team parquet: the season parquet is compiled from these json files
    # by the non-sharded build_season_aggregate pass.
    return df


def persist_schedule(
    raw_html: str,
    team_id: Union[int, str],
    season: int,
    *,
    league: str,
    root: Union[str, Path],
    overwrite: bool = False,
) -> pl.DataFrame:
    """Parse one team-page HTML and write html + json + parquet.

    Called from :func:`ncaa_discover.discover_season` with the HTML it already
    fetched, and from :func:`rebuild_missing` with HTML already on disk. Writing
    is idempotent -- an existing artifact is left alone unless *overwrite*.
    """
    df = _enrich_schedule(parse_ncaa_bb_team_schedule(raw_html, int(team_id), league=league), team_id, season, league)
    return _persist("schedules", df, raw_html, team_id, season, league, root, overwrite)


def persist_roster(
    raw_html: str,
    team_id: Union[int, str],
    season: int,
    *,
    league: str,
    root: Union[str, Path],
    overwrite: bool = False,
) -> pl.DataFrame:
    """Parse one roster-page HTML and write html + json + parquet."""
    df = _enrich_roster(parse_ncaa_bb_team_roster(raw_html, int(team_id)), team_id, season, league)
    return _persist("rosters", df, raw_html, team_id, season, league, root, overwrite)


_PERSISTERS = {"schedules": persist_schedule, "rosters": persist_roster}


def _backfill_rosters_from_legacy(season: int, league: str, root: Union[str, Path], overwrite: bool) -> int:
    """Seed the rosters tree from the pre-existing ``team_rosters/`` JSON.

    ``ncaa_rosters`` has always written ``{lg}/team_rosters/{season}/{team_id}.
    json``, whose ``players`` array is exactly the parser's output. Seasons
    already captured that way get their per-team json here WITHOUT a re-fetch
    (and so feed the season parquet) -- only ``html`` is unavailable for them,
    because that payload never kept the page.
    """
    legacy_dir = Path(root) / league / "team_rosters" / str(season)
    if not legacy_dir.is_dir():
        return 0
    count = 0
    for legacy in sorted(legacy_dir.glob("*.json")):
        team_id = legacy.stem
        json_path = dataset_path(root, league, "rosters", "json", season, team_id)
        if not overwrite and json_path.exists():
            continue
        players = json.loads(legacy.read_text(encoding="utf-8")).get("players") or []
        parsed = pl.DataFrame(players) if players else _empty(ROSTER_COLUMNS, _ROSTER_TYPES)
        df = _enrich_roster(parsed, team_id, season, league)
        _persist("rosters", df, None, team_id, season, league, root, overwrite)
        count += 1
    if count:
        logger.info("backfilled %d rosters for season=%s from team_rosters/ (no html available)", count, season)
    return count


def rebuild_missing(kind: str, season: int, *, league: str, root: Union[str, Path], overwrite: bool = False) -> int:
    """Re-derive the per-team json from committed HTML. Fully offline.

    Every page this repo holds can be re-parsed without the network, so a parser
    fix propagates to every captured season with one non-sharded pass. Run
    :func:`build_season_aggregate` afterwards to recompile the season parquet.

    Returns:
        Number of teams (re)written.
    """
    count = _backfill_rosters_from_legacy(season, league, root, overwrite) if kind == "rosters" else 0
    html_dir = Path(root) / league / kind / "html" / str(season)
    if not html_dir.is_dir():
        return count
    for html_file in sorted(html_dir.glob("*.html")):
        team_id = html_file.stem
        json_path = dataset_path(root, league, kind, "json", season, team_id)
        if not overwrite and json_path.exists():
            continue
        _PERSISTERS[kind](
            html_file.read_text(encoding="utf-8"),
            team_id,
            season,
            league=league,
            root=root,
            overwrite=overwrite,
        )
        count += 1
    if count:
        logger.info("rebuilt %d %s frames for season=%s from committed html", count, kind, season)
    return count


def build_season_aggregate(kind: str, season: int, *, league: str, root: Union[str, Path]) -> pl.DataFrame:
    """Compile every per-team json into the ONE season parquet.

    Run this as a SEPARATE non-sharded pass. A ``--shard i/N`` worker sees only
    its team slice, and there is exactly one output file -- concurrent shard
    writers would race each other into a truncated dataset.

    Each row carries ``team_id`` + the team NAME, so the concatenated season
    file stays self-describing once every team is folded in.
    """
    columns = SCHEDULE_COLUMNS if kind == "schedules" else ROSTER_COLUMNS
    types = _SCHEDULE_TYPES if kind == "schedules" else _ROSTER_TYPES
    json_dir = Path(root) / league / kind / "json" / str(season)
    parts = sorted(json_dir.glob("*.json")) if json_dir.is_dir() else []
    # Conform each team before concatenating: a team whose column came back
    # all-null reads as Null dtype, which would otherwise poison the union.
    frames = [_conform(pl.read_json(p), columns, types) for p in parts]
    if frames:
        combined = pl.concat(frames, how="diagonal_relaxed").select(columns)
        sort_key = ["team_id", "game_date", "contest_id"] if kind == "schedules" else ["team_id", "player_id"]
        combined = combined.sort([c for c in sort_key if c in combined.columns])
    else:
        combined = _empty(columns, types)
    _write_parquet(combined, dataset_path(root, league, kind, "parquet", season))
    logger.info("%s season parquet: %d rows from %d teams (season=%s)", kind, combined.height, len(parts), season)
    return combined


def _espn_crosswalk(league: str) -> Optional[pl.DataFrame]:
    """The sdv-py NCAA<->ESPN crosswalk, or ``None`` when it isn't published.

    Built in parallel as an sdv-py data asset + loader; the teams tree must not
    block on it, so a missing loader degrades to null ESPN columns.
    """
    for modname in (
        f"sportsdataverse.{league}.{league}_ncaa_espn_crosswalk",
        f"sportsdataverse.{league}",
        "sportsdataverse",
    ):
        try:
            mod = importlib.import_module(modname)
        except ImportError:
            continue
        fn = getattr(mod, "ncaa_espn_team_crosswalk", None)
        if fn is not None:
            return fn(league=league)
    return None


def _with_espn(teams: pl.DataFrame, league: str, season: int) -> pl.DataFrame:
    """Left-join ESPN identifiers onto *teams*; null-fill when unavailable.

    The loader keys ``ncaa_team_id`` as Int64 and ``season`` as ``"YYYY-YY"``,
    so the id is cast Int64 -> Utf8 (never via Float64, which would stringify as
    ``"609554.0"``) and the season is matched against ``season_ncaa``.
    """
    nulls = [pl.lit(None, dtype=pl.Utf8).alias(c) for c in _ESPN_COLUMNS]
    xwalk = _espn_crosswalk(league)
    if xwalk is None:
        logger.info("ncaa_espn_team_crosswalk not importable -- ESPN columns emitted as nulls (season=%s)", season)
        return teams.with_columns(nulls)

    have = [c for c in _ESPN_COLUMNS if c in xwalk.columns]
    if _ESPN_ID_COLUMN not in xwalk.columns or not have:
        logger.warning(
            "ncaa_espn_team_crosswalk schema drifted (want %r + %s, saw %s) -- ESPN columns emitted as nulls",
            _ESPN_ID_COLUMN,
            _ESPN_COLUMNS,
            xwalk.columns,
        )
        return teams.with_columns(nulls)

    if _ESPN_SEASON_COLUMN in xwalk.columns:
        xwalk = xwalk.filter(pl.col(_ESPN_SEASON_COLUMN).cast(pl.Utf8) == season_ncaa(season))

    right = (
        xwalk.select(
            pl.col(_ESPN_ID_COLUMN).cast(pl.Int64, strict=False).cast(pl.Utf8).alias("ncaa_team_id"),
            *[pl.col(c).cast(pl.Utf8) for c in have],
        )
        .drop_nulls("ncaa_team_id")
        .unique(subset="ncaa_team_id", keep="first")
    )
    assert teams.schema["ncaa_team_id"] == right.schema["ncaa_team_id"], "ncaa_team_id dtype mismatch on the ESPN join"
    joined = teams.join(right, on="ncaa_team_id", how="left")
    missing = [pl.lit(None, dtype=pl.Utf8).alias(c) for c in _ESPN_COLUMNS if c not in have]
    joined = joined.with_columns(missing) if missing else joined
    matched = joined.filter(pl.col(have[0]).is_not_null()).height
    logger.info("ESPN crosswalk joined: %d/%d teams matched (season=%s)", matched, joined.height, season)
    return joined


def _teams_html(df: pl.DataFrame, league: str, season: int) -> str:
    """A generated HTML table of the teams frame.

    There is no single upstream team-list PAGE behind this dataset -- the teams
    frame is the bundled crosswalk (itself distilled from many scoreboard
    pages), and the sweep fetches per-TEAM pages, not a roster of teams. Rather
    than skip the html format, the frame is rendered as a table so all three
    formats exist for every dataset.
    """
    head = "".join(f"<th>{_html.escape(c)}</th>" for c in df.columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{'' if v is None else _html.escape(str(v))}</td>" for v in row) + "</tr>"
        for row in df.iter_rows()
    )
    return (
        "<!doctype html>\n<html><head><meta charset='utf-8'>"
        f"<title>{_html.escape(league)} NCAA Division-{DIVISION} teams {season}</title></head><body>"
        f"<h1>{_html.escape(league)} NCAA Division-{DIVISION} teams — {season}</h1>"
        f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
        "</body></html>\n"
    )


def build_teams(season: int, *, league: str, root: Union[str, Path], overwrite: bool = True) -> pl.DataFrame:
    """Build ``{lg}/teams/{html,json,parquet}/{season}.*``. Fully offline.

    One row per team per season: the NCAA id + NCAA name + conference +
    ``division`` (constant ``"I"`` -- the bundled crosswalk is scoped to the
    Division-I ``season_divisions`` id), plus ESPN identifiers when the sdv-py
    ``ncaa_espn_team_crosswalk`` loader is importable (nulls otherwise).
    """
    df = _teams_with_espn(league, season).select(TEAMS_COLUMNS)
    html_path = dataset_path(root, league, "teams", "html", season)
    if overwrite or not html_path.exists():
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(_teams_html(df, league, season), encoding="utf-8")
    json_path = dataset_path(root, league, "teams", "json", season)
    if overwrite or not json_path.exists():
        _write_json(df, json_path)
    pq_path = dataset_path(root, league, "teams", "parquet", season)
    if overwrite or not pq_path.exists():
        _write_parquet(df, pq_path)
    logger.info("teams season=%s league=%s: %d teams", season, league, df.height)
    return df


def _main(default_league: str) -> None:
    parser = argparse.ArgumentParser(
        description="Build the committed schedules/teams/rosters trees (offline; no network)."
    )
    parser.add_argument("--season", type=int, required=True, help="Ending year of the season, e.g. 2026.")
    parser.add_argument(
        "--league", default=default_league, help=f"League slug: 'mbb' or 'wbb' (default: {default_league})."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]), help="Root of the raw data tree.")
    parser.add_argument(
        "--what",
        default="all",
        choices=["all", "schedules", "rosters", "teams"],
        help="Which tree(s) to build (default: all).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-derive json/parquet even where they already exist (after a parser fix).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    summary: Dict[str, int] = {}
    for kind in ("schedules", "rosters"):
        if args.what in ("all", kind):
            rebuild_missing(kind, args.season, league=args.league, root=args.root, overwrite=args.overwrite)
            summary[kind] = build_season_aggregate(kind, args.season, league=args.league, root=args.root).height
    if args.what in ("all", "teams"):
        summary["teams"] = build_teams(args.season, league=args.league, root=args.root).height
    print(f"datasets season={args.season} league={args.league}: " + json.dumps(summary))
