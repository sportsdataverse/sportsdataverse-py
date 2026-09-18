"""Long-format play-stats builder: Shield ``stats`` arrays -> one row per (play, stat).

Faithful Python port of nflfastR's ``build_playstats`` (R/build_playstats.R,
reference §13) plus its consumer contract ``load_playstats``
(R/calculate_stats.R). Reshapes the same per-play ``stats`` arrays
:func:`sportsdataverse.nfl.shield_pbp.stat_ids.sum_play_stats` collapses into named pbp columns
into a LONG format instead — one row per raw stat entry, covering every
``stat_id`` (not just the ones the wide pivot names). Deliberately does NOT
import anything from ``stat_ids`` (no decode-table duplication): this module
never interprets what a ``stat_id`` *means*, it only reshapes the raw entries.

Port-contract notes (reference §13) — two nflfastR branches are
document-and-skipped rather than ported, because both exist to compensate for
quirks of nflfastR's *own* raw source, which this port does not share:

- **Hardcoded skip-list NOT ported.** nflfastR's ``build_playstats`` skips 3
  games with "missing raw game data" in its own scrape
  (``2000_03_SD_KC``, ``2000_06_BUF_MIA``, ``1999_01_BAL_STL``). All three are
  present in this port's Shield-sourced ``nfl/raw`` corpus with real,
  non-empty ``stats`` payloads (verified against the committed files) — the
  gap is specific to nflfastR's own raw source, not this one, so skipping
  would silently drop good data. Callers get every game the raw library
  actually has.
- **Pre-2001 raw-JSON-shape branch NOT ported.** nflfastR's ``season <= 2000``
  branch exists solely because ITS OWN raw source changes shape for old
  seasons (``raw_data[[1]][["drives"]]`` with a
  ``purrr::keep(is.list)``/``unnest_wider``/``unnest_longer``/``uniquify_ids()``
  chain to reshape into the 2001+ ``playStats`` shape before concatenation).
  This port's Shield JSON corpus is uniformly
  ``driveChart.plays[].stats[]``-shaped across every season back to 1999
  (verified against the committed 1999 fixture), so one code path already
  handles every season — no branch needed.
- **``playDeleted`` guard ADDED** (no equivalent in the R source): deleted
  plays are skipped for parity with :func:`sportsdataverse.nfl.shield_pbp.parse.parse_game`'s own
  ``playDeleted`` handling, so this table never emits a
  ``(game_id, play_id)`` with no matching row in the wide pbp frame. The one
  other row build_pbp drops -- ``shield_play_type == "TIMEOUT"`` markers -- is
  NOT filtered here; those markers empirically carry empty ``stats`` arrays,
  so no row is emitted for them either, but this module relies on that being
  true rather than enforcing it with a filter.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import polars as pl

from sportsdataverse.nfl.shield_pbp.parse import _resolve_teams_and_game_id

# ---------------------------------------------------------------------------
# Output schema (reference §13's exact `build_playstats` `dplyr::select` order
# and column names: game_id, season, week, play_id, stat_id, yards, team_abbr,
# player_name, gsis_player_id).
# ---------------------------------------------------------------------------

PLAYSTATS_SCHEMA: Dict[str, pl.DataType | type[pl.DataType]] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "week": pl.Int64,
    "play_id": pl.Int64,
    "stat_id": pl.Int64,
    "yards": pl.Int64,
    "team_abbr": pl.Utf8,
    "player_name": pl.Utf8,
    "gsis_player_id": pl.Utf8,
}

# nflfastR's default `stat_ids = 1:1000` filter, applied verbatim.
_DEFAULT_STAT_IDS = range(1, 1001)


def build_playstats_frame(
    game: Dict[str, Any],
    game_id: Optional[str] = None,
    *,
    stat_ids: Iterable[int] = _DEFAULT_STAT_IDS,
) -> pl.DataFrame:
    """Reshape one Shield game payload into the long-format play-stats table.

    One row per raw ``(play_id, stat_id)`` entry in every play's ``stats``
    array — the same arrays :func:`sportsdataverse.nfl.shield_pbp.stat_ids.sum_play_stats`
    collapses into named pbp columns, but unreduced here.

    Args:
        game: A single Shield game object (one ``nfl/raw/{season}/{game_id}.json``).
        game_id: Override the nflverse game_id; computed from the payload when
            None (same resolution :func:`sportsdataverse.nfl.shield_pbp.parse.parse_game` uses).
        stat_ids: Keep only stat entries whose ``stat_id`` is in this set
            (nflfastR default: ``1:1000``, i.e. every currently-cataloged
            GSIS code — effectively a no-op unless narrowed by the caller).

    Returns:
        A polars DataFrame matching :data:`PLAYSTATS_SCHEMA`. Empty payloads
        (no plays, or no stats on any play) return a zero-row frame carrying
        the same schema — never raises.
    """
    season = int(game.get("season")) if game.get("season") is not None else None
    week = game.get("week")
    dc = game.get("driveChart") or {}
    plays = dc.get("plays") or []

    # game_id carries season/week when the payload itself is missing them
    # (mirrors R's `season = substr(game_id, 1, 4)`, `week = substr(game_id, 6, 7)`).
    # Runs BEFORE the team resolver: _nflverse_abbr's relocation fixup is
    # season-aware, so a caller-supplied game_id must feed season into the
    # team_by_id mapping. (When game_id is None the resolver derives it from
    # the payload, which then must carry season itself — nothing to recover.)
    if season is None and game_id:
        try:
            season = int(game_id[:4])
        except ValueError:
            season = None
    if week is None and game_id:
        try:
            week = int(game_id[5:7])
        except ValueError:
            week = None

    _, _, team_by_id, game_id = _resolve_teams_and_game_id(game, season, game_id)

    keep_ids = set(stat_ids)

    rows: List[Dict[str, Any]] = []
    for p in plays:
        if p.get("playDeleted"):
            # Same guard as parse_game: a deleted play's stats must not emit
            # rows with no matching (game_id, play_id) in the wide pbp frame.
            continue
        play_id = p.get("playId")
        for entry in p.get("stats") or []:
            try:
                stat_id = int(entry.get("statType"))
            except (TypeError, ValueError):
                continue
            if stat_id not in keep_ids:
                continue
            team_raw = entry.get("teamId")
            rows.append(
                {
                    "game_id": game_id,
                    "season": season,
                    "week": week,
                    "play_id": play_id,
                    "stat_id": stat_id,
                    "yards": entry.get("yards"),
                    "team_abbr": team_by_id.get(team_raw) if team_raw is not None else None,
                    "player_name": entry.get("gsisPlayerName"),
                    "gsis_player_id": entry.get("gsisPlayerId"),
                }
            )

    if not rows:
        return pl.DataFrame(schema=PLAYSTATS_SCHEMA)

    # Build from row-dicts (every dict carries all 9 keys), then cast
    # explicitly -- a column that happens to be all-None in every row for this
    # game (e.g. no team_abbr resolved) would otherwise infer as pl.Null.
    df = pl.DataFrame(rows).select(list(PLAYSTATS_SCHEMA)).cast(PLAYSTATS_SCHEMA)

    # R's final step: coerce empty-string character columns to NA/null.
    str_cols = [c for c, dtype in PLAYSTATS_SCHEMA.items() if dtype == pl.Utf8]
    df = df.with_columns([pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in str_cols])
    return df


def build_playstats_season(
    season: int,
    raw_dir: str | Path = "nfl/raw",
    *,
    game_ids: Optional[List[str]] = None,
    stat_ids: Iterable[int] = _DEFAULT_STAT_IDS,
) -> pl.DataFrame:
    """Build one season's long-format play-stats frame from the raw library.

    Mirrors :func:`sportsdataverse.nfl.shield_pbp.build.build_season`'s directory-iteration
    pattern: one row per ``(game_id, play_id, stat_id)`` across every
    ``{raw_dir}/{season}/*.json`` game file. Preseason games are skipped, as
    in ``build_season``.

    Args:
        season: NFL season year.
        raw_dir: Root of the committed per-game JSON library.
        game_ids: Optional subset of game_ids to build (default: all in the season).
        stat_ids: Forwarded to :func:`build_playstats_frame` for every game.

    Returns:
        Concatenated polars DataFrame (``diagonal_relaxed`` union) matching
        :data:`PLAYSTATS_SCHEMA`. A season with no games (or none producing
        rows) returns a zero-row frame with the same schema.
    """
    season_dir = Path(raw_dir) / str(season)
    wanted = set(game_ids) if game_ids is not None else None
    frames: List[pl.DataFrame] = []
    for path in sorted(season_dir.glob(f"{season}_*.json")):
        if wanted is not None and path.stem not in wanted:
            continue
        game = json.loads(path.read_text(encoding="utf-8"))
        if game.get("seasonType") == "PRE":
            continue
        df = build_playstats_frame(game, game_id=path.stem, stat_ids=stat_ids)
        if df.height:
            frames.append(df)
    if not frames:
        return pl.DataFrame(schema=PLAYSTATS_SCHEMA)
    return pl.concat(frames, how="diagonal_relaxed")
