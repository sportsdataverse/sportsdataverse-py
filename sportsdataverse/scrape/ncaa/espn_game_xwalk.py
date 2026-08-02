"""Season crosswalk: NCAA ``contest_id`` -> ESPN event id (``espn_game_id``).

Distinct from sdv-py's ``ncaa_espn_team_crosswalk`` (a TEAM-id crosswalk) --
this one keys GAMES. Nothing in the ecosystem carried an ESPN event id against
an NCAA contest id before, so it is derived here and cached to disk:

    {root}/{league}/xwalk/espn_game_id/{season}.json

Building it is the ONLY step that touches the network (one sdv-py release-loader
call per season). :func:`load_espn_game_index` is then a pure offline read, which
is what ``ncaa_identity.enrich_parsed`` uses -- so parsing 18k bundles never
hits the wire.

**Both sides are already ESPN-team-id-bearing**, so no name matching is needed:

* NCAA side -- ``{root}/{league}/schedules/parquet/{season}.parquet`` carries
  ``contest_id``, ``game_date``, ``espn_team_id`` (the row's own team) and
  ``opponent_espn_team_id``. Each contest appears twice (once per team's
  schedule page); orientation comes from whether the row's ``team`` is the
  ``home`` or the ``away`` name.
* ESPN side -- ``load_{mbb,wbb}_schedule(seasons=[season])``: ``game_id``,
  ``game_date``, ``home_id``, ``away_id``.

**Four tiers, tried in order, each recorded in ``match_method``.** Every tier
drops any key resolving to more than one ESPN game before it joins, so an
ambiguous contest lands on NULL rather than a guess:

1. ``exact`` -- ``(game_date, home_espn_team_id, away_espn_team_id)``.
2. ``date_window`` -- same key, ESPN date shifted +/-1 day. Late tip-offs and
   timezone handling put a handful of games on the neighbouring date.
3. ``unordered_pair`` -- exact date, home/away treated as an unordered pair.
   Neutral-site games are the bulk of this tier: the NCAA and ESPN sides
   disagree about which side is nominally "home".
4. ``single_team`` -- exact date plus the ONE ESPN team id we have, and only
   when that team has exactly one ESPN game that date. This is what covers
   games against a non-D-I opponent: stats.ncaa.org gives such an opponent no
   NCAA team id at all (and therefore no ESPN one), so tiers 1-3 cannot fire.

A contest that survives all four keeps a NULL ``espn_game_id``. Rows are never
dropped for failing to match, and an ESPN game id claimed by two different
contests is voided on both sides rather than assigned to either.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Union

import polars as pl

logger = logging.getLogger(__name__)


__all__ = [
    "MATCH_METHODS",
    "build_season_xwalk",
    "load_espn_game_index",
    "write_season_xwalk",
    "xwalk_path",
]

#: Tier labels, in the order they are attempted.
MATCH_METHODS = ("exact", "date_window", "unordered_pair", "single_team")

_EXACT_KEYS = ["game_date", "home_espn_team_id", "away_espn_team_id"]
_PAIR_KEYS = ["game_date", "team_lo", "team_hi"]
_SINGLE_KEYS = ["game_date", "espn_team_id"]


def xwalk_path(root: Union[str, Path], league: str, season: int) -> Path:
    """``{root}/{league}/xwalk/espn_game_id/{season}.json``."""
    return Path(root) / league / "xwalk" / "espn_game_id" / f"{season}.json"


def _utf8_id(column: str) -> pl.Expr:
    """Int-bearing id column -> Utf8. Via ``Int64`` so a float never renders ``"123.0"``."""
    return pl.col(column).cast(pl.Int64).cast(pl.Utf8)


def _pair_bounds() -> "tuple[pl.Expr, pl.Expr]":
    return (
        pl.min_horizontal("home_espn_team_id", "away_espn_team_id").alias("team_lo"),
        pl.max_horizontal("home_espn_team_id", "away_espn_team_id").alias("team_hi"),
    )


def ncaa_schedule_side(root: Union[str, Path], league: str, season: int) -> pl.DataFrame:
    """One row per contest: ``contest_id``, ``game_date``, both ESPN team ids.

    Empty frame (documented schema) when the season's schedules tree was never
    built -- the caller then writes an empty crosswalk rather than raising.
    """
    schema = {
        "contest_id": pl.Utf8,
        "game_date": pl.Date,
        "home_espn_team_id": pl.Utf8,
        "away_espn_team_id": pl.Utf8,
    }
    path = Path(root) / league / "schedules" / "parquet" / f"{season}.parquet"
    if not path.is_file():
        return pl.DataFrame(schema=schema)

    is_home = pl.col("team") == pl.col("home")
    is_away = pl.col("team") == pl.col("away")
    frame = pl.read_parquet(path).select(
        pl.col("contest_id").cast(pl.Utf8),
        pl.col("game_date").str.to_date("%m/%d/%Y", strict=False).alias("game_date"),
        pl.when(is_home)
        .then(pl.col("espn_team_id"))
        .when(is_away)
        .then(pl.col("opponent_espn_team_id"))
        .otherwise(None)
        .cast(pl.Utf8)
        .alias("home_espn_team_id"),
        pl.when(is_away)
        .then(pl.col("espn_team_id"))
        .when(is_home)
        .then(pl.col("opponent_espn_team_id"))
        .otherwise(None)
        .cast(pl.Utf8)
        .alias("away_espn_team_id"),
    )
    # Both of a contest's two schedule-page rows describe the same game; prefer
    # the one that resolved BOTH sides (a row whose own team matched neither the
    # home nor the away name yields two nulls and is the weaker copy).
    return (
        frame.filter(pl.col("contest_id").is_not_null())
        .sort(pl.col("home_espn_team_id").is_null() | pl.col("away_espn_team_id").is_null())
        .unique(subset=["contest_id"], keep="first", maintain_order=True)
        .sort("contest_id")
    )


def espn_schedule_side(league: str, season: int) -> pl.DataFrame:
    """ESPN's own season schedule via the sdv-py release loaders (the one network call)."""
    if league == "wbb":
        from sportsdataverse.wbb import load_wbb_schedule as _load
    else:
        from sportsdataverse.mbb import load_mbb_schedule as _load

    return (
        _load(seasons=[season])
        .select(
            _utf8_id("game_id").alias("espn_game_id"),
            pl.col("game_date").cast(pl.Date),
            _utf8_id("home_id").alias("home_espn_team_id"),
            _utf8_id("away_id").alias("away_espn_team_id"),
        )
        .drop_nulls("espn_game_id")
    )


def _unambiguous(frame: pl.DataFrame, keys: List[str]) -> pl.DataFrame:
    """``keys -> espn_game_id``, keeping only keys that resolve to exactly one game."""
    return (
        frame.group_by(keys)
        .agg(
            pl.col("espn_game_id").n_unique().alias("_candidates"),
            pl.col("espn_game_id").first().alias("espn_game_id"),
        )
        .filter(pl.col("_candidates") == 1)
        .drop("_candidates")
    )


def _apply_tier(
    pending: pl.DataFrame, lookup: pl.DataFrame, keys: List[str], method: str
) -> "tuple[pl.DataFrame, pl.DataFrame]":
    """Left-join *pending* against *lookup*; return ``(matched, still_pending)``."""
    joined = pending.join(lookup, on=keys, how="left")
    matched = joined.filter(pl.col("espn_game_id").is_not_null()).with_columns(
        pl.lit(method, dtype=pl.Utf8).alias("match_method")
    )
    still = joined.filter(pl.col("espn_game_id").is_null()).drop("espn_game_id")
    return matched, still


def build_season_xwalk(root: Union[str, Path], league: str, season: int) -> pl.DataFrame:
    """``contest_id -> espn_game_id`` for one season, with the tier that matched it.

    Returns:
        ``contest_id`` (Utf8), ``espn_game_id`` (Utf8, nullable),
        ``match_method`` (Utf8, one of :data:`MATCH_METHODS`, null when
        unmatched) -- one row per NCAA contest, none ever dropped.
    """
    ncaa = ncaa_schedule_side(root, league, season)
    out_schema = {
        "contest_id": pl.Utf8,
        "espn_game_id": pl.Utf8,
        "match_method": pl.Utf8,
    }
    if ncaa.height == 0:
        logger.info(
            "ncaa_espn_game_xwalk: %s %s -- no schedules parquet; empty crosswalk",
            league,
            season,
        )
        return pl.DataFrame(schema=out_schema)

    espn = espn_schedule_side(league, season)
    if espn.height == 0:
        logger.warning("ncaa_espn_game_xwalk: %s %s -- ESPN loader returned 0 rows", league, season)
        return ncaa.select(
            "contest_id",
            pl.lit(None, dtype=pl.Utf8).alias("espn_game_id"),
            pl.lit(None, dtype=pl.Utf8).alias("match_method"),
        )

    lo, hi = _pair_bounds()
    espn_long = pl.concat(
        [
            espn.select("espn_game_id", "game_date", pl.col(side).alias("espn_team_id"))
            for side in ("home_espn_team_id", "away_espn_team_id")
        ]
    )
    windowed = pl.concat([espn.with_columns((pl.col("game_date") + pl.duration(days=d))) for d in (-1, 1)])

    tiers = (
        ("exact", _EXACT_KEYS, lambda: _unambiguous(espn, _EXACT_KEYS), None),
        (
            "date_window",
            _EXACT_KEYS,
            lambda: _unambiguous(windowed, _EXACT_KEYS),
            None,
        ),
        (
            "unordered_pair",
            _PAIR_KEYS,
            lambda: _unambiguous(espn.with_columns(lo, hi), _PAIR_KEYS),
            (lo, hi),
        ),
        (
            "single_team",
            _SINGLE_KEYS,
            lambda: _unambiguous(espn_long, _SINGLE_KEYS),
            (pl.coalesce("home_espn_team_id", "away_espn_team_id").alias("espn_team_id"),),
        ),
    )

    pending = ncaa
    matched: List[pl.DataFrame] = []
    for method, keys, lookup_fn, extra in tiers:
        if pending.height == 0:
            break
        prepared = pending.with_columns(*extra) if extra else pending
        hit, pending = _apply_tier(prepared, lookup_fn(), keys, method)
        if hit.height:
            matched.append(hit.select("contest_id", "espn_game_id", "match_method"))
        pending = pending.select(ncaa.columns)

    unmatched = pending.select(
        "contest_id",
        pl.lit(None, dtype=pl.Utf8).alias("espn_game_id"),
        pl.lit(None, dtype=pl.Utf8).alias("match_method"),
    )
    result = pl.concat([*matched, unmatched]) if matched else unmatched

    # One ESPN game belongs to one contest. A collision means at least one of
    # the two is wrong and we cannot tell which -- void both rather than assign.
    contested = (
        result.drop_nulls("espn_game_id")
        .group_by("espn_game_id")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
        .get_column("espn_game_id")
        .to_list()
    )
    if contested:
        logger.warning(
            "ncaa_espn_game_xwalk: %s %s -- %d ESPN game ids claimed by >1 contest; voided",
            league,
            season,
            len(contested),
        )
        clash = pl.col("espn_game_id").is_in(contested)
        result = result.with_columns(
            pl.when(clash).then(None).otherwise(pl.col("espn_game_id")).alias("espn_game_id"),
            pl.when(clash).then(None).otherwise(pl.col("match_method")).alias("match_method"),
        )
    return result.sort("contest_id")


def write_season_xwalk(root: Union[str, Path], league: str, season: int, frame: pl.DataFrame) -> Path:
    """Write *frame* to :func:`xwalk_path` as plain utf-8 JSON (atomic)."""
    path = xwalk_path(root, league, season)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(frame.to_dicts()), encoding="utf-8")
    tmp.replace(path)
    return path


@lru_cache(maxsize=8)
def load_espn_game_index(root: str, league: str, season: int) -> Dict[str, str]:
    """``{contest_id: espn_game_id}`` for one season -- offline, cached.

    Unmatched contests are omitted (the caller emits NULL for them), and a
    missing crosswalk file yields an empty dict rather than an exception, so a
    season whose crosswalk was never built still parses.
    """
    path = xwalk_path(root, league, season)
    if not path.is_file():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.warning(
            "ncaa_espn_game_xwalk: unreadable crosswalk %s; ids emitted as nulls",
            path,
            exc_info=True,
        )
        return {}
    return {str(r["contest_id"]): str(r["espn_game_id"]) for r in rows if r.get("contest_id") and r.get("espn_game_id")}


def summarize(frame: pl.DataFrame) -> Dict[str, int]:
    """Per-tier hit counts + the unmatched residue, for the build log."""
    counts = {m: 0 for m in MATCH_METHODS}
    if frame.height:
        for row in (
            frame.drop_nulls("match_method").group_by("match_method").agg(pl.len().alias("n")).iter_rows(named=True)
        ):
            counts[row["match_method"]] = row["n"]
    counts["unmatched"] = int(frame.get_column("espn_game_id").null_count()) if frame.height else 0
    counts["contests"] = frame.height
    return counts


def _main(default_league: str, default_root: str) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Build the NCAA contest_id -> ESPN game_id crosswalk for one or more seasons."
    )
    parser.add_argument(
        "--root",
        default=default_root,
        help="Root of the raw data tree (default: repo root).",
    )
    parser.add_argument("--league", default=default_league, help=f"League slug (default: {default_league}).")
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        required=True,
        help="Ending years, e.g. --seasons 2023 2024 2025 2026.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report match rates without writing the crosswalk files.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for season in args.seasons:
        frame = build_season_xwalk(args.root, args.league, season)
        stats = summarize(frame)
        matched = stats["contests"] - stats["unmatched"]
        pct = round(100 * matched / stats["contests"], 2) if stats["contests"] else 0.0
        if not args.dry_run and stats["contests"]:
            write_season_xwalk(args.root, args.league, season, frame)
        print(
            f"{args.league} {season}: contests={stats['contests']} matched={matched} ({pct}%) "
            + " ".join(f"{m}={stats[m]}" for m in MATCH_METHODS)
            + f" unmatched={stats['unmatched']}",
            flush=True,
        )
