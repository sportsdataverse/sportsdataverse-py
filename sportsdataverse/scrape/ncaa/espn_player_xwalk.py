"""Season crosswalk: NCAA roster ``player_id`` -> ESPN ``athlete_id``.

The player-level sibling of :mod:`sportsdataverse.scrape.ncaa.espn_game_xwalk`.
It is built per league-season from what the ``-raw`` repo already holds plus one
sdv-py release-loader read, and cached to disk:

    {root}/{league}/xwalk/espn_athlete_id/{season}.json

``season`` is the repo's own tree key: the ENDING year for ``mbb`` / ``wbb``
and the ACADEMIC year for ``mfb`` (``ay = fall season + 1``; only the ``-raw``
repo speaks academic year, so the ESPN read uses ``ay - 1``).

**Blocking.** Players are only ever compared inside one ``(season, ESPN team)``
block. The NCAA team resolves to an ESPN team through the published team
crosswalk -- hoops rosters already carry ``espn_team_id`` (from
``ncaa_espn_team_crosswalk``); football reads the stage-06 team map
``mfb/xwalk/espn_team_id.json``.

**ESPN side comes from the published releases, never live rosters** (ESPN's
roster endpoints ignore the season): ``load_{mbb,wbb}_player_boxscore`` for
hoops and ``load_cfb_game_rosters`` for football (the football player box has
no jersey). One row per ``(ESPN team, athlete)`` with the athlete's most-worn
jersey that season.

**Four tiers, tried in order and recorded in ``match_method``.** Each tier only
joins on a key that is unique on BOTH sides of the block among the rows still
unmatched, so a tie lands on NULL rather than a guess:

1. ``name_jersey`` -- normalized full name + jersey.
2. ``name`` -- normalized full name alone.
3. ``last_jersey`` -- last name token + jersey (nicknames: Mike/Michael).
4. ``jaro_winkler`` -- the ONE remaining ESPN athlete scoring
   >= :data:`JW_FLOOR` against the NCAA name, and that athlete has no other
   NCAA candidate >= the floor either.

Names go through the basketball crosswalk normalizer
(:func:`sportsdataverse._common_crosswalk_basketball.normalize_name`) and the
same R-parity Jaro-Winkler scorer. An ESPN athlete claimed by two NCAA players
is voided on both. Unmatched rows are kept, with ``unmatched_reason``:
``no_espn_team`` / ``no_espn_players`` / ``ambiguous`` / ``no_candidate``.

Example:
    Build one season from a repo checkout::

        from sportsdataverse.scrape.ncaa import espn_player_xwalk as x
        frame = x.build_season_xwalk("/path/to/ncaa-mbb-hoops-raw", league="mbb", season=2024)
        print(x.summarize(frame))
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Union

import polars as pl

from sportsdataverse._common_crosswalk_basketball import jaro_winkler, normalize_name

logger = logging.getLogger(__name__)

__all__ = [
    "JW_FLOOR",
    "MATCH_METHODS",
    "UNMATCHED_REASONS",
    "build_season_xwalk",
    "espn_athlete_side",
    "match_players",
    "ncaa_roster_side",
    "summarize",
    "write_season_xwalk",
    "xwalk_path",
]

#: Tier labels, in the order they are attempted.
MATCH_METHODS = ("name_jersey", "name", "last_jersey", "jaro_winkler")
#: Why a row stayed unmatched.
UNMATCHED_REASONS = ("no_espn_team", "no_espn_players", "ambiguous", "no_candidate")
#: Jaro-Winkler floor (the R crosswalk engine's default).
JW_FLOOR = 0.92

_SIDE_SCHEMA = {"espn_team_id": pl.Utf8, "id": pl.Utf8, "name": pl.Utf8, "jersey": pl.Utf8}
_OUT_SCHEMA = {
    "ncaa_team_id": pl.Utf8,
    "ncaa_player_id": pl.Utf8,
    "espn_team_id": pl.Utf8,
    "espn_athlete_id": pl.Utf8,
    "match_method": pl.Utf8,
    "unmatched_reason": pl.Utf8,
}


def xwalk_path(root: Union[str, Path], league: str, season: int) -> Path:
    """``{root}/{league}/xwalk/espn_athlete_id/{season}.json``."""
    return Path(root) / league / "xwalk" / "espn_athlete_id" / f"{season}.json"


def _id(expr: pl.Expr) -> pl.Expr:
    """An id as Utf8 with no float residue (``"123"``, never ``"123.0"``) and blanks as null."""
    s = expr.cast(pl.Int64, strict=False).cast(pl.Utf8).fill_null(expr.cast(pl.Utf8).str.strip_chars())
    return pl.when(s == "").then(None).otherwise(s)


def _jersey(expr: pl.Expr) -> pl.Expr:
    """Jersey as written (``"0"`` and ``"00"`` are different numbers), trimmed, blank -> null."""
    s = expr.cast(pl.Utf8).str.strip_chars().str.strip_chars_start("#")
    return pl.when(s == "").then(None).otherwise(s)


def _person(name: Optional[str]) -> str:
    """``"Last, First"`` -> ``"First Last"``, then :func:`normalize_name`."""
    if name and "," in name:
        last, _, first = name.partition(",")
        name = f"{first} {last}"
    return normalize_name(name)


def ncaa_roster_side(root: Union[str, Path], league: str, season: int) -> pl.DataFrame:
    """The repo's NCAA roster for one season, ESPN-team resolved.

    Returns ``ncaa_team_id``, ``id`` (NCAA player id), ``name``, ``jersey`` and
    ``espn_team_id`` (null when the team has no ESPN mapping), all Utf8.
    """
    path = Path(root) / league / "rosters" / "parquet" / f"{season}.parquet"
    roster = pl.read_parquet(path)
    if league == "mfb":
        team_map = json.loads((Path(root) / "mfb" / "xwalk" / "espn_team_id.json").read_text(encoding="utf-8"))
        roster = roster.select(
            _id(pl.col("team_id")).alias("ncaa_team_id"),
            _id(pl.col("player_id")).alias("id"),
            pl.col("player_name").alias("name"),
            _jersey(pl.col("jersey")).alias("jersey"),
        ).with_columns(
            pl.col("ncaa_team_id").replace_strict(team_map, default=None, return_dtype=pl.Utf8).alias("espn_team_id")
        )
    else:
        roster = roster.select(
            _id(pl.col("team_id")).alias("ncaa_team_id"),
            _id(pl.col("player_id")).alias("id"),
            pl.col("clean_name").alias("name"),
            _jersey(pl.col("jersey")).alias("jersey"),
            _id(pl.col("espn_team_id")).alias("espn_team_id"),
        )
    return roster.drop_nulls("id").unique(["ncaa_team_id", "id"], keep="first", maintain_order=True)


def espn_athlete_side(league: str, season: int) -> pl.DataFrame:
    """ESPN athletes per team for one season, from the published release (one network read).

    Returns ``espn_team_id``, ``id`` (athlete id), ``name`` and ``jersey`` (the
    athlete's most-worn jersey for that team), one row per (team, athlete).
    """
    if league == "mfb":
        from sportsdataverse.cfb import load_cfb_game_rosters

        raw = load_cfb_game_rosters(seasons=[season - 1])
        jersey_col = "jersey"
    elif league == "wbb":
        from sportsdataverse.wbb import load_wbb_player_boxscore

        raw = load_wbb_player_boxscore(seasons=[season])
        jersey_col = "athlete_jersey"
    elif league == "mbb":
        from sportsdataverse.mbb import load_mbb_player_boxscore

        raw = load_mbb_player_boxscore(seasons=[season])
        jersey_col = "athlete_jersey"
    else:
        raise ValueError(f"espn_player_xwalk: unknown league {league!r}")
    if raw.height == 0:
        return pl.DataFrame(schema=_SIDE_SCHEMA)
    return _collapse_espn(
        raw.select(
            _id(pl.col("team_id")).alias("espn_team_id"),
            _id(pl.col("athlete_id")).alias("id"),
            pl.col("athlete_display_name").alias("name"),
            _jersey(pl.col(jersey_col)).alias("jersey"),
        )
    )


def _collapse_espn(rows: pl.DataFrame) -> pl.DataFrame:
    """Game-level rows -> one row per (team, athlete): last-seen name, modal jersey."""
    rows = rows.drop_nulls(["espn_team_id", "id"])
    jersey = (
        rows.drop_nulls("jersey")
        .group_by("espn_team_id", "id", "jersey")
        .agg(pl.len().alias("n"))
        .sort(["n", "jersey"], descending=[True, False])
        .unique(["espn_team_id", "id"], keep="first")
        .drop("n")
    )
    names = rows.drop_nulls("name").unique(["espn_team_id", "id"], keep="last").select("espn_team_id", "id", "name")
    return (
        rows.select("espn_team_id", "id")
        .unique()
        .join(names, on=["espn_team_id", "id"], how="left")
        .join(jersey, on=["espn_team_id", "id"], how="left")
        .sort(["espn_team_id", "id"])
        .select(list(_SIDE_SCHEMA))
    )


def _keyed(frame: pl.DataFrame) -> pl.DataFrame:
    nk = [_person(n) for n in frame.get_column("name").to_list()]
    return frame.with_columns(
        pl.Series("nk", nk, dtype=pl.Utf8),
        pl.Series("lk", [k.rsplit(" ", 1)[-1] if k else "" for k in nk], dtype=pl.Utf8),
    )


def _tier(left: pl.DataFrame, right: pl.DataFrame, keys: List[str], ambiguous: Set[str]) -> pl.DataFrame:
    """``(left id, right id)`` for keys unique on both sides; left ids on a tied key go into *ambiguous*."""
    keys = ["espn_team_id", *keys]
    lk = left.drop_nulls(keys).filter(pl.col(keys[1]) != "")
    rk = right.drop_nulls(keys).filter(pl.col(keys[1]) != "")
    l_n = lk.group_by(keys).agg(pl.len().alias("ln"))
    r_n = rk.group_by(keys).agg(pl.len().alias("rn"))
    counts = l_n.join(r_n, on=keys, how="inner")
    ambiguous.update(
        lk.join(counts.filter((pl.col("ln") > 1) | (pl.col("rn") > 1)), on=keys, how="semi").get_column("id").to_list()
    )
    unique = counts.filter((pl.col("ln") == 1) & (pl.col("rn") == 1)).select(keys)
    return (
        lk.join(unique, on=keys, how="semi")
        .join(rk.select(*keys, pl.col("id").alias("right_id")), on=keys, how="inner")
        .select("id", "right_id")
    )


def _jw_tier(left: pl.DataFrame, right: pl.DataFrame, ambiguous: Set[str]) -> pl.DataFrame:
    """Mutually-unique Jaro-Winkler pairs >= :data:`JW_FLOOR` within each ESPN team block."""
    by_team: Dict[str, List[tuple]] = {}
    for team, rid, rk in right.select("espn_team_id", "id", "nk").iter_rows():
        if rk:
            by_team.setdefault(team, []).append((rid, rk))
    cands: Dict[str, List[tuple]] = {}
    claims: Dict[tuple, int] = {}
    for team, lid, lk in left.select("espn_team_id", "id", "nk").iter_rows():
        if not lk:
            continue
        hits = [(team, rid) for rid, rk in by_team.get(team, ()) if jaro_winkler(lk, rk) >= JW_FLOOR]
        cands[lid] = hits
        for key in hits:
            claims[key] = claims.get(key, 0) + 1
    pairs: List[dict] = []
    for lid, hits in cands.items():
        if len(hits) == 1 and claims[hits[0]] == 1:
            pairs.append({"id": lid, "right_id": hits[0][1]})
        elif hits:
            ambiguous.add(lid)
    return pl.DataFrame(pairs, schema={"id": pl.Utf8, "right_id": pl.Utf8})


def match_players(ncaa: pl.DataFrame, espn: pl.DataFrame) -> pl.DataFrame:
    """Match an NCAA roster side to an ESPN athlete side (pure, offline).

    Args:
        ncaa: :func:`ncaa_roster_side` shape -- ``ncaa_team_id``, ``id``,
            ``name``, ``jersey``, ``espn_team_id``.
        espn: :func:`espn_athlete_side` shape -- ``espn_team_id``, ``id``,
            ``name``, ``jersey``.

    Returns:
        One row per NCAA player: ``ncaa_team_id``, ``ncaa_player_id``,
        ``espn_team_id``, ``espn_athlete_id``, ``match_method`` and
        ``unmatched_reason`` (exactly one of the last two is non-null).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.scrape.ncaa.espn_player_xwalk import match_players

            ncaa = pl.DataFrame({"ncaa_team_id": ["1"], "id": ["p1"], "name": ["Caitlin Clark"],
                                 "jersey": ["22"], "espn_team_id": ["2294"]})
            espn = pl.DataFrame({"espn_team_id": ["2294"], "id": ["4433403"],
                                 "name": ["Caitlin Clark"], "jersey": ["22"]})
            print(match_players(ncaa, espn))
    """
    # `id` on the left is a row key: an NCAA player id can sit on two team rosters in one season.
    left = _keyed(
        ncaa.select("ncaa_team_id", pl.col("id").alias("ncaa_player_id"), "name", "jersey", "espn_team_id")
        .with_row_index("id")
        .with_columns(pl.col("id").cast(pl.Utf8))
    )
    right = _keyed(espn.select(list(_SIDE_SCHEMA)))
    ambiguous: Set[str] = set()
    matched: List[pl.DataFrame] = []
    pending, free = left, right
    for method, keys in (
        ("name_jersey", ["nk", "jersey"]),
        ("name", ["nk"]),
        ("last_jersey", ["lk", "jersey"]),
        ("jaro_winkler", None),
    ):
        pairs = _jw_tier(pending, free, ambiguous) if keys is None else _tier(pending, free, keys, ambiguous)
        pairs = pairs.join(pending.select("id", "espn_team_id"), on="id", how="left")
        matched.append(pairs.with_columns(pl.lit(method).alias("match_method")))
        pending = pending.join(pairs, on="id", how="anti")
        # athlete ids are only consumed inside their own team block
        free = free.join(
            pairs.select("espn_team_id", pl.col("right_id").alias("id")), on=["espn_team_id", "id"], how="anti"
        )
    hits = (
        pl.concat(matched)
        .select("id", "right_id", "match_method")
        .join(left.select("id", "ncaa_player_id"), on="id", how="left")
    )
    # An ESPN athlete claimed by two DIFFERENT NCAA players is voided on both (one player on two
    # rosters -- a mid-season transfer -- may legitimately claim it twice).
    dup = (
        hits.group_by("right_id")
        .agg(pl.col("ncaa_player_id").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
        .get_column("right_id")
        .implode()
    )
    ambiguous.update(hits.filter(pl.col("right_id").is_in(dup)).get_column("id").to_list())
    hits = hits.filter(~pl.col("right_id").is_in(dup)).drop("ncaa_player_id")
    teams_with_players = right.get_column("espn_team_id").unique().implode()
    out = left.join(hits, on="id", how="left").with_columns(
        pl.when(pl.col("right_id").is_not_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("espn_team_id").is_null())
        .then(pl.lit("no_espn_team"))
        .when(~pl.col("espn_team_id").is_in(teams_with_players))
        .then(pl.lit("no_espn_players"))
        .when(pl.col("id").is_in(pl.Series(sorted(ambiguous), dtype=pl.Utf8).implode()))
        .then(pl.lit("ambiguous"))
        .otherwise(pl.lit("no_candidate"))
        .alias("unmatched_reason")
    )
    return out.select(
        "ncaa_team_id",
        "ncaa_player_id",
        "espn_team_id",
        pl.col("right_id").alias("espn_athlete_id"),
        "match_method",
        "unmatched_reason",
    ).sort(["ncaa_team_id", "ncaa_player_id"])


def build_season_xwalk(
    root: Union[str, Path], *, league: str, season: int, espn: Optional[pl.DataFrame] = None
) -> pl.DataFrame:
    """Build one season's player crosswalk (one release read unless *espn* is given).

    Args:
        root: The ``-raw`` repo checkout.
        league: ``"mbb"``, ``"wbb"`` or ``"mfb"`` (required -- never defaulted).
        season: The repo's tree key (ending year for hoops, academic year for mfb).
        espn: Optional pre-built :func:`espn_athlete_side` frame (tests, local runs).

    Returns:
        The :func:`match_players` frame.
    """
    ncaa = ncaa_roster_side(root, league, season)
    if espn is None:
        espn = espn_athlete_side(league, season)
    return match_players(ncaa, espn)


def write_season_xwalk(root: Union[str, Path], league: str, season: int, frame: pl.DataFrame) -> Path:
    """Write *frame* to :func:`xwalk_path` as plain utf-8 JSON (atomic)."""
    path = xwalk_path(root, league, season)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(frame.to_dicts()), encoding="utf-8")
    tmp.replace(path)
    return path


def summarize(frame: pl.DataFrame) -> Dict[str, int]:
    """Per-tier hit counts and per-reason misses, for the build log."""
    counts = {k: 0 for k in (*MATCH_METHODS, *UNMATCHED_REASONS)}
    for col in ("match_method", "unmatched_reason"):
        for key, n in frame.drop_nulls(col).group_by(col).agg(pl.len()).iter_rows():
            counts[key] = n
    counts["players"] = frame.height
    counts["matched"] = sum(counts[m] for m in MATCH_METHODS)
    return counts


def _main(default_league: str, default_root: str) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build the NCAA player_id -> ESPN athlete_id crosswalk.")
    parser.add_argument("--root", default=default_root, help="Root of the raw data tree (default: repo root).")
    parser.add_argument("--league", default=default_league, help=f"League slug (default: {default_league}).")
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        required=True,
        help="Tree keys: ending years for mbb/wbb, academic years (fall + 1) for mfb.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report match rates without writing files.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for season in args.seasons:
        frame = build_season_xwalk(args.root, league=args.league, season=season)
        s = summarize(frame)
        pct = round(100 * s["matched"] / s["players"], 2) if s["players"] else 0.0
        if not args.dry_run and s["players"]:
            write_season_xwalk(args.root, args.league, season, frame)
        print(
            f"{args.league} {season}: players={s['players']} matched={s['matched']} ({pct}%) "
            + " ".join(f"{k}={s[k]}" for k in (*MATCH_METHODS, *UNMATCHED_REASONS)),
            flush=True,
        )
