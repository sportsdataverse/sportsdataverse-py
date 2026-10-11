"""Offline (archive) builders for the NBA / WNBA ESPN <-> Stats crosswalks.

The live builders in :mod:`sportsdataverse.nba.nba_crosswalk` and
:mod:`sportsdataverse.wnba.wnba_crosswalk` read current-season endpoints, so they
cannot produce history. This module rebuilds any past season from two stored
archives instead:

* **ESPN side** -- the season's ESPN schedule (``espn_{nba,wnba}_schedules``) and
  ESPN game rosters (``espn_{nba,wnba}_game_rosters``), as published parquet.
* **Stats side** -- the per-game ``stats.nba.com`` / ``stats.wnba.com`` JSON kept in
  ``hoopR-nba-stats-raw`` / ``wehoop-wnba-stats-raw``: ``boxscoretraditionalv3``
  for teams and players, ``boxscoresummaryv2`` (then ``leaguegamelog``) for the
  game date, which the box score does not carry.

Matching, in order:

1. **Teams** pair by game-date co-occurrence: a Stats team and an ESPN team that are
   the home (or away) side on the same dates are the same franchise. No name or
   franchise table is needed, so relocations and renames resolve themselves.
2. **Games** join on ``(US-Eastern date, home ESPN team, away ESPN team)``, through
   the league module's own ``_assemble_schedule_crosswalk``.
3. **Players** match inside each matched (game, team) -- about 15 names a side --
   on normalized name, tie-broken by jersey, with
   :func:`~sportsdataverse._common_crosswalk_basketball.fuzzy_match`. The per-game
   pairs are then voted into one row per ESPN athlete per team.
"""

from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

from sportsdataverse._common_crosswalk_basketball import fuzzy_match, normalize_name
from sportsdataverse._temporal import as_date

log = logging.getLogger(__name__)

_SEASON_TYPES = {
    "1": "Pre-Season",
    "2": "Regular Season",
    "3": "All-Star",
    "4": "Playoffs",
    "5": "Play-In Game",
}
_CUP_LABEL = {"nba": "NBA Cup", "wnba": "Commissioner's Cup"}

# A Stats team pairs with an ESPN team only if they share at least this share
# of the ESPN team's (date, side) appearances on dates both sources cover.
_TEAM_PAIR_FLOOR = 0.5

_MATCH_KEYS = "game_id+team+name+jersey"


def _load_json(path: Path) -> Any:
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _result_rows(payload: Any, name: str) -> List[Dict[str, Any]]:
    """Rows of the named ``resultSets`` entry as dicts (empty when absent)."""
    sets = payload.get("resultSets") if isinstance(payload, dict) else None
    for rs in sets or []:
        if isinstance(rs, dict) and rs.get("name") == name:
            headers = rs.get("headers") or []
            return [dict(zip(headers, row)) for row in rs.get("rowSet") or []]
    return []


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def read_stats_archive(stats_dir: Union[str, Path], league: str, season: int) -> Dict[str, pl.DataFrame]:
    """Read one season of the local stats JSON archive into frames.

    Args:
        stats_dir: The archive's ``json`` root, e.g.
            ``hoopR-nba-stats-raw/nba_stats/json``. Season directories use the
            season's ENDING year (``2025`` = 2024-25 for the NBA).
        league: ``"nba"`` or ``"wnba"`` -- the column prefix.
        season: Season (ending year).

    Returns:
        ``{"games", "players", "teams"}``. ``games`` has ``{p}_game_id``,
        ``{p}_game_code``, ``game_date`` (``pl.Date``, US-Eastern), ``season_type``,
        ``{p}_home_team_id``, ``{p}_away_team_id``. ``players`` has one row per
        player per box score: ``{p}_game_id``, ``{p}_team_id``, ``{p}_player_id``,
        ``{p}_player_name``, ``{p}_jersey_num``, ``{p}_position``. ``teams`` has
        ``{p}_team_id``, ``tricode``, ``city``, ``name``, ``slug``. Every id is
        ``Utf8``. A season with no box scores returns empty typed frames.
    """
    p = league
    root = Path(stats_dir)
    games: List[Dict[str, Optional[str]]] = []
    players: List[Dict[str, Optional[str]]] = []
    teams: Dict[str, Dict[str, Optional[str]]] = {}
    unreadable = 0
    for path in sorted((root / "boxscoretraditionalv3" / str(season)).glob("*.json")):
        try:
            box = _load_json(path)["boxScoreTraditional"]
            gid = str(box["gameId"])
        except (OSError, ValueError, KeyError, TypeError):
            unreadable += 1
            continue
        games.append(
            {
                f"{p}_game_id": gid,
                f"{p}_home_team_id": _text(box.get("homeTeamId")),
                f"{p}_away_team_id": _text(box.get("awayTeamId")),
            }
        )
        for side in ("homeTeam", "awayTeam"):
            team = box.get(side) or {}
            tid = _text(team.get("teamId"))
            if tid is None:
                continue
            teams[tid] = {
                f"{p}_team_id": tid,
                "tricode": _text(team.get("teamTricode")),
                "city": _text(team.get("teamCity")),
                "name": _text(team.get("teamName")),
                "slug": _text(team.get("teamSlug")),
            }
            for player in team.get("players") or []:
                name = " ".join(x for x in (_text(player.get("firstName")), _text(player.get("familyName"))) if x)
                players.append(
                    {
                        f"{p}_game_id": gid,
                        f"{p}_team_id": tid,
                        f"{p}_player_id": _text(player.get("personId")),
                        f"{p}_player_name": name or None,
                        f"{p}_jersey_num": _text(player.get("jerseyNum")),
                        f"{p}_position": _text(player.get("position")),
                    }
                )
    if unreadable:
        log.warning("%s %s: %d unreadable box score files skipped", league, season, unreadable)

    # Game date + code: boxscoresummaryv2 first, the team game log for the rest.
    dates: Dict[str, Optional[str]] = {}
    codes: Dict[str, Optional[str]] = {}
    for path in sorted((root / "boxscoresummaryv2" / str(season)).glob("*.json")):
        try:
            rows = _result_rows(_load_json(path), "GameSummary")
        except (OSError, ValueError):
            continue
        for row in rows[:1]:
            sid = _text(row.get("GAME_ID"))
            if sid is not None:
                dates[sid] = (_text(row.get("GAME_DATE_EST")) or "")[:10] or None
                codes[sid] = _text(row.get("GAMECODE"))
    for name in ("regular-season.json", "playoffs.json"):
        path = root / "leaguegamelog" / str(season) / name
        if not path.exists():
            continue
        try:
            rows = _result_rows(_load_json(path), "LeagueGameLog")
        except (OSError, ValueError):
            continue
        for row in rows:
            lid = _text(row.get("GAME_ID"))
            if lid is not None and dates.get(lid) is None:
                dates[lid] = (_text(row.get("GAME_DATE")) or "")[:10] or None

    cup = _CUP_LABEL.get(league)
    game_schema = {
        f"{p}_game_id": pl.Utf8,
        f"{p}_game_code": pl.Utf8,
        "game_date": pl.Utf8,
        "season_type": pl.Utf8,
        f"{p}_home_team_id": pl.Utf8,
        f"{p}_away_team_id": pl.Utf8,
    }
    game_rows = [
        {
            **g,
            f"{p}_game_code": codes.get(str(g[f"{p}_game_id"])),
            "game_date": dates.get(str(g[f"{p}_game_id"])),
            "season_type": _SEASON_TYPES.get(str(g[f"{p}_game_id"])[2:3], cup),
        }
        for g in games
    ]
    games_df = pl.DataFrame(game_rows, schema=game_schema).with_columns(as_date(pl.col("game_date")))
    players_df = pl.DataFrame(
        players,
        schema={
            f"{p}_game_id": pl.Utf8,
            f"{p}_team_id": pl.Utf8,
            f"{p}_player_id": pl.Utf8,
            f"{p}_player_name": pl.Utf8,
            f"{p}_jersey_num": pl.Utf8,
            f"{p}_position": pl.Utf8,
        },
    )
    teams_df = pl.DataFrame(
        list(teams.values()),
        schema={f"{p}_team_id": pl.Utf8, "tricode": pl.Utf8, "city": pl.Utf8, "name": pl.Utf8, "slug": pl.Utf8},
    )
    return {"games": games_df, "players": players_df, "teams": teams_df}


def _espn_games(schedule: pl.DataFrame) -> pl.DataFrame:
    """ESPN games keyed for the date join. ``game_date`` must already be the US-Eastern date,
    as in the published ``espn_*_schedules`` (a UTC date would shift late West-coast games)."""
    return schedule.select(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_game_id"),
        as_date(pl.col("game_date")).alias("game_date"),
        pl.col("home_id").cast(pl.Int32).alias("home_espn_team_id"),
        pl.col("away_id").cast(pl.Int32).alias("away_espn_team_id"),
    ).unique("espn_game_id", keep="first", maintain_order=True)


def _espn_teams(schedule: pl.DataFrame) -> pl.DataFrame:
    """One row per ESPN team in the season schedule, as the season named it (latest game)."""
    sides = [
        schedule.select(
            pl.col("game_date"),
            pl.col(f"{s}_id").cast(pl.Int32).alias("espn_team_id"),
            pl.col(f"{s}_abbreviation").cast(pl.Utf8).alias("espn_abbreviation"),
            pl.col(f"{s}_display_name").cast(pl.Utf8).alias("espn_display_name"),
            pl.col(f"{s}_short_display_name").cast(pl.Utf8).alias("espn_short_name"),
            pl.col(f"{s}_location").cast(pl.Utf8).alias("espn_location"),
            pl.col(f"{s}_name").cast(pl.Utf8).alias("espn_mascot"),
        )
        for s in ("home", "away")
    ]
    return (
        pl.concat(sides)
        .filter(pl.col("espn_team_id").is_not_null())
        .sort("game_date", nulls_last=False, maintain_order=True)
        .unique("espn_team_id", keep="last", maintain_order=True)
        .drop("game_date")
        .sort("espn_team_id")
    )


def _pair_teams(espn_games: pl.DataFrame, stats_games: pl.DataFrame, p: str) -> pl.DataFrame:
    """Stats team id -> ESPN team id by (date, side) co-occurrence, greedy and one-to-one."""

    def long(df: pl.DataFrame, home: str, away: str, team: str) -> pl.DataFrame:
        return pl.concat(
            [
                df.select(pl.col("game_date"), pl.lit(side).alias("side"), pl.col(col).alias(team))
                for side, col in (("home", home), ("away", away))
            ]
        ).drop_nulls()

    e = long(espn_games, "home_espn_team_id", "away_espn_team_id", "espn_team_id")
    s = long(stats_games, f"{p}_home_team_id", f"{p}_away_team_id", f"{p}_team_id")
    assert e.schema["game_date"] == s.schema["game_date"], "game_date dtype mismatch between ESPN and Stats"
    e = e.filter(pl.col("game_date").is_in(s["game_date"].unique().implode()))
    appearances = e.group_by("espn_team_id").agg(pl.len().alias("n_espn"))
    counts = (
        e.join(s, on=["game_date", "side"])
        .group_by(["espn_team_id", f"{p}_team_id"])
        .agg(pl.len().alias("n"))
        .join(appearances, on="espn_team_id")
        .with_columns((pl.col("n") / pl.col("n_espn")).alias("share"))
        .filter(pl.col("share") >= _TEAM_PAIR_FLOOR)
        .sort(["n", "share", "espn_team_id", f"{p}_team_id"], descending=[True, True, False, False])
    )
    used_e: set = set()
    used_s: set = set()
    rows = []
    for espn_id, stats_id, share in counts.select("espn_team_id", f"{p}_team_id", "share").iter_rows():
        if espn_id in used_e or stats_id in used_s:
            continue
        used_e.add(espn_id)
        used_s.add(stats_id)
        rows.append({"espn_team_id": espn_id, f"{p}_team_id": stats_id, "match_confidence": share})
    return pl.DataFrame(
        rows, schema={"espn_team_id": pl.Int32, f"{p}_team_id": pl.Utf8, "match_confidence": pl.Float64}
    )


def _team_crosswalk(
    league: str, season: int, espn_teams: pl.DataFrame, pairs: pl.DataFrame, stats_teams: pl.DataFrame
) -> pl.DataFrame:
    p = league
    mod = _league_module(league)
    city, name = pl.col("city"), pl.col("name")
    stats2 = stats_teams.select(
        pl.col(f"{p}_team_id"),
        pl.col("tricode").alias(f"{p}_team_abbreviation" if p == "nba" else f"{p}_team_tricode"),
        (pl.concat_str([city, name], separator=" ", ignore_nulls=True) if p == "nba" else name).alias(f"{p}_team_name"),
        city.alias(f"{p}_team_city"),
        pl.col("slug").alias(f"{p}_team_slug"),
    )
    out = (
        espn_teams.join(pairs, on="espn_team_id", how="left", maintain_order="left")
        .join(stats2, on=f"{p}_team_id", how="left", maintain_order="left")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.when(pl.col(f"{p}_team_id").is_not_null())
            .then(pl.lit("game_dates"))
            .otherwise(pl.lit("unmatched"))
            .alias("match_method"),
        )
    )
    missing = [c for c in mod.TEAM_COLUMNS if c not in out.columns]
    out = out.with_columns([pl.lit(None, dtype=pl.Utf8).alias(c) for c in missing])
    if p == "nba":
        out = out.with_columns(mod._yahoo_team_id(season))
    return out.select(mod.TEAM_COLUMNS)


def _league_module(league: str) -> Any:
    if league == "nba":
        from sportsdataverse.nba import nba_crosswalk

        return nba_crosswalk
    if league == "wnba":
        from sportsdataverse.wnba import wnba_crosswalk

        return wnba_crosswalk
    raise ValueError(f"league must be 'nba' or 'wnba', got {league!r}")


def _player_crosswalk(
    league: str,
    season: int,
    espn_games: pl.DataFrame,
    rosters: pl.DataFrame,
    schedule_xwalk: pl.DataFrame,
    pairs: pl.DataFrame,
    stats_games: pl.DataFrame,
    stats_players: pl.DataFrame,
    min_confidence: float,
) -> pl.DataFrame:
    p = league
    pid, pname, pjersey, ppos = f"{p}_player_id", f"{p}_player_name", f"{p}_jersey_num", f"{p}_position"

    # ESPN roster rows, dated through the schedule so "latest" means the last game played.
    espn = (
        rosters.select(
            pl.col("game_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_game_id"),
            pl.col("team_id").cast(pl.Int32).alias("espn_team_id"),
            pl.col("team_abbreviation").cast(pl.Utf8),
            pl.col("athlete_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_athlete_id"),
            pl.col("athlete_display_name").cast(pl.Utf8).alias("espn_full_name"),
            pl.col("athlete_jersey").cast(pl.Utf8).alias("espn_jersey"),
            pl.col("athlete_position").cast(pl.Utf8).alias("espn_position"),
        )
        .filter(pl.col("espn_athlete_id").is_not_null() & pl.col("espn_team_id").is_not_null())
        .join(espn_games.select("espn_game_id", "game_date"), on="espn_game_id", how="left")
    )

    # Matched games -> block per (ESPN game, ESPN team); the Stats side reaches it through the team pairs.
    matched = schedule_xwalk.filter(pl.col("match_method") == "both").select("espn_game_id", f"{p}_game_id")
    to_espn = pairs.select(f"{p}_team_id", "espn_team_id")
    block = pl.concat_str([pl.col("espn_game_id"), pl.col("espn_team_id").cast(pl.Utf8)], separator="|")
    left = espn.join(matched, on="espn_game_id").with_columns(block.alias("block"))
    left = left.select(
        "block",
        "espn_team_id",
        pl.col("espn_athlete_id").alias("id"),
        pl.Series("name_key", [normalize_name(v) for v in left["espn_full_name"].to_list()], dtype=pl.Utf8),
        pl.col("espn_jersey").alias("jersey"),
    )
    right = stats_players.join(matched, on=f"{p}_game_id").join(to_espn, on=f"{p}_team_id")
    right = right.select(
        block.alias("block"),
        pl.col(pid).alias("id"),
        pl.Series("name_key", [normalize_name(v) for v in right[pname].to_list()], dtype=pl.Utf8),
        pl.col(pjersey).alias("jersey"),
    )

    by_block = right.partition_by("block", as_dict=True)
    empty_right = right.clear()
    pair_frames = [
        fuzzy_match(lf, by_block.get(key, empty_right), min_confidence, exact_tiebreak=True)
        for key, lf in left.partition_by("block", as_dict=True).items()
    ]
    game_pairs = (
        pl.concat(pair_frames)
        if pair_frames
        else pl.DataFrame(
            schema={
                "block": pl.Utf8,
                "left_id": pl.Utf8,
                "right_id": pl.Utf8,
                "match_method": pl.Utf8,
                "match_confidence": pl.Float64,
            }
        )
    ).join(left.select("block", pl.col("id").alias("left_id"), "espn_team_id"), on=["block", "left_id"])

    keys = ["espn_team_id", "espn_athlete_id"]
    # Denominator = games where the athlete matched anyone, so the score is agreement, not presence.
    games_seen = (
        game_pairs.filter(pl.col("right_id").is_not_null())
        .group_by(["espn_team_id", "left_id"])
        .agg(pl.len().alias("n_games"))
    )
    best = (
        game_pairs.filter(pl.col("right_id").is_not_null())
        .group_by(["espn_team_id", "left_id", "right_id"])
        .agg(pl.len().alias("n"), (pl.col("match_method") == "exact_name").sum().alias("n_exact"))
        .sort(["n", "n_exact", "right_id"], descending=[True, True, False])
        # A tie between the top two candidates is left unmatched rather than guessed.
        .with_columns(
            ((pl.col("n").shift(-1) == pl.col("n")) & (pl.col("n_exact").shift(-1) == pl.col("n_exact")))
            .over(["espn_team_id", "left_id"])
            .fill_null(False)
            .alias("_tied")
        )
        .group_by(["espn_team_id", "left_id"], maintain_order=True)
        .first()
        .filter(pl.col("_tied") == False)
        .join(games_seen, on=["espn_team_id", "left_id"])
        .select(
            pl.col("espn_team_id"),
            pl.col("left_id").alias("espn_athlete_id"),
            pl.col("right_id").alias(pid),
            pl.when(pl.col("n_exact") * 2 >= pl.col("n"))
            .then(pl.lit("exact_name"))
            .otherwise(pl.lit("fuzzy_jw"))
            .alias("match_method"),
            (pl.col("n") / pl.col("n_games")).alias("match_confidence"),
        )
    )

    latest_espn = espn.sort("game_date", nulls_last=False).unique(keys, keep="last", maintain_order=False)
    stats_latest = (
        stats_players.join(stats_games.select(f"{p}_game_id", "game_date"), on=f"{p}_game_id", how="left")
        .join(to_espn, on=f"{p}_team_id")
        .sort("game_date", nulls_last=False)
        .unique([pid, "espn_team_id"], keep="last")
        .select(pid, "espn_team_id", pname, pjersey, ppos)
    )
    assert best.schema[pid] == stats_latest.schema[pid], f"{pid} dtype mismatch"
    out = (
        latest_espn.join(best, on=keys, how="left")
        .join(stats_latest, on=[pid, "espn_team_id"], how="left")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.col("espn_full_name")
            .map_elements(normalize_name, return_dtype=pl.Utf8, skip_nulls=False)
            .alias("player_name"),
            pl.col("match_method").fill_null("unmatched"),
            pl.when(pl.col(pid).is_not_null()).then(pl.lit(_MATCH_KEYS)).otherwise(None).alias("match_keys"),
        )
        .with_columns([pl.lit(None, dtype=pl.Utf8).alias(c) for c in _NULL_PLAYER_COLUMNS])
        .sort(["espn_team_id", "espn_full_name", "espn_athlete_id"])
    )
    return out.select(
        "season",
        "espn_team_id",
        "team_abbreviation",
        "player_name",
        "espn_athlete_id",
        "espn_full_name",
        "espn_jersey",
        "espn_position",
        pid,
        pname,
        pjersey,
        ppos,
        "fox_athlete_id",
        "fox_player",
        "fox_jersey",
        "fox_position_group",
        "yahoo_player_id",
        "yahoo_player_name",
        "match_method",
        "match_confidence",
        "match_keys",
    )


_NULL_PLAYER_COLUMNS = (
    "fox_athlete_id",
    "fox_player",
    "fox_jersey",
    "fox_position_group",
    "yahoo_player_id",
    "yahoo_player_name",
)


def build_archive_crosswalks(
    league: str,
    season: int,
    espn_schedule: pl.DataFrame,
    espn_game_rosters: pl.DataFrame,
    stats: Dict[str, pl.DataFrame],
    min_confidence: float = 0.92,
) -> Dict[str, pl.DataFrame]:
    """Build the team, schedule and player crosswalks for one past season (no I/O).

    Args:
        league: ``"nba"`` or ``"wnba"``.
        season: Season (ending year), stamped on every row.
        espn_schedule: The season's ESPN schedule parquet (``game_id``,
            ``game_date``, ``home_id`` / ``away_id`` and the ``home_*`` /
            ``away_*`` name columns).
        espn_game_rosters: The season's ESPN game rosters parquet (``game_id``,
            ``team_id``, ``team_abbreviation``, ``athlete_id``,
            ``athlete_display_name``, ``athlete_jersey``, ``athlete_position``).
        stats: Output of :func:`read_stats_archive`.
        min_confidence: Jaro-Winkler floor for a fuzzy player match.

    Returns:
        ``{"team", "schedule", "player"}`` frames with the league module's
        ``TEAM_COLUMNS`` / ``SCHEDULE_COLUMNS`` and the 21 player columns.
    """
    p = league
    mod = _league_module(league)
    espn_games = _espn_games(espn_schedule)
    stats_games = stats["games"]
    pairs = _pair_teams(espn_games, stats_games, p)
    team = _team_crosswalk(league, season, _espn_teams(espn_schedule), pairs, stats["teams"])
    schedule = mod._assemble_schedule_crosswalk(espn_games, stats_games, team, season)
    player = _player_crosswalk(
        league, season, espn_games, espn_game_rosters, schedule, pairs, stats_games, stats["players"], min_confidence
    )
    return {"team": team, "schedule": schedule, "player": player}
