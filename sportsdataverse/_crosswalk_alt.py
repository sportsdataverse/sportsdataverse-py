"""Parse the stored Fox / Yahoo / CBS history payloads and match them to ESPN (private).

The harvester (:mod:`sportsdataverse._crosswalk_history`) keeps raw per-game payloads in
the per-sport ``*-alt-raw`` repos. This module is the next two steps:

* **Parse** (:func:`parse_season`, ``python -m sportsdataverse._crosswalk_alt``): one
  season of raw payloads into two tidy frames, written to the matching ``*-alt-data``
  repo as ``{provider}/{dataset}/parquet/{league}_{provider}_{dataset}_{season}.parquet``:

  - ``games``: provider game id, US-Eastern date, home / away provider team id and name.
  - ``game_rosters``: one row per (game, team, player): provider player id, name,
    jersey, position. Yahoo lists every dressed player, CBS every participant and
    inactive, Fox every player in the box score.

* **Match** (:func:`match_season`), reading those frames back with :func:`read_season`:

  1. **Teams** pair by (date, home/away side) co-occurrence with ESPN's schedule, the
     archive builders' rule, so no name or franchise table is needed and relocated
     teams resolve themselves.
  2. **Games** join on (US-Eastern date, unordered ESPN team pair); a key held by
     two games on either side is ambiguous and left unmatched.
  3. **Players** match inside each matched (game, team) on normalized name with a
     jersey tie-break, then vote across the season into one row per ESPN athlete per
     team (:func:`~sportsdataverse._crosswalk_basketball_archive.vote_block_matches`):
     a tie between the top two candidates stays unmatched, never guessed.

:func:`fill_player_legs` then writes the voted ids into an existing player crosswalk,
only where its provider id is null, so an existing provider leg is never dropped;
:func:`fill_game_legs` does the same for a schedule crosswalk's game and team ids.

Seasons follow SDV (basketball: ending year; WNBA and football: calendar year), as the
harvester stores them.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import polars as pl

from sportsdataverse._common_crosswalk_basketball import normalize_name, to_eastern
from sportsdataverse._crosswalk_basketball_archive import _pair_teams, vote_block_matches
from sportsdataverse._fox_layout import _uri_id

log = logging.getLogger(__name__)

PROVIDERS = ("fox", "yahoo", "cbs")

GAMES_SCHEMA: Dict[str, Any] = {
    "season": pl.Int32,
    "game_id": pl.Utf8,
    "game_date": pl.Date,
    "home_team_id": pl.Utf8,
    "home_team_name": pl.Utf8,
    "away_team_id": pl.Utf8,
    "away_team_name": pl.Utf8,
}
ROSTERS_SCHEMA: Dict[str, Any] = {
    "season": pl.Int32,
    "game_id": pl.Utf8,
    "team_id": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "jersey": pl.Utf8,
    "position": pl.Utf8,
}
#: ESPN inputs to :func:`match_season`.
ESPN_GAMES_SCHEMA: Dict[str, Any] = {
    "espn_game_id": pl.Utf8,
    "game_date": pl.Date,
    "home_espn_team_id": pl.Int32,
    "away_espn_team_id": pl.Int32,
}
ESPN_ROSTERS_SCHEMA: Dict[str, Any] = {
    "espn_game_id": pl.Utf8,
    "espn_team_id": pl.Int32,
    "espn_athlete_id": pl.Utf8,
    "espn_full_name": pl.Utf8,
    "espn_jersey": pl.Utf8,
}
PLAYERS_SCHEMA: Dict[str, Any] = {
    "espn_team_id": pl.Int32,
    "espn_athlete_id": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "jersey": pl.Utf8,
    "position": pl.Utf8,
    "match_method": pl.Utf8,
    "match_confidence": pl.Float64,
}

#: Basketball player-crosswalk columns each provider fills (crosswalk column -> matched column).
#: Fox box scores carry no jersey and a position (``SG``), not the roster's position group,
#: so ``fox_jersey`` / ``fox_position_group`` stay as the live builder left them.
BASKETBALL_PLAYER_LEGS: Dict[str, Dict[str, str]] = {
    "yahoo": {"yahoo_player_id": "player_id", "yahoo_player_name": "player_name"},
    "fox": {"fox_athlete_id": "player_id", "fox_player": "player_name"},
}
#: Basketball schedule-crosswalk legs per provider: (game id, home team id, away team id).
BASKETBALL_GAME_LEGS: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {
    "yahoo": ("yahoo_game_id", None, None),
    "fox": ("fox_game_id", "fox_home_team_id", "fox_away_team_id"),
}


def known_teams(provider: str, league: str) -> Optional[pl.DataFrame]:
    """Fixed ESPN -> provider team pairs for :func:`match_season`, where sdv-py holds a map.

    Only CBS college football has one today (the ``cfb.cbs_pbp`` resolver's static map);
    every other (provider, league) returns ``None`` and pairs by co-occurrence alone.
    """
    if (provider, league) != ("cbs", "cfb"):
        return None
    from sportsdataverse.cfb.cbs_pbp.teams import _CBS_TEAM_BY_ESPN

    return pl.DataFrame(
        {
            "espn_team_id": [int(k) for k in _CBS_TEAM_BY_ESPN],
            "team_id": [str(v[0]) for v in _CBS_TEAM_BY_ESPN.values()],
        },
        schema={"espn_team_id": pl.Int32, "team_id": pl.Utf8},
    )


#: Raw endpoint directory per provider, under ``{provider}/{league}/``.
_BOX_ENDPOINT = {"fox": "event_data", "yahoo": "playbook_boxscore", "cbs": "game_boxscore"}


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read(path: Path) -> Any:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _game_files(raw_root: Path, provider: str, league: str, season: int) -> List[Path]:
    return sorted((raw_root / provider / league / _BOX_ENDPOINT[provider] / str(season)).glob("*.json.gz"))


# --------------------------------------------------------------------------- parse


def _yahoo_game(payload: Any, season: int) -> tuple:
    game = (((payload or {}).get("data") or {}).get("games") or [None])[0] or {}
    gid = _text(game.get("gameId"))
    teams = {side: game.get(f"{side}Team") or {} for side in ("home", "away")}
    row = {
        "season": season,
        "game_id": gid,
        "game_date": to_eastern(game.get("startTime")),
        **{f"{s}_team_id": _text(game.get(f"{s}TeamId")) for s in ("home", "away")},
        **{f"{s}_team_name": _text(teams[s].get("fullName")) for s in ("home", "away")},
    }
    players = []
    for side in ("home", "away"):
        # The side's team, not player.teamId: Yahoo may show a player under today's team.
        team_id = row[f"{side}_team_id"]
        for entry in game.get(f"{side}TeamLineup") or []:
            p = (entry or {}).get("player") or {}
            positions = p.get("positionIds") or []
            players.append(
                {
                    "season": season,
                    "game_id": gid,
                    "team_id": team_id,
                    "player_id": _text(p.get("playerId")),
                    "player_name": _text(p.get("displayName")),
                    "jersey": _text(p.get("uniformNumber")),
                    "position": _text(positions[0]) if positions else None,
                }
            )
    return row, players


def _fox_player_name(link: Mapping[str, Any]) -> Optional[str]:
    alt = _text(link.get("imageAltText"))
    if alt and alt.lower() != "default headshot":
        return alt
    title = _text(link.get("title"))  # upper case, e.g. "TRACY MCGRADY"
    return title.title() if title else None


def _fox_game(payload: Any, season: int) -> tuple:
    header = (payload or {}).get("header") or {}
    gid = _text(header.get("id"))
    # Bifrost lists the away side left and the home side right.
    sides = {"away": header.get("leftTeam") or {}, "home": header.get("rightTeam") or {}}

    def team_name(t: Mapping[str, Any]) -> Optional[str]:
        return _text(t.get("imageAltText")) or _text(
            " ".join(x for x in (t.get("stackedNameTop"), t.get("stackedNameBottom")) if x)
        )

    row = {
        "season": season,
        "game_id": gid,
        "game_date": to_eastern(header.get("eventTime")),
        **{f"{s}_team_id": _uri_id(sides[s].get("uri")) for s in ("home", "away")},
        **{f"{s}_team_name": team_name(sides[s]) for s in ("home", "away")},
    }
    players: Dict[tuple, Dict[str, Any]] = {}
    for sec in ((payload or {}).get("boxscore") or {}).get("boxscoreSections") or []:
        team_id = _uri_id(sec.get("contentUri"))
        for item in sec.get("boxscoreItems") or []:
            for r in (item.get("boxscoreTable") or {}).get("rows") or []:
                link = r.get("entityLink") or {}
                pid = _uri_id(link.get("contentUri"))
                if not pid or team_id is None:
                    continue
                cell = ((r.get("columns") or [{}])[0]) or {}
                players.setdefault(
                    (team_id, pid),
                    {
                        "season": season,
                        "game_id": gid,
                        "team_id": team_id,
                        "player_id": pid,
                        "player_name": _fox_player_name(link),
                        "jersey": None,
                        "position": _text(cell.get("superscript")),
                    },
                )
    return row, list(players.values())


def _cbs_players(payload: Any, season: int, gid: str) -> List[Dict[str, Any]]:
    out: Dict[tuple, Dict[str, Any]] = {}
    for box in (payload or {}).get("boxScores") or []:
        team_id = _text(box.get("teamId"))
        stats = (box or {}).get("playerStats")
        if not isinstance(stats, dict):  # CBS answers a few football ids with a basketball box (a list)
            continue
        for entries in stats.values():
            for entry in entries if isinstance(entries, list) else []:
                p = (entry or {}).get("player") or {}
                pid = _text(p.get("playerId"))
                if not pid or team_id is None:
                    continue
                name = " ".join(x for x in (_text(p.get("firstName")), _text(p.get("lastName"))) if x)
                out.setdefault(
                    (team_id, pid),
                    {
                        "season": season,
                        "game_id": gid,
                        "team_id": team_id,
                        "player_id": pid,
                        "player_name": name or None,
                        "jersey": _text(p.get("uniform")),
                        "position": None,
                    },
                )
    return list(out.values())


def _cbs_games(raw_root: Path, league: str, season: int) -> List[Dict[str, Any]]:
    """Games from the stored scoreboard week pages (the box payload carries no date or sides)."""
    games: Dict[str, Dict[str, Any]] = {}
    for f in sorted((raw_root / "cbs" / league / "scoreboard" / str(season)).glob("*.json")):
        for card in (_read(f) or {}).get("cards") or []:
            gid = _text(card.get("cbs_game_id"))
            if not gid or gid in games:
                continue
            day = _text(card.get("date"))
            games[gid] = {
                "season": season,
                "game_id": gid,
                "game_date": datetime.strptime(day, "%Y%m%d").date() if day else None,
                "home_team_id": _text(card.get("home_cbs_team_id")),
                "home_team_name": _text(card.get("home")),
                "away_team_id": _text(card.get("away_cbs_team_id")),
                "away_team_name": _text(card.get("away")),
            }
    return list(games.values())


def parse_season(provider: str, league: str, season: int, raw_root: Union[str, Path]) -> Dict[str, pl.DataFrame]:
    """Parse one season of stored payloads into ``games`` and ``game_rosters`` frames.

    Args:
        provider: ``fox``, ``yahoo`` or ``cbs``.
        league: ``nfl``, ``cfb``, ``nba``, ``wnba``, ``mbb`` or ``wbb``.
        season: SDV season, as the harvester stored it.
        raw_root: The ``*-alt-raw`` checkout.

    Returns:
        ``{"games": GAMES_SCHEMA frame, "game_rosters": ROSTERS_SCHEMA frame}``. Fox and
        Yahoo games come from the per-game payloads; CBS games come from the week
        scoreboard pages, so they include listed games without a stored box score.
    """
    if provider not in PROVIDERS:
        raise ValueError(f"provider must be one of {PROVIDERS}, got {provider!r}")
    root = Path(raw_root)
    games: List[Dict[str, Any]] = []
    players: List[Dict[str, Any]] = []
    for f in _game_files(root, provider, league, season):
        payload = _read(f)
        if provider == "cbs":
            players.extend(_cbs_players(payload, season, f.name.split(".")[0]))
            continue
        row, rows = (_yahoo_game if provider == "yahoo" else _fox_game)(payload, season)
        if row["game_id"]:
            games.append(row)
            players.extend(rows)
    if provider == "cbs":
        games = _cbs_games(root, league, season)
    return {
        "games": pl.DataFrame(games, schema=GAMES_SCHEMA).unique("game_id", keep="first", maintain_order=True),
        "game_rosters": pl.DataFrame(players, schema=ROSTERS_SCHEMA),
    }


def season_path(alt_root: Union[str, Path], provider: str, league: str, dataset: str, season: int) -> Path:
    """``{alt_root}/{provider}/{dataset}/parquet/{league}_{provider}_{dataset}_{season}.parquet``."""
    return Path(alt_root) / provider / dataset / "parquet" / f"{league}_{provider}_{dataset}_{season}.parquet"


def write_season(
    frames: Mapping[str, pl.DataFrame], alt_root: Union[str, Path], provider: str, league: str, season: int
) -> List[Path]:
    """Write :func:`parse_season` output under ``alt_root`` (see :func:`season_path`)."""
    paths = []
    for dataset, df in frames.items():
        path = season_path(alt_root, provider, league, dataset, season)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
        paths.append(path)
    return paths


def read_season(
    alt_root: Union[str, Path], provider: str, league: str, season: int
) -> Optional[Dict[str, pl.DataFrame]]:
    """Read a parsed season back; ``None`` when either file is missing."""
    paths = {d: season_path(alt_root, provider, league, d, season) for d in ("games", "game_rosters")}
    if not all(p.exists() for p in paths.values()):
        return None
    return {d: pl.read_parquet(p) for d, p in paths.items()}


# --------------------------------------------------------------------------- match


def match_season(
    espn_games: pl.DataFrame,
    espn_rosters: pl.DataFrame,
    alt: Mapping[str, pl.DataFrame],
    min_confidence: float = 0.92,
    known_teams: Optional[pl.DataFrame] = None,
) -> Dict[str, Any]:
    """Match one provider season to ESPN: teams, then games, then players (no I/O).

    Args:
        espn_games: ``ESPN_GAMES_SCHEMA`` (``game_date`` = the US-Eastern date).
        espn_rosters: ``ESPN_ROSTERS_SCHEMA``: the athletes ESPN lists per game and team
            (NBA/WNBA game rosters, or the box-score athletes where that is all ESPN has).
        alt: :func:`read_season` / :func:`parse_season` output.
        min_confidence: Jaro-Winkler floor for a fuzzy player match.
        known_teams: Optional fixed pairs (``espn_team_id`` Int32, ``team_id`` Utf8), e.g.
            :func:`known_teams`. They are taken as given (``match_confidence`` 1.0);
            co-occurrence pairs only the teams they leave out, and a co-occurrence pair that
            contradicts one is dropped (counted in ``stats["teams_contradicting_known"]``).
            Co-occurrence alone cannot tell apart teams that share every game date, such as
            an FCS team with one FBS opponent or any team early in a season.

    Returns:
        ``{"teams", "games", "players", "stats"}``: ``teams`` (``espn_team_id``,
        ``team_id``, ``match_confidence``), ``games`` (``espn_game_id``, ``game_id``),
        ``players`` (``PLAYERS_SCHEMA``, one row per voted ESPN athlete per team) and a
        ``stats`` dict of counts for the match-rate report.
    """
    for frame, schema, name in (
        (espn_games, ESPN_GAMES_SCHEMA, "espn_games"),
        (espn_rosters, ESPN_ROSTERS_SCHEMA, "espn_rosters"),
    ):
        bad = {c: frame.schema.get(c) for c, t in schema.items() if frame.schema.get(c) != t}
        if bad:
            raise ValueError(f"{name}: columns {sorted(bad)} missing or not {schema}")
    alt_games = alt["games"].filter(pl.col("game_date").is_not_null())
    pairs = _pair_teams(
        espn_games,
        alt_games.select(
            "game_date",
            pl.col("home_team_id").alias("alt_home_team_id"),
            pl.col("away_team_id").alias("alt_away_team_id"),
        ),
        "alt",
    ).rename({"alt_team_id": "team_id"})
    contradicting = 0
    if known_teams is not None:
        known = known_teams.select(
            pl.col("espn_team_id").cast(pl.Int32),
            pl.col("team_id").cast(pl.Utf8),
            pl.lit(1.0).alias("match_confidence"),
        ).unique("espn_team_id", keep="first")
        clash = pl.col("espn_team_id").is_in(known["espn_team_id"].implode()) | pl.col("team_id").is_in(
            known["team_id"].implode()
        )
        contradicting = pairs.filter(clash).join(known, on=["espn_team_id", "team_id"], how="anti").height
        pairs = pl.concat([known, pairs.filter(~clash)])
    to_espn = pairs.select("team_id", "espn_team_id")

    def keyed(df: pl.DataFrame, home: str, away: str) -> pl.DataFrame:
        lo, hi = pl.min_horizontal(home, away), pl.max_horizontal(home, away)
        return df.with_columns(
            pl.concat_str([pl.col("game_date").cast(pl.Utf8), lo.cast(pl.Utf8), hi.cast(pl.Utf8)], separator="|").alias(
                "_key"
            )
        ).filter(pl.col("_key").is_not_null() & (pl.len().over("_key") == 1))

    e = keyed(espn_games, "home_espn_team_id", "away_espn_team_id")
    a = keyed(
        alt_games.join(
            to_espn.rename({"team_id": "home_team_id", "espn_team_id": "_h"}), on="home_team_id", how="left"
        ).join(to_espn.rename({"team_id": "away_team_id", "espn_team_id": "_a"}), on="away_team_id", how="left"),
        "_h",
        "_a",
    )
    games = e.select("_key", "espn_game_id").join(a.select("_key", "game_id"), on="_key").drop("_key")

    block = pl.concat_str([pl.col("espn_game_id"), pl.col("espn_team_id").cast(pl.Utf8)], separator="|")
    left = espn_rosters.join(games.select("espn_game_id"), on="espn_game_id").filter(
        pl.col("espn_athlete_id").is_not_null()
    )
    left = left.select(
        block.alias("block"),
        "espn_team_id",
        pl.col("espn_athlete_id").alias("id"),
        pl.Series("name_key", [normalize_name(v) for v in left["espn_full_name"].to_list()], dtype=pl.Utf8),
        pl.col("espn_jersey").alias("jersey"),
    )
    rosters = alt["game_rosters"].filter(pl.col("player_id").is_not_null())
    assert rosters.schema["team_id"] == to_espn.schema["team_id"], "team_id dtype mismatch"
    right = rosters.join(games, on="game_id").join(to_espn, on="team_id")
    right = right.select(
        block.alias("block"),
        pl.col("player_id").alias("id"),
        pl.Series("name_key", [normalize_name(v) for v in right["player_name"].to_list()], dtype=pl.Utf8),
        "jersey",
    )
    best = vote_block_matches(left, right, min_confidence).rename({"right_id": "player_id"})

    # Name / jersey / position as the provider showed them in the athlete's last game for that team.
    order = alt_games.select("game_id", "game_date")
    latest = (
        rosters.join(to_espn, on="team_id")
        .join(order, on="game_id", how="left")
        .sort("game_date", nulls_last=False)
        .unique(["player_id", "espn_team_id"], keep="last")
        .select("player_id", "espn_team_id", "player_name", "jersey", "position")
    )
    players = best.join(latest, on=["player_id", "espn_team_id"], how="left").select(list(PLAYERS_SCHEMA))
    stats = {
        "alt_games": alt_games.height,
        "espn_games": espn_games.height,
        "teams_paired": pairs.height,
        "teams_contradicting_known": contradicting,
        "games_matched": games.height,
        "espn_athletes": espn_rosters.join(games.select("espn_game_id"), on="espn_game_id")
        .select("espn_team_id", "espn_athlete_id")
        .unique()
        .height,
        "players_matched": players.height,
    }
    return {"teams": pairs, "games": games, "players": players, "stats": stats}


def fill_player_legs(player_xwalk: pl.DataFrame, players: pl.DataFrame, columns: Mapping[str, str]) -> pl.DataFrame:
    """Write matched provider ids into a player crosswalk where its provider id is null.

    Rows join on (``espn_team_id``, ``espn_athlete_id``). A row whose provider id
    (the first key of ``columns``) is already set keeps all its provider columns, so a
    fill never drops or overwrites an existing leg.

    Args:
        player_xwalk: The crosswalk; must hold ``espn_team_id``, ``espn_athlete_id``
            and every key of ``columns``.
        players: :func:`match_season` ``players``.
        columns: Crosswalk column -> ``players`` column, id column first (e.g.
            :data:`BASKETBALL_PLAYER_LEGS` ``["yahoo"]``).

    Returns:
        ``player_xwalk`` with the same columns, order and dtypes.
    """
    keys = ["espn_team_id", "espn_athlete_id"]
    id_col = next(iter(columns))
    new = players.select(
        *[pl.col(k).cast(player_xwalk.schema[k]) for k in keys],
        *[pl.col(src).cast(player_xwalk.schema[dst]).alias(f"_new_{dst}") for dst, src in columns.items()],
    ).unique(keys, keep="first")
    empty = pl.col(id_col).is_null()
    return (
        player_xwalk.join(new, on=keys, how="left", maintain_order="left")
        .with_columns([pl.when(empty).then(pl.col(f"_new_{dst}")).otherwise(pl.col(dst)).alias(dst) for dst in columns])
        .select(player_xwalk.columns)
    )


def fill_game_legs(
    schedule_xwalk: pl.DataFrame, games: pl.DataFrame, teams: pl.DataFrame, columns: Sequence[Optional[str]]
) -> pl.DataFrame:
    """Write matched provider game and team ids into a schedule crosswalk where its game id is null.

    Rows join on ``espn_game_id``; the home / away provider team ids come from ``teams``
    through ESPN's home / away team ids, so they follow ESPN's sides. A row whose
    provider game id (``columns[0]``) is already set keeps every provider column.

    Args:
        schedule_xwalk: The crosswalk; must hold ``espn_game_id``, ``home_espn_team_id``,
            ``away_espn_team_id`` and every named column of ``columns``.
        games: :func:`match_season` ``games``.
        teams: :func:`match_season` ``teams``.
        columns: (game id, home team id, away team id) crosswalk columns; ``None`` skips a
            team column (e.g. :data:`BASKETBALL_GAME_LEGS` ``["yahoo"]``).

    Returns:
        ``schedule_xwalk`` with the same columns, order and dtypes.
    """
    gid_col = columns[0]
    assert gid_col is not None, "columns[0] must name the game id column"
    out = schedule_xwalk.join(
        games.select(
            pl.col("espn_game_id").cast(schedule_xwalk.schema["espn_game_id"]), pl.col("game_id").alias("_gid")
        ).unique("espn_game_id", keep="first"),
        on="espn_game_id",
        how="left",
        maintain_order="left",
    )
    fills = {gid_col: "_gid"}
    for col, side in zip(columns[1:], ("home", "away")):
        if col is None:
            continue
        key = f"{side}_espn_team_id"
        side_teams = teams.select(pl.col("espn_team_id").alias(key), pl.col("team_id").alias(f"_{side}"))
        assert side_teams.schema[key] == out.schema[key], f"{key} dtype mismatch"
        out = out.join(side_teams, on=key, how="left", maintain_order="left")
        fills[col] = f"_{side}"
    empty = pl.col(gid_col).is_null() & pl.col("_gid").is_not_null()
    return out.with_columns(
        [
            pl.when(empty).then(pl.col(src).cast(schedule_xwalk.schema[dst])).otherwise(pl.col(dst)).alias(dst)
            for dst, src in fills.items()
        ]
    ).select(schedule_xwalk.columns)


def basketball_espn_side(espn_schedule: pl.DataFrame, espn_game_rosters: pl.DataFrame) -> tuple:
    """ESPN games + rosters for :func:`match_season` from the published basketball parquet."""
    from sportsdataverse._crosswalk_basketball_archive import _espn_games

    games = _espn_games(espn_schedule)
    rosters = espn_game_rosters.select(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_game_id"),
        pl.col("team_id").cast(pl.Int32).alias("espn_team_id"),
        pl.col("athlete_id").cast(pl.Int64).cast(pl.Utf8).alias("espn_athlete_id"),
        pl.col("athlete_display_name").cast(pl.Utf8).alias("espn_full_name"),
        pl.col("athlete_jersey").cast(pl.Utf8).alias("espn_jersey"),
    ).filter(pl.col("espn_team_id").is_not_null())
    return games, rosters


def with_alt_player_legs(
    league: str,
    season: int,
    player_xwalk: pl.DataFrame,
    espn_schedule: pl.DataFrame,
    espn_game_rosters: pl.DataFrame,
    alt_root: Union[str, Path],
    min_confidence: float = 0.92,
) -> pl.DataFrame:
    """Fill a basketball player crosswalk's Fox / Yahoo legs from the parsed ``*-alt-data`` season.

    Providers without a parsed season under ``alt_root`` are skipped. Existing ids are
    kept (:func:`fill_player_legs`).
    """
    games, rosters = basketball_espn_side(espn_schedule, espn_game_rosters)
    out = player_xwalk
    for provider, columns in BASKETBALL_PLAYER_LEGS.items():
        alt = read_season(alt_root, provider, league, season)
        if alt is None:
            continue
        m = match_season(games, rosters, alt, min_confidence)
        log.info("%s %s %s: %s", league, season, provider, m["stats"])
        out = fill_player_legs(out, m["players"], columns)
    return out


def with_alt_game_legs(
    league: str,
    season: int,
    schedule_xwalk: pl.DataFrame,
    espn_schedule: pl.DataFrame,
    espn_game_rosters: pl.DataFrame,
    alt_root: Union[str, Path],
) -> pl.DataFrame:
    """Fill a basketball schedule crosswalk's Fox / Yahoo game legs from the parsed ``*-alt-data`` season.

    The schedule twin of :func:`with_alt_player_legs`: providers without a parsed season
    under ``alt_root`` are skipped, and existing ids are kept (:func:`fill_game_legs`).
    """
    games, rosters = basketball_espn_side(espn_schedule, espn_game_rosters)
    out = schedule_xwalk
    for provider, columns in BASKETBALL_GAME_LEGS.items():
        alt = read_season(alt_root, provider, league, season)
        if alt is None:
            continue
        m = match_season(games, rosters, alt)
        log.info("%s %s %s games: %s", league, season, provider, m["stats"])
        out = fill_game_legs(out, m["games"], m["teams"], columns)
    return out


# --------------------------------------------------------------------------- CLI


def main(argv: Optional[Iterable[str]] = None) -> int:
    """``python -m sportsdataverse._crosswalk_alt`` -- parse one or more seasons into ``--out-root``."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--provider", required=True, choices=PROVIDERS)
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", type=int, nargs="+", required=True)
    ap.add_argument("--raw-root", required=True, help="the *-alt-raw checkout")
    ap.add_argument("--out-root", required=True, help="the *-alt-data checkout")
    a = ap.parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for season in a.season:
        frames = parse_season(a.provider, a.league, season, a.raw_root)
        if frames["games"].height == 0:
            log.error("%s %s %s: no games parsed under %s", a.provider, a.league, season, a.raw_root)
            return 1
        write_season(frames, a.out_root, a.provider, a.league, season)
        log.info(
            "%s %s %s: %d games, %d game-roster rows (%d players)",
            a.provider,
            a.league,
            season,
            frames["games"].height,
            frames["game_rosters"].height,
            frames["game_rosters"]["player_id"].n_unique(),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
