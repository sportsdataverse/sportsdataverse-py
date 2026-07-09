"""Scratch capture script for the NHL microstat oracle corpus (Task 0.1).

NOT part of the shipped package -- lives under ``dev/`` (gitignored except
for this file, which is force-added per plan so the fitting provenance ships
in-tree). Run once, offline afterwards:

    SDV_PY_LIVE_TESTS=1 uv run python dev/nhl_microstat/capture_corpus.py

Writes:
    tests/fixtures/nhl_microstat/pbp_2024_slice.parquet
    tests/fixtures/nhl_microstat/edge_skater_detail_sample.parquet

Column contract: see tests/fixtures/nhl_microstat/README.md.
"""

from __future__ import annotations

import os
import sys
import time

import polars as pl

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sportsdataverse.nhl.nhl_api_web import nhl_boxscore, nhl_web_pbp  # noqa: E402
from sportsdataverse.nhl.nhl_edge import (  # noqa: E402
    nhl_edge_skater_skating_distance_detail,
    nhl_edge_skater_skating_speed_detail,
    nhl_edge_skater_zone_time,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "tests", "fixtures", "nhl_microstat")

# 120 completed 2023-24 regular-season games (game_id = 2023020001..2023020120).
# Bumped from an initial 40-game slice: penalty/assist events are sparse
# enough per-player that a 40-game sample hit a "zero-sum" split-half
# artifact (players filtered to a narrow total-count band get spuriously
# anti-correlated halves purely because their two half-counts must sum to
# a similar small total) -- see dev/nhl_microstat notes in Task 2.2. More
# games widens the per-player total-count range enough for real skill
# signal to show through that artifact. A first attempt at 200 games with no
# request pacing silently degraded to near-empty data (api-web's _get()
# swallows a 429/timeout into `{}` with no exception -- see REQUEST_SLEEP_S).
GAME_IDS = [2023020001 + i for i in range(120)]

# A handful of games whose rosters seed the EDGE skater sample.
EDGE_ROSTER_GAME_IDS = [2023020001, 2023020010, 2023020020]
EDGE_SEASON = 2024  # -> "20232024"

# api-web's shared `_get()` returns `{}` on ANY failure (timeout, 429, 5xx)
# with no exception raised -- a rate-limit trip is invisible unless we check
# for and retry an empty result ourselves. Small inter-request delay keeps
# the sequential request rate polite; MIN_EXPECTED_PLAYS flags a
# suspiciously-empty pbp payload (a real completed game has ~250-400 plays)
# for one retry after a short backoff instead of silently accepting it.
REQUEST_SLEEP_S = 0.2
RETRY_BACKOFF_S = 2.0
MIN_EXPECTED_PLAYS = 100

PBP_RENAME = {
    "details_event_owner_team_id": "event_owner_team_id",
    "details_zone_code": "zone_code",
    "details_x_coord": "x_coord",
    "details_y_coord": "y_coord",
    "details_winning_player_id": "winning_player_id",
    "details_losing_player_id": "losing_player_id",
    "details_scoring_player_id": "scoring_player_id",
    "details_assist1_player_id": "assist1_player_id",
    "details_assist2_player_id": "assist2_player_id",
    "details_shooting_player_id": "shooting_player_id",
    "details_committed_by_player_id": "committed_player_id",
    "details_drawn_by_player_id": "drawn_player_id",
    "details_type_code": "penalty_type_code",
    "details_shot_type": "shot_type",
    "period_descriptor_number": "period",
}

PBP_ID_COLS = [
    "event_owner_team_id",
    "winning_player_id",
    "losing_player_id",
    "scoring_player_id",
    "assist1_player_id",
    "assist2_player_id",
    "shooting_player_id",
    "committed_player_id",
    "drawn_player_id",
]

PBP_CONTRACT = [
    "game_id",
    "season",
    "event_idx",
    "period",
    "time_in_period",
    "type_desc_key",
    "event_owner_team_id",
    "zone_code",
    "x_coord",
    "y_coord",
    "situation_code",
    "home_team_id",
    "home_team_defending_side",
    "winning_player_id",
    "losing_player_id",
    "scoring_player_id",
    "assist1_player_id",
    "assist2_player_id",
    "shooting_player_id",
    "committed_player_id",
    "drawn_player_id",
    "penalty_type_code",
    "shot_type",
]


def _capture_game_pbp(game_id: int) -> pl.DataFrame:
    df = nhl_web_pbp(game_id)
    if df.height < MIN_EXPECTED_PLAYS:
        time.sleep(RETRY_BACKOFF_S)
        df = nhl_web_pbp(game_id)
    if df.height == 0:
        print(f"WARNING: game {game_id} returned 0 plays after retry (likely rate-limited/blocked)")
        return pl.DataFrame(schema={c: pl.Utf8 for c in PBP_CONTRACT})
    box = nhl_boxscore(game_id, return_parsed=False)
    home_team_id = str((box or {}).get("homeTeam", {}).get("id", ""))
    df = df.rename({k: v for k, v in PBP_RENAME.items() if k in df.columns})
    for c in PBP_ID_COLS:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    df = df.with_columns(
        pl.lit(str(game_id)).alias("game_id"),
        pl.lit(int(str(game_id)[:4])).cast(pl.Int64).alias("season"),
        pl.int_range(pl.len()).alias("event_idx"),
        pl.lit(home_team_id).alias("home_team_id"),
    )
    for c in ["event_owner_team_id", *PBP_ID_COLS]:
        df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8))
    df = df.with_columns(pl.col("home_team_id").cast(pl.Utf8))
    missing = [c for c in PBP_CONTRACT if c not in df.columns]
    for c in missing:
        df = df.with_columns(pl.lit(None).alias(c))
    return df.select(PBP_CONTRACT)


def _edge_roster_ids() -> list[int]:
    ids: set[int] = set()
    for gid in EDGE_ROSTER_GAME_IDS:
        box = nhl_boxscore(gid, return_parsed=False)
        pbgs = (box or {}).get("playerByGameStats", {})
        for side in ("awayTeam", "homeTeam"):
            for grp in ("forwards", "defense"):
                for p in pbgs.get(side, {}).get(grp, []):
                    ids.add(int(p["playerId"]))
    return sorted(ids)


def _capture_edge_skater(player_id: int) -> dict | None:
    speed = nhl_edge_skater_skating_speed_detail(player_id, season=EDGE_SEASON, return_parsed=False)
    dist = nhl_edge_skater_skating_distance_detail(player_id, season=EDGE_SEASON, return_parsed=False)
    zone = nhl_edge_skater_zone_time(player_id, season=EDGE_SEASON, return_parsed=False)
    if not speed or not dist or not zone:
        return None
    speed_d = speed.get("skatingSpeedDetails", {})
    top_speed = (speed_d.get("maxSkatingSpeed") or {}).get("metric")
    bursts_20 = (speed_d.get("bursts20To22") or {}).get("value")
    dist_rows = dist.get("skatingDistanceDetails") or []
    dist_all = next((r for r in dist_rows if r.get("strengthCode") == "all"), None)
    distance_km = ((dist_all or {}).get("distanceTotal") or {}).get("metric")
    zone_rows = zone if isinstance(zone, list) else zone.get("zoneTimeDetails") or []
    zone_all = next((r for r in zone_rows if r.get("strengthCode") in ("all", "allsituations")), None)
    if zone_all is None and zone_rows:
        zone_all = zone_rows[0]
    if top_speed is None or distance_km is None or zone_all is None:
        return None
    return {
        "player_id": str(player_id),
        "season": EDGE_SEASON,
        "top_speed": float(top_speed),
        "distance_km": float(distance_km),
        "speed_bursts_20": float(bursts_20) if bursts_20 is not None else 0.0,
        "oz_time_pct": float(zone_all.get("offensiveZonePctg", 0.0)),
        "dz_time_pct": float(zone_all.get("defensiveZonePctg", 0.0)),
        "nz_time_pct": float(zone_all.get("neutralZonePctg", 0.0)),
    }


def main() -> None:
    os.makedirs(FIXTURES_DIR, exist_ok=True)

    frames = []
    for gid in GAME_IDS:
        try:
            frames.append(_capture_game_pbp(gid))
        except Exception as exc:  # noqa: BLE001 -- best-effort capture, skip a bad game
            print(f"skip game {gid}: {exc}")
        time.sleep(REQUEST_SLEEP_S)
    pbp = pl.concat(frames, how="vertical_relaxed")
    pbp_path = os.path.join(FIXTURES_DIR, "pbp_2024_slice.parquet")
    pbp.write_parquet(pbp_path)
    print(f"wrote {pbp_path}: {pbp.height} rows across {len(GAME_IDS)} games")

    skater_ids = _edge_roster_ids()
    print(f"seeded {len(skater_ids)} EDGE skater ids from rosters")
    rows = []
    for pid in skater_ids:
        try:
            row = _capture_edge_skater(pid)
        except Exception as exc:  # noqa: BLE001
            print(f"skip skater {pid}: {exc}")
            time.sleep(REQUEST_SLEEP_S)
            continue
        time.sleep(REQUEST_SLEEP_S)
        if row is not None:
            rows.append(row)
    edge = (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "season": pl.Int64,
                "top_speed": pl.Float64,
                "distance_km": pl.Float64,
                "speed_bursts_20": pl.Float64,
                "oz_time_pct": pl.Float64,
                "dz_time_pct": pl.Float64,
                "nz_time_pct": pl.Float64,
            }
        )
    )
    edge_path = os.path.join(FIXTURES_DIR, "edge_skater_detail_sample.parquet")
    edge.write_parquet(edge_path)
    print(f"wrote {edge_path}: {edge.height} skaters")


if __name__ == "__main__":
    main()
