"""Capture the offline oracle corpus for the NBA draft/projection spine (T3.4).

Run with ``SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_draft/capture_corpus.py``
from a residential IP (stats.nba.com TLS/JA3-blocks datacenter IPs).

Confirmed wrapper surface (2026-07-08, via live capture):
    - nba_stats_draftcombineplayeranthro / draftcombinedrillresults /
      draftcombinespotshooting / draftcombinenonstationaryshooting exist.
      ``nba_stats_draftcombinestats`` returns 0 rows for NBA seasons in the
      capture sweep (WNBA is the one that relies on it, per the design doc's
      "reduced WNBA combine" note) -- NOT used for the NBA corpus.
    - nba_stats_drafthistory / draftboard are ABSENT from the generated NBA
      surface (dropped in the capture sweep) -- confirms design-doc decision
      #2: draft outcome comes from nba_stats_commonplayerinfo
      (CommonPlayerInfo.draft_year/draft_round/draft_number).
    - nba_stats_playercareerstats returns a multi-set dict; this script uses
      ``SeasonTotalsRegularSeason`` (season-level per-player totals).

This script writes two tiers of output:
    1. Six committed fixtures under tests/fixtures/nba_draft/ (Task 0.1 list).
    2. One extra *scratch* intermediate, ``season_stats_raw.parquet``, also
       written under tests/fixtures/nba_draft/ and committed alongside the six
       -- it is the per-(player_id, season) box-total corpus that
       ``dev/nba_draft/fit_box_value.py`` (Task 0.3) needs to compute
       per-100 rates and the final ``career_values.parquet`` /
       ``rookie_values.parquet`` outputs. Task 0.1 and Task 0.3 have a
       chicken-and-egg dependency in the plan's file list (career_values.parquet
       needs box_value_coef, which Task 0.3 fits FROM this same raw corpus) --
       resolved by capturing raw totals here and materializing the two
       *_values.parquet fixtures in the Task-0.3 fitting script instead.
"""

from __future__ import annotations

import time

import polars as pl

from sportsdataverse.nba.nba_bpm import nba_bpm
from sportsdataverse.nba.nba_box_logs import nba_box_logs
from sportsdataverse.nba.nba_player_positions import nba_player_positions
from sportsdataverse.nba.nba_stats import (
    nba_stats_commonplayerinfo,
    nba_stats_draftcombinedrillresults,
    nba_stats_draftcombinenonstationaryshooting,
    nba_stats_draftcombineplayeranthro,
    nba_stats_draftcombinespotshooting,
    nba_stats_playercareerstats,
)

FIXTURE_DIR = "tests/fixtures/nba_draft"
COMBINE_YEARS = ["2016", "2017", "2018", "2019"]
BPM_SEASONS = ["2016-17", "2017-18", "2018-19", "2019-20"]
SLEEP_S = 0.4  # low parallelism / gentle pace per CLAUDE.md scraping rules


def _pid_utf8(col: str = "player_id") -> pl.Expr:
    return pl.col(col).cast(pl.Int64).cast(pl.Utf8)


def capture_combine(years: list[str]) -> pl.DataFrame:
    frames = []
    for year in years:
        anthro = nba_stats_draftcombineplayeranthro(season_year=year)
        time.sleep(SLEEP_S)
        drills = nba_stats_draftcombinedrillresults(season_year=year)
        time.sleep(SLEEP_S)
        spot = nba_stats_draftcombinespotshooting(season_year=year)
        time.sleep(SLEEP_S)
        nonstat = nba_stats_draftcombinenonstationaryshooting(season_year=year)
        time.sleep(SLEEP_S)
        if anthro.is_empty():
            print(f"  [combine] {year}: no anthro rows, skipping")
            continue

        keep_anthro = [
            "player_id",
            "height_wo_shoes",
            "weight",
            "wingspan",
            "standing_reach",
            "body_fat_pct",
            "hand_length",
            "hand_width",
        ]
        a = anthro.select([c for c in keep_anthro if c in anthro.columns]).with_columns(_pid_utf8())
        d = (
            drills.select(
                [
                    c
                    for c in [
                        "player_id",
                        "lane_agility_time",
                        "three_quarter_sprint",
                        "standing_vertical_leap",
                        "max_vertical_leap",
                    ]
                    if c in drills.columns
                ]
            )
            .rename(
                {
                    "lane_agility_time": "lane_agility",
                    "standing_vertical_leap": "standing_vertical",
                    "max_vertical_leap": "max_vertical",
                }
            )
            .with_columns(_pid_utf8())
            if not drills.is_empty()
            else pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})
        )
        s = (
            spot.select([c for c in ["player_id", "fifteen_corner_left_pct"] if c in spot.columns])
            .rename({"fifteen_corner_left_pct": "spot_fifteen_corner_left_pct"})
            .with_columns(_pid_utf8())
            if not spot.is_empty()
            else pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})
        )
        n = (
            nonstat.select([c for c in ["player_id", "off_drib_fifteen_top_key_pct"] if c in nonstat.columns])
            .rename({"off_drib_fifteen_top_key_pct": "offdrib_fifteen_top_pct"})
            .with_columns(_pid_utf8())
            if not nonstat.is_empty()
            else pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8})
        )

        joined = a
        for other in (d, s, n):
            if other.height > 0:
                joined = joined.join(other, on="player_id", how="left")
        joined = joined.with_columns(pl.lit(int(year)).cast(pl.Int64).alias("draft_year"))
        frames.append(joined)
        print(f"  [combine] {year}: {joined.height} players")

    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return out


def capture_draft_outcomes(player_ids: list[str]) -> pl.DataFrame:
    rows = []
    for i, pid in enumerate(player_ids):
        try:
            info = nba_stats_commonplayerinfo(player_id=pid)
        except Exception as exc:  # pragma: no cover - live network guard
            print(f"  [commonplayerinfo] {pid}: ERROR {exc!r}")
            continue
        cpi = info.get("CommonPlayerInfo") if isinstance(info, dict) else None
        time.sleep(SLEEP_S)
        if cpi is None or cpi.is_empty():
            continue
        row = cpi.row(0, named=True)
        draft_number = row.get("draft_number")
        try:
            draft_number_int = int(draft_number) if draft_number not in (None, "Undrafted") else None
        except (TypeError, ValueError):
            draft_number_int = None
        try:
            draft_round_int = int(row.get("draft_round")) if row.get("draft_round") not in (None, "Undrafted") else None
        except (TypeError, ValueError):
            draft_round_int = None
        try:
            draft_year_int = int(row.get("draft_year")) if row.get("draft_year") else None
        except (TypeError, ValueError):
            draft_year_int = None
        rows.append(
            {
                "player_id": str(int(pid)),
                "draft_year": draft_year_int,
                "draft_round": draft_round_int,
                "draft_number": draft_number_int,
                "drafted": bool(draft_number_int is not None and draft_number_int > 0),
            }
        )
        if (i + 1) % 25 == 0:
            print(f"  [commonplayerinfo] {i + 1}/{len(player_ids)}")
    return pl.DataFrame(
        rows,
        schema={
            "player_id": pl.Utf8,
            "draft_year": pl.Int64,
            "draft_round": pl.Int64,
            "draft_number": pl.Int64,
            "drafted": pl.Boolean,
        },
    )


def capture_season_stats(player_ids: list[str]) -> pl.DataFrame:
    frames = []
    for i, pid in enumerate(player_ids):
        try:
            career = nba_stats_playercareerstats(player_id=pid)
        except Exception as exc:  # pragma: no cover - live network guard
            print(f"  [playercareerstats] {pid}: ERROR {exc!r}")
            continue
        time.sleep(SLEEP_S)
        seasons = career.get("SeasonTotalsRegularSeason") if isinstance(career, dict) else None
        if seasons is None or seasons.is_empty():
            continue
        frames.append(seasons.with_columns(_pid_utf8()))
        if (i + 1) % 25 == 0:
            print(f"  [playercareerstats] {i + 1}/{len(player_ids)}")
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def capture_bpm_overlap(combine_player_ids: set[str], seasons: list[str]) -> pl.DataFrame:
    frames = []
    for season in seasons:
        logs = nba_box_logs(season)
        time.sleep(SLEEP_S)
        pos = nba_player_positions(season)
        time.sleep(SLEEP_S)
        if logs["player"].is_empty():
            print(f"  [bpm] {season}: no logs")
            continue
        bpm = nba_bpm(logs["player"], logs["team"], pos)
        bpm = bpm.with_columns(_pid_utf8(), pl.lit(int(season[:4])).cast(pl.Int64).alias("season"))
        bpm = bpm.filter(pl.col("player_id").is_in(list(combine_player_ids)))
        bpm = bpm.rename({"min": "minutes"}).select("player_id", "season", "bpm", "minutes")
        frames.append(bpm)
        print(f"  [bpm] {season}: {bpm.height} combine-player rows")
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


# Hand-transcribed, order-of-magnitude published NBA aging pattern -- a
# rise-then-decline curve peaking at age 27, consistent with the broad shape
# documented across published aging-curve studies (Silver/Lichtman "delta
# method", Kevin Pelton's WARP aging curves, Basketball-Reference's aging
# research). Normalized so rel_value peaks at 1.0. See README for citation.
AGING_PUBLISHED = {
    20: 0.55,
    21: 0.66,
    22: 0.76,
    23: 0.85,
    24: 0.92,
    25: 0.97,
    26: 0.995,
    27: 1.00,
    28: 0.995,
    29: 0.97,
    30: 0.93,
    31: 0.87,
    32: 0.80,
    33: 0.72,
    34: 0.64,
    35: 0.56,
    36: 0.48,
    37: 0.41,
    38: 0.35,
}


def main() -> None:
    print("Capturing combine classes 2016-2019 ...")
    combine = capture_combine(COMBINE_YEARS)
    combine.write_parquet(f"{FIXTURE_DIR}/combine_2016_2019.parquet")
    print(f"  wrote combine_2016_2019.parquet ({combine.height} rows)")

    player_ids = combine["player_id"].unique().to_list()

    print("Capturing draft outcomes (commonplayerinfo) ...")
    outcomes = capture_draft_outcomes(player_ids)
    outcomes.write_parquet(f"{FIXTURE_DIR}/draft_outcomes.parquet")
    print(f"  wrote draft_outcomes.parquet ({outcomes.height} rows)")

    print("Capturing season totals (playercareerstats) ...")
    season_stats = capture_season_stats(player_ids)
    season_stats.write_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    print(f"  wrote season_stats_raw.parquet ({season_stats.height} rows)")

    print("Capturing nba_bpm overlap (2016-17 .. 2019-20, combine players only) ...")
    bpm_overlap = capture_bpm_overlap(set(player_ids), BPM_SEASONS)
    bpm_overlap.write_parquet(f"{FIXTURE_DIR}/nba_bpm_overlap.parquet")
    print(f"  wrote nba_bpm_overlap.parquet ({bpm_overlap.height} rows)")

    print("Writing hand-transcribed published aging curve ...")
    aging = pl.DataFrame({"age": list(AGING_PUBLISHED.keys()), "rel_value": list(AGING_PUBLISHED.values())}).cast(
        {"age": pl.Int64, "rel_value": pl.Float64}
    )
    aging.write_parquet(f"{FIXTURE_DIR}/aging_published.parquet")
    print(f"  wrote aging_published.parquet ({aging.height} rows)")

    print("Done. career_values.parquet + rookie_values.parquet are materialized")
    print("by dev/nba_draft/fit_box_value.py (Task 0.3) from season_stats_raw.parquet.")


if __name__ == "__main__":
    main()
