"""Capture the MLB game-state oracle corpus (T6.4, Task 0.1).

Run (from repo root, network required):

    SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_game_state/capture_oracle.py

Produces (committed to ``tests/fixtures/mlb_game_state/``):
  - pbp_corpus.parquet          statsapi play-by-play, April-June windows of
                                 each season 1999-2002 (stratified across the
                                 whole span the Tango RE24 table covers, to
                                 average out year-to-year offense drift)
  - re24_tango_book.parquet     published Tango/*The Book* RE24 (1999-2002 era)
  - winprob_game.parquet        statsapi win-probability for one era-matched
                                 game already inside pbp_corpus (WE_GAME_PK)
  - results_corpus.parquet      game-level final scores for the same windows
  - savant_called_pitches.parquet  Savant called-pitch sample + real per-game
                                 home-plate umpire id (joined via game_pk)

Wrapper functions used (confirmed present in sportsdataverse/mlb/):
  mlb_schedule (mlb_api_extra), mlb_play_by_play / mlb_win_probability /
  mlb_boxscore (mlb_api), mlb_statcast_search (mlb_statcast_extra).

Deviations from the plan's literal column contract, discovered against REAL
captures (never invented — see fixture README for the full writeup):
  - statsapi ``winProbability`` ships ``leverageIndex`` and ``atBatIndex`` as
    TOP-LEVEL fields, not nested under ``contextMetrics`` (which is `{}` in
    every observed row). The fixture column is ``leverage_index``, not
    ``context_metrics_leverage_index``.
  - Savant's ``umpire`` CSV column is unpopulated in every sampled window
    (a known-dead Savant field). Real per-game home-plate umpire identity
    instead comes from ``mlb_boxscore(game_pk, return_parsed=False)``'s
    ``officials`` list (``officialType == "Home Plate"``), joined onto the
    Statcast sample by the ``game_pk`` both surfaces share.
  - The WE/WPA/LI oracle game (``winprob_game.parquet``) MUST come from the
    same 1999-2002 era as ``pbp_corpus`` -- an earlier draft reused a modern
    game (745282, already committed under ``tests/fixtures/mlb_api/``) for
    convenience, but a 1999-2002-built WE table applied to a 2024 game
    showed a real, reproducible ~0.09-0.13 systematic gap in several
    mid-game states (home team down by 2 in the 5th, etc.) -- cross-era
    comparison, not a model bug. statsapi's ``winProbability`` endpoint does
    carry data for 1999-2002 games (confirmed live), so the fix is simply to
    pick the oracle game from inside ``pbp_corpus`` instead of outside it.
"""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl

from sportsdataverse.mlb.mlb_api import mlb_boxscore, mlb_play_by_play, mlb_win_probability
from sportsdataverse.mlb.mlb_api_extra import mlb_schedule
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "mlb_game_state"

# RE24-era capture window(s): the published Tango/*The Book* RE24 table is
# averaged over MLB seasons 1999-2002. A single season is NOT era-neutral --
# runs/game varied materially within that span (2000 was the highest-offense
# year of the four, ~5.1 R/G, vs ~4.6 R/G in 2002), and a first capture pass
# using April-June 2000 alone showed a *systematic* upward bias (nearly every
# state above Tango, largest in run-scoring-heavy states), not just sampling
# noise from small n (the per-game runs_on_play sum was verified exact against
# results_corpus, ruling out a reconstruction bug). Stratifying one season per
# April fixed the anchor (bases-empty/0-out landed at 0.563, inside [0.45,
# 0.58]) but left 4 of 24 states (all runner-on-third states: "_23"/0, "__3"/0,
# "123"/0, "123"/1 -- structurally rare in real baseball) still over the 0.05
# per-state tolerance at n=238-1069, with NO consistent sign (2 states ran high,
# 2 ran low) -- sampling noise from cell size, not a directional bug. Widened
# April-June (3x the plate appearances) to shrink that noise roughly by sqrt(3).
PBP_WINDOWS = [
    (1999, "1999-04-05", "1999-06-30"),
    (2000, "2000-04-03", "2000-06-30"),
    (2001, "2001-04-02", "2001-06-30"),
    (2002, "2002-04-01", "2002-06-30"),
]

# WE/WPA/LI oracle game: era-matched (2001-06-21, inside the 2001 PBP_WINDOWS
# entry above), so the empirical WE table and the statsapi oracle it's
# compared against are drawn from the same run environment.
WE_GAME_PK = 7746

# Savant called-pitch sample window for the umpire zone model. Widened from
# an initial 1-week pass (12,920 pitches) after the calibration gate showed
# several borderline-probability deciles with n=100-145 -- inherently noisy
# for a 0.03-gap floor (sampling SE ~ sqrt(p(1-p)/n) ~ 0.04-0.05 at that n,
# ABOVE the floor regardless of model quality). A 4-week window roughly
# quadruples per-decile n in the sparse borderline bins.
SAVANT_START, SAVANT_END = "2023-06-01", "2023-06-28"

# Published RE24 matrix (Tango, Lichtman, Dolphin, "The Book", 2007) computed
# over MLB seasons 1999-2002 -- the canonical table cited across the
# sabermetric literature (FanGraphs, Baseball-Reference "run expectancy"
# glossary entry, the Wikipedia "Run expectancy" article's sourced table).
TANGO_RE24 = [
    ("___", 0, 0.555),
    ("___", 1, 0.297),
    ("___", 2, 0.117),
    ("1__", 0, 0.953),
    ("1__", 1, 0.573),
    ("1__", 2, 0.251),
    ("_2_", 0, 1.189),
    ("_2_", 1, 0.725),
    ("_2_", 2, 0.344),
    ("__3", 0, 1.482),
    ("__3", 1, 0.983),
    ("__3", 2, 0.387),
    ("12_", 0, 1.573),
    ("12_", 1, 0.971),
    ("12_", 2, 0.466),
    ("1_3", 0, 1.904),
    ("1_3", 1, 1.243),
    ("1_3", 2, 0.538),
    ("_23", 0, 2.052),
    ("_23", 1, 1.467),
    ("_23", 2, 0.634),
    ("123", 0, 2.417),
    ("123", 1, 1.650),
    ("123", 2, 0.815),
]


def _completed_games(start_date: str, end_date: str, season: int) -> list[dict]:
    raw = mlb_schedule(start_date=start_date, end_date=end_date, season=season, game_type="R")
    games = [g for d in (raw.get("dates") or []) for g in (d.get("games") or [])]
    return [g for g in games if (g.get("status") or {}).get("codedGameState") == "F"]


def _game_row(g: dict, *, default_season: int) -> dict:
    home, away = g.get("teams", {}).get("home", {}), g.get("teams", {}).get("away", {})
    return {
        "game_id": str(int(g["gamePk"])),
        "season": int(g.get("season") or default_season),
        "date": g.get("officialDate") or g.get("gameDate", "")[:10],
        "home_team_id": str(int(home.get("team", {}).get("id"))),
        "away_team_id": str(int(away.get("team", {}).get("id"))),
        "home_score": int(home.get("score", 0)),
        "away_score": int(away.get("score", 0)),
    }


def collect_pbp(game_pks: list[int], *, sleep: float = 0.05) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for i, pk in enumerate(game_pks):
        df = mlb_play_by_play(int(pk), return_parsed=True)
        if isinstance(df, pl.DataFrame) and df.height:
            frames.append(df.with_columns(pl.lit(str(int(pk))).alias("game_id")))
        if sleep:
            time.sleep(sleep)
        if (i + 1) % 25 == 0:
            print(f"  ... pbp {i + 1}/{len(game_pks)} games fetched", flush=True)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def build_pbp_and_results() -> tuple[pl.DataFrame, pl.DataFrame]:
    all_games: list[dict] = []
    all_seasons: list[int] = []
    for season, start, end in PBP_WINDOWS:
        print(f"Fetching schedule {start}..{end} (season={season}) ...")
        games = _completed_games(start, end, season)
        print(f"  {len(games)} completed regular-season games")
        all_games.extend(games)
        all_seasons.extend([season] * len(games))

    game_pks = [int(g["gamePk"]) for g in all_games]
    print(f"Fetching play-by-play for {len(game_pks)} games total across {len(PBP_WINDOWS)} seasons ...")
    pbp = collect_pbp(game_pks)
    results_rows = [_game_row(g, default_season=season) for g, season in zip(all_games, all_seasons)]

    results = pl.DataFrame(results_rows).with_columns(pl.col("date").str.to_date())
    return pbp, results


def build_winprob_game() -> pl.DataFrame:
    """Statsapi win-probability for WE_GAME_PK (era-matched to pbp_corpus)."""
    wp = mlb_win_probability(WE_GAME_PK, return_parsed=False)
    rows = [
        {
            # "at_bat_index" (not "about_at_bat_index") to match the join key
            # pbp_base_out_states()/mlb_win_expectancy() output uses.
            "at_bat_index": p.get("about", {}).get("atBatIndex"),
            "home_team_win_probability": p.get("homeTeamWinProbability"),
            "home_team_win_probability_added": p.get("homeTeamWinProbabilityAdded"),
            "leverage_index": p.get("leverageIndex"),
        }
        for p in wp
    ]
    return pl.DataFrame(rows).with_columns(
        pl.lit(str(WE_GAME_PK)).alias("game_id"),
        pl.col("at_bat_index").cast(pl.Int64),
        pl.col("home_team_win_probability").cast(pl.Float64),
        pl.col("home_team_win_probability_added").cast(pl.Float64),
        pl.col("leverage_index").cast(pl.Float64),
    )


def build_savant_called_pitches() -> pl.DataFrame:
    print(f"Fetching Savant called pitches {SAVANT_START}..{SAVANT_END} ...")
    df = mlb_statcast_search(SAVANT_START, SAVANT_END, pitch_result=["called_strike", "ball"])
    df = df.select("plate_x", "plate_z", "sz_top", "sz_bot", "description", "pitch_type", "game_pk").filter(
        pl.col("plate_x").is_not_null() & pl.col("plate_z").is_not_null()
    )
    game_pks = df["game_pk"].unique().drop_nulls().cast(pl.Int64).to_list()
    print(f"  {df.height} called pitches across {len(game_pks)} games; fetching HP umpires ...")
    ump_rows = []
    for pk in game_pks:
        raw = mlb_boxscore(int(pk), return_parsed=False)
        officials = raw.get("officials") or []
        hp = next((o for o in officials if o.get("officialType") == "Home Plate"), None)
        if hp:
            ump_rows.append({"game_pk": pk, "umpire_id": str(int(hp["official"]["id"]))})
        time.sleep(0.1)
    umps = pl.DataFrame(ump_rows, schema={"game_pk": pl.Int64, "umpire_id": pl.Utf8})
    assert df.schema["game_pk"] == umps.schema["game_pk"]
    out = df.with_columns(pl.col("game_pk").cast(pl.Int64)).join(umps, on="game_pk", how="inner")
    return out.select("plate_x", "plate_z", "sz_top", "sz_bot", "description", "pitch_type", "umpire_id")


#: Columns pbp_base_out_states() actually reads. The raw mlb_play_by_play
#: frame carries ~50 columns (playEvents, credits, review details, hot/cold
#: zone blobs, ...); at 4700+ games that raw frame is ~46MB, comfortably over
#: the repo's 10MB check-added-large-files guard. Trimming to just what the
#: game-state spine consumes drops it to <1MB with zero loss of test signal.
_PBP_KEEP_COLUMNS = [
    "game_id",
    "about_inning",
    "about_half_inning",
    "about_at_bat_index",
    "count_outs",
    "result_home_score",
    "result_away_score",
    "matchup_post_on_first_id",
    "matchup_post_on_second_id",
    "matchup_post_on_third_id",
]


def main() -> None:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    pbp, results = build_pbp_and_results()
    pbp.select(_PBP_KEEP_COLUMNS).write_parquet(FIXTURE_DIR / "pbp_corpus.parquet")
    results.write_parquet(FIXTURE_DIR / "results_corpus.parquet")
    print(f"pbp_corpus: {pbp.height} rows, {pbp['game_id'].n_unique()} games")
    print(f"results_corpus: {results.height} rows")

    tango = pl.DataFrame(TANGO_RE24, schema=["base_state", "outs", "re"], orient="row").with_columns(
        pl.col("outs").cast(pl.Int64), pl.col("re").cast(pl.Float64)
    )
    tango.write_parquet(FIXTURE_DIR / "re24_tango_book.parquet")
    print(f"re24_tango_book: {tango.height} rows")

    winprob = build_winprob_game()
    winprob.write_parquet(FIXTURE_DIR / "winprob_game.parquet")
    print(f"winprob_game: {winprob.height} rows")

    savant = build_savant_called_pitches()
    savant.write_parquet(FIXTURE_DIR / "savant_called_pitches.parquet")
    print(f"savant_called_pitches: {savant.height} rows")

    print("Done.")


if __name__ == "__main__":
    main()
