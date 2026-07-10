"""Scratch (not committed to the wheel) capture script for the NHL prediction-spine
2023 (2022-23 season) oracle/backtest corpus -- Task 0.1.

Run once, offline afterwards:

    SDV_PY_LIVE_TESTS=1 uv run python dev/nhl_prediction/capture_oracle.py

Writes fixtures into tests/fixtures/nhl_prediction/. See the committed
README.md in that directory for full provenance + the documented gaps
found during this capture (ESPN predictor/probabilities are permanently
unsupported for the NHL league at the API level; propbets + power-index
returned zero rows for every date/season tried).

``team_xg_2023.parquet`` is intentionally NOT produced here -- it is the
*output* of Task 1.1's ``team_game_xg_rates`` and is captured once that
function exists (see ``dev/nhl_prediction/build_team_xg_fixture.py``),
to avoid duplicating the aggregation logic in two places.
"""

from __future__ import annotations

import io
import csv

import polars as pl
import requests

import sportsdataverse.nhl as nhl

FIXTURES_DIR = "tests/fixtures/nhl_prediction"
SEASON = 2023  # sdv-py / MoneyPuck convention: 2023 == the 2022-23 season

# ESPN uses shorter abbreviations than the NHL feed for four franchises.
ESPN_TO_NHL_ABBR = {"LA": "LAK", "NJ": "NJD", "SJ": "SJS", "TB": "TBL"}


def _norm_abbr(a: str) -> str:
    return ESPN_TO_NHL_ABBR.get(a, a)


def capture_moneypuck_teams() -> pl.DataFrame:
    """Fetch MoneyPuck's teams.csv (5on5 situation) and normalise to per-game rates.

    MoneyPuck's plain ``requests`` default User-Agent gets served a data-license
    notice instead of the CSV; a realistic browser User-Agent + Referer resolves
    the same public snapshot without needing a license agreement. This is a
    single one-time fetch (the plan's "fetch once, commit the snapshot" rule).
    """
    url = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2022/regular/teams.csv"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/csv,*/*",
        "Referer": "https://moneypuck.com/data.htm",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = [row for row in reader if row["situation"] == "5on5"]
    assert len(rows) == 32, f"expected 32 teams, got {len(rows)}"
    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.col("team").cast(pl.Utf8),
        pl.col("games_played").cast(pl.Int64),
        pl.col("xGoalsFor").cast(pl.Float64),
        pl.col("xGoalsAgainst").cast(pl.Float64),
        pl.col("goalsFor").cast(pl.Int64),
        pl.col("goalsAgainst").cast(pl.Int64),
    ).with_columns(
        (pl.col("xGoalsFor") / pl.col("games_played")).alias("xgf"),
        (pl.col("xGoalsAgainst") / pl.col("games_played")).alias("xga"),
    )
    df = df.with_columns((pl.col("xgf") - pl.col("xga")).alias("xg_diff"))
    return df.select(
        pl.col("team"),
        pl.col("xgf"),
        pl.col("xga"),
        pl.col("xg_diff"),
        pl.col("goalsFor").alias("gf"),
        pl.col("goalsAgainst").alias("ga"),
    )


def capture_results(pbp: pl.DataFrame | None = None) -> pl.DataFrame:
    """Regular-season, completed 2022-23 games from ``load_nhl_schedules``.

    ``game_state`` is ``"OFF"`` for completed regular-season games in this
    loader's convention (not ``"FINAL"``, which only appears for preseason
    rows) -- confirmed against the live 2023 payload at capture time.

    ``home_goals``/``away_goals`` are derived by counting the pbp's own
    ``GOAL`` events per team, **never** from the schedule loader's
    ``home_score``/``away_score``. Those columns were found at grounding to
    be a placeholder constant for every ``load_nhl_schedule(s)`` season
    <= 2023 (e.g. every single 2022-23 game reporting the same "2-3" score;
    2021 reports a constant "5-2", 2022 a constant "6-3" -- confirmed fixed
    from the 2024 release onward). Documented in the fixtures README.
    """
    sched = nhl.load_nhl_schedules([SEASON])
    completed = sched.filter((pl.col("game_type") == "R") & (pl.col("game_state") != "FUT"))

    if pbp is None:
        pbp = nhl.load_nhl_pbp_full([SEASON])
    goals = (
        pbp.filter((pl.col("event_type") == "GOAL") & pl.col("game_id").is_not_null())
        .group_by(["game_id", "event_team_abbr"])
        .agg(pl.len().alias("goals"))
        .with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))
    )

    out = completed.select(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
        pl.lit(SEASON).cast(pl.Int64).alias("season"),
        pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d").alias("date"),
        pl.col("home_team_abbr").alias("home_team"),
        pl.col("away_team_abbr").alias("away_team"),
        pl.lit(False).alias("neutral_site"),
    )
    home_goals = goals.rename({"event_team_abbr": "home_team", "goals": "home_goals"})
    away_goals = goals.rename({"event_team_abbr": "away_team", "goals": "away_goals"})
    out = out.join(home_goals, on=["game_id", "home_team"], how="left").join(
        away_goals, on=["game_id", "away_team"], how="left"
    )
    out = out.with_columns(
        pl.col("home_goals").fill_null(0).cast(pl.Int64),
        pl.col("away_goals").fill_null(0).cast(pl.Int64),
    )
    return out.select("game_id", "season", "date", "home_team", "away_team", "home_goals", "away_goals", "neutral_site")


def capture_espn_power_index() -> pl.DataFrame:
    """ESPN NHL season power-index leaders -- confirmed empty (0 items) for
    every season tried (2021-2026) directly against the Core API at capture
    time; ESPN simply has not populated this endpoint for the NHL league.
    Committed as a documented zero-row fixture (see README) rather than
    fabricated.
    """
    schema = {"team": pl.Utf8, "power_index": pl.Float64, "rank": pl.Int64}
    try:
        raw = nhl.espn_nhl_season_powerindex_leaders(season=SEASON, return_parsed=False)
        if raw.get("count", 0) == 0:
            return pl.DataFrame(schema=schema)
    except Exception:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(schema=schema)


def capture_espn_predictor_sample() -> pl.DataFrame:
    """ESPN NHL game predictor -- confirmed permanently unsupported for the
    hockey/nhl league at the API level ("Predictor is not supported for
    [hockey/nhl]", HTTP 400) at capture time. Committed as a documented
    zero-row fixture; Task 2.3's win-prob gate is adapted to compare against
    a naive baseline instead of an ESPN predictor Brier score.
    """
    schema = {"game_id": pl.Utf8, "home_team": pl.Utf8, "away_team": pl.Utf8, "home_win_prob": pl.Float64}
    return pl.DataFrame(schema=schema)


def capture_espn_odds_and_propbets(results: pl.DataFrame, sample_dates: list[int]):
    """Sample ESPN odds (works for historical games) + propbets (confirmed
    404 for every 2022-23 game tried -- ESPN purges/never stores propbets
    for past games; committed zero-row per README) across a handful of dates.
    """
    odds_rows = []
    propbet_rows = []
    results_by_key = {
        (row["date"], row["home_team"], row["away_team"]): row["game_id"] for row in results.iter_rows(named=True)
    }
    for date in sample_dates:
        sb = nhl.espn_nhl_scoreboard(dates=date, return_parsed=False)
        for event in sb.get("events", []):
            event_id = event.get("id")
            comps = event.get("competitions", [])
            if not comps:
                continue
            competitors = comps[0].get("competitors", [])
            home_abbr = away_abbr = None
            for comp in competitors:
                abbr = _norm_abbr(comp.get("team", {}).get("abbreviation", ""))
                if comp.get("homeAway") == "home":
                    home_abbr = abbr
                else:
                    away_abbr = abbr
            game_date = event.get("date", "")[:10]
            import datetime as _dt

            try:
                d = _dt.date.fromisoformat(game_date)
            except ValueError:
                continue
            game_id = results_by_key.get((d, home_abbr, away_abbr))
            if game_id is None:
                continue
            try:
                odds_df = nhl.espn_nhl_game_odds(event_id=event_id, return_parsed=True)
            except Exception:
                odds_df = pl.DataFrame()
            if odds_df.height > 0:
                # The nested home_team_odds_close_spread_value / close_over_value
                # fields are confirmed null for every NHL game/provider tried at
                # capture time (the close_*/current_* nested odds structure is
                # simply not populated for hockey). The only reliably-populated
                # fields are the flat top-level ``spread`` (already signed
                # relative to the HOME team -- confirmed against home_team_odds_
                # favorite across several games: negative when home favored,
                # positive when away favored) and ``over_under`` (a "current"
                # total, not a verified closing total -- documented limitation).
                row = odds_df.row(0, named=True)
                close_spread = row.get("spread")
                close_total = row.get("over_under")
                if close_spread is not None or close_total is not None:
                    odds_rows.append(
                        {
                            "game_id": game_id,
                            "close_puck_line_home": close_spread,
                            "close_total": close_total,
                        }
                    )
            try:
                nhl.espn_nhl_game_propbets(event_id=event_id, return_parsed=True)
            except Exception:
                pass  # confirmed unavailable for historical NHL games -- documented gap
    odds_out = (
        pl.DataFrame(
            odds_rows,
            schema={"game_id": pl.Utf8, "close_puck_line_home": pl.Float64, "close_total": pl.Float64},
        )
        if odds_rows
        else pl.DataFrame(schema={"game_id": pl.Utf8, "close_puck_line_home": pl.Float64, "close_total": pl.Float64})
    )
    propbets_schema = {
        "game_id": pl.Utf8,
        "player_id": pl.Utf8,
        "stat": pl.Utf8,
        "line": pl.Float64,
    }
    propbets_out = (
        pl.DataFrame(propbet_rows, schema=propbets_schema) if propbet_rows else pl.DataFrame(schema=propbets_schema)
    )
    return odds_out, propbets_out


def capture_pbp_sample(full: pl.DataFrame, n_games: int = 5) -> pl.DataFrame:
    """A small (~5-game) real slice of ``load_nhl_pbp_full`` for offline,
    schema-faithful unit tests (not the season-scale oracle corpus).
    """
    early_ids = full.filter(pl.col("game_id").is_not_null())["game_id"].unique().sort().head(n_games).to_list()
    return full.filter(pl.col("game_id").is_in(early_ids))


def main() -> None:
    import os

    os.makedirs(FIXTURES_DIR, exist_ok=True)

    print("Capturing MoneyPuck teams.csv (5on5, 2022-23)...")
    mp = capture_moneypuck_teams()
    mp.write_parquet(f"{FIXTURES_DIR}/moneypuck_teams_2023.parquet")
    print(f"  -> {mp.shape}")

    print("Downloading full-season pbp (shared by results + pbp_sample)...")
    full_pbp = nhl.load_nhl_pbp_full([SEASON])

    print("Capturing results_2023 (goals derived from pbp GOAL events)...")
    results = capture_results(full_pbp)
    results.write_parquet(f"{FIXTURES_DIR}/results_2023.parquet")
    print(f"  -> {results.shape}")

    print("Capturing ESPN power-index (expected empty)...")
    power = capture_espn_power_index()
    power.write_parquet(f"{FIXTURES_DIR}/espn_power_2023.parquet")
    print(f"  -> {power.shape}")

    print("Capturing ESPN predictor sample (expected empty -- unsupported for NHL)...")
    predictor = capture_espn_predictor_sample()
    predictor.write_parquet(f"{FIXTURES_DIR}/espn_predictor_sample.parquet")
    print(f"  -> {predictor.shape}")

    print("Capturing ESPN odds + propbets sample across the season...")
    sample_dates = [20221020, 20221115, 20221220, 20230115, 20230210, 20230305, 20230401]
    odds, propbets = capture_espn_odds_and_propbets(results, sample_dates)
    odds.write_parquet(f"{FIXTURES_DIR}/espn_odds_sample.parquet")
    propbets.write_parquet(f"{FIXTURES_DIR}/espn_propbets_sample.parquet")
    print(f"  -> odds {odds.shape}, propbets {propbets.shape}")

    print("Capturing a small pbp sample (5 games)...")
    pbp_sample = capture_pbp_sample(full_pbp)
    pbp_sample.write_parquet(f"{FIXTURES_DIR}/pbp_sample_2023.parquet")
    print(f"  -> {pbp_sample.shape}")

    print(
        "Done. team_xg_2023.parquet is captured separately once nhl_team_ratings.team_game_xg_rates exists (Task 1.1)."
    )


if __name__ == "__main__":
    main()
