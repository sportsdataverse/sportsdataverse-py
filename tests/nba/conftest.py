"""Shared fixtures for the NBA test suite."""

from pathlib import Path

import polars as pl
import pytest

_SHOT_VALUE_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_shot_value"
_PLAYTYPE_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_playtype"


@pytest.fixture(scope="session")
def shot_value_corpus() -> "dict[str, pl.DataFrame]":
    """The committed 2022-23 shot-value oracle corpus (see the fixtures README).

    Returns a dict of the four frames: ``shots`` (per-shot Shot_Chart_Detail),
    ``league_avgs`` (LeagueAverages zone table), ``ptshots`` (stacked
    defender/shot-clock buckets), ``ptdefend`` (shot-defend rows).
    """
    return {
        "shots": pl.read_parquet(_SHOT_VALUE_FIX / "shotchart_2023.parquet"),
        "league_avgs": pl.read_parquet(_SHOT_VALUE_FIX / "league_averages_2023.parquet"),
        "ptshots": pl.read_parquet(_SHOT_VALUE_FIX / "playerdashptshots_sample.parquet"),
        "ptdefend": pl.read_parquet(_SHOT_VALUE_FIX / "playerdashptshotdefend_sample.parquet"),
    }


@pytest.fixture(scope="session")
def playtype_corpus() -> "dict[str, pl.DataFrame]":
    """The committed 2023-24 play-type/impact oracle corpus (see the fixtures README).

    Returns a dict of the nine frames captured live from ``stats.nba.com``:
    ``synergy_off_team``/``synergy_def_team``/``synergy_off_player``,
    ``matchups``, ``leaguedash_base``/``leaguedash_adv``, ``gamelog``,
    ``rapm`` (shipped stint-RAPM snapshot over the FULL 1230-game 2023-24
    season -- see the fixtures README's "Model (2) construct-gap finding"),
    and ``team_off_rating`` (independent ORTG ground truth for the
    rank-sanity gate).
    """
    return {
        "synergy_off_team": pl.read_parquet(_PLAYTYPE_FIX / "synergy_off_team_2024.parquet"),
        "synergy_def_team": pl.read_parquet(_PLAYTYPE_FIX / "synergy_def_team_2024.parquet"),
        "synergy_off_player": pl.read_parquet(_PLAYTYPE_FIX / "synergy_off_player_2024.parquet"),
        "matchups": pl.read_parquet(_PLAYTYPE_FIX / "matchups_2024.parquet"),
        "leaguedash_base": pl.read_parquet(_PLAYTYPE_FIX / "leaguedash_base_2024.parquet"),
        "leaguedash_adv": pl.read_parquet(_PLAYTYPE_FIX / "leaguedash_adv_2024.parquet"),
        "gamelog": pl.read_parquet(_PLAYTYPE_FIX / "gamelog_2024.parquet"),
        "rapm": pl.read_parquet(_PLAYTYPE_FIX / "rapm_2024.parquet"),
        "team_off_rating": pl.read_parquet(_PLAYTYPE_FIX / "team_off_rating_2024.parquet"),
    }
