from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "nhl_microstat"


@pytest.fixture(scope="session")
def oracle_pbp() -> pl.DataFrame:
    """The committed 2023-24 pbp slice (Task 0.1) used by every oracle gate."""
    return pl.read_parquet(FIXTURES_DIR / "pbp_2024_slice.parquet")


@pytest.fixture(scope="session")
def oracle_edge_skaters() -> pl.DataFrame:
    """The committed EDGE skater detail sample (Task 0.1)."""
    return pl.read_parquet(FIXTURES_DIR / "edge_skater_detail_sample.parquet")


_PLAYER_ID_COLS = (
    "winning_player_id",
    "losing_player_id",
    "scoring_player_id",
    "assist1_player_id",
    "assist2_player_id",
    "shooting_player_id",
    "committed_player_id",
    "drawn_player_id",
)


def games_appeared(pbp: pl.DataFrame) -> pl.DataFrame:
    """One row per (player_id, game_id) a player appears in ANY event role.

    Rare-event stability oracles (penalties, zone entries) need a per-game
    rate with an INDEPENDENT denominator -- games played, not the event
    count itself. Correlating raw odd/even half event *counts* after
    filtering on their total induces a spurious conditioning-on-the-sum
    negative correlation; dividing by games-appeared removes it. Games
    played is unioned from every player-id column because a player who drew
    zero penalties in a game still played it.
    """
    frames = [pbp.select(pl.col(c).alias("player_id"), "game_id") for c in _PLAYER_ID_COLS if c in pbp.columns]
    return pl.concat(frames, how="vertical_relaxed").filter(pl.col("player_id").is_not_null()).unique()
