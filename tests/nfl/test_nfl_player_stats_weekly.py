"""`load_nfl_player_stats` reads the nflverse `stats_player` weekly release.

nflverse froze `player_stats/player_stats.parquet` in 2025-05 (it ends at season
2024) and publishes `stats_player/stats_player_week_{season}.parquet` instead —
a 150-column superset with four renames, one sign flip and one dropped column.
These tests lock the reconciliation back to the legacy contract, which is what
every downstream consumer (sdv-db's `nfl.player_stats`, Game on Paper's player
routes) selects against.

The offline tests read a committed slice of the real release
(`tests/fixtures/nfl_player_stats/`), not a synthetic frame.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_loaders
from sportsdataverse.nfl.nfl_loaders import (
    _PLAYER_STATS_KICKING_SCHEMA,
    _PLAYER_STATS_SCHEMA,
    _player_stats_to_legacy,
    load_nfl_player_stats,
)
from tests.conftest import skip_if_no_live

FIXTURE = Path(__file__).parent.parent / "fixtures" / "nfl_player_stats" / "stats_player_week_2025_slice.parquet"


@pytest.fixture(autouse=True)
def _no_cache():
    """`SDV_PY_NFL_CACHE=filesystem` in a contributor's env would serve a stale
    frame and make the monkeypatched-fetch assertions vacuously pass."""
    from sportsdataverse.nfl import get_config, reset_config, update_config

    before = get_config().cache_mode
    update_config(cache_mode="off")
    yield
    reset_config()
    update_config(cache_mode=before)


@pytest.fixture(scope="module")
def raw() -> pl.DataFrame:
    return pl.read_parquet(FIXTURE)


def test_fixture_is_the_upstream_shape(raw: pl.DataFrame) -> None:
    """Guard the premise: the slice is the un-reconciled weekly release."""
    assert "team" in raw.columns and "recent_team" not in raw.columns
    assert "sack_yards_lost" in raw.columns and "sack_yards" not in raw.columns
    assert "passing_interceptions" in raw.columns and "sacks_suffered" in raw.columns


def test_offense_output_is_exactly_the_legacy_schema(raw: pl.DataFrame) -> None:
    out = _player_stats_to_legacy(raw, kicking=False)
    assert out.columns == list(_PLAYER_STATS_SCHEMA)
    assert out.schema == pl.Schema(_PLAYER_STATS_SCHEMA)


def test_kicking_output_is_exactly_the_legacy_kicking_schema(raw: pl.DataFrame) -> None:
    out = _player_stats_to_legacy(raw, kicking=True)
    assert out.columns == list(_PLAYER_STATS_KICKING_SCHEMA)
    assert out.schema == pl.Schema(_PLAYER_STATS_KICKING_SCHEMA)
    # `team` is the legacy kicking frame's spelling — NOT renamed to recent_team.
    assert "recent_team" not in out.columns
    assert out.height == raw.filter(pl.col("fg_att").fill_null(0) > 0).height


def test_sack_yards_is_negated_not_renamed(raw: pl.DataFrame) -> None:
    """The one silent-corruption risk: upstream ships yards lost as NEGATIVE."""
    out = _player_stats_to_legacy(raw, kicking=False)
    joined = out.join(raw.select("player_id", "week", "sack_yards_lost"), on=["player_id", "week"], how="inner").filter(
        pl.col("sack_yards_lost") != 0
    )
    assert joined.height > 0, "fixture must carry sacked-QB rows"
    assert (joined["sack_yards"] == -joined["sack_yards_lost"]).all()
    assert (out["sack_yards"].fill_null(0) >= 0).all()


def test_renamed_columns_carry_the_upstream_values(raw: pl.DataFrame) -> None:
    out = _player_stats_to_legacy(raw, kicking=False).sort("player_id", "week")
    src = raw.filter(pl.col("player_id").is_in(out["player_id"].implode())).sort("player_id", "week")
    assert out["recent_team"].to_list() == src["team"].to_list()
    assert out["interceptions"].to_list() == src["passing_interceptions"].cast(pl.Float64).to_list()
    assert out["sacks"].to_list() == src["sacks_suffered"].cast(pl.Float64).to_list()


def test_dakota_survives_as_an_all_null_column(raw: pl.DataFrame) -> None:
    """Upstream dropped it; the column stays so the output schema never moves."""
    assert "dakota" not in raw.columns
    out = _player_stats_to_legacy(raw, kicking=False)
    assert out["dakota"].null_count() == out.height


def test_defense_only_rows_are_dropped(raw: pl.DataFrame) -> None:
    """They have no column to land in under the legacy contract."""
    out = _player_stats_to_legacy(raw, kicking=False)
    defenders = raw.filter(pl.col("position").is_in(["DE", "OLB", "LB"]))["player_id"]
    assert defenders.len() > 0, "fixture must carry defense-only rows"
    assert out.filter(pl.col("player_id").is_in(defenders.implode())).height == 0


def test_seasons_arg_requests_one_asset_per_season(monkeypatch, raw: pl.DataFrame) -> None:
    seen: list[str] = []

    def fake_fetch(url: str) -> pl.DataFrame:
        seen.append(url)
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        return raw.with_columns(pl.lit(season, dtype=pl.Int32).alias("season"))

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fake_fetch)
    out = load_nfl_player_stats(seasons=[2025, 2026])
    assert seen == [
        "https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_2025.parquet",
        "https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_2026.parquet",
    ]
    assert sorted(out["season"].unique().to_list()) == [2025, 2026]
    assert out.columns == list(_PLAYER_STATS_SCHEMA)


def test_seasons_none_still_loads_the_whole_history(monkeypatch, raw: pl.DataFrame) -> None:
    """Backwards compatibility: the no-argument call keeps meaning "everything"."""
    seen: list[int] = []

    def fake_fetch(url: str) -> pl.DataFrame:
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        seen.append(season)
        return raw.with_columns(pl.lit(season, dtype=pl.Int32).alias("season"))

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fake_fetch)
    load_nfl_player_stats()
    assert seen[0] == 1999
    assert seen[-1] >= 2025
    assert seen == sorted(seen)


def test_positional_kicking_flag_is_rejected_loudly() -> None:
    """`seasons` took the first positional slot — a silent reinterpretation would lie."""
    with pytest.raises(TypeError, match="kicking"):
        load_nfl_player_stats(True)


def test_season_below_1999_raises() -> None:
    from sportsdataverse.errors import SeasonNotFoundError

    with pytest.raises(SeasonNotFoundError):
        load_nfl_player_stats(seasons=[1998])


@skip_if_no_live
def test_live_weekly_release_covers_the_frozen_files_dead_zone() -> None:
    """2025 and 2026 exist ONLY in the weekly release — this is the bug being fixed."""
    out = load_nfl_player_stats(seasons=[2025])
    assert out.height > 0
    assert out.columns == list(_PLAYER_STATS_SCHEMA)
    assert out.schema == pl.Schema(_PLAYER_STATS_SCHEMA)
    assert out["season"].unique().to_list() == [2025]
    assert set(out["season_type"].unique().to_list()) <= {"REG", "POST"}
    assert (out["sack_yards"].fill_null(0) >= 0).all()

    kick = load_nfl_player_stats(seasons=[2025], kicking=True)
    assert kick.height > 0
    assert kick.columns == list(_PLAYER_STATS_KICKING_SCHEMA)
