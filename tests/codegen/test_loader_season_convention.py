"""Season-identity regression for the NBA/WNBA stats loaders (offline).

The Program V release assets are keyed by the season's END year while the public
``seasons`` argument stayed the START year, so the loaders carry a ``{season + 1}``
token. An off-by-one there silently mislabels every season -- a row-count or
non-empty assertion would not notice. These tests pin the exact asset year each
public season resolves to.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
from codegen import spec  # noqa: E402

REL = ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

# (loader, module, public season, asset basename it MUST resolve to).
# NBA: start year -> end-year asset (+1). WNBA seasons are single-calendar-year,
# so start == end and the asset year must equal the argument exactly.
CASES = [
    ("load_nba_stats_schedules", "nba", 1996, "nba_schedule_1997.parquet"),
    ("load_nba_stats_schedules", "nba", 2025, "nba_schedule_2026.parquet"),
    ("load_nba_stats_pbp", "nba", 1996, "nba_play_by_play_1997.parquet"),
    ("load_nba_stats_pbp", "nba", 2025, "nba_play_by_play_2026.parquet"),
    ("load_nba_stats_possessions", "nba", 1996, "nba_possessions_1997.parquet"),
    ("load_nba_stats_possessions", "nba", 2025, "nba_possessions_2026.parquet"),
    ("load_nba_stats_game_lineups", "nba", 1996, "nba_lineups_1997.parquet"),
    ("load_nba_stats_game_lineups", "nba", 2025, "nba_lineups_2026.parquet"),
    ("load_wnba_stats_schedules", "wnba", 1997, "wnba_schedule_1997.parquet"),
    ("load_wnba_stats_schedules", "wnba", 2025, "wnba_schedule_2025.parquet"),
    ("load_wnba_stats_pbp", "wnba", 1997, "wnba_play_by_play_1997.parquet"),
    ("load_wnba_stats_pbp", "wnba", 2025, "wnba_play_by_play_2025.parquet"),
    ("load_wnba_stats_possessions", "wnba", 1997, "wnba_possessions_1997.parquet"),
    ("load_wnba_stats_possessions", "wnba", 2025, "wnba_possessions_2025.parquet"),
    ("load_wnba_stats_game_lineups", "wnba", 1997, "wnba_lineups_1997.parquet"),
    ("load_wnba_stats_game_lineups", "wnba", 2025, "wnba_lineups_2025.parquet"),
]


def _requested_url(fn_name: str, league: str, season: int) -> str:
    """Call the public loader and return the asset URL it actually asked for."""
    import importlib

    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    box: dict = {}

    def fake(url, *a, **k):
        box["url"] = url
        raise FileNotFoundError("404 not found")  # 404-safe path: warn + skip

    with patch.object(mod.pl, "read_parquet", side_effect=fake):
        with pytest.warns(UserWarning):
            getattr(mod, fn_name)(seasons=season)
    return box["url"]


@pytest.mark.parametrize(("fn_name", "league", "season", "asset"), CASES)
def test_public_season_resolves_to_expected_asset(fn_name, league, season, asset):
    assert _requested_url(fn_name, league, season).endswith("/" + asset)


def test_fill_season_offset_and_identity():
    assert spec.fill_season("x_{season}.parquet", 2025) == "x_2025.parquet"
    assert spec.fill_season("x_{season + 1}.parquet", 2025) == "x_2026.parquet"
    # Whitespace-tolerant, and every occurrence is substituted.
    assert spec.fill_season("{season}/y_{season+1}.parquet", 1996) == "1996/y_1997.parquet"


def test_only_nba_stats_families_carry_the_end_year_offset():
    """Guard the blast radius: the +1 belongs to the four NBA stats families only.

    WNBA seasons are single-calendar-year, so an offset there would be an off-by-one.
    """
    offset = {ld.fn for ld in spec.load_releases(REL).loaders if (m := spec.SEASON_TOKEN.search(ld.url)) and m.group(1)}
    assert offset == {
        "load_nba_stats_schedules",
        "load_nba_stats_pbp",
        "load_nba_stats_possessions",
        "load_nba_stats_game_lineups",
    }
