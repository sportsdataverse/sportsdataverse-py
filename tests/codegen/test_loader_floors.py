"""Release-loader season floors start at the first published season (sportsdataverse-py#1091)."""

import importlib
import re
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from sportsdataverse import release
from sportsdataverse.errors import SeasonNotFoundError
from tests.conftest import skip_if_no_live
from tools.codegen import spec

REL = Path("tools/codegen/endpoints/releases.yaml")
_BASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"

# The first published asset of each loader whose floor sat above it (checked with
# ``gh release view <tag>`` on 2026-11-04). The paths are spelled out so a releases.yaml
# regression cannot pass by echo.
_FIRST_SEASON_ASSETS = {
    "load_cfb_power_index": ("cfb", "espn_cfb_power_index/power_index_2005.parquet"),
    "load_cfb_returning_production": ("cfb", "cfb_returning_production/cfb_returning_production_2004.parquet"),
    "load_cfb_schedule_crosswalk": ("cfb", "cfb_crosswalk/cfb_schedule_crosswalk_2004.parquet"),
    "load_cfb_teams_crosswalk": ("cfb", "cfb_crosswalk/cfb_teams_crosswalk_2004.parquet"),
    "load_mbb_game_rosters": ("mbb", "espn_mens_college_basketball_game_rosters/game_rosters_2002.parquet"),
    "load_mbb_officials": ("mbb", "espn_mens_college_basketball_officials/officials_2003.parquet"),
    "load_mbb_schedule_crosswalk": ("mbb", "mbb_crosswalk/mbb_schedule_crosswalk_2008.parquet"),
    "load_nba_player_boxscore": ("nba", "espn_nba_player_boxscores/player_box_1994.parquet"),
    "load_nba_player_core": ("nba", "espn_nba_player_core/player_core_1994.parquet"),
    "load_nba_player_season_stats": ("nba", "espn_nba_player_season_stats/player_season_stats_1994.parquet"),
    "load_nba_schedule": ("nba", "espn_nba_schedules/nba_schedule_1994.parquet"),
    "load_nba_schedule_crosswalk": ("nba", "nba_crosswalk/nba_schedule_crosswalk_2002.parquet"),
    "load_nba_team_boxscore": ("nba", "espn_nba_team_boxscores/team_box_1994.parquet"),
    "load_nba_team_crosswalk": ("nba", "nba_crosswalk/nba_team_crosswalk_2002.parquet"),
    "load_ncaa_baseball_games": ("mlb", "ncaa_baseball_games/ncaa_baseball_games_2016.parquet"),
    "load_ncaa_baseball_linescore": ("mlb", "ncaa_baseball_linescore/ncaa_baseball_linescore_2016.parquet"),
    "load_ncaa_baseball_pbp": ("mlb", "ncaa_baseball_pbp/ncaa_baseball_pbp_2016.parquet"),
    "load_ncaa_baseball_player_stats": ("mlb", "ncaa_baseball_player_stats/ncaa_baseball_player_stats_2016.parquet"),
    "load_ncaa_baseball_rosters": ("mlb", "ncaa_baseball_rosters/ncaa_baseball_rosters_2016.parquet"),
    "load_ncaa_baseball_situational_stats": (
        "mlb",
        "ncaa_baseball_situational_stats/ncaa_baseball_situational_stats_2016.parquet",
    ),
    "load_ncaa_baseball_team_stats": ("mlb", "ncaa_baseball_team_stats/ncaa_baseball_team_stats_2016.parquet"),
    "load_ncaa_baseball_teams": ("mlb", "ncaa_baseball_teams/ncaa_baseball_teams_2016.parquet"),
    "load_wbb_schedule": ("wbb", "espn_womens_college_basketball_schedules/wbb_schedule_2001.parquet"),
    "load_wbb_team_crosswalk": ("wbb", "wbb_crosswalk/wbb_team_crosswalk_2002.parquet"),
    "load_wnba_schedule": ("wnba", "espn_wnba_schedules/wnba_schedule_1997.parquet"),
}


@pytest.mark.parametrize("fn,league,asset", [(fn, *v) for fn, v in sorted(_FIRST_SEASON_ASSETS.items())])
def test_loader_accepts_its_first_published_season(fn, league, asset):
    module = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    loader = getattr(module, fn)
    first = int(re.search(r"_(\d{4})\.parquet$", asset).group(1))
    with patch.object(module, "_read_release_parquet", return_value=pl.DataFrame({"season": [first]})) as reader:
        loader(seasons=first)
    reader.assert_called_once_with(_BASE + asset)

    with patch.object(module, "_read_release_parquet") as reader:
        with pytest.raises(SeasonNotFoundError):
            loader(seasons=first - 1)
    reader.assert_not_called()


@skip_if_no_live
def test_no_floor_blocks_a_published_season():
    """Every season-templated sportsdataverse-data loader's floor is at or below its first published asset."""
    if not release.gh_cli_available():
        pytest.skip("gh CLI not available")
    blocked = {}
    for ld in spec.load_releases(REL).loaders:
        if ld.base != "sdv_releases" or "{season}" not in ld.url or ld.min_season is None:
            continue
        tag, name = ld.url.split("/", 1)
        pattern = re.compile("^" + re.escape(name).replace(re.escape("{season}"), r"(\d{4})") + "$")
        try:
            names = release.gh_cli_release_assets(ld.tag or tag)["name"].to_list()
        except Exception:  # noqa: BLE001 - a missing tag is not a floor problem
            continue
        seasons = [int(m.group(1)) for n in names if (m := pattern.match(n))]
        if seasons and min(seasons) < ld.min_season:
            blocked[ld.fn] = (ld.min_season, min(seasons))
    assert not blocked, f"floor above the first published season (floor, first): {blocked}"
