"""Shared engine for the ESPN ``-raw`` archives (nba, mbb, wnba, wbb).

The four ``hoopR``/``wehoop`` ESPN raw repos each run a numbered
``espn_<lg>_NN_*_scrape.py`` stage sequence against the same tree shape. The
stages are league-specific; everything two of them would otherwise duplicate
lives here, parameterized on :class:`~.league_config.LeagueConfig`.

This is the same one-core-plus-league-bindings shape already proven by
:mod:`sportsdataverse.scrape.stats` (NBA/WNBA stats API) and
:mod:`sportsdataverse.scrape.ncaa` (MBB/WBB stats.ncaa.org). It was lifted from
``wehoop-wbb-raw``'s in-repo ``wbb_raw_scrape`` package, which was the only one
of the four to have grown a shared package, a test suite, and the write guard.

Every entry point takes ``league`` as a **required keyword**. A defaulted
league is how a well-formed capture ends up under the wrong league's tree --
a failure with no error, only wrong data.
"""

from sportsdataverse.scrape.espn.cli import season_args, str2bool
from sportsdataverse.scrape.espn.ids import to_int64, with_int64_ids
from sportsdataverse.scrape.espn.league_config import MBB, NBA, WBB, WNBA, LeagueConfig, by_key
from sportsdataverse.scrape.espn.master import build_coverage, build_master
from sportsdataverse.scrape.espn.paths import (
    family_json_path,
    game_json_path,
    raw_github_url,
)
from sportsdataverse.scrape.espn.persist import (
    is_error_payload,
    scan_for_error_payloads,
    write_payload,
)
from sportsdataverse.scrape.espn.schedule import add_capture_columns, resolve_league

__all__ = [
    "MBB",
    "NBA",
    "WBB",
    "WNBA",
    "LeagueConfig",
    "add_capture_columns",
    "build_coverage",
    "build_master",
    "by_key",
    "family_json_path",
    "game_json_path",
    "is_error_payload",
    "raw_github_url",
    "resolve_league",
    "scan_for_error_payloads",
    "season_args",
    "str2bool",
    "to_int64",
    "with_int64_ids",
    "write_payload",
]
