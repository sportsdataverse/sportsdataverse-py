"""Deprecated public names keep resolving and warn ``DeprecationWarning`` on access.

The helpers themselves are tested in ``tests/test_deprecation.py``; this file pins the
individual deprecations, one block per module.
"""

from __future__ import annotations

import importlib
import subprocess
import sys

import pytest

# --- sportsdataverse.scrape.ncaa.league_config (B-L29) ----------------------


@pytest.mark.parametrize("name", ["NcaaLeagueConfig", "MBB", "WBB", "by_league"])
def test_league_config_names_warn_on_access(name: str) -> None:
    league_config = importlib.import_module("sportsdataverse.scrape.ncaa.league_config")
    with pytest.warns(
        DeprecationWarning,
        match=rf"league_config\.{name} is deprecated since 0\.1\.6 and will be removed in 0\.2\.0; use .*wbb_period_model",
    ):
        getattr(league_config, name)


def test_league_config_still_resolves_while_deprecated() -> None:
    league_config = importlib.import_module("sportsdataverse.scrape.ncaa.league_config")
    with pytest.warns(DeprecationWarning):
        by_league = league_config.by_league
    with pytest.warns(DeprecationWarning):
        wbb = league_config.WBB
    with pytest.warns(DeprecationWarning):
        config_cls = league_config.NcaaLeagueConfig
    assert by_league("wbb") is wbb
    assert isinstance(wbb, config_cls)
    assert wbb.periods == 4  # unchanged value; it is wrong for halves-era seasons, hence the deprecation
    with pytest.raises(KeyError):
        by_league("nba")
    with pytest.raises(AttributeError):
        league_config.no_such_name  # noqa: B018


def test_league_config_import_is_silent() -> None:
    """Warn on access, not on import: importing the module (or the engine) must stay quiet."""
    code = "import sportsdataverse.scrape.ncaa.league_config, sportsdataverse.scrape.ncaa.parse"
    proc = subprocess.run(
        [sys.executable, "-W", "error::DeprecationWarning:sportsdataverse.scrape.ncaa.league_config", "-c", code],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
