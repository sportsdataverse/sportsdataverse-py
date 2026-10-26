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
        match=rf"league_config\.{name} is deprecated since 0\.2\.0 and will be removed in 0\.4\.0; use .*wbb_period_model",
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


# --- sportsdataverse.dl_utils: ESPNHTTP / ESPNResponse / janitor.to_*_case (A-L32, C-L59) ---


@pytest.mark.parametrize("name", ["ESPNHTTP", "ESPNResponse"])
def test_dl_utils_http_classes_warn_on_access_and_still_resolve(name: str) -> None:
    dl_utils = importlib.import_module("sportsdataverse.dl_utils")
    with pytest.warns(
        DeprecationWarning,
        match=rf"dl_utils\.{name} is deprecated since 0\.2\.0 and will be removed in 0\.4\.0; use .*download\(\)",
    ):
        cls = getattr(dl_utils, name)
    assert isinstance(cls, type)
    with pytest.raises(AttributeError):
        dl_utils.no_such_name  # noqa: B018


def test_dl_utils_espn_response_still_works() -> None:
    from sportsdataverse import dl_utils

    with pytest.warns(DeprecationWarning):
        response_cls = dl_utils.ESPNResponse
    response = response_cls(response='{"a": 1}', status_code=200, url="https://x")
    assert response.get_dict() == {"a": 1} and response.valid_json() and response.get_url() == "https://x"


@pytest.mark.parametrize(
    ("method", "expected"),
    [("to_pascal_case", ["GameId"]), ("to_camel_case", ["gameId"]), ("to_kebab_case", ["game-id"])],
)
def test_janitor_case_methods_warn_and_still_rename(method: str, expected: list[str]) -> None:
    import polars as pl

    import sportsdataverse.dl_utils  # noqa: F401  (registers the ``janitor`` namespace)

    df = pl.DataFrame({"game_id": [1]})
    with pytest.warns(DeprecationWarning, match=rf"janitor\.{method} is deprecated since 0\.2\.0"):
        out = getattr(df.janitor, method)()
    assert out.columns == expected


def test_janitor_clean_names_is_not_deprecated() -> None:
    import warnings

    import polars as pl

    import sportsdataverse.dl_utils  # noqa: F401

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert pl.DataFrame({"gameId": [1]}).janitor.clean_names().columns == ["game_id"]


# --- sportsdataverse.decorators: record_mem_usage / record_time_usage (C-L46) ------------


def test_record_time_usage_warns_when_applied_and_still_times(capsys: pytest.CaptureFixture[str]) -> None:
    from sportsdataverse.decorators import record_time_usage

    with pytest.warns(DeprecationWarning, match=r"record_time_usage is deprecated since 0\.2\.0.*timer\(number=1\)"):
        wrapped = record_time_usage(lambda x: x + 1)
    assert wrapped(1) == 2
    assert "Took" in capsys.readouterr().out


def test_record_mem_usage_warns_when_applied() -> None:
    from sportsdataverse.decorators import record_mem_usage

    with pytest.warns(
        DeprecationWarning, match=r"record_mem_usage is deprecated since 0\.2\.0 and will be removed in 0\.4\.0"
    ):
        wrapped = record_mem_usage(lambda: 7)
    assert callable(wrapped)


def test_dl_utils_and_decorators_import_is_silent() -> None:
    """Warn on use, not on import."""
    code = "import sportsdataverse.dl_utils, sportsdataverse.decorators"
    proc = subprocess.run(
        [sys.executable, "-W", "error::DeprecationWarning", "-c", code],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]


# --- sportsdataverse.config: the unread release URLs (C-L45) ---------------------------------


def test_config_dead_urls_warn_on_access_and_keep_their_value() -> None:
    config = importlib.import_module("sportsdataverse.config")
    assert len(config._DEPRECATED) == 29
    with pytest.warns(
        DeprecationWarning,
        match=r"config\.NBA_BASE_URL is deprecated since 0\.2\.0 and will be removed in 0\.4\.0; use the league's generated",
    ):
        url = config.NBA_BASE_URL
    assert url == config.SDVRELEASES + "espn_nba_pbp/play_by_play_{season}.parquet"
    with pytest.raises(AttributeError):
        config.NO_SUCH_URL  # noqa: B018


def test_config_live_urls_do_not_warn() -> None:
    import warnings

    config = importlib.import_module("sportsdataverse.config")
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert config.CFB_BETTING_LINES_URL and config.CFB_ROSTERS_CROSSWALK_URL and config.NFL_BASE_URL
