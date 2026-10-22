"""PWHL ratings and props must read PWHL data, never the NHL loaders (supp-001; fixed by task 2.13).

Before 2.13 `pwhl_team_ratings` / `pwhl_player_props` ran the NHL engines on `load_nhl_pbp_full`,
`load_nhl_schedules` and `load_nhl_skater_boxscores` whatever `league` said: NHL ratings computed with
PWHL constants. Every NHL loader is replaced by one that fails the test
naming itself, and sockets are closed, so the check never reaches the network.
"""

from __future__ import annotations

import socket

import pytest

import importlib

from sportsdataverse.pwhl import pwhl_player_props, pwhl_team_ratings

nhl_loaders = importlib.import_module("sportsdataverse.nhl.nhl_loaders")
nhl_props = importlib.import_module("sportsdataverse.nhl.nhl_player_props")
_NHL_LOADERS = ("load_nhl_pbp_full", "load_nhl_schedules", "load_nhl_skater_boxscores")


@pytest.fixture(autouse=True)
def _nhl_loaders_fail(monkeypatch):
    def refuse(name):
        def _loader(*args, **kwargs):
            raise AssertionError(f"PWHL call reached the NHL loader {name}{args}")

        return _loader

    for name in _NHL_LOADERS:
        monkeypatch.setattr(nhl_loaders, name, refuse(name))
    monkeypatch.setattr(nhl_props, "load_nhl_skater_boxscores", refuse("load_nhl_skater_boxscores"))

    def no_network(*args, **kwargs):
        raise OSError("offline test: no sockets")

    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(socket.socket, "connect", no_network)


@pytest.mark.parametrize("fn", [pwhl_team_ratings, pwhl_player_props], ids=["team_ratings", "player_props"])
def test_pwhl_engines_never_read_nhl_data(fn):
    try:
        fn(2024)
    except AssertionError:
        raise
    except Exception:  # noqa: BLE001 - any other failure (e.g. the offline PWHL loader) is not the NHL route
        pass
