"""0.0.54 contract: parser-backed wrappers default to return_parsed=True."""

from __future__ import annotations

import inspect

from sportsdataverse.nba import nba_espn_ext


def test_parser_backed_wrapper_defaults_to_parsed():
    sig = inspect.signature(nba_espn_ext.espn_nba_scoreboard)
    assert "return_parsed" in sig.parameters
    assert sig.parameters["return_parsed"].default is True


def test_return_as_pandas_still_defaults_false():
    sig = inspect.signature(nba_espn_ext.espn_nba_scoreboard)
    assert sig.parameters["return_as_pandas"].default is False
