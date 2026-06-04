"""The ``sportsdataverse.parsed.*`` namespace is now concrete generated files
(``parsed_module.py.jinja``), not the old runtime ``types.ModuleType`` shim.

These tests lock in: real on-disk modules, the ``return_parsed=True`` default
flip on parser-backed wrappers, the explicit-override escape hatch, and that
every league mirror imports cleanly.
"""

from __future__ import annotations

import os

import pytest

_LEAGUES = ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl")


@pytest.mark.parametrize("league", _LEAGUES)
def test_parsed_module_is_concrete_file_not_virtual(league):
    mod = __import__(f"sportsdataverse.parsed.{league}", fromlist=[league])
    # A real file on disk, not a synthesized types.ModuleType registered in sys.modules.
    assert mod.__file__ is not None
    assert os.path.basename(mod.__file__) == f"{league}.py"
    assert os.path.basename(os.path.dirname(mod.__file__)) == "parsed"


def test_parsed_flips_return_parsed_default(monkeypatch):
    import sportsdataverse.parsed.nba as m

    seen = {}

    def fake(*args, **kwargs):
        seen.update(kwargs)
        return "df"

    # The wrapper resolves ``_raw_espn_nba_scoreboard`` from the module global at
    # call time, so patching it here exercises the default-flip path.
    monkeypatch.setattr(m, "_raw_espn_nba_scoreboard", fake)
    m.espn_nba_scoreboard(dates="20240115")
    assert seen.get("return_parsed") is True


def test_parsed_explicit_return_parsed_false_is_respected(monkeypatch):
    import sportsdataverse.parsed.nba as m

    seen = {}

    def fake(*args, **kwargs):
        seen.update(kwargs)
        return {}

    monkeypatch.setattr(m, "_raw_espn_nba_scoreboard", fake)
    m.espn_nba_scoreboard(return_parsed=False)
    assert seen.get("return_parsed") is False  # setdefault must not clobber an explicit value


def test_parsed_surface_covers_raw_espn_wrappers():
    """Every parser-backed espn_* wrapper on the raw module is exposed by the
    parsed mirror (the DataFrame-default surface is a superset of the raw
    parser wrappers)."""

    import sportsdataverse.nba as raw
    import sportsdataverse.parsed.nba as parsed

    raw_parser_fns = {
        n
        for n in dir(raw)
        if not n.startswith("_")
        and callable(getattr(raw, n))
        and getattr(getattr(raw, n), "__module__", "").startswith("sportsdataverse")
        and "return_parsed" in _safe_params(getattr(raw, n))
    }
    missing = raw_parser_fns - set(dir(parsed))
    assert not missing, f"parsed.nba missing parser wrappers: {sorted(missing)}"


def _safe_params(fn):
    import inspect

    try:
        return set(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        return set()
