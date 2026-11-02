"""Offline tests for the cyclopts ``sdv`` CLI (sdv_cli/) and its fallback to the classic one."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

pytest.importorskip("cyclopts")

from sdv_cli import app as entry  # noqa: E402
from sdv_cli.commands import run  # noqa: E402
from sportsdataverse import cli as classic  # noqa: E402
from tests.test_cli import fake_nba_lookup  # noqa: E402,F401  (fixture: deterministic ESPN payloads)


def _both(argv, capsys):
    """Run argv through the new and the classic CLI; return ((rc, out), (rc, out))."""
    new_rc = run(argv)
    new_out = capsys.readouterr().out
    old_rc = classic.main(argv)
    old_out = capsys.readouterr().out
    return (new_rc, new_out), (old_rc, old_out)


def test_help_does_not_import_sportsdataverse():
    """`sdv --help` must not load the library: that import is what made it take seconds."""
    code = (
        "import sys, contextlib, io\n"
        "from sdv_cli.app import main\n"
        "with contextlib.redirect_stdout(io.StringIO()):\n"
        "    rc = main(['--help'])\n"
        "print(rc, 'sportsdataverse' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True).stdout.split()
    assert out == ["0", "False"]


def test_help_lists_every_command(capsys):
    assert run(["--help"]) == 0
    out = capsys.readouterr().out
    for cmd in ("find-team", "find-athlete", "find-event", "list-functions", "function-count", "cache"):
        assert cmd in out
    assert "8 leagues" not in out


@pytest.mark.parametrize(
    "argv",
    [
        ["--json", "find-team", "lakers", "--league", "nba"],
        ["--json", "find-team", "lakers", "--league", "nba", "--multi"],
        ["--json", "find-team", "ZZZZZ", "--league", "nba"],
        ["--json", "find-event", "2024-06-17", "--league", "nba", "--home", "Boston"],
        ["--json", "find-event", "2024-06-17", "--league", "nba", "--multi"],
    ],
)
def test_json_output_matches_the_classic_cli(fake_nba_lookup, capsys, argv):  # noqa: F811
    (new_rc, new_out), (old_rc, old_out) = _both(argv, capsys)
    assert new_rc == old_rc
    assert new_out == old_out
    json.loads(new_out)


@pytest.mark.parametrize(
    "argv",
    [
        ["--json", "function-count"],
        ["--json", "function-count", "--league", "nba"],
        ["--json", "list-functions", "--league", "mlb", "--search", "statcast"],
        ["--json", "list-functions", "--league", "nba", "--parsers-only"],
    ],
)
def test_discover_json_matches_the_classic_cli(capsys, argv):
    (new_rc, new_out), (old_rc, old_out) = _both(argv, capsys)
    assert (new_rc, new_out) == (old_rc, old_out)


def test_json_flag_after_the_command(fake_nba_lookup, capsys):  # noqa: F811
    assert run(["find-team", "lakers", "--league", "nba", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id"] == "13"


def test_find_team_pretty_card(fake_nba_lookup, capsys):  # noqa: F811
    assert run(["find-team", "lakers", "-l", "nba"]) == 0
    out = capsys.readouterr().out
    assert "Los Angeles Lakers" in out and "13" in out and "LAL" in out


def test_find_team_miss_exits_1_with_suggestions(fake_nba_lookup, capsys):  # noqa: F811
    assert run(["find-team", "Lakres", "-l", "nba"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "no nba team matched 'Lakres'" in captured.err
    assert "Los Angeles Lakers" in captured.err  # the did-you-mean suggestion


def test_find_event_pretty_table(fake_nba_lookup, capsys):  # noqa: F811
    assert run(["find-event", "2024-06-17", "-l", "nba", "--home", "Boston"]) == 0
    out = capsys.readouterr().out
    assert "401585607" in out and "Boston Celtics" in out and "Dallas Mavericks" in out


@pytest.mark.parametrize(("status", "error"), [(403, "AssetFetchError"), (404, "NoDataError")])
def test_fetch_errors_print_sdv_message_and_exit_2(monkeypatch, capsys, status, error):
    """A failed or empty fetch is `sdv: <msg>` on stderr and exit 2, never a traceback."""
    import sportsdataverse._codegen_runtime as rt
    from sportsdataverse.find import clear_team_cache

    class _Resp:
        status_code = status
        text = "Forbidden"

        def json(self):
            raise ValueError("not json")

    monkeypatch.setattr(rt, "download", lambda url, params=None, **kw: _Resp())
    clear_team_cache()
    try:
        exit_code = run(["find-team", "lakers", "--league", "nba"])
    finally:
        clear_team_cache()
    err = capsys.readouterr().err
    assert exit_code == 2, error
    assert err.startswith("sdv: ") and f"HTTP {status}" in err and "Traceback" not in err


@pytest.mark.parametrize(
    "argv",
    [
        ["find-team", "lakers"],  # --league is required
        ["find-team", "lakers", "--league", "xfl"],  # not a find league
        ["no-such-command"],
    ],
)
def test_usage_errors_exit_2(capsys, argv):
    assert run(argv) == 2


def test_parsers_only_and_wrappers_only_are_exclusive(capsys):
    assert run(["list-functions", "--parsers-only", "--wrappers-only"]) == 2
    assert "mutually exclusive" in capsys.readouterr().err


def test_function_count_table_has_a_total(capsys):
    assert run(["function-count"]) == 0
    assert "total" in capsys.readouterr().out


def test_cache_mode_roundtrip(capsys):
    from sportsdataverse import cache

    before = cache.get_cache_mode()
    try:
        assert run(["cache", "mode", "--set", "off"]) == 0
        assert run(["cache", "mode"]) == 0
        assert capsys.readouterr().out.splitlines()[-1] == "off"
    finally:
        cache.set_cache_mode(before)


def test_falls_back_to_the_classic_cli_without_cyclopts(monkeypatch, capsys):
    """Python 3.10 (or an install without cyclopts) still gets a working `sdv`."""

    def missing():
        raise ModuleNotFoundError("No module named 'cyclopts'", name="cyclopts")

    monkeypatch.setattr(entry, "_load_commands", missing)
    assert entry.main(["--json", "function-count", "--league", "nba"]) == 0
    assert isinstance(json.loads(capsys.readouterr().out), int)


def test_an_unrelated_import_error_is_not_swallowed(monkeypatch):
    def broken():
        raise ModuleNotFoundError("No module named 'something_else'", name="something_else")

    monkeypatch.setattr(entry, "_load_commands", broken)
    with pytest.raises(ModuleNotFoundError):
        entry.main(["--help"])
