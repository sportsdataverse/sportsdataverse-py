"""The modules the audit found short of napoleon sections stay complete (C-L50, C-L55, C-L60, B-L24)."""

from __future__ import annotations

from pathlib import Path

from tools.check_docstrings import check_file, main

ROOT = Path(__file__).resolve().parents[1]
AUDITED = [
    "sportsdataverse/find.py",
    "sportsdataverse/validation",
    "sportsdataverse/_rds.py",
    "sportsdataverse/dl_utils.py",
    "sportsdataverse/errors.py",
    "sportsdataverse/pwhl/pwhl_api.py",
    "sportsdataverse/nfl/nfl_game_rosters.py",
]


def test_audited_modules_have_every_section(capsys):
    assert main([str(ROOT / p) for p in AUDITED]) == 0, capsys.readouterr().out


def test_every_audited_path_holds_python():
    """R1000-4: a renamed or emptied directory would make the check above pass on nothing."""
    for p in AUDITED:
        path = ROOT / p
        assert path.exists(), p
        assert path.is_file() or any(path.rglob("*.py")), f"{p} holds no .py file"


def test_checker_wants_a_literal_block_in_example(tmp_path):
    """R1000-1: an ``Example:`` with prose only (no ``::`` literal block) is a gap."""
    f = tmp_path / "m.py"
    f.write_text(
        'def f():\n    """Do it.\n\n    Returns:\n        int.\n\n    Example:\n        Call f() and print it.\n\n'
        '    See Also:\n        * g\n    """\n    return 1\n',
        encoding="utf-8",
    )
    lines, missing, prompts = check_file(f)
    assert (missing, prompts) == (1, 0) and "'::' literal block" in lines[0]


def test_checker_reports_a_missing_example(tmp_path):
    f = tmp_path / "m.py"
    f.write_text(
        'def f(x):\n    """Do it.\n\n    Args:\n        x: x.\n\n    Returns:\n        int.\n\n'
        '    See Also:\n        * g\n    """\n    if x:\n        raise ValueError\n    return 1\n',
        encoding="utf-8",
    )
    lines, missing, prompts = check_file(f)
    assert (missing, prompts) == (2, 0)  # Raises: (the body raises) and Example:
    assert "Raises:" in lines[0] and "Example:" in lines[0]
