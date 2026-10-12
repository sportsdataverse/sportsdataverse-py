"""The NCAA vendor-config readers import and parse TOML on the 3.10 floor (B-L31).

``tomllib`` is 3.11+. Every TOML read in the package goes through
``scrape.ncaa.canary._toml_loads`` (``tomli`` on 3.10), so a bare ``import tomllib``
anywhere under ``sportsdataverse/`` is a floor regression this test catches on any
interpreter, not only on 3.10.
"""

from __future__ import annotations

import re
from pathlib import Path

import sportsdataverse

PKG = Path(sportsdataverse.__file__).parent
# Horizontal whitespace only: under MULTILINE `^\s*` would also start at a blank line and swallow
# its newline, so a top-level import after a blank line would be classified as indented.
_BARE_TOMLLIB = re.compile(r"^[ \t]*(import tomllib|from tomllib import)\b", re.MULTILINE)


def test_ncaa_vendor_modules_import_and_parse_toml():
    from sportsdataverse.scrape.ncaa import canary, capture

    assert capture._vendor_fetcher is not None
    assert canary._toml_loads('[[vendor]]\nname = "x"\n') == {"vendor": [{"name": "x"}]}


def test_bare_import_pattern_sees_a_top_level_import_after_a_blank_line():
    # Red proof for the `^[ \t]*` form: with `^\s*` the match would be "\nimport tomllib" (indented).
    matches = [m.group(0) for m in _BARE_TOMLLIB.finditer("import time\n\nimport tomllib\n")]
    assert matches == ["import tomllib"]
    assert [m.group(0) for m in _BARE_TOMLLIB.finditer("def f():\n    import tomllib\n")] == ["    import tomllib"]


def test_only_the_guarded_helper_imports_tomllib():
    offenders = []
    for path in PKG.rglob("*.py"):
        for m in _BARE_TOMLLIB.finditer(path.read_text(encoding="utf-8")):
            raw = m.group(0)
            rel = path.relative_to(PKG).as_posix()
            top_level = not raw[0].isspace()
            # The one allowed import is the indented, version-guarded one inside _toml_loads.
            if top_level or rel != "scrape/ncaa/canary.py":
                offenders.append(f"{rel}: {raw.strip()}")
    assert offenders == [], f"bare tomllib import (3.11+) outside canary._toml_loads: {offenders}"
