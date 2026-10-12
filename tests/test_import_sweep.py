"""Every module under ``sportsdataverse/`` imports, and none of them reaches a removed dependency.

``matplotlib``, ``pyjanitor``, ``inflection`` and ``attrs`` were runtime dependencies that nothing
imported (A-L73, A-L74); they are gone from ``pyproject.toml``. The dev environment may still have
them installed transitively, so the sweep runs in a subprocess with a meta-path finder that blocks
them (the pattern of ``tests/contracts/test_cli_importable_without_yaml.py``): a module that imports
one fails the sweep whether or not the package happens to be present.

The same subprocess is what CI's ``floor-import`` job runs on the 3.10 floor, so a 3.11+-only
construct anywhere in the package (not only the three modules that tripped before) cannot ship.

A ``ModuleNotFoundError`` for one of the OPTIONAL extras (``kloppy``, ``playwright``, ...) is not a
failure: those modules are documented as needing ``sportsdataverse[<extra>]`` and the floor job
installs no extras. Anything else that fails to import is.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

_PROGRAM = textwrap.dedent(
    """
    import importlib, importlib.util, json, os, sys, warnings
    from pathlib import Path

    BLOCKED = {"matplotlib", "janitor", "inflection", "attr", "attrs"} | set(
        filter(None, os.environ.get("SDV_SWEEP_BLOCK", "").split(","))
    )
    OPTIONAL = {"kloppy", "playwright", "patchright", "curl_cffi", "yaml", "psutil", "mcp"}

    class _Block:
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in BLOCKED:
                raise ImportError(f"{name} is a removed dependency; nothing in sportsdataverse may import it")
            return None

    sys.meta_path.insert(0, _Block())
    warnings.simplefilter("ignore")  # the deprecated parsed/* shims warn on import by design

    # Locate the package without importing it: a failure inside sportsdataverse/__init__.py
    # must be reported as a finding, not crash the sweep before it prints anything.
    root = Path(importlib.util.find_spec("sportsdataverse").origin).parent
    only = os.environ.get("SDV_SWEEP_ONLY")
    names = [only] if only else sorted(
        ("sportsdataverse." + ".".join(p.relative_to(root).with_suffix("").parts)).removesuffix(".__init__")
        for p in root.rglob("*.py")
    )
    imported, optional, failed = 0, [], []
    for name in names:
        try:
            importlib.import_module(name)
            imported += 1
        except ModuleNotFoundError as exc:
            if exc.name and exc.name.split(".")[0] in OPTIONAL:
                optional.append([name, exc.name])
            else:
                failed.append([name, f"{type(exc).__name__}: {exc}"])
        except Exception as exc:  # noqa: BLE001 - any import-time error is a finding
            failed.append([name, f"{type(exc).__name__}: {exc}"])
    print(json.dumps({"imported": imported, "optional_missing": optional, "failed": failed}))
    """
)


def _sweep(**env: str) -> dict:
    import os

    proc = subprocess.run(
        [sys.executable, "-c", _PROGRAM],
        capture_output=True,
        text=True,
        env={**os.environ, **env},
    )
    assert proc.returncode == 0, f"stdout={proc.stdout[-2000:]!r}\nstderr={proc.stderr[-3000:]!r}"
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_every_module_imports_without_the_removed_dependencies():
    out = _sweep()
    assert out["failed"] == [], "\n".join(f"{m}: {e}" for m, e in out["failed"])
    assert out["imported"] > 700, out["imported"]


def test_sweep_reports_a_module_that_imports_a_blocked_package():
    """The harness itself can fail: blocking a package the package uses names the importer."""
    out = _sweep(SDV_SWEEP_ONLY="sportsdataverse.dl_utils", SDV_SWEEP_BLOCK="requests")
    assert out["failed"] and out["failed"][0][0] == "sportsdataverse.dl_utils", out
    assert "removed dependency" in out["failed"][0][1]
