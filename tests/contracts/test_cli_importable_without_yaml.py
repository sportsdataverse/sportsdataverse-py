"""`tools.validation.cli compare` must work without pyyaml installed.

pyyaml lives in ``[dependency-groups]`` — dev-only, not a runtime dependency —
but every SDV ``-data`` repo consumes this code by pinning ``sportsdataverse``
from git, so their venvs have no dev group and no yaml. A module-level
``from tools.validation.registry import ...`` (registry imports yaml for
thresholds.yaml) therefore made the whole CLI unimportable for exactly the
consumers that need `compare`, and it failed only at runtime, in their CI.

`compare` needs neither the dataset registry nor the thresholds, so this asserts
it does not pay for them. Run in a subprocess with a meta-path finder that
blocks `yaml`, because the property is about IMPORT TIME and this test process
has already imported yaml via other tests.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

_PROGRAM = textwrap.dedent(
    """
    import sys

    class _BlockYaml:
        def find_module(self, name, path=None):
            return self if name == "yaml" or name.startswith("yaml.") else None
        def find_spec(self, name, path=None, target=None):
            if name == "yaml" or name.startswith("yaml."):
                raise ImportError("yaml is blocked for this test")
            return None

    sys.meta_path.insert(0, _BlockYaml())

    # Sanity: the block actually works, otherwise this test proves nothing.
    try:
        import yaml  # noqa: F401
    except ImportError:
        pass
    else:
        raise SystemExit("BLOCK-FAILED: yaml was importable")

    import polars as pl
    from tools.validation import cli

    a = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.2]})
    b = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.9]})
    import tempfile, pathlib
    d = pathlib.Path(tempfile.mkdtemp())
    a.write_parquet(d / "a.parquet")
    b.write_parquet(d / "b.parquet")

    out = cli.compare_outputs("d", str(d / "a.parquet"), str(d / "b.parquet"), ("game_id",), "nfl")
    assert any("disagrees" in f["message"] for f in out), out
    print("OK")
    """
)


def test_compare_works_without_yaml():
    proc = subprocess.run(
        [sys.executable, "-c", _PROGRAM],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr[-2000:]!r}"
    assert "OK" in proc.stdout


def test_run_dataset_still_reaches_the_registry():
    """The lazy import must not have silently disabled the registry path: with
    yaml present, an unknown dataset should still raise KeyError from the
    registry lookup rather than ImportError or NameError."""
    from tools.validation import cli

    try:
        cli.run_dataset("definitely_not_a_registered_dataset")
    except KeyError:
        pass
    else:  # pragma: no cover - only reached if the registry stopped being consulted
        raise AssertionError("expected KeyError from the registry lookup")
