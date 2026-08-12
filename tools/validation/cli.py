from __future__ import annotations

import argparse
import json
import sys
from types import ModuleType

import polars as pl

from tools.validation.checks import (
    boundary_leakage,
    boxscore_parity,
    combo_drift,
    constant_column,
    definitional,
    extraction,
    numeric_parity,
    rate_anomaly,
    r_python_output_parity,
    schema_contract,
    sweep,
)
from tools.validation.findings import Finding, Severity
from tools.validation.lint import leakage_python, leakage_r

# NOTE: `tools.validation.registry` is imported lazily, inside the two functions
# that need it, rather than here. It pulls in `yaml` (for thresholds.yaml), and
# pyyaml lives in [dependency-groups] -- dev-only, NOT a runtime dependency. A
# module-level import therefore makes this whole CLI unimportable from an
# installed sportsdataverse, which is how every `-data` repo consumes it: they
# pin the package from git and have no dev group. `compare` needs neither the
# dataset registry nor the thresholds, so it must not pay for them.

_CHECKS = (
    schema_contract,
    extraction,
    numeric_parity,
    sweep,
    boundary_leakage,
    constant_column,
    definitional,
    combo_drift,
    rate_anomaly,
    boxscore_parity,
)

_LINTERS: dict[str, ModuleType] = {"python": leakage_python, "r": leakage_r}


def run_dataset(dataset: str, release: str | None = None) -> list[dict]:
    """Run all single-frame checks over a registered dataset.

    Args:
        dataset: Registered dataset name.
        release: Optional release tag (reserved; see spec §11).

    Returns:
        A flat list of finding dicts (each from ``Finding.to_dict()``).
    """
    from tools.validation.registry import resolve

    frame, ctx = resolve(dataset, release=release)
    findings: list[Finding] = []
    for mod in _CHECKS:
        findings.extend(mod.run(dataset, frame, ctx))
    return [f.to_dict() for f in findings]


def compare_outputs(
    dataset: str,
    r_parquet: str,
    py_parquet: str,
    join_keys: tuple[str, ...],
    domain: str,
    *,
    tolerance: float = 1e-6,
    ignore_columns: tuple[str, ...] = (),
) -> list[dict]:
    """Compare an R-produced and a Python-produced artifact for one dataset.

    Both `-data` chains write ``{dataset}/{rds,parquet}/`` to the SAME path, so
    in practice the R side is the artifact already on its release tag (fetch it
    with ``gh release download``) and the Python side a fresh local build. No R
    runtime is involved — this reads two parquet files.

    Args:
        dataset: Dataset identifier recorded on each finding.
        r_parquet: Path to the R-produced parquet.
        py_parquet: Path to the Python-produced parquet.
        join_keys: Columns identifying a row in both frames.
        domain: Domain identifier recorded on each finding.
        tolerance: Absolute tolerance for numeric divergence.
        ignore_columns: Columns excluded from value comparison (build stamps etc.).

    Returns:
        A flat list of finding dicts.

    Raises:
        FileNotFoundError: If either parquet path does not exist.
        polars.exceptions.ComputeError: If either path is not readable as parquet.

    Example:
        Compare a released R artifact against a fresh Python build::

            from tools.validation.cli import compare_outputs

            findings = compare_outputs(
                "wnba_pbp", "r_side.parquet", "py_side.parquet", ("game_id",), "wnba"
            )
            print(len(findings), "divergence(s)")

        From the shell, exiting 1 when the pipelines disagree::

            python -m tools.validation.cli compare --dataset wnba_pbp --domain wnba \\
                --r-parquet r.parquet --py-parquet py.parquet --join-keys game_id
    """
    # Both paths come from the command line and a release-download path is easy
    # to mistype. A traceback here would lose the documented exit code and, in
    # --json mode, print something unparseable after the caller asked for JSON.
    frames = []
    for side, path in (("R", r_parquet), ("Python", py_parquet)):
        try:
            frames.append(pl.read_parquet(path))
        # Narrow on purpose. A bare `except Exception` here caught a NameError
        # during development and reported it as "could not read the parquet" —
        # a code defect wearing a data defect's clothes, which is the exact
        # masking this whole check exists to prevent.
        except (OSError, pl.exceptions.PolarsError) as exc:
            return [
                Finding(
                    "r_python_output_parity",
                    Severity.ERROR,
                    domain,
                    dataset,
                    f"could not read the {side} parquet at {path!r}: {type(exc).__name__}: {exc}",
                    locator={"side": side, "path": str(path)},
                ).to_dict()
            ]

    findings = r_python_output_parity.run(
        dataset,
        frames[0],
        frames[1],
        join_keys,
        domain,
        tolerance=tolerance,
        ignore_columns=ignore_columns,
    )
    return [f.to_dict() for f in findings]


def _non_negative_float(raw: str) -> float:
    """argparse type for ``--tolerance``.

    A negative tolerance makes ``abs(r - py) > tolerance`` true for every
    non-null numeric pair, so the run would report a divergence on every row of
    every column — a total failure that looks like a thorough one.

    Args:
        raw: The raw command-line string.

    Returns:
        The parsed value.

    Raises:
        argparse.ArgumentTypeError: If ``raw`` is not a number, or is negative.

    Example:
        Parse a tolerance::

            _non_negative_float("1e-6")
    """
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{raw!r} is not a number") from None
    if value < 0:
        raise argparse.ArgumentTypeError(
            f"tolerance must be >= 0, got {value} — a negative tolerance flags every row as diverging"
        )
    return value


def _emit(out: list[dict], as_json: bool) -> int:
    """Print findings and return the process exit code.

    Args:
        out: Finding dicts.
        as_json: Emit JSON instead of the human-readable lines.

    Returns:
        1 if any ERROR finding is present, else 0.
    """
    if as_json:
        # Findings can carry `sample` rows straight from `DataFrame.to_dicts()`,
        # so a Date / Datetime / Decimal / Duration column yields values the
        # default encoder rejects — which would raise AFTER all the comparison
        # work was done. Sports frames are full of dates; stringify instead.
        json.dump(out, sys.stdout, default=str)
        sys.stdout.write("\n")
    else:
        for f in out:
            print(f"{f['severity'].upper():5} [{f['check']}] {f['dataset']} :: {f['message']}")
    return 1 if any(f["severity"] == "error" for f in out) else 0


def lint_target(name: str) -> list[dict]:
    """Run the language-appropriate linter over a registered lint target.

    Args:
        name: A key of ``LINT_TARGETS``.

    Returns:
        A flat list of finding dicts.

    Raises:
        KeyError: If ``name`` is not registered.
        NotImplementedError: If the target's language has no registered linter
            ("python" and "r" are both registered; this fires only for some other
            unregistered language).
    """
    from tools.validation.registry import LINT_TARGETS

    target = LINT_TARGETS[name]
    linter = _LINTERS.get(target.language)
    if linter is None:
        raise NotImplementedError(f"no linter for language {target.language!r}")
    return [f.to_dict() for f in linter.run(target.path)]


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: run validation checks on a dataset and print findings.

    Args:
        argv: Optional argument list (defaults to ``sys.argv``).

    Returns:
        Exit code 1 if any ERROR finding is present, else 0.
    """
    parser = argparse.ArgumentParser(prog="tools.validation.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("--dataset", required=True)
    run.add_argument("--release", default=None)
    run.add_argument("--json", action="store_true")
    lint = sub.add_parser("lint")
    lint.add_argument("--target", required=True)
    lint.add_argument("--json", action="store_true")
    cmp_ = sub.add_parser(
        "compare",
        help="R-vs-Python output parity for one dataset (two parquet artifacts)",
    )
    cmp_.add_argument("--dataset", required=True)
    cmp_.add_argument("--domain", required=True)
    cmp_.add_argument("--r-parquet", required=True, help="R-produced artifact (usually the release asset)")
    cmp_.add_argument("--py-parquet", required=True, help="Python-produced artifact (usually a fresh local build)")
    cmp_.add_argument("--join-keys", required=True, nargs="+", metavar="COL")
    cmp_.add_argument(
        "--tolerance",
        type=_non_negative_float,
        default=1e-6,
        help="absolute tolerance for numeric divergence (must be >= 0)",
    )
    cmp_.add_argument("--ignore-columns", nargs="*", default=[], metavar="COL")
    cmp_.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.cmd == "run":
        return _emit(run_dataset(args.dataset, args.release), args.json)

    if args.cmd == "lint":
        return _emit(lint_target(args.target), args.json)

    if args.cmd == "compare":
        return _emit(
            compare_outputs(
                args.dataset,
                args.r_parquet,
                args.py_parquet,
                tuple(args.join_keys),
                args.domain,
                tolerance=args.tolerance,
                ignore_columns=tuple(args.ignore_columns),
            ),
            args.json,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
