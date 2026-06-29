from __future__ import annotations

import argparse
import json
import sys
from types import ModuleType

from tools.validation.checks import boundary_leakage, extraction, numeric_parity, schema_contract, sweep
from tools.validation.findings import Finding
from tools.validation.lint import leakage_python
from tools.validation.registry import LINT_TARGETS, resolve

_CHECKS = (schema_contract, extraction, numeric_parity, sweep, boundary_leakage)

_LINTERS: dict[str, ModuleType] = {"python": leakage_python}


def run_dataset(dataset: str, release: str | None = None) -> list[dict]:
    """Run all single-frame checks over a registered dataset.

    Args:
        dataset: Registered dataset name.
        release: Optional release tag (reserved; see spec §11).

    Returns:
        A flat list of finding dicts (each from ``Finding.to_dict()``).
    """
    frame, ctx = resolve(dataset, release=release)
    findings: list[Finding] = []
    for mod in _CHECKS:
        findings.extend(mod.run(dataset, frame, ctx))
    return [f.to_dict() for f in findings]


def lint_target(name: str) -> list[dict]:
    """Run the language-appropriate linter over a registered lint target.

    Args:
        name: A key of ``LINT_TARGETS``.

    Returns:
        A flat list of finding dicts.

    Raises:
        KeyError: If ``name`` is not registered.
        NotImplementedError: If the target's language has no linter yet (e.g. "r").
    """
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
    args = parser.parse_args(argv)

    if args.cmd == "run":
        out = run_dataset(args.dataset, args.release)
        if args.json:
            json.dump(out, sys.stdout)
            sys.stdout.write("\n")
        else:
            for f in out:
                print(f"{f['severity'].upper():5} [{f['check']}] {f['dataset']} :: {f['message']}")
        return 1 if any(f["severity"] == "error" for f in out) else 0

    if args.cmd == "lint":
        out = lint_target(args.target)
        if args.json:
            json.dump(out, sys.stdout)
            sys.stdout.write("\n")
        else:
            for f in out:
                print(f"{f['severity'].upper():5} [{f['check']}] {f['dataset']} :: {f['message']}")
        return 1 if any(f["severity"] == "error" for f in out) else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
