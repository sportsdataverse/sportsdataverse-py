"""``python -m sportsdataverse.registry --ts --target gop|web`` -- the TypeScript module on stdout."""

from __future__ import annotations

import argparse
import sys

from sportsdataverse.registry import load_metric_registry, render_ts


def main(argv: list[str] | None = None) -> int:
    """Render the metric registry for a consumer; returns the exit code."""
    parser = argparse.ArgumentParser(
        prog="python -m sportsdataverse.registry",
        description="Render the metric registry (sportsdataverse/registry/metrics.yaml) for a TypeScript consumer.",
    )
    parser.add_argument("--ts", action="store_true", required=True, help="emit the TypeScript module (the only format)")
    parser.add_argument("--target", choices=("gop", "web"), required=True, help="indent style of the consumer")
    args = parser.parse_args(argv)
    sys.stdout.write(render_ts(load_metric_registry(), target=args.target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
