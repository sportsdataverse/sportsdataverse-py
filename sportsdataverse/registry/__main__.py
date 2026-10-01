"""``python -m sportsdataverse.registry --ts --target gop|web`` -- the TypeScript module on stdout."""

from __future__ import annotations

import argparse
import sys

from sportsdataverse.registry import load_metric_registry, render_ts


def main(argv: list[str] | None = None) -> int:
    """Render the metric registry for a TypeScript consumer onto stdout.

    Args:
        argv: the command-line arguments (``None`` reads ``sys.argv``).
            ``--target gop|web`` is required; ``--ts`` names the only output
            format and may be omitted.

    Returns:
        int: ``0`` -- the module was written to stdout.

    Raises:
        SystemExit: Raised by argparse (code 2) on a missing or unknown
            ``--target`` or an unknown option.
        ValueError: If the packaged ``metrics.yaml`` fails validation.
    """
    parser = argparse.ArgumentParser(
        prog="python -m sportsdataverse.registry",
        description="Render the metric registry (sportsdataverse/registry/metrics.yaml) for a TypeScript consumer.",
    )
    parser.add_argument("--ts", action="store_true", help="emit the TypeScript module (the only format; implied)")
    parser.add_argument("--target", choices=("gop", "web"), required=True, help="indent style of the consumer")
    args = parser.parse_args(argv)
    sys.stdout.write(render_ts(load_metric_registry(), target=args.target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
