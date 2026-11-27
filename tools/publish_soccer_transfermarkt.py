"""Mirror the CC0 Transfermarkt tables to the ``soccer_transfermarkt`` sportsdataverse-data release.

Reads each table from its original published gzip CSV through the loaders' own source path (so the mirror carries
exactly the dtypes ``load_soccer_transfermarkt_*`` serve), writes ``<table>.parquet``, a ``SHA256SUMS`` file and a
``README.md`` naming the source, the CC0 1.0 licence and the snapshot date, then uploads them (``--upload``).

Run::

    uv run python tools/publish_soccer_transfermarkt.py --out build/soccer_transfermarkt            # build only
    uv run python tools/publish_soccer_transfermarkt.py --out build/soccer_transfermarkt --upload   # and publish
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import warnings
from pathlib import Path

TAG = "soccer_transfermarkt"
SNAPSHOT = "2026-07-06"

README = """# soccer_transfermarkt

A mirror of the Transfermarkt dataset, five tables, read by sportsdataverse-py's `load_soccer_transfermarkt_*`.

- Source: https://github.com/dcaribou/transfermarkt-datasets (files at {source})
- Licence: CC0 1.0 Universal (public domain dedication), the licence the source publishes the data under.
- Snapshot: the source's data are current to {snapshot} and are no longer being refreshed.
- Mirrored: {mirrored} (UTC). Values are the source's; typed columns are stripped of surrounding whitespace, an
  empty cell is null, ids are Int64 and dates Date (see the loader docs). `clubs` drops the producer's raw-file path
  and the `total_market_value` column, which is empty in every row.

| file | rows | sha256 |
|---|---|---|
{rows}
"""


def build(out: Path) -> list[Path]:
    from datetime import datetime, timezone

    import polars as pl

    from sportsdataverse.errors import SchemaDriftWarning
    from sportsdataverse.soccer import soccer_transfermarkt as tm

    out.mkdir(parents=True, exist_ok=True)
    files, sums, rows = [], [], []
    for table in tm._TABLES:
        with warnings.catch_warnings():
            warnings.simplefilter("error", SchemaDriftWarning)  # never publish a column the loader would refuse
            df = tm._typed(tm._fetch(table), table)
        if df.schema != pl.Schema(tm._TABLES[table]) or df.is_empty():
            raise SystemExit(f"{table}: schema or row count is not what the loader expects; nothing published")
        path = out / f"{table}.parquet"
        df.write_parquet(path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        files.append(path)
        sums.append(f"{digest}  {path.name}")
        rows.append(f"| {path.name} | {df.height:,} | `{digest}` |")
        print(f"{path.name}: {df.height:,} rows, {digest}")
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8", newline="\n")
    (out / "README.md").write_text(
        README.format(
            source=tm.TRANSFERMARKT_DATA_URL.format(table="<table>"),
            snapshot=SNAPSHOT,
            mirrored=datetime.now(timezone.utc).date().isoformat(),
            rows="\n".join(rows),
        ),
        encoding="utf-8",
        newline="\n",
    )
    return [*files, out / "SHA256SUMS", out / "README.md"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--upload", action="store_true", help=f"upload to the {TAG} release (created if absent)")
    args = ap.parse_args(argv)
    files = build(args.out)
    if args.upload:
        import subprocess

        from sportsdataverse.release import _gh_env, _invoke_gh, sportsdataverse_upload

        view = subprocess.run(
            ["gh", "release", "view", TAG, "-R", "sportsdataverse/sportsdataverse-data"],
            capture_output=True, text=True, env=_gh_env(),
        )  # fmt: skip
        if view.returncode and "not found" not in view.stderr.lower():
            raise SystemExit(f"gh release view {TAG} failed: {view.stderr.strip()}")  # auth or network, not absence
        if view.returncode:
            _invoke_gh(
                ["release", "create", TAG, "-R", "sportsdataverse/sportsdataverse-data", "--title", TAG, "--notes",
                 f"Transfermarkt dataset mirror (CC0 1.0), snapshot {SNAPSHOT}. See README.md."]
            )  # fmt: skip
        sportsdataverse_upload(files, TAG, pkg_function="sportsdataverse.soccer.load_soccer_transfermarkt_*()")
    return 0


if __name__ == "__main__":
    sys.exit(main())
