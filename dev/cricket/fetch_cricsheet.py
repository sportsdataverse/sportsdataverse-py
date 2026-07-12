"""Cricsheet ball-by-ball ingester (dev-only, T7.3 cricket models).

Pure-Python (no R runtime dependency) reader for the Cricsheet per-competition
JSON zips. Downloads ``t20s_json.zip`` / ``odis_json.zip`` from cricsheet.org
(cached under ``dev/cricket/.cache/``, gitignored) and parses the nested
``innings[].overs[].deliveries[]`` structure into a flat per-legal-ball polars
corpus used to fit the win-probability resource surface and build the committed
calibration/holdout fixtures.

Only the fitted artifact (``cricket_resource_surface.parquet``) and the small
holdout fixture are committed; the raw zips + full corpus stay local.

Usage::

    uv run python dev/cricket/fetch_cricsheet.py            # download + build both formats
    uv run python dev/cricket/fetch_cricsheet.py --stats     # print corpus summary

Schema of :func:`build_corpus` (one row per LEGAL ball, both innings):

    match_id:Utf8, fmt:Utf8, season_year:Int64, innings_number:Int64,
    batting_team:Utf8, bowling_team:Utf8, balls_total:Int64, legal_balls:Int64,
    runs:Int64, wickets:Int64, target:Int64|null, innings_final_runs:Int64,
    innings_final_balls:Int64, batting_team_won:Int64|null
"""

from __future__ import annotations

import argparse
import io
import json
import os
import urllib.request
import zipfile
from pathlib import Path

import polars as pl

CACHE = Path(__file__).resolve().parent / ".cache"
CRICSHEET_URL = "https://cricsheet.org/downloads/{stem}_json.zip"

# (format slug, cricsheet stem, standard innings over-limit)
FORMATS: dict[str, tuple[str, int]] = {
    "t20": ("t20s", 20),
    "odi": ("odis", 50),
}

CORPUS_SCHEMA: dict[str, pl.DataType] = {
    "match_id": pl.Utf8,
    "fmt": pl.Utf8,
    "season_year": pl.Int64,
    "innings_number": pl.Int64,
    "batting_team": pl.Utf8,
    "bowling_team": pl.Utf8,
    "balls_total": pl.Int64,
    "legal_balls": pl.Int64,
    "runs": pl.Int64,
    "wickets": pl.Int64,
    "target": pl.Int64,
    "innings_final_runs": pl.Int64,
    "innings_final_balls": pl.Int64,
    "batting_team_won": pl.Int64,
}


def download_zip(fmt: str, *, force: bool = False) -> Path:
    """Download the Cricsheet JSON zip for a format into the cache (idempotent)."""
    stem, _ = FORMATS[fmt]
    CACHE.mkdir(parents=True, exist_ok=True)
    dest = CACHE / f"{stem}_json.zip"
    if dest.exists() and not force:
        return dest
    url = CRICSHEET_URL.format(stem=stem)
    print(f"downloading {url} -> {dest}")
    with urllib.request.urlopen(url, timeout=300) as resp:  # noqa: S310 (trusted static host)
        data = resp.read()
    dest.write_bytes(data)
    return dest


def _season_year(season: object) -> int:
    """Leading calendar year of a Cricsheet season string (``"2017/18"`` -> 2017)."""
    s = str(season)
    return int(s[:4]) if s[:4].isdigit() else 0


def _is_legal(delivery: dict) -> bool:
    """A delivery is a legal ball unless it is a wide or a no-ball."""
    extras = delivery.get("extras") or {}
    return "wides" not in extras and "noballs" not in extras


def parse_match(match: dict, match_id: str, fmt: str, over_limit: int) -> list[dict]:
    """Flatten one Cricsheet match into per-legal-ball state rows (both innings).

    Returns an empty list for matches that fail the homogeneity filters
    (wrong gender handled by the caller, non-standard over limit, non-6 ball
    overs). ``batting_team_won`` is null when the match has no clean winner
    (tie / no-result / super-over eliminator).
    """
    info = match["info"]
    if info.get("balls_per_over", 6) != 6:
        return []
    if info.get("overs") != over_limit:
        return []
    balls_total = over_limit * 6
    winner = info.get("outcome", {}).get("winner")  # None for tie/no-result
    season_year = _season_year(info.get("season"))
    teams = info.get("teams", [])

    rows: list[dict] = []
    for i, inn in enumerate(match.get("innings", []), start=1):
        batting_team = inn.get("team")
        bowling_team = next((t for t in teams if t != batting_team), None)
        target = inn.get("target", {}).get("runs")
        # First pass: final totals for this innings.
        runs = wkts = legal = 0
        ball_rows: list[dict] = []
        for ov in inn.get("overs", []):
            for d in ov.get("deliveries", []):
                runs += int(d.get("runs", {}).get("total", 0))
                wkts += len(d.get("wickets", []))
                if _is_legal(d):
                    legal += 1
                    ball_rows.append({"legal_balls": legal, "runs": runs, "wickets": min(wkts, 10)})
        if not ball_rows:
            continue
        final_runs = ball_rows[-1]["runs"]
        final_balls = ball_rows[-1]["legal_balls"]
        won = None if winner is None else int(batting_team == winner)
        for br in ball_rows:
            rows.append(
                {
                    "match_id": match_id,
                    "fmt": fmt,
                    "season_year": season_year,
                    "innings_number": i,
                    "batting_team": batting_team,
                    "bowling_team": bowling_team,
                    "balls_total": balls_total,
                    "legal_balls": br["legal_balls"],
                    "runs": br["runs"],
                    "wickets": br["wickets"],
                    "target": int(target) if target is not None else None,
                    "innings_final_runs": final_runs,
                    "innings_final_balls": final_balls,
                    "batting_team_won": won,
                }
            )
    return rows


def build_corpus(fmt: str, *, gender: str = "male", limit: int | None = None) -> pl.DataFrame:
    """Build the per-legal-ball corpus for a format from the cached zip."""
    _, over_limit = FORMATS[fmt]
    zpath = download_zip(fmt)
    all_rows: list[dict] = []
    with zipfile.ZipFile(zpath) as zf:
        names = [n for n in zf.namelist() if n.endswith(".json")]
        if limit is not None:
            names = names[:limit]
        for n in names:
            match = json.loads(zf.read(n))
            if match.get("info", {}).get("gender") != gender:
                continue
            match_id = os.path.splitext(os.path.basename(n))[0]
            all_rows.extend(parse_match(match, match_id, fmt, over_limit))
    if not all_rows:
        return pl.DataFrame(schema=CORPUS_SCHEMA)
    return pl.DataFrame(all_rows, schema=CORPUS_SCHEMA)


def build_all(gender: str = "male") -> pl.DataFrame:
    """Corpus across all supported formats, vertically concatenated."""
    frames = [build_corpus(fmt, gender=gender) for fmt in FORMATS]
    return pl.concat(frames, how="vertical")


def _print_stats(df: pl.DataFrame) -> None:
    buf = io.StringIO()
    per_fmt = df.group_by("fmt").agg(
        pl.col("match_id").n_unique().alias("matches"),
        pl.len().alias("deliveries"),
        pl.col("season_year").min().alias("first_season"),
        pl.col("season_year").max().alias("last_season"),
    )
    print("=== corpus summary ===", file=buf)
    print(per_fmt.sort("fmt"), file=buf)
    print(buf.getvalue())


def main() -> None:
    ap = argparse.ArgumentParser(description="Cricsheet ball-by-ball ingester")
    ap.add_argument("--gender", default="male")
    ap.add_argument("--stats", action="store_true", help="print corpus summary only")
    args = ap.parse_args()
    df = build_all(gender=args.gender)
    _print_stats(df)
    if not args.stats:
        out = CACHE / "deliveries_all.parquet"
        df.write_parquet(out)
        print(f"wrote {out} ({df.height:,} rows)")


if __name__ == "__main__":
    main()
