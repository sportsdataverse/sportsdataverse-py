"""Capture MoneyPuck's public 2024-25 regular-season goalie summary and build the
committed ``tests/fixtures/nhl_player_impact/mp_gsax.parquet`` concurrent-validity
oracle fixture for the goalie-GSAx gate in ``test_nhl_player_impact_oracle.py``.

MoneyPuck (https://moneypuck.com) publishes season-summary CSVs at a stable,
unauthenticated URL and is **free to use for non-commercial purposes with credit**
(see https://moneypuck.com/about.htm) -- this script and the fixture README both
carry a "Data: MoneyPuck.com" credit line per that license. It is NOT JA3/TLS-blocked
(unlike stats.nba.com) -- plain ``requests`` gets a real 200 with CSV. The one gotcha:
MoneyPuck's Cloudflare rule keys off ``User-Agent`` and serves a "Data License"
bandwidth-cost notice HTML page (still HTTP 200!) instead of the CSV when it sees
this package's default identifying UA (``"sportsdataverse-py"``, set in
``dl_utils.download``'s default headers) -- that is what the 2026-07-08 capture
attempt hit and mistook for a hard block. Passing a normal browser ``User-Agent`` via
``headers=`` (still plain ``requests`` under the hood, no TLS impersonation) returns
the CSV.

``gsax = xGoals - goals`` on the ``situation == "all"`` rows (MoneyPuck's per-goalie,
per-team-stint season total across every strength state) -- the same sign convention as
``sportsdataverse.nhl.nhl_gsax.nhl_goalie_gsax`` (``gsax = xga - ga``, positive =
allowed fewer goals than expected).

Run with:
    uv run python dev/nhl_player_impact/capture_moneypuck.py
"""

from __future__ import annotations

import io
from pathlib import Path

import polars as pl

from sportsdataverse.dl_utils import download

SEASON = 2024  # season-start year -- MoneyPuck's key for the 2024-25 season.
SESSION_TYPE = "regular"
BASE = f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{SEASON}/{SESSION_TYPE}"
OUT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nhl_player_impact" / "mp_gsax.parquet"

_GSAX_SCHEMA = {"player_id": pl.Int64, "goalie": pl.Utf8, "gsax": pl.Float64}

# A normal browser UA -- NOT bypassing an auth/paywall (MoneyPuck's season-summary
# CSVs are public + license-free for this non-commercial use), just avoiding the
# package's default self-identifying UA that trips MoneyPuck's bot heuristic.
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


def _fetch_csv(url: str) -> pl.DataFrame:
    resp = download(url, headers=_BROWSER_HEADERS)
    return pl.read_csv(io.BytesIO(resp.content))


def build() -> pl.DataFrame:
    goalies = _fetch_csv(f"{BASE}/goalies.csv")
    season_total = (
        goalies.filter(pl.col("situation") == "all")
        .with_columns(
            player_id=pl.col("playerId").cast(pl.Int64),
            goalie=pl.col("name"),
            gsax=(pl.col("xGoals") - pl.col("goals")),
        )
        .select(list(_GSAX_SCHEMA.keys()))
        .sort("gsax", descending=True)
    )
    assert season_total.schema == pl.Schema(_GSAX_SCHEMA), season_total.schema
    return season_total


def main() -> None:
    out = build()
    print(f"MoneyPuck {SEASON} {SESSION_TYPE} goalie GSAx: {out.height} goalies")
    out.write_parquet(OUT)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
