"""Capture the spring-football (UFL/XFL) fixtures for the T7.3 port.

Scratch script (gitignored parent ``dev/``, force-added by the task). ONE run
regenerates every committed ``tests/fixtures/league_ports/{ufl,xfl,nfl}_*``
fixture the spring-football tests consume — see the "Regenerate-together"
note in ``tests/fixtures/league_ports/README.md``:

1. ``{xfl_summary,xfl_summary_2,xfl_summary_3,ufl_summary}.json`` — raw
   ``espn_{league}_summary(event_id, return_parsed=False)`` payloads for the
   PINNED event ids below. The ids were discovered by probing every completed
   event on the scoreboard date windows documented next to each id; XFL 2023
   events carry full ``drives.previous[].plays[]`` while NO completed UFL
   2024/2025 event does (``playByPlaySource`` never ``"full"`` — the summary
   has no ``drives`` key at all). The UFL fixture is therefore a REAL
   no-play-by-play capture, kept deliberately (the tests pin that finding).
2. A probe of ``espn_{ufl,xfl}_game_probabilities`` — both return HTTP 400
   ("Probabilities are not supported for sport: football, league: ...");
   printed as evidence for ``FEASIBILITY.md`` (nothing is written).
3. ``nfl_parity_2023_game.parquet`` — one real NFL 2023 game in nflverse
   shape for the gate-(a) byte-for-byte parity test: sliced from the local
   nfl-data producer output (``$SDV_VALIDATION_NFL_DATA_ROOT/out/
   model_pbp_2023.parquet``), model-OUTPUT columns stripped so the fixture
   is a pure model INPUT frame, and the ``roof`` one-hots
   (``retractable``/``dome``/``outdoors``) filled with the same
   unknown-roof default ``ep_wp`` itself uses (retractable=1).

Run:
    SDV_PY_LIVE_TESTS=1 uv run python dev/league_ports/capture_spring_football_fixtures.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "league_ports"

# Discovered via scoreboard probes (see module docstring):
#   XFL windows probed: 20230401 / 20230415 / 20230315 / 20200301
#   UFL windows probed: 20240330-20240616 / 20250321-20250608 (no event with plays)
PINNED_SUMMARIES: list[tuple[str, str, str]] = [
    # (league, event_id, fixture filename)
    ("xfl", "401517780", "xfl_summary.json"),  # 2023-04-01 Vegas 26, San Antonio 12
    ("xfl", "401517747", "xfl_summary_2.json"),  # 2023-03-12 Orlando 16, Houston 44
    ("xfl", "401517746", "xfl_summary_3.json"),  # 2023-03-12 Seattle 15, San Antonio 6
    ("ufl", "401638335", "ufl_summary.json"),  # 2024-06-01 Birmingham 20, Michigan 19 (no pbp)
]

NFL_PARITY_GAME = "2023_01_ARI_WAS"
NFL_PARITY_OUT = "nfl_parity_2023_game.parquet"
# enrich_nfl_pbp OUTPUT columns stripped from the parity input frame.
_NFL_MODEL_OUTPUT_COLS = {"ep", "epa", "wp", "vegas_wp", "wpa", "cp", "cpoe"}
_NFL_MODEL_OUTPUT_PREFIXES = ("xyac_",)


def _n_plays(summary: dict) -> int:
    drives = (summary.get("drives") or {}).get("previous") or []
    return sum(len(d.get("plays") or []) for d in drives if isinstance(d, dict))


def capture_summaries() -> None:
    from sportsdataverse.football.ufl import espn_ufl_summary
    from sportsdataverse.football.xfl import espn_xfl_summary

    summary_fns = {"ufl": espn_ufl_summary, "xfl": espn_xfl_summary}
    for league, event_id, fname in PINNED_SUMMARIES:
        summary = summary_fns[league](event_id, return_parsed=False)
        out_path = FIXTURE_DIR / fname
        out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"[{league}] event_id={event_id} plays={_n_plays(summary)} -> {out_path}")


def probe_probabilities() -> None:
    """Print the (expected-400) oracle responses — FEASIBILITY.md evidence.

    Probes BOTH candidate ESPN win-probability oracles: the per-play Core v2
    probabilities feed and the pregame Core v2 predictor. As of 2026-07-12
    both return HTTP 400 ("... not supported for sport: football, league:
    <ufl|xfl>") — see FEASIBILITY.md Finding 2.
    """
    from sportsdataverse.football.ufl import espn_ufl_game_predictor, espn_ufl_game_probabilities
    from sportsdataverse.football.xfl import espn_xfl_game_predictor, espn_xfl_game_probabilities

    prob_fns = {"ufl": espn_ufl_game_probabilities, "xfl": espn_xfl_game_probabilities}
    pred_fns = {"ufl": espn_ufl_game_predictor, "xfl": espn_xfl_game_predictor}
    for league, event_id, _ in PINNED_SUMMARIES:
        raw = prob_fns[league](event_id, return_parsed=False)
        print(f"[{league} probabilities {event_id}] {json.dumps(raw)[:200]}")
        raw = pred_fns[league](event_id, return_parsed=False)
        print(f"[{league} predictor {event_id}] {json.dumps(raw)[:200]}")


def build_nfl_parity_fixture() -> None:
    import polars as pl

    root = os.environ.get(
        "SDV_VALIDATION_NFL_DATA_ROOT",
        "C:/Users/saiem/Documents/GitHub-Data/sdv-dev/nflverse-dev/nfl-data",
    )
    src = Path(root) / "out" / "model_pbp_2023.parquet"
    df = pl.read_parquet(src).filter(pl.col("game_id") == NFL_PARITY_GAME)
    drop = [c for c in df.columns if c in _NFL_MODEL_OUTPUT_COLS or c.startswith(_NFL_MODEL_OUTPUT_PREFIXES)]
    df = df.drop(drop).with_columns(
        # roof is all-null in the producer output; use ep_wp's own
        # unknown-roof default one-hots (retractable=1, dome=0, outdoors=0).
        pl.lit(1, dtype=pl.Int8).alias("retractable"),
        pl.lit(0, dtype=pl.Int8).alias("dome"),
        pl.lit(0, dtype=pl.Int8).alias("outdoors"),
    )
    out_path = FIXTURE_DIR / NFL_PARITY_OUT
    df.write_parquet(out_path)
    print(f"[nfl] {NFL_PARITY_GAME}: {df.height} plays x {df.width} cols (dropped {drop}) -> {out_path}")


def main() -> None:
    if os.environ.get("SDV_PY_LIVE_TESTS") != "1":
        print("Set SDV_PY_LIVE_TESTS=1 to run this capture script.")
        sys.exit(1)
    capture_summaries()
    probe_probabilities()
    build_nfl_parity_fixture()


if __name__ == "__main__":
    main()
