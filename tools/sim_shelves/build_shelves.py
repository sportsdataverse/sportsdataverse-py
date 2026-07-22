"""Multi-game shelf builder — widen the sim PMFs beyond the test fixtures.

The committed single-game fixtures make the gates honest but leave the PMFs
thin (the widest band driver). This CLI classifies MANY real games into the
durable artifact — the classified-events parquet, from which every sport's
shelf rebuilds in milliseconds — and stamps a WS1 publish-audit fingerprint
sidecar for staleness tracking.

Two input modes:

* ``--from-files`` — local payload JSONs (offline; the committed fixtures
  work as smoke inputs);
* ``--live`` — fetch by id through the package's own wrappers (user-run;
  stats.nba.com requires a residential IP — see the live-test gates).

Usage::

    uv run python -m tools.sim_shelves.build_shelves nba \\
        --from-files a.json b.json --out dev/shelves/nba_events.parquet
    uv run python -m tools.sim_shelves.build_shelves wnba \\
        --live 401620342 401620358 --out dev/shelves/wnba_events.parquet
    uv run python -m tools.sim_shelves.build_shelves nba \\
        --from-files a.json --out shelf.parquet --shelf --models

Basketball sports additionally accept ``--shelf`` (write the built Shelf
parquet instead of raw events) and ``--models`` (the ``models2shelf``
zero-fallback grid build).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

from sportsdataverse._common.publish_audit import fingerprint_parquet, write_fingerprint
from sportsdataverse.modeling.registry.model_registry import make_card, write_manifest

BASKETBALL_V3 = ("nba", "nbagl")
BASKETBALL_ESPN = ("wnba", "mbb", "wbb")
FOOTBALL = ("nfl", "cfb")
SPORTS = (*BASKETBALL_V3, *BASKETBALL_ESPN, *FOOTBALL, "mlb", "nhl")


def _load_files(paths: List[str]) -> List[Dict[str, Any]]:
    return [json.loads(Path(p).read_text(encoding="utf-8")) for p in paths]


def _fetch_live(sport: str, ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch payloads by game/event id through the package wrappers."""
    payloads: List[Dict[str, Any]] = []
    for game_id in ids:
        if sport in BASKETBALL_V3:
            from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3

            payloads.append(nba_stats_playbyplayv3(game_id=str(game_id), return_parsed=False))
        elif sport == "wnba":
            from sportsdataverse.wnba import espn_wnba_summary

            payloads.append(espn_wnba_summary(event_id=game_id))
        elif sport == "mbb":
            from sportsdataverse.mbb import espn_mbb_summary

            payloads.append(espn_mbb_summary(event_id=game_id))
        elif sport == "wbb":
            from sportsdataverse.wbb import espn_wbb_summary

            payloads.append(espn_wbb_summary(event_id=game_id))
        elif sport == "nfl":
            from sportsdataverse.nfl import espn_nfl_summary

            payloads.append(espn_nfl_summary(event_id=game_id))
        elif sport == "cfb":
            from sportsdataverse.cfb import espn_cfb_summary

            payloads.append(espn_cfb_summary(event_id=game_id))
        elif sport == "mlb":
            from sportsdataverse.mlb import mlb_api_game_play_by_play

            payloads.append(mlb_api_game_play_by_play(game_pk=int(game_id)))
        elif sport == "nhl":
            from sportsdataverse.nhl import nhl_web_pbp

            payloads.append(nhl_web_pbp(game_id=int(game_id)))
    return payloads


def _classify(sport: str, payloads: List[Dict[str, Any]]) -> pl.DataFrame:
    """Route payloads through the sport's committed classifier."""
    if sport in BASKETBALL_V3:
        from sportsdataverse.nba.nba_possession_sim import possessions_from_pbp

        frames: List[pl.DataFrame] = []
        for payload in payloads:
            game = payload.get("game") or payload
            acts = game.get("actions") or []
            gid = str(game.get("gameId") or len(frames))
            frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
        return possessions_from_pbp(pl.concat(frames, how="diagonal_relaxed"))
    if sport in BASKETBALL_ESPN:
        from sportsdataverse.nba.nba_possession_sim.espn_adapter import espn_summary_to_events

        return pl.concat(
            [espn_summary_to_events(p, game_id=str(i)) for i, p in enumerate(payloads)],
            how="vertical",
        )
    if sport in FOOTBALL:
        from sportsdataverse.nfl.nfl_drive_sim import plays_from_espn_drives

        return pl.concat(
            [plays_from_espn_drives(p).with_columns(pl.lit(str(i)).alias("game_id")) for i, p in enumerate(payloads)],
            how="vertical",
        )
    if sport == "mlb":
        from sportsdataverse.mlb.mlb_at_bat_sim import at_bats_from_pbp

        return pl.concat(
            [at_bats_from_pbp(p).with_columns(pl.lit(str(i)).alias("game_id")) for i, p in enumerate(payloads)],
            how="vertical",
        )
    from sportsdataverse.nhl.nhl_game_sim import events_from_nhl_pbp

    return pl.concat(
        [events_from_nhl_pbp(p).with_columns(pl.lit(str(i)).alias("game_id")) for i, p in enumerate(payloads)],
        how="vertical",
    )


def main(argv: Optional[List[str]] = None) -> int:
    """Build multi-game classified-events artifacts (and basketball shelves)."""
    parser = argparse.ArgumentParser(prog="build_shelves", description=__doc__)
    parser.add_argument("sport", choices=SPORTS)
    parser.add_argument("--from-files", nargs="+", help="Local payload JSON paths.")
    parser.add_argument("--live", nargs="+", help="Game/event ids to fetch live.")
    parser.add_argument("--out", required=True, help="Output parquet path.")
    parser.add_argument(
        "--shelf",
        action="store_true",
        help="Basketball only: write the built Shelf parquet instead of events.",
    )
    parser.add_argument(
        "--models",
        action="store_true",
        help="Basketball only (with --shelf): models2shelf zero-fallback build.",
    )
    args = parser.parse_args(argv)

    if not args.from_files and not args.live:
        parser.error("provide --from-files and/or --live")
    payloads = _load_files(args.from_files or [])
    if args.live:
        payloads += _fetch_live(args.sport, args.live)

    events = _classify(args.sport, payloads)
    if events.height == 0:
        print("build_shelves: classified zero events", file=sys.stderr)
        return 2

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    basketball = args.sport in (*BASKETBALL_V3, *BASKETBALL_ESPN)
    if args.shelf and basketball:
        from sportsdataverse.nba.nba_possession_sim import build_shelf, shelf_to_parquet
        from sportsdataverse.nba.nba_possession_sim.node_models import models_to_shelf

        shelf = models_to_shelf(events) if args.models else build_shelf(events)
        shelf_to_parquet(shelf, out)
    else:
        events.write_parquet(out)
    sidecar = write_fingerprint(out, fingerprint_parquet(out))
    games = events["game_id"].n_unique() if "game_id" in events.columns else len(payloads)
    # registry card: fingerprint the classified-events frame that fed the
    # artifact so a rebuild can name exactly WHICH input columns drifted
    # (feature_drift) instead of just observing a different output hash
    card = make_card(
        f"{args.sport}_{'shelf' if args.shelf and basketball else 'events'}",
        events,
        features=list(events.columns),
        training_script="tools/sim_shelves/build_shelves.py",
        trained_seasons=[],
        metrics={"n_events": float(events.height), "n_games": float(games)},
    )
    card_path = Path(str(out) + ".card.json")
    write_manifest(card_path, [card])
    print(f"build_shelves: {args.sport} | {events.height} events / {games} games -> {out}")
    print(f"build_shelves: fingerprint sidecar {sidecar}")
    print(f"build_shelves: registry card {card_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
