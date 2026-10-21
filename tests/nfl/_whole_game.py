"""Run ``NFLPlayProcess`` offline over the 37 committed ESPN captures, once per pytest worker.

Shared by ``test_nfl_whole_game_oracle.py`` and ``test_nfl_schema_allowlist.py`` (both are pinned
to one xdist worker with ``xdist_group("nfl_whole_game")`` so the cache is shared). Provenance of
every capture: ``tests/nfl/fixtures/README.md``.

Hermetic by default: the two download-on-demand nfl4th boosters (``fd_model.ubj``,
``wp_model.ubj``) are never loaded, whether or not a local cache holds them, so every machine
checks the same columns and nothing reaches the network. Their outputs (the fourth-down and
two-point decision columns) are null in these runs and no oracle test reads them; a test that
needs them asks for ``run(game_id, fourth_down_models=True)`` and skips when they cannot load.
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path
from typing import NamedTuple

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_fourth_down as nfl_fourth_down
import sportsdataverse.nfl.nfl_pbp as nfl_pbp
from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures"

# The 36 trimmed summaries (30 from nfl-raw, 6 fetched live) plus the one full summary (401872922).
CAPTURES: tuple[int, ...] = tuple(sorted(int(p.name.split("_")[1].split(".")[0]) for p in FIX.glob("summary_*.json*")))


class Run(NamedTuple):
    plays: pl.DataFrame  # proc.plays_frame, sorted by game_play_number
    game: dict  # run_processing_pipeline()'s return (advBoxScore, ...)
    summary: dict  # the capture as committed (ESPN's own boxscore is an oracle)
    epa_input: pl.DataFrame  # the frame the pipeline handed to ep_wp.calculate_epa
    wpa_input: pl.DataFrame  # the frame the pipeline handed to ep_wp.calculate_wpa
    proc: NFLPlayProcess  # the processor itself (Game on Paper re-runs create_box_score on plays_frame)


def load_summary(game_id: int) -> dict:
    gz = FIX / f"summary_{game_id}_trimmed.json.gz"
    if gz.exists():
        with gzip.open(gz, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads((FIX / f"summary_{game_id}.json").read_text(encoding="utf-8"))


@functools.cache
def run(game_id: int, fourth_down_models: bool = False) -> Run:
    summary = load_summary(game_id)
    seen: dict[str, pl.DataFrame] = {}

    def record(name, fn):
        def wrapper(df, *a, **k):
            seen[name] = df
            return fn(df, *a, **k)

        return wrapper

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_pbp, "calculate_epa", record("epa", nfl_pbp.calculate_epa))
        mp.setattr(nfl_pbp, "calculate_wpa", record("wpa", nfl_pbp.calculate_wpa))
        if not fourth_down_models:  # the download-on-demand resolver; bundled models load as usual
            mp.setattr(nfl_fourth_down, "_try_load", lambda name: None)
        proc = NFLPlayProcess(gameId=game_id)
        proc.espn_nfl_pbp(summary=summary)  # a supplied summary never touches the network
        game = proc.run_processing_pipeline()
    return Run(proc.plays_frame.sort("game_play_number"), game, summary, seen["epa"], seen["wpa"], proc)
