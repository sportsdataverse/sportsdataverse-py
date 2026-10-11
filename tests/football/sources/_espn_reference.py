"""ESPN-reference parity for the adapters that had none (supp-012): Fox CFB, Fox NFL, NCAA.

Each adapter's game is also run through the unmodified processor from ESPN's own summary of the same
game (captured 2026-10-11 from `site.api.espn.com/apis/site/v2/sports/football/{league}/summary`,
committed under `tests/{cfb,nfl}/fixtures/espn_reference/`, README there), with one closing line on
both sides, and paired the way the Yahoo/CBS finals gates pair: on the clock-free state key, after
dropping clock-stoppage rows and keys that occur twice in a game.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl

from sportsdataverse.football.sources.dispatch import _process_game
from sportsdataverse.football.sources.parity import ParityReport, _compare_plays

TESTS = Path(__file__).resolve().parents[2]
KEY = ("period", "start.down", "start.distance", "start.yardsToEndzone", "start.pos_team.id")
ADMIN = (
    "Timeout",
    "End Period",
    "End of Half",
    "End of Game",
    "End of Regulation",
    "Official Timeout",
    "Two-minute warning",
)
NUMERIC = ("EPA", "EP_start", "EP_end", "wp_before", "wp_after")
COLUMNS = (
    "type.id",
    "type.text",
    "type.abbreviation",
    "start.yardLine",
    "end.pos_team.id",
    "rush",
    "pass",
    "td_play",
    *NUMERIC,
)


def espn_summary(league: str, game_id: int) -> dict:
    with gzip.open(
        TESTS / league / "fixtures" / "espn_reference" / f"summary_{game_id}.json.gz", "rt", encoding="utf-8"
    ) as fh:
        return json.load(fh)


def _snaps(frame: pl.DataFrame) -> pl.DataFrame:
    frame = frame.filter(~pl.col("type.text").is_in(ADMIN))
    return frame.filter(pl.struct(list(KEY)).count().over(list(KEY)) == 1)


def espn_reference_report(
    league: str, game_id: int, source: str, payload: dict, row: dict, odds: dict | None
) -> ParityReport:
    kw = {"fallthrough": False, "idmap_row": row, "odds_override": odds}
    reference = _process_game(league, game_id, source="espn", payloads={"espn": espn_summary(league, game_id)}, **kw)
    candidate = _process_game(league, game_id, source=source, payloads={source: payload}, **kw)
    assert candidate.provenance["served"] == source
    return _compare_plays(
        _snaps(reference.plays_frame), _snaps(candidate.plays_frame), key=KEY, columns=COLUMNS, numeric=NUMERIC
    )
