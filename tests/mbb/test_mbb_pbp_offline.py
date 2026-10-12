"""``espn_mbb_pbp`` offline on a committed ESPN capture (C-L18: the module's only other test is live-gated).

The capture is 401830342, UTU @ MVSU, 2025-11-15, double overtime (provenance:
``tests/fixtures/espn/basketball_pbp/README.md``). Every value is read back against the
raw ESPN payload, never against stored processor output.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import polars as pl

from sportsdataverse.mbb import espn_mbb_pbp, mbb_pbp

CAPTURE = Path(__file__).resolve().parents[1] / "fixtures" / "espn" / "basketball_pbp" / "mbb_401830342.json.gz"


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return copy.deepcopy(self._payload)


def test_mbb_pbp_offline_reads_every_play_off_the_raw_clock(monkeypatch) -> None:
    with gzip.open(CAPTURE, "rt", encoding="utf-8") as f:
        raw = json.load(f)
    monkeypatch.setattr(mbb_pbp, "download", lambda url, **kwargs: _Resp(raw))
    out = espn_mbb_pbp(game_id=401830342)
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)

    assert out["gameId"] == 401830342
    assert plays["id"].to_list() == [int(p["id"]) for p in raw["plays"]]
    assert plays["period.number"].to_list() == [p["period"]["number"] for p in raw["plays"]]
    assert plays["period.number"].max() == 4  # two halves + 2OT

    clock = [60 * int(m) + int(s) for m, s in (p["clock"]["displayValue"].split(":") for p in raw["plays"])]
    assert plays["start.period_seconds_remaining"].to_list() == clock
    assert plays["start.game_seconds_remaining"].to_list() == [
        c + 1200 if p["period"]["number"] == 1 else c for c, p in zip(clock, raw["plays"])
    ]
    assert (plays["end.game_seconds_remaining"] <= plays["start.game_seconds_remaining"]).all()

    final = {int(t["id"]): int(t["score"]) for t in raw["header"]["competitions"][0]["competitors"]}
    last = plays.row(-1, named=True)
    assert (last["homeScore"], last["awayScore"]) == (final[last["homeTeamId"]], final[last["awayTeamId"]])
