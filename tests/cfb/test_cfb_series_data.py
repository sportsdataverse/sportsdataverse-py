"""R-oracle parity tests for the cfbfastR series / first-down decomposition port.

Provenance of the ported logic (cfbfastR sibling checkout):
    * ``R/pbp_prep_epa_df_after.R`` L194-298 -- ``first_by_penalty`` /
      ``first_by_yards``, their lag ladder, ``new_series``, and the four
      ``firstD_by_*`` outputs.
    * ``R/pbp_clean_drive_dat.R`` L18-66 + L300-312 -- half-scoped lag(1..3)
      event flags and ``drive_event_number``.
    * ``R/pbp_clean_pbp_dat.R`` L388-390 -- ungrouped ``lag_play_type{,2}``.

The expected CSVs were produced by ``dev/boxscore_parity/series_oracle.R``
(R 4.6.1, dplyr) applying that logic VERBATIM to the input CSVs, which are
real ESPN plays dumped from the offline pipeline by
``dev/boxscore_parity/make_series_fixture.py``. Flags are 0/1 so equality is
exact -- no correlation thresholds. R leaves ``firstD_by_yards``/``new_series``
``NA`` where an unfilled lag2/lag3 feeds the condition (rows 1-2 of a half);
the port emits ``False`` there, so the comparison folds R ``NA`` to 0.
"""

from __future__ import annotations

import csv
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

SERIES_FIX = Path(__file__).parent / "fixtures" / "series"
GIDS = [400869270, 401135269, 401309854, 401754598]  # 2016 / 2019 / 2021 / 2024
OUT_COLS = ["new_series", "firstD_by_kickoff", "firstD_by_poss", "firstD_by_penalty", "firstD_by_yards"]


def _read_rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _input_frame(gid: int) -> pl.DataFrame:
    rows = _read_rows(SERIES_FIX / f"series_input_{gid}.csv")
    return pl.DataFrame(
        {
            "type.text": [r["play_type"] for r in rows],
            "statYardage": [int(r["yards_gained"]) for r in rows],
            "start.distance": [int(r["distance"]) for r in rows],
            "start.down": [int(r["down"]) for r in rows],
            "half": [int(r["half"]) for r in rows],
            "drive.id": [r["id_drive"] for r in rows],
            "penalty_1st_conv": [r["penalty_1st_conv"] == "true" for r in rows],
            "penalty_offset": [r["penalty_offset"] == "true" for r in rows],
            "penalty_declined": [r["penalty_declined"] == "true" for r in rows],
            "kickoff_play": [r["kickoff_play"] == "1" for r in rows],
            "punt": [r["punt"] == "1" for r in rows],
            "downs_turnover": [r["downs_turnover"] == "1" for r in rows],
            "turnover_vec": [r["turnover_vec"] == "1" for r in rows],
            "scoring_play": [r["scoring_play"] == "1" for r in rows],
            "change_of_pos_team": [r["change_of_pos_team"] == "1" for r in rows],
        },
    )


@pytest.mark.parametrize("gid", GIDS)
def test_series_data_matches_r_oracle(gid: int):
    df = _input_frame(gid)
    proc = CFBPlayProcess(gameId=gid)
    out = proc._CFBPlayProcess__add_series_data(df)
    expected = _read_rows(SERIES_FIX / f"series_expected_{gid}.csv")
    assert out.height == len(expected)
    for col in OUT_COLS:
        got = out.get_column(col).cast(pl.Int32).to_list()
        want = [0 if r[col] == "NA" else int(r[col]) for r in expected]
        diffs = [i for i, (g, w) in enumerate(zip(got, want)) if g != w]
        assert not diffs, f"{gid} {col}: {len(diffs)} mismatches vs R oracle at rows {diffs[:10]}"


def test_series_columns_in_pipeline(monkeypatch):
    """The series stage is wired into run_processing_pipeline and emits Booleans."""
    import json

    summary = json.loads(
        (Path(__file__).parent / "fixtures" / "summary_401754598.json").read_text(encoding="utf-8"),
    )

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598, join_participants=False)
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline(fourth_down_probs=False, two_pt_probs=False)
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    for col in OUT_COLS:
        assert col in df.columns, f"missing {col}"
        assert df.schema[col] == pl.Boolean, f"{col} is {df.schema[col]}, expected Boolean"
    # every drive's first row starts a new series
    assert df.get_column("new_series").any()
