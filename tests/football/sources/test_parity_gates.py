"""What the Yahoo / CBS finals gates do not score (supp-003; ledger gate adapter-parity-gates).

Both finals gates (`tests/cfb/{yahoo,cbs}_pbp/test_to_espn_summary.py`) correlate the PAIRED unambiguous
snaps of Ohio State at Texas (ESPN 401856682) and drop everything else: the clock-stoppage ("admin")
rows, the snaps whose state key the other feed does not carry (unpaired), and the keys that occur twice.
An adapter that drops or mis-scores an unpaired scoring play cannot fail them, while the team EPA
totals the summary and ratings producers consume diverge by ~4.6 points (TEX 8.92 Yahoo / 9.17 CBS vs
4.36 ESPN). Here the remainder is asserted explicitly on the same committed payloads, offline:

* every admin row is scored on both sides (non-null EPA, exactly 0: the state carries over) and every
  unpaired snap carries a non-null EPA;
* the unpaired snaps' EPA sums are pinned at the observed gap (2026-10-11: ESPN 7.861 vs Yahoo 3.557,
  ESPN 4.519 vs CBS 4.725); never raised;
* the whole-game team EPA totals agree within 1.0 per team. Green since task 3.4(a) (supp-002): the ESPN
  side's degenerate end state on 401856682824 ("Fumble Recovery (Own)") scored EPA -5.906 against -0.39
  on both alternates and now scores -0.41.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

import tests.cfb.cbs_pbp.test_to_espn_summary as cbs_tests
import tests.cfb.yahoo_pbp.test_to_espn_summary as yahoo_tests
from sportsdataverse.football.sources.dispatch import _process_game

GAME_ID = 401856682
ODDS = {"gameSpread": 1.5, "overUnder": 44.5, "homeFavorite": True, "gameSpreadAvailable": True}
KEY = list(yahoo_tests.KEY)
ADMIN = yahoo_tests._ADMIN
assert tuple(KEY) == cbs_tests.KEY and ADMIN == cbs_tests._ADMIN
#: |sum EPA over ESPN's unpaired snaps - sum over the adapter's|, observed 2026-10-11 (pinned, never raised)
UNPAIRED_GAP = {"yahoo": 4.31, "cbs": 0.21}


def _run(source: str, payload: dict, row: dict) -> pl.DataFrame:
    """One pipeline run with each finals gate's own id-map row and the one closing line both use."""
    return _process_game(
        "cfb", GAME_ID, source=source, fallthrough=False, payloads={source: payload}, idmap_row=row, odds_override=ODDS
    ).plays_frame


@pytest.fixture(scope="module")
def frames() -> dict[str, pl.DataFrame]:
    espn = json.loads((yahoo_tests.ESPN_FIX / f"summary_{GAME_ID}.json").read_text(encoding="utf-8"))
    return {
        "espn": _run("espn", espn, yahoo_tests.OSU_TEX_ROW),
        "yahoo": _run("yahoo", yahoo_tests._load(f"{yahoo_tests.OSU_TEX_YAHOO_ID}.json.gz"), yahoo_tests.OSU_TEX_ROW),
        "cbs": _run("cbs", cbs_tests._load("cbs_cfb_401856682.json.gz"), cbs_tests.OSU_TEX_ROW),
    }


def _unpaired(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """Left's unambiguous snaps whose state key the right side does not carry (the finals gates' anti-join)."""
    return yahoo_tests._snaps(left).join(yahoo_tests._snaps(right).select(KEY), on=KEY, how="anti")


def _team_epa(frame: pl.DataFrame) -> dict[int, float]:
    return dict(frame.group_by("start.pos_team.id").agg(pl.col("EPA").sum()).rows())


@pytest.mark.parametrize("source", ["yahoo", "cbs"])
def test_unpaired_and_admin_rows_are_scored(frames, source):
    ref, cand = frames["espn"], frames[source]
    for side, frame in (("espn", ref), (source, cand)):
        admin = frame.filter(pl.col("type.text").is_in(ADMIN))
        assert admin.height >= 7, (side, admin.height)
        assert admin["EPA"].null_count() == 0, f"{side}: an admin row lost its EPA"
        assert admin["EPA"].abs().max() <= 1e-9, f"{side}: an admin row moved EPA"
    ref_un, cand_un = _unpaired(ref, cand), _unpaired(cand, ref)
    assert ref_un.height >= 10 and cand_un.height >= 10
    assert ref_un["EPA"].null_count() == 0 and cand_un["EPA"].null_count() == 0
    gap = abs(float(ref_un["EPA"].sum()) - float(cand_un["EPA"].sum()))
    assert gap <= UNPAIRED_GAP[source], f"{source}: unpaired EPA gap {gap:.3f} > pinned {UNPAIRED_GAP[source]}"
    print("UNPAIRED_SCORED_OK", source, ref_un.height, cand_un.height, round(gap, 3))


@pytest.mark.parametrize("source", ["yahoo", "cbs"])
def test_unpaired_rows_close_the_team_epa_totals(frames, source):
    ref, cand = _team_epa(frames["espn"]), _team_epa(frames[source])
    assert set(ref) == set(cand)
    worst = max(abs(ref[t] - cand[t]) for t in ref)
    assert worst <= 1.0, f"{source}: team EPA totals ESPN {ref} vs {cand}"
