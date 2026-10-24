"""NFL advBoxScore team tables come out home team first, in every process (#913, R828-4 NFL side).

The shared usage / special-teams sections (``football/usage_box.SECTIONS``) are sorted by their keys in
``usage_box._to_rows`` (#910); before that, ``team_usage`` and the ``st_*`` tables listed their rows in a different
order in two processes in 15-23 of the 37 captures (2.23, R2.22-4).

``create_box_score`` builds ``team`` / ``situational`` / ``drives`` / ``defensive`` with ``group_by`` and full
joins, whose row order is per-process hash order: before the fix, 15-23 of the 37 committed NFL captures listed
a table in a different order in two processes. CFB's side and its tests are #910
(``tests/football/test_box_row_order.py``).
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
from pathlib import Path

from sportsdataverse.football.usage_box import _SUM_KEYS, SECTIONS
from tests.football.test_box_row_order import _key_sorted

FIX = Path(__file__).parent / "fixtures"
_TEAM_TABLES = {"team": "pos_team", "situational": "pos_team", "drives": "pos_team", "defensive": "def_pos_team"}


def _summary(gid: int) -> dict:
    (path,) = FIX.glob(f"summary_{gid}*.json*")
    text = gzip.decompress(path.read_bytes()).decode("utf-8") if path.suffix == ".gz" else path.read_text("utf-8")
    return json.loads(text)


#: committed NFL captures, 2002-2025 (provenance: tests/nfl/fixtures/README.md). Every table here is two rows,
#: so a dropped sort leaves it home-first by chance in each game, and survives all eight ~1 time in 250
_ORDER_CAPTURES = (
    221013007,
    261022011,
    331103020,
    400874485,
    401128075,
    401220341,
    401326581,
    401872922,
)


def test_team_tables_list_the_home_team_first():
    import sportsdataverse.nfl.nfl_pbp as mod

    wrong: list[tuple[int, str]] = []
    for gid in _ORDER_CAPTURES:
        summary = _summary(gid)
        competitors = summary["header"]["competitions"][0]["competitors"]
        home = next(int(c["id"]) for c in competitors if c["homeAway"] == "home")
        proc = mod.NFLPlayProcess(gameId=gid, join_participants=False)
        proc.espn_nfl_pbp(summary=summary)
        box = proc.run_processing_pipeline()["advBoxScore"]
        for section, team in _TEAM_TABLES.items():
            assert len(box[section]) == 2, f"{gid} {section}: the capture must exercise the sort"
            if box[section][0][team] != home:
                wrong.append((gid, section))
        # the usage / special-teams sections equal themselves sorted by their keys
        for section in SECTIONS:
            rows = box[section]
            if rows != _key_sorted(rows, [k for k in _SUM_KEYS[section] if rows and k in rows[0]]):
                wrong.append((gid, section))
        assert box["team_usage"] and box["st_team"], f"{gid}: the capture must exercise the usage sort"
    assert not wrong, f"(game, section) out of order: {wrong}"


#: captures whose four team tables all came out in a different order in two pre-fix processes, so a pre-fix
#: run matches by chance only when 16 two-way coin flips all land the same way
_TWO_PROCESS_CAPTURES = (240919004, 401220341, 401326383, 401326581)

_DUMP = r"""
import gzip, json, sys, warnings
from pathlib import Path
warnings.simplefilter("ignore")
from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess
out = {}
for gid in json.loads(sys.argv[1]):
    (path,) = (Path("tests") / "nfl" / "fixtures").glob(f"summary_{gid}*.json*")
    text = gzip.decompress(path.read_bytes()).decode("utf-8") if path.suffix == ".gz" else path.read_text("utf-8")
    proc = NFLPlayProcess(gameId=gid, join_participants=False)
    proc.espn_nfl_pbp(summary=json.loads(text))
    out[str(gid)] = proc.run_processing_pipeline()["advBoxScore"]
print(json.dumps(out, default=str))
"""


def _boxes_in_a_fresh_process(hash_seed: str, threads: str) -> dict:
    env = {**os.environ, "PYTHONHASHSEED": hash_seed, "POLARS_MAX_THREADS": threads, "PYTHONIOENCODING": "utf-8"}
    done = subprocess.run(
        [sys.executable, "-c", _DUMP, json.dumps(_TWO_PROCESS_CAPTURES)],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=600,
        check=True,
    )
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_advboxscore_is_identical_in_two_processes():
    first = _boxes_in_a_fresh_process("1", "1")
    second = _boxes_in_a_fresh_process("2", "8")
    assert first.keys() == second.keys()
    for game, box in first.items():
        sections = box.keys() | second[game].keys()
        assert set(_TEAM_TABLES) <= sections
        differ = sorted(s for s in sections if box.get(s) != second[game].get(s))
        assert not differ, f"{game}: sections in a different order across processes: {differ}"
