"""A contest parsed before its season's rosters landed is re-enriched once they do.

Real data: NCAA MBB contest 6607592, TCU vs Lindenwood on 2 Nov 2026 (season 2027). The raw
bundle is the one captured on 3 Nov 2026; the two rosters are the 2027 rosters stage 04 wrote
later that evening, and the teams file is ``mbb/teams/json/2027.json`` cut to these two rows
(``ncaa-mbb-hoops-raw`` at 0f62d888eb). Stage 03 parsed the game before stage 04 had the
rosters, so all 124 of its shots kept a null ``shooter_player_id``, and the parse skip never
revisited it until a forced re-parse.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

from sportsdataverse.scrape.ncaa import identity
from sportsdataverse.scrape.ncaa.bundle import read_bundle
from sportsdataverse.scrape.ncaa.parse import _main, parse_bundle

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "ncaa" / "mbb_raw" / "mbb"
CID = "6607592"


def _stage03(root: Path, monkeypatch, capsys) -> str:
    """One nightly ``run_03_parse.sh --season 2027``: a fresh process, so no cached index."""
    identity.load_roster_index.cache_clear()
    identity.load_team_index.cache_clear()
    monkeypatch.setattr(sys, "argv", ["ncaa_mbb_03_games_parse.py", "--season", "2027"])
    _main("mbb", str(root))
    return capsys.readouterr().out


def _parsed(root: Path) -> Path:
    return root / "mbb" / "json" / f"{CID}.json"


def _shooter_ids(root: Path) -> list:
    return [s["shooter_player_id"] for s in json.loads(_parsed(root).read_text(encoding="utf-8"))["shots"]]


def test_shots_parsed_before_rosters_get_shooter_ids_once_rosters_land(tmp_path, monkeypatch, capsys):
    shutil.copytree(FIX / "raw", tmp_path / "mbb" / "raw")
    shutil.copytree(FIX / "teams", tmp_path / "mbb" / "teams")

    # Night 1: stage 03 parses the game; stage 04 has not captured the rosters yet.
    _stage03(tmp_path, monkeypatch, capsys)
    before = _shooter_ids(tmp_path)
    assert len(before) == 124 and all(s is None for s in before)
    an_hour_ago = time.time() - 3600
    os.utime(_parsed(tmp_path), (an_hour_ago, an_hour_ago))

    # Stage 04 lands the rosters; night 2's stage 03 has nothing new to parse.
    shutil.copytree(FIX / "rosters", tmp_path / "mbb" / "rosters")
    out = _stage03(tmp_path, monkeypatch, capsys)
    after = _shooter_ids(tmp_path)
    assert len(after) == 124 and all(after)
    assert "pending=0" in out and "reenriched=1" in out

    # The healed file is exactly what a parse with the rosters on disk writes.
    fresh = parse_bundle(read_bundle(FIX / "raw" / "2027" / f"{CID}.json.gz"), league="mbb", root=tmp_path)
    assert _parsed(tmp_path).read_text(encoding="utf-8") == json.dumps(fresh)

    # Night 3: already enriched against these rosters, so nothing is re-read or rewritten.
    healed_at = _parsed(tmp_path).stat().st_mtime_ns
    out = _stage03(tmp_path, monkeypatch, capsys)
    assert "reenriched=0" in out
    assert _parsed(tmp_path).stat().st_mtime_ns == healed_at
