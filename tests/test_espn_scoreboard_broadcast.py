"""The ESPN scoreboard ``broadcast`` column keeps every network name ("ESPNU, ESPN+"), on real captures.

``_scoreboard_event_parsing`` is shared by every ESPN league's scoreboard, so the test runs it over the
committed scoreboard captures of several sports and proves that only rows with more than one network
change (and that those gain the extra names, in feed order, deduped).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List

import pytest

from sportsdataverse._common_espn_parsers import _scoreboard_event_parsing, parse_scoreboard

ESPN = Path(__file__).parent / "fixtures" / "espn"
CAPTURES = sorted(
    p
    for p in ESPN.rglob("*scoreboard*.json")
    if "cdn" not in p.parts and json.loads(p.read_text("utf-8")).get("events")
)


def _first_only(comp: dict) -> str:
    """The pre-fix rule: the first name of each broadcast entry, ", "-joined."""
    return ", ".join(
        b.get("names", [""])[0] if b.get("names") else b.get("market", "") for b in comp.get("broadcasts") or []
    )


def _names(comp: dict) -> List[str]:
    flat = [n for b in comp.get("broadcasts") or [] for n in (b.get("names") or [b.get("market")]) if n]
    return list(dict.fromkeys(flat))


def test_captures_cover_several_sports() -> None:
    assert len(CAPTURES) >= 8


@pytest.mark.parametrize("path", CAPTURES, ids=lambda p: str(p.relative_to(ESPN)))
def test_only_multi_network_rows_change(path: Path) -> None:
    for ev in json.loads(path.read_text("utf-8"))["events"]:
        comp = ev["competitions"][0]
        got = _scoreboard_event_parsing(ev)["broadcast"]
        assert got == ", ".join(_names(comp))
        entries = comp.get("broadcasts") or []
        single = len(entries) <= 1 and all(len(b.get("names") or []) <= 1 for b in entries)
        if single:
            assert got == _first_only(comp), (path.name, ev["id"])


def test_a_multi_name_broadcast_keeps_every_name_in_feed_order() -> None:
    ev: Any = {
        "id": "1",
        "competitions": [
            {
                "competitors": [],
                "broadcasts": [
                    {"market": "national", "names": ["ESPNU", "ESPN+"]},
                    {"names": ["ESPN+", "SECN"]},
                    {"market": "away"},
                ],
            }
        ],
    }
    assert _scoreboard_event_parsing(ev)["broadcast"] == "ESPNU, ESPN+, SECN, away"
    dup: Any = {"id": "1", "competitions": [{"competitors": [], "broadcasts": [{"names": ["A", "A", "B"]}]}]}
    assert _scoreboard_event_parsing(dup)["broadcast"] == "A, B"


def test_no_broadcasts_is_an_empty_string_and_the_column_stays_text() -> None:
    ev: Any = {"id": "1", "competitions": [{"competitors": []}]}
    assert _scoreboard_event_parsing(ev)["broadcast"] == ""
    nba = json.loads((ESPN / "scoreboard_nba.json").read_text("utf-8"))
    assert str(parse_scoreboard(nba).schema["broadcast"]) == "String"


def test_a_string_names_counts_once_and_a_non_string_market_is_skipped() -> None:
    def one(broadcasts: Any) -> str:
        return _scoreboard_event_parsing({"id": "1", "competitions": [{"competitors": [], "broadcasts": broadcasts}]})[
            "broadcast"
        ]

    assert one([{"names": "ESPN"}, {"names": ["ESPN", "ABC"]}]) == "ESPN, ABC"
    assert one([{"market": {"id": "1", "type": "National"}}, {"names": ["TNT"]}, "junk"]) == "TNT"
