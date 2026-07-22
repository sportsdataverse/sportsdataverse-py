"""Committed event-taxonomy contracts — the observed type domains as expectations.

Every parser and sim classifier is only as complete as the event-type
inventory it was built against. These contracts freeze the OBSERVED domains
from the committed real captures (v3 actionTypes, ESPN play types, NHL
typeDescKeys, MLB result events) into
``tests/fixtures/contracts/{feed}.contract.json`` so that:

* the offline gate (``tests/modeling/test_taxonomy_contracts.py``) fails
  when a re-captured fixture ships an unseen type — regenerating here is the
  deliberate acknowledgement that the taxonomy grew;
* the live-gated checks validate LIVE feeds against the same domains, so a
  new type surfaces through the weekly cron's drift-issue flow before it
  becomes a silent parser gap.

Regenerate all contracts::

    uv run python -m tools.taxonomy_contracts.build
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List

import polars as pl

from sportsdataverse.modeling.integrity import ColumnContract, DataContract, write_contract

FIXTURES = Path("tests/fixtures")
OUT_DIR = FIXTURES / "contracts"


def _read(rel: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


def _v3_action_types() -> List[str]:
    types: List[str] = []
    for root, gids in (
        ("nba_engine", ("0022100001", "0022200001", "0022300001")),
        ("nbagl_engine", ("2022400003", "2022400009")),
    ):
        for gid in gids:
            payload = _read(f"{root}/{gid}/playbyplayv3.json")
            for action in payload.get("game", {}).get("actions") or payload.get("actions") or []:
                value = str(action.get("actionType") or "")
                if value:
                    types.append(value)
    return types


def _espn_basketball_play_types() -> List[str]:
    types: List[str] = []
    for league in ("mbb", "wbb", "wnba"):
        for play in _read(f"espn/summary_{league}.json").get("plays") or []:
            value = str((play.get("type") or {}).get("text") or "")
            if value:
                types.append(value)
    return types


def _espn_football_play_types() -> List[str]:
    types: List[str] = []
    for league in ("nfl", "cfb"):
        for drive in (_read(f"espn/summary_{league}.json").get("drives") or {}).get("previous") or []:
            for play in drive.get("plays") or []:
                value = str((play.get("type") or {}).get("text") or "")
                if value:
                    types.append(value)
    return types


def _nhl_type_desc_keys() -> List[str]:
    payload = _read("nhl_api_web/pbp_2024_scf_g7.json")
    return [str(play.get("typeDescKey") or "") for play in payload.get("plays") or [] if play.get("typeDescKey")]


def _mlb_result_events() -> List[str]:
    payload = _read("mlb_api/play_by_play_745282.json")
    plays = payload.get("allPlays")
    if plays is None:
        plays = payload.get("liveData", {}).get("plays", {}).get("allPlays", [])
    return [
        str((play.get("result") or {}).get("event") or "") for play in plays if (play.get("result") or {}).get("event")
    ]


def _espn_scoreboard_states() -> List[str]:
    states: List[str] = []
    for league in ("nba", "nfl", "mbb", "mlb", "nhl", "wnba", "wbb", "cfb"):
        for event in _read(f"espn/scoreboard_{league}.json").get("events") or []:
            value = str(((event.get("status") or {}).get("type") or {}).get("state") or "")
            if value:
                states.append(value)
    # the feed's full lifecycle: committed captures are all post-game, but
    # pre/in are the documented ESPN states a live scoreboard legitimately shows
    states.extend(["pre", "in"])
    return states


FEEDS: Dict[str, Callable[[], List[str]]] = {
    "nba_v3_action_types": _v3_action_types,
    "espn_basketball_play_types": _espn_basketball_play_types,
    "espn_football_play_types": _espn_football_play_types,
    "nhl_type_desc_keys": _nhl_type_desc_keys,
    "mlb_result_events": _mlb_result_events,
    "espn_scoreboard_states": _espn_scoreboard_states,
}


def type_frame(values: List[str]) -> pl.DataFrame:
    """The one-column occurrence frame a taxonomy contract validates."""
    return pl.DataFrame({"event_type": values}, schema={"event_type": pl.Utf8})


def build_feed_contract(feed: str) -> DataContract:
    """Freeze one feed's observed type domain into a contract.

    Args:
        feed: A :data:`FEEDS` key.

    Returns:
        A :class:`DataContract` whose single ``event_type`` column carries
        the sorted observed domain (no row floors — game volumes vary).
    """
    values = FEEDS[feed]()
    return DataContract(
        name=feed,
        columns={"event_type": ColumnContract(dtype="String", allowed_values=sorted(set(values)))},
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for feed in FEEDS:
        contract = build_feed_contract(feed)
        path = OUT_DIR / f"{feed}.contract.json"
        write_contract(path, contract)
        domain = contract.columns["event_type"].allowed_values or []
        print(f"{path}  domain={len(domain)} types")


if __name__ == "__main__":
    main()
