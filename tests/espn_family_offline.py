"""Offline checks for the 8 hand-written ESPN schedule and teams families (B-M34).

Each ``tests/<lg>/test_<lg>_schedule_teams_offline.py`` parametrizes over the cases here and
serves the real ``tests/fixtures/espn/scoreboard_<lg>.json`` / ``teams_<lg>.json`` capture
through the real ``dl_utils.download`` (only ``requests.Session.get`` is replaced). Every
expected value is read from the captured payload or from the documented returns table
(``tools/codegen/schemas/autodoc/<lg>/espn_<lg>_*.yaml``), never from a stored copy of the
function's own output.

Task 2.9 fixed the defects these cases pinned as strict xfails (B-H2, B-M12, B-M14, B-M16 shape
half, B-M24, B-M25, B-GB-21, B-L2); the frame assembly they exercise lives once, in
``sportsdataverse._espn_schedule``. A new known defect goes back in as a strict xfail naming its
finding id, the task that fixes it and the exception it raises today (``raises=``).
"""

from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import polars as pl
import yaml

from tests.codegen.test_runtime_errors import _route

ROOT = Path(__file__).resolve().parents[1]
ESPN = ROOT / "tests" / "fixtures" / "espn"
AUTODOC = ROOT / "tools" / "codegen" / "schemas" / "autodoc"

#: The scoreboard date each capture was taken for (tools/capture_espn_fixtures.py), passed as
#: ``dates=`` so the request matches the capture.
CAPTURE_DATES = {
    "mbb": 20241201,
    "wbb": 20241201,
    "nba": 20241201,
    "wnba": 20240622,
    "nfl": 20251012,
    "nhl": 20241201,
    "cfb": 20241012,
    "mlb": 20241025,
}

#: Explicit id-column list per family: column -> the dtype its returns table documents. Every ESPN id is
#: ``Int64`` (task 5.2, B-M15); an id column the parser emits that is not listed here fails the check, so a
#: new id column needs a decision, not a silent dtype.
_STR = (pl.String,)
_INT = (pl.Int64,)
_SCHEDULE_IDS_COMMON = {
    "id": _INT,
    "game_id": _INT,
    "type_id": _INT,
    "venue_id": _INT,
    "status_type_id": _INT,
    "home_id": _INT,
    "home_venue_id": _INT,
    "away_id": _INT,
    "away_venue_id": _INT,
}
_COLLEGE_IDS = {"home_conference_id": _INT, "away_conference_id": _INT, "tournament_id": _INT}
SCHEDULE_IDS: Dict[str, Dict[str, tuple]] = {
    "mbb": {**_SCHEDULE_IDS_COMMON, **_COLLEGE_IDS},
    "wbb": {**_SCHEDULE_IDS_COMMON, **_COLLEGE_IDS},
    "nba": dict(_SCHEDULE_IDS_COMMON),
    "wnba": dict(_SCHEDULE_IDS_COMMON),
    "nfl": dict(_SCHEDULE_IDS_COMMON),
    "nhl": dict(_SCHEDULE_IDS_COMMON),
    "cfb": {**_SCHEDULE_IDS_COMMON, **_COLLEGE_IDS, "groups_id": _INT},
    "mlb": {"game_id": _INT, "venue_id": _INT, "home_id": _INT, "away_id": _INT},
}
TEAMS_IDS = {"team_id": _INT, "team_uid": _STR}  # a uid ("s:40~l:46~t:1") is not an integer id


def _documented(league: str, family: str) -> List[str]:
    doc = yaml.safe_load((AUTODOC / league / f"espn_{league}_{family}.yaml").read_text(encoding="utf-8"))
    return [c["name"] for c in doc["columns"]]


def _payload(name: str) -> Dict[str, Any]:
    return json.loads((ESPN / name).read_text(encoding="utf-8"))


def _call(league: str, family: str, monkeypatch, body: Dict[str, Any], **kwargs: Any):
    """Serve ``body`` to ``espn_<league>_<family>`` and return ``(frame, urls hit)``.

    Exceptions propagate unchanged, so each strict xfail's ``raises=`` names the one failure mode
    it expects; a return that is not a DataFrame (B-H2's ``None``) is an AssertionError.
    """
    calls = _route(monkeypatch, lambda url: (200, json.dumps(body), "application/json"))
    fn = getattr(importlib.import_module(f"sportsdataverse.{league}.{league}_{family}"), f"espn_{league}_{family}")
    out = fn(**kwargs)  # the teams functions no longer cache (task 2.11), so nothing to clear
    assert isinstance(out, pl.DataFrame), f"espn_{league}_{family} returned {type(out).__name__}, not a DataFrame"
    return out, calls


# --------------------------------------------------------------------------------------
# schedule
# --------------------------------------------------------------------------------------


def _drop_notes(event: Dict[str, Any]) -> None:
    event["competitions"][0].pop("notes", None)


def _one_competitor(event: Dict[str, Any]) -> None:
    event["competitions"][0]["competitors"] = event["competitions"][0]["competitors"][:1]


def _no_competitions(event: Dict[str, Any]) -> None:
    event["competitions"] = []


def _no_season(event: Dict[str, Any]) -> None:
    event.pop("season", None)


#: Mutations of ONE real event (B-M24's proof test): the other events of the day must survive.
EVENT_MUTATIONS: Dict[str, Callable[[Dict[str, Any]], None]] = {
    "event_missing_notes": _drop_notes,
    "event_one_competitor": _one_competitor,
    "event_empty_competitions": _no_competitions,
    "event_missing_season": _no_season,
}


def schedule_cases(league: str) -> List[Any]:
    """The schedule case ids for one league (task 2.9 fixed every known defect these pinned)."""
    cases: List[Any] = ["snapshot", "id_dtypes", "query_key", "returns_table_columns", "empty_slate"]
    cases += list(EVENT_MUTATIONS)
    if league != "mlb":  # mlb emits no linescores (nor attendance)
        cases += ["canceled_masked", "parse_twice", "integer_with_a_missing_value"]
    if league == "nfl":
        cases.append("rank_not_default")
    if league == "cfb":
        cases.append("placeholder_event_2014")
    return cases


def _events(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [e for e in body.get("events") or [] if e]


def _season_col(df: pl.DataFrame) -> str:
    return "season_year" if "season_year" in df.columns else "season"


def check_schedule(league: str, case: str, monkeypatch) -> None:
    body = _payload(f"scoreboard_{league}.json")
    kwargs: Dict[str, Any] = {"dates": CAPTURE_DATES[league]}
    events = _events(body)
    ids = [int(e["id"]) for e in events]

    if case == "snapshot":
        df, _ = _call(league, "schedule", monkeypatch, body, **kwargs)
        assert df.height == len(events) > 0
        assert [int(g) for g in df["game_id"].to_list()] == ids
        assert df[_season_col(df)].cast(pl.Int64).to_list() == [e["season"]["year"] for e in events]
        assert df["season_type"].cast(pl.Int64).to_list() == [e["season"]["type"] for e in events]
        home = [next(c for c in e["competitions"][0]["competitors"] if c["homeAway"] == "home") for e in events]
        assert df["home_id"].to_list() == [int(c["team"]["id"]) for c in home]
        assert [str(s) for s in df["home_score"].to_list()] == [str(c["score"]) for c in home]
    elif case == "id_dtypes":
        df, _ = _call(league, "schedule", monkeypatch, body, **kwargs)
        allow = SCHEDULE_IDS[league]
        id_cols = [c for c in df.columns if c == "id" or c.endswith("_id")]
        unlisted = [c for c in id_cols if c not in allow]
        assert not unlisted, f"id columns without an allowlist entry: {unlisted}"
        wrong = {c: str(df.schema[c]) for c in id_cols if df.schema[c] not in allow[c]}
        assert not wrong, f"id columns off their documented dtype: {wrong}"
    elif case == "query_key":
        _, calls = _call(league, "schedule", monkeypatch, body, season_type=2, **kwargs)
        assert "seasontype=2" in calls[0], calls[0]
    elif case == "returns_table_columns":
        df, _ = _call(league, "schedule", monkeypatch, body, **kwargs)
        documented = _documented(league, "schedule")
        assert set(df.columns) == set(documented), {
            "undocumented": sorted(set(df.columns) - set(documented)),
            "missing": sorted(set(documented) - set(df.columns)),
        }
    elif case == "empty_slate":
        df, _ = _call(league, "schedule", monkeypatch, {**body, "events": []}, **kwargs)
        assert df.height == 0
        assert set(df.columns) == set(_documented(league, "schedule"))
    elif case in EVENT_MUTATIONS:
        bad = copy.deepcopy(body)
        if len(_events(bad)) < 2:  # a one-event capture borrows a sibling's real event to survive
            sibling = "wbb" if league == "mbb" else "nba"
            bad["events"].append(copy.deepcopy(_payload(f"scoreboard_{sibling}.json")["events"][0]))
        good_ids = [int(e["id"]) for e in _events(bad)][1:]
        EVENT_MUTATIONS[case](bad["events"][0])
        df, _ = _call(league, "schedule", monkeypatch, bad, **kwargs)
        got = [int(g) for g in df["game_id"].to_list()]
        # every untouched event survives, in order; keeping the damaged one (with nulls) is allowed
        assert [g for g in got if g in good_ids] == good_ids, got
    elif case == "canceled_masked":
        bad = copy.deepcopy(body)
        event = bad["events"][0]
        for status in (event.get("status"), event["competitions"][0].get("status")):
            if status and status.get("type"):
                status["type"].update(
                    {"name": "STATUS_CANCELED", "state": "post", "description": "Canceled", "detail": "Canceled"}
                )
        df, _ = _call(league, "schedule", monkeypatch, bad, **kwargs)
        row = df.filter(pl.col("game_id").cast(pl.Int64) == int(event["id"]))
        assert row.height == 1
        assert row["home_linescores"][0] is None and row["away_linescores"][0] is None
    elif case == "parse_twice":
        # B-L2: the parser used to pop keys off the payload, so a second parse of it raised
        module = importlib.import_module(f"sportsdataverse.{league}.{league}_schedule")
        monkeypatch.setattr(module, "_download_json", lambda *a, **k: body)  # the SAME object, twice
        first = getattr(module, f"espn_{league}_schedule")(**kwargs)
        second = getattr(module, f"espn_{league}_schedule")(**kwargs)
        assert first.height == len(events) and first.equals(second)
        assert body == _payload(f"scoreboard_{league}.json")
    elif case == "integer_with_a_missing_value":
        # B-M16 / R981-1: one event without ``attendance`` turns the column Float64 on the pandas hop; the frame
        # restores the documented integer (ids no longer reach that branch: they are cast first, task 5.2)
        bad = copy.deepcopy(body)
        if len(_events(bad)) < 2:  # a one-event capture borrows a sibling's real event
            sibling = "wbb" if league == "mbb" else "nba"
            bad["events"].append(copy.deepcopy(_payload(f"scoreboard_{sibling}.json")["events"][0]))
        bad["events"][0]["competitions"][0].pop("attendance", None)
        want = [c.get("attendance") for e in _events(bad) for c in e["competitions"][:1]]
        df, _ = _call(league, "schedule", monkeypatch, bad, **kwargs)
        assert df.schema["attendance"] == pl.Int64, df.schema["attendance"]
        assert df["attendance"].to_list() == want
    elif case == "rank_not_default":
        df, _ = _call(league, "schedule", monkeypatch, body, **kwargs)
        # NFL competitors carry no curatedRank (0 of 15 in the capture): the columns are all null, and
        # with the documented dtype (integer), not the String an all-null column takes on the pandas hop (R855-3)
        assert not any(c.get("curatedRank") for e in events for c in e["competitions"][0]["competitors"])
        for col in ("home_current_rank", "away_current_rank"):
            assert df[col].null_count() == df.height, df[col].unique().to_list()
            assert df.schema[col] == pl.Int64, df.schema[col]
    elif case == "placeholder_event_2014":
        real = _payload("scoreboard_cfb_2014_placeholder.json")
        assert any(not e for e in real["events"])  # the capture carries ESPN's `{}` placeholder
        df, _ = _call(league, "schedule", monkeypatch, real, dates=2014)
        assert [int(g) for g in df["game_id"].to_list()] == [int(e["id"]) for e in _events(real)]
    else:  # pragma: no cover
        raise ValueError(case)


# --------------------------------------------------------------------------------------
# teams
# --------------------------------------------------------------------------------------

TEAMS_CASES = ["snapshot", "id_dtypes", "query_key"]


def teams_cases(league: str) -> List[str]:
    return list(TEAMS_CASES)


def check_teams(league: str, case: str, monkeypatch) -> None:
    body = _payload(f"teams_{league}.json")
    teams = [t["team"] for t in body["sports"][0]["leagues"][0]["teams"]]
    df, calls = _call(league, "teams", monkeypatch, body)
    if case == "snapshot":
        assert df.height == len(teams) > 0
        assert df["team_id"].to_list() == [int(t["id"]) for t in teams]
        assert df["team_abbreviation"].to_list() == [t["abbreviation"] for t in teams]
        assert df["team_display_name"].to_list() == [t["displayName"] for t in teams]
        assert list(df.columns) == _documented(league, "teams")
    elif case == "id_dtypes":
        id_cols = [c for c in df.columns if c.endswith("_id") or c.endswith("_uid")]
        assert sorted(id_cols) == sorted(TEAMS_IDS)
        assert {c: df.schema[c] for c in id_cols} == {c: TEAMS_IDS[c][0] for c in id_cols}
    elif case == "query_key":
        query = calls[0].split("?", 1)[1]
        group: Optional[str] = {"mbb": "50", "wbb": "50", "cfb": "80"}.get(league)
        assert ("groups=" + group in query) if group else ("groups=" not in query), query
        assert "limit=1000" in query, query
    else:  # pragma: no cover
        raise ValueError(case)
