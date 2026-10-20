"""The Atrium runtime: season mapping, every refusal, the error vocabulary and the throttle (mocked transport)."""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path

import pytest

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.atrium import atrium_parsers as P
from sportsdataverse.basketball.atrium import atrium_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "atrium"
BASE = rt.BASE
F14 = "0dc631bd-c34c-11f1-8d16-4d5a21afb79d"
F12 = "5423072b-6715-11f0-bd8b-9ffadebc148f"
PERSON = "4458f165-f865-11f0-8ebf-539c931c1654"
TEAM = "f43b1b0f-55bc-11f0-84d9-01499d2f0a07"


def body(name: str) -> dict:
    path = FIX / "full" / name if (FIX / "full" / name).exists() else FIX / name
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def fresh_cache():
    rt._seasons_cache.clear()
    yield
    rt._seasons_cache.clear()


@pytest.fixture
def wire(monkeypatch):
    """Replace the polite getter with a script: ``wire(answer)`` where ``answer(url, params)`` returns a body."""
    calls = []

    def install(answer):
        def fake(url, params=None, **kw):
            calls.append((url, dict(params or {})))
            return answer(url, params or {})

        monkeypatch.setattr(rt._bb, "_fetch_json", fake)  # the runtime reads the raising fetch (EX-SYNC-a)
        return calls

    return install


def season_list_body(site: int = 12, name: str = "12__fixtures.json") -> dict:
    b = copy.deepcopy(body(name))
    b["links"]["self"] = f"{BASE}/{site}/fixtures"
    return b


# --- seasons -----------------------------------------------------------------------------------------


def test_end_year_maps_to_the_season_id_on_a_start_year_site(wire):
    lnb = body("12__fixtures.json")
    seasons = P.parse_atrium_seasons(lnb)
    row = seasons.filter(seasons["season_end_year"] == 2026).row(0, named=True)  # LNB year 2025 = 2025-26
    calls = wire(lambda url, p: lnb)
    assert rt.atrium_season_id(12, 2026, row["competition_id"]) == row["season_id"]
    assert calls == [(f"{BASE}/12/fixtures", {})]
    assert row["year"] == 2025


def test_start_year_is_not_the_end_year_on_lnb(wire):
    lnb = body("12__fixtures.json")
    wire(lambda url, p: lnb)
    comps = {r["competition_id"] for r in P.parse_atrium_seasons(lnb).iter_rows(named=True) if r["year"] == 2023}
    for comp in comps:
        with pytest.raises(NoDataError):  # 2023 is the START year there; the END year of that season is 2024
            rt.atrium_season_id(12, 2023, comp)
        assert rt.atrium_season_id(12, 2024, comp)


def test_end_year_is_the_calendar_year_on_lnbp(wire):
    b = body("14__fixtures__default.json")
    wire(lambda url, p: b)
    row = P.parse_atrium_seasons(b).row(0, named=True)
    assert rt.atrium_season_id(14, row["year"], row["competition_id"]) == row["season_id"]


def test_a_shared_end_year_needs_a_competition(wire):
    wire(lambda url, p: body("12__fixtures.json"))
    with pytest.raises(ValueError, match=r"ambiguous.*competition_id"):
        rt.atrium_season_id(12, 2026)
    with pytest.raises(NoDataError, match="no season with END year 1999"):
        rt.atrium_season_id(12, 1999)
    with pytest.raises(NoDataError, match="for competition nope"):
        rt.atrium_season_id(12, 2026, "nope")


def test_unmappable_sites_and_bad_years_raise_value_error_before_any_request(wire):
    calls = wire(lambda url, p: pytest.fail("no request expected"))
    for site, season in ((999, 2026), (236, 2026)):
        with pytest.raises(ValueError, match="season_id"):
            rt.atrium_season_id(site, season)
    for bad in ("2026", 2026.0, True, None):
        with pytest.raises(ValueError, match="END year"):
            rt.atrium_season_id(12, bad)
    assert calls == []


def test_the_season_list_is_fetched_once_per_site(wire):
    lnb = body("12__fixtures.json")
    calls = wire(lambda url, p: lnb)
    comp = P.parse_atrium_seasons(lnb).row(0, named=True)
    for _ in range(3):
        rt.atrium_season_id(12, comp["season_end_year"], comp["competition_id"])
    assert len(calls) == 1


def test_the_season_list_is_refetched_after_its_ttl(wire, monkeypatch):
    lnb = body("12__fixtures.json")
    calls = wire(lambda url, p: lnb)
    comp = P.parse_atrium_seasons(lnb).row(0, named=True)
    clock = {"t": 1000.0}
    monkeypatch.setattr(rt.time, "monotonic", lambda: clock["t"])
    rt.atrium_season_id(12, comp["season_end_year"], comp["competition_id"])
    clock["t"] += rt.SEASONS_TTL + 1
    rt.atrium_season_id(12, comp["season_end_year"], comp["competition_id"])
    assert len(calls) == 2


def test_get_replaces_season_with_season_id_and_drops_the_competition_that_picked_it(wire):
    lnb = body("12__fixtures.json")
    row = P.parse_atrium_seasons(lnb).filter(P.parse_atrium_seasons(lnb)["is_default_season"]).row(0, named=True)

    def answer(url, p):
        if url.endswith("/fixtures") and "seasonId" not in p:
            return lnb
        b = copy.deepcopy(lnb)
        b["data"]["seasonId"] = p["seasonId"]
        return b

    calls = wire(answer)
    rt._get(
        f"{BASE}/12/fixtures",
        {"season": row["season_end_year"], "competitionId": row["competition_id"], "locale": None},
    )
    assert calls[-1] == (f"{BASE}/12/fixtures", {"seasonId": row["season_id"]})


def test_competition_routes_keep_their_competition_id(wire):
    b = {"data": {"statistics": [{"statistics": [{"x": 1}]}]}, "links": {"self": f"{BASE}/12/statistics_competition_persons"},
         "meta": {"code": 200}}  # fmt: skip
    calls = wire(lambda url, p: b)
    rt._get(f"{BASE}/12/statistics_competition_persons", {"competitionId": "c"})
    assert calls == [(f"{BASE}/12/statistics_competition_persons", {"competitionId": "c"})]


def test_season_and_season_id_together_are_refused(wire):
    wire(lambda url, p: pytest.fail("no request expected"))
    with pytest.raises(ValueError, match="not both"):
        rt._get(f"{BASE}/12/standings", {"season": 2026, "seasonId": "x"})
    with pytest.raises(ValueError, match="not an Atrium embed URL"):
        rt._get("https://example.com/other", {})


# --- refusals ----------------------------------------------------------------------------------------


def serve(b):
    return lambda url, p: b


def pbp_request():
    return f"{BASE}/14/fixture_detail", {"fixtureId": F14, "sub": "pbp"}


def test_a_real_answer_passes_every_check(wire):
    wire(serve(body("14__fixture_detail__pbp.json")))
    out = rt._get(*pbp_request())
    assert len(P.parse_atrium_pbp(out)) == 497


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda b: {"error": {"code": 400, "message": "boom"}, "links": b["links"]}, "error object"),
        (lambda b: {"links": b["links"]}, "no data object"),
        (lambda b: {**b, "data": []}, "no data object"),
        (lambda b: {**b, "meta": {"code": 500}}, "meta.code 500"),
        (lambda b: {**b, "meta": {"code": "200"}}, "meta.code"),
        (lambda b: {**b, "links": {}}, "links.self"),
        (lambda b: {**b, "links": {"self": f"{BASE}/12/fixture_detail"}}, "asked for /14/fixture_detail"),
        (lambda b: {**b, "links": {"self": f"{BASE}/14/fixtures"}}, "asked for /14/fixture_detail"),
        (lambda b: {**b, "data": {**b["data"], "sub": "preview"}}, "sub='pbp'"),
        (lambda b: {**b, "data": {**b["data"], "fixture": {**b["data"]["fixture"], "fixtureId": F12}}}, "fixtureId"),
        ([], "not a JSON object"),
        ("<html>", "not a JSON object"),
    ],
)
def test_wrong_scope_error_and_stub_bodies_are_asset_fetch_errors(wire, mutate, match):
    real = body("14__fixture_detail__pbp.json")
    wire(serve(mutate(real) if callable(mutate) else mutate))
    with pytest.raises(AssetFetchError, match=match):
        rt._get(*pbp_request())


def test_a_summary_body_for_an_unknown_sub_is_not_mistaken_for_a_box_score(wire):
    summary = copy.deepcopy(body("14__fixture_detail__statistics.json"))
    summary["data"]["sub"] = "preview"
    wire(serve(summary))
    with pytest.raises(AssetFetchError, match="sub"):
        rt._get(f"{BASE}/14/fixture_detail", {"fixtureId": F14, "sub": "statistics"})


def test_a_body_for_another_season_is_refused(wire):
    st = body("12__standings.json")
    wire(serve(st))
    with pytest.raises(AssetFetchError, match="seasonId"):
        rt._get(f"{BASE}/12/standings", {"seasonId": "00000000-0000-1000-8000-000000000000"})
    assert rt._get(f"{BASE}/12/standings", {"seasonId": st["data"]["seasonId"]}) is st


def test_a_player_page_for_another_player_is_refused(wire):
    wire(serve(body("12__person_detail.json")))
    with pytest.raises(AssetFetchError, match="personId"):
        rt._get(f"{BASE}/12/person_detail", {"personId": "00000000-0000-1000-8000-000000000000"})
    assert rt._get(f"{BASE}/12/person_detail", {"personId": PERSON})["data"]["person"]["id"] == PERSON


def test_a_team_page_for_another_team_is_refused(wire):
    wire(serve(body("12__entity_detail.json")))
    with pytest.raises(AssetFetchError, match="entityId"):
        rt._get(f"{BASE}/12/entity_detail", {"entityId": "00000000-0000-1000-8000-000000000000"})
    assert rt._get(f"{BASE}/12/entity_detail", {"entityId": TEAM})


def test_a_default_season_outside_the_sites_own_list_is_refused(wire):
    b = copy.deepcopy(body("14__fixtures__default.json"))
    b["data"]["seasonId"] = "00000000-0000-1000-8000-000000000000"
    wire(serve(b))
    with pytest.raises(AssetFetchError, match="not in the site's own season list"):
        rt._get(f"{BASE}/14/fixtures", {})


def _no_rows(name, edit):
    b = copy.deepcopy(body(name))
    edit(b["data"])
    return b


@pytest.mark.parametrize(
    ("name", "typ", "params", "edit"),
    [
        ("12__fixtures.json", "fixtures", {}, lambda d: d.update(fixtures=[])),
        ("14__fixtures_ribbon.json", "fixtures_ribbon", {}, lambda d: d.update(fixtures=[])),
        ("14__fixture_detail__pbp.json", "fixture_detail", {"fixtureId": F14, "sub": "pbp"},
         lambda d: [p.update(events=[]) for p in d["periodData"]]),
        ("14__fixture_detail__shot_chart.json", "fixture_detail", {"fixtureId": F14, "sub": "shot_chart"},
         lambda d: [p.update(shots=[]) for p in d["periodData"]]),
        ("14__fixture_detail__statistics.json", "fixture_detail", {"fixtureId": F14, "sub": "statistics"},
         lambda d: [s.update(persons=[]) for s in d["statistics"]["data"]["base"].values() if isinstance(s, dict)]),
        ("12__standings.json", "standings", {}, lambda d: d.update(standings=[{"rows": []}])),
        ("12__entities.json", "entities", {}, lambda d: d.update(teams=[])),
        ("12__persons.json", "persons", {}, lambda d: d["persons"].update(rows=[])),
        ("12__person_detail__log.json", "person_detail", {"personId": PERSON, "sub": "log"},
         lambda d: d["log"].update(rows=[])),
        ("12__statistics_leaders.json", "statistics_leaders", {}, lambda d: d.update(statistics=[])),
        ("12__statistics_persons.json", "statistics_persons", {}, lambda d: d.update(statistics=[])),
    ],
)  # fmt: skip
def test_a_well_formed_body_without_rows_is_no_data_not_a_failure(wire, name, typ, params, edit):
    b = _no_rows(name, edit)
    if "seasonId" in b["data"] and typ not in ("entity_detail", "person_detail"):
        params = {**params}  # no seasonId requested: nothing to echo
    wire(serve(b))
    site = int(name.split("__")[0])
    with pytest.raises(NoDataError, match="no rows"):
        rt._get(f"{BASE}/{site}/{typ}", params)


def test_the_season_list_lookup_tolerates_a_default_page_without_fixtures(wire):
    b = copy.deepcopy(body("12__fixtures.json"))
    b["data"]["fixtures"] = []
    wire(serve(b))
    comp = P.parse_atrium_seasons(b).row(0, named=True)
    assert rt.atrium_season_id(12, comp["season_end_year"], comp["competition_id"]) == comp["season_id"]


# --- the transport: status vocabulary and the throttle -----------------------------------------------


class Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self.content = json.dumps(payload if payload is not None else {}).encode()
        self.text = self.content.decode()
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.url = f"{BASE}/14/fixtures"
        self.reason = "x"

    def json(self):
        return json.loads(self.content)


@pytest.fixture
def transport(monkeypatch):
    """Script ``requests.Session.request``; count the polite waits; never sleep."""
    state = {"sent": [], "waits": [], "slept": []}

    def install(*answers):
        queue = list(answers)

        def fake(self, method, url, *a, **k):
            state["sent"].append((url, k.get("params")))
            return queue.pop(0) if len(queue) > 1 else queue[0]

        monkeypatch.setattr(bb.requests.Session, "request", fake)

    monkeypatch.setattr(bb, "_polite_wait", lambda url, *a, **k: state["waits"].append(url))
    monkeypatch.setattr(time, "sleep", lambda s: state["slept"].append(s))
    state["install"] = install
    return state


def test_every_attempt_waits_its_turn_retries_included(transport):
    ok = body("14__fixtures__default.json")
    transport["install"](Resp(503), Resp(200, ok))
    out = rt._get(f"{BASE}/14/fixtures", {})
    assert out["data"]["seasonId"] == ok["data"]["seasonId"]
    assert len(transport["sent"]) == 2 and len(transport["waits"]) == 2  # one wait per attempt, not one per call


def test_the_season_lookup_and_the_request_are_both_throttled(transport):
    lnbp = body("14__fixtures__default.json")
    row = P.parse_atrium_seasons(lnbp).row(0, named=True)
    answer = copy.deepcopy(body("14__standings.json"))
    answer["data"]["seasonId"] = row["season_id"]
    transport["install"](Resp(200, lnbp), Resp(200, answer))
    rt._get(f"{BASE}/14/standings", {"season": row["season_end_year"], "competitionId": row["competition_id"]})
    assert [u.rsplit("/", 1)[1] for u, _ in transport["sent"]] == ["fixtures", "standings"]
    assert transport["sent"][1][1] == {"seasonId": row["season_id"]}
    assert len(transport["waits"]) == 2


def test_retry_after_is_honoured_on_429_and_503(transport):
    ok = body("14__fixtures__default.json")
    for status in (429, 503):
        transport["slept"].clear()
        transport["install"](Resp(status, headers={"Retry-After": "3"}), Resp(200, ok))
        rt._get(f"{BASE}/14/fixtures", {})
        assert 3.0 in transport["slept"], status


@pytest.mark.parametrize(
    ("status", "exc"),
    [(404, NoDataError), (400, ValueError), (422, ValueError), (403, AssetFetchError), (500, AssetFetchError)],
)
def test_status_codes_follow_the_package_vocabulary(transport, status, exc):
    transport["install"](Resp(status, {"error": "x"}))
    with pytest.raises(exc):
        rt._get(f"{BASE}/14/fixtures", {})


def test_an_empty_or_non_json_200_is_a_failed_fetch(transport, monkeypatch):
    bad = Resp(200)
    bad.content, bad.text = b"<html>nope</html>", "<html>nope</html>"
    transport["install"](bad)
    with pytest.raises(AssetFetchError):
        rt._get(f"{BASE}/14/fixtures", {})


def test_the_session_is_the_shared_polite_one():
    assert rt._bb.SESSION is bb.SESSION and rt._bb._get is bb._get
