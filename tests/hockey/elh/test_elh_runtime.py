"""The ``elh`` runtime: END-year season mapping, the throttle, the listing pager and every refusal.

The refusals mirror the reference capture's checks (``sdv-internal-refs/hokejcz/tools/capture.py``),
exercised through a mocked transport (``_codegen_runtime.download``) on real committed bodies.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as ehr
from sportsdataverse.hockey.elh import elh_runtime as rt

FIXTURES = Path(__file__).parents[2] / "fixtures" / "elh"
S3 = "https://s3-eu-west-1.amazonaws.com/hokej.cz"
PBP = f"{S3}/match/2026/full/2928321.json"
MOBILE = f"{S3}/match/2026/mobile/2928321.json"
SHOTS = f"{S3}/visualization/shots/2928321.json"
EVENTS = f"{S3}/visualization/events/2928321.json"
BOARD = "https://json.esports.cz/hokejcz/scoreboard/2026-10-11.json"
LIST = f"{S3}/"


def _body(era: str, stem: str) -> Any:
    return json.loads((FIXTURES / era / f"{stem}.json").read_text(encoding="utf-8"))


def _listing() -> str:
    return (FIXTURES / "2026" / "list.xml").read_text(encoding="utf-8")


class _Resp:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """Serve queued answers; record every request and every attempt sent over the throttled session."""
    state: Dict[str, Any] = {"answers": [], "calls": [], "waits": []}

    def download(url: str, params: Any = None, **kw: Any) -> _Resp:
        state["calls"].append((url, dict(params or {})))
        if kw.get("session") is ehr.SESSION:  # every attempt on SESSION waits for the host's slot
            state["waits"].append(url)
        answer = state["answers"].pop(0)
        if isinstance(answer, Exception):
            raise answer
        text, status = answer if isinstance(answer, tuple) else (answer, 200)
        return _Resp(text if isinstance(text, str) else json.dumps(text), status)

    monkeypatch.setattr(rt._rt, "download", download)
    return state


def _serve(transport: Dict[str, Any], *answers: Any) -> None:
    transport["answers"].extend(answers)


# ---------------------------------------------------------------- season mapping


@pytest.mark.parametrize(("end", "start"), [(2027, 2026), ("2017", 2016), (2015, 2014)])
def test_end_year_maps_to_the_start_year_folder(end: Any, start: int) -> None:
    assert rt.elh_start_year(end) == start


@pytest.mark.parametrize("bad", [None, "2026-27", True, 26, 12345])
def test_a_non_year_season_raises_value_error(bad: Any) -> None:
    with pytest.raises(ValueError, match="END year"):
        rt.elh_start_year(bad)


# ---------------------------------------------------------------- passing answers + throttle


@pytest.mark.parametrize(
    ("url", "era", "stem"),
    [
        (PBP, "full", "match__season__full__match_id"),
        (f"{S3}/match/2026/short/2928321.json", "2026", "match__season__short__match_id"),
        (MOBILE, "2026", "match__season__mobile__match_id"),
        (SHOTS, "full", "visualization__shots__match_id"),
        (EVENTS, "full", "visualization__events__match_id"),
        (BOARD, "full", "scoreboard__date"),
    ],
)
def test_a_real_body_passes_and_every_request_is_throttled(transport, url: str, era: str, stem: str) -> None:
    body = _body(era, stem)
    _serve(transport, body)
    assert rt._get(url) == body
    assert transport["waits"] == [url]


def test_a_lone_comment_object_passes(transport) -> None:
    body = _body("full", "match__season__full__match_id")
    body["comments"]["comment"] = body["comments"]["comment"][0]
    _serve(transport, body)
    assert rt._get(PBP) == body


def test_the_listing_is_returned_as_xml_text(transport) -> None:
    _serve(transport, _listing())
    out = rt._get(LIST, {"prefix": "match/2026/full/", "max-keys": "3", "marker": None})
    assert isinstance(out, str) and out.startswith("<?xml")
    assert transport["calls"] == [(LIST, {"prefix": "match/2026/full/", "max-keys": "3"})]
    assert transport["waits"] == [LIST]


# ---------------------------------------------------------------- refusals


@pytest.mark.parametrize(
    ("answer", "exc"),
    [
        (NoDataError("No data found"), NoDataError),  # dl_utils.download raises 404 itself (NoSuchKey, old day)
        (("<html>404</html>", 404), NoDataError),
        (('{"error":"bad"}', 400), ValueError),
        (("", 503), AssetFetchError),
        (("<html><body>Not Found</body></html>", 200), AssetFetchError),  # non-JSON 2xx
        (("", 200), AssetFetchError),  # empty 2xx
        (OSError("reset"), AssetFetchError),  # dead connection after retries
    ],
)
def test_error_vocabulary(transport, answer: Any, exc: type) -> None:
    _serve(transport, answer)
    with pytest.raises(exc):
        rt._get(BOARD)


@pytest.mark.parametrize("url", [PBP, SHOTS, EVENTS, BOARD, MOBILE])
@pytest.mark.parametrize("envelope", [{"error": "NoSuchKey"}, {"message": "Forbidden", "code": 403}])
def test_an_error_envelope_is_a_failed_fetch(transport, url: str, envelope: Dict[str, Any]) -> None:
    _serve(transport, envelope)
    with pytest.raises(AssetFetchError, match="error envelope"):
        rt._get(url)


@pytest.mark.parametrize(
    ("url", "stub"),
    [
        (PBP, {"comments": {"comment": []}}),
        (PBP, {"comments": []}),
        (PBP, {"comments": {"comment": [{}]}}),
        (MOBILE, {"commentEvents": []}),
        (SHOTS, {"match": {"id": "2928321", "shots": []}}),
        (EVENTS, {"home": {"shots": {}}, "visitor": {"shots": {"1": 0}}}),
        (BOARD, {}),
    ],
)
def test_a_stub_body_raises_no_data(transport, url: str, stub: Any) -> None:
    _serve(transport, stub)
    with pytest.raises(NoDataError):
        rt._get(url)


def test_a_shot_file_for_another_game_is_a_failed_fetch(transport) -> None:
    _serve(transport, _body("2016", "visualization__shots__match_id"))  # game 2508598
    with pytest.raises(AssetFetchError, match="2508598"):
        rt._get(SHOTS)


def test_a_scoreboard_for_another_day_is_a_failed_fetch(transport) -> None:
    body = _body("full", "scoreboard__date")
    _serve(transport, body)
    with pytest.raises(AssetFetchError, match="11-10-2026"):
        rt._get("https://json.esports.cz/hokejcz/scoreboard/2026-10-12.json")


def test_a_scoreboard_day_without_an_elh_game_raises_no_data(transport) -> None:
    body = _body("full", "scoreboard__date")
    body = {"82": {"league_name": "Maxa liga", "matches": body["101"]["matches"][:1]}}
    _serve(transport, body)
    with pytest.raises(NoDataError, match="101"):
        rt._get(BOARD)


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("<html>no</html", "not XML"),
        ('<?xml version="1.0"?><Error><Code>AccessDenied</Code></Error>', "not ListBucketResult"),
        (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated></ListBucketResult>',
            "no key",
        ),
    ],
)
def test_a_broken_listing_page_is_a_failed_fetch(transport, text: str, match: str) -> None:
    _serve(transport, text)
    with pytest.raises(AssetFetchError, match=match):
        rt._get(LIST, {"prefix": "match/2026/full/"})


# ---------------------------------------------------------------- elh_game_ids


def _page(keys: List[str], truncated: bool) -> str:
    """A listing page cut from the real capture: same envelope, chosen keys."""
    real = _listing()
    head, rest = real.split("<Contents>", 1)
    block = "<Contents>" + rest.split("</Contents>", 1)[0] + "</Contents>"
    old_key = block.split("<Key>", 1)[1].split("</Key>", 1)[0]
    head = head.replace("<IsTruncated>true</IsTruncated>", f"<IsTruncated>{str(truncated).lower()}</IsTruncated>")
    return head + "".join(block.replace(old_key, k) for k in keys) + "</ListBucketResult>\n"


def test_game_ids_page_through_the_season_folder(transport) -> None:
    _serve(
        transport,
        _page(["match/2026/full/2928256.json", "match/2026/full/2928255.json"], truncated=True),
        _page(["match/2026/full/2928300.json", "match/2026/full/2928256.json"], truncated=False),
    )
    df = rt.elh_game_ids(2027)
    assert df["hokejcz_game_id"].to_list() == [2928255, 2928256, 2928300]  # sorted, de-duplicated
    assert df.schema["hokejcz_game_id"] == pl.Int64 and set(df["season"].to_list()) == {2027}
    assert [p for _u, p in transport["calls"]] == [
        {"prefix": "match/2026/full/"},
        {"prefix": "match/2026/full/", "marker": "match/2026/full/2928255.json"},  # last key of page 1
    ]
    assert len(transport["waits"]) == 2


def test_game_ids_of_an_empty_season_raise_no_data(transport) -> None:
    empty = _page([], truncated=False)
    _serve(transport, empty)
    with pytest.raises(NoDataError, match="match/2013/full/"):
        rt.elh_game_ids(2014)


def test_game_ids_stop_when_the_listing_does_not_advance(transport) -> None:
    page = _page(["match/2026/full/2928255.json"], truncated=True)
    _serve(transport, page, page)
    with pytest.raises(AssetFetchError, match="does not advance"):
        rt.elh_game_ids(2027)


def test_game_ids_return_as_pandas(transport) -> None:
    _serve(transport, _page(["match/2026/full/2928255.json"], truncated=False))
    out = rt.elh_game_ids(2027, return_as_pandas=True)
    assert list(out.columns) == ["hokejcz_game_id", "season", "key", "last_modified", "size"]


# ---------------------------------------------------------------- generated wrappers (after codegen)


def test_wrapper_maps_the_season_to_the_start_year_folder(transport) -> None:
    mod = pytest.importorskip("sportsdataverse.hockey.elh.elh")  # rendered by generate.py at integration
    _serve(transport, _body("full", "match__season__full__match_id"))
    df = mod.elh_pbp(season=2027, match_id=2928321)
    assert transport["calls"][0][0] == PBP
    assert df.height == 119
