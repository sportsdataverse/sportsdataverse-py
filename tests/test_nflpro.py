"""Tests for the NFL Pro (Next Gen Stats) parser and runtime.

The runtime tests pin the two behaviours that were discovered by measuring the
live API and that a caller cannot see for themselves: an unsupported query param
comes back as HTTP 200 with an empty body, and responses truncate silently at the
page size.
"""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nfl.nflpro_parsers import parse_nfl_pro_stats
from sportsdataverse.nfl.nflpro_runtime import NFLProAuthError, _get, nflpro_token

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "nflpro"


def load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text())


@pytest.mark.parametrize(
    "name,key",
    [
        ("players_offense_passing_season", "passers"),
        ("team_offense_overview_season", "offense"),
        ("fantasy_game", "players"),
    ],
)
def test_parses_each_collection_key(name, key):
    payload = load(name)
    df = parse_nfl_pro_stats(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == len(payload[key])
    assert df.width > 5


def test_columns_are_snake_cased():
    df = parse_nfl_pro_stats(load("players_offense_passing_season"))
    assert "nfl_id" in df.columns
    assert not [c for c in df.columns if any(ch.isupper() for ch in c)]


def test_echoed_list_param_is_not_mistaken_for_the_data():
    """The envelope echoes request params back, and an echo can itself be a list.

    `fantasy_game` echoes `positionGroup` as a one-element list, so a parser that
    took 'the first list value' would return a 1-row frame of position codes.
    """
    payload = load("fantasy_game")
    assert isinstance(payload.get("positionGroup"), list)
    df = parse_nfl_pro_stats(payload)
    assert df.height == len(payload["players"])
    assert df.height > 1


def test_empty_and_malformed_payloads_yield_zero_rows():
    for payload in ({}, None, [], {"players": []}, "nonsense"):
        df = parse_nfl_pro_stats(payload)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


@pytest.mark.parametrize(
    "payload",
    [
        # The shape that actually ships: a valid query matching zero rows, where
        # the API omits the collection key but still echoes positionGroup as a
        # one-element list of strings. Picking that echo as the records reaches
        # json_normalize as a TypeError.
        {"season": 2024, "positionGroup": ["QB"], "total": 0},
        {"players": ["a", "b"]},
        [1, 2, 3],
    ],
)
def test_scalar_lists_never_raise(payload):
    df = parse_nfl_pro_stats(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_a_stray_scalar_does_not_discard_the_valid_records():
    """One junk element must not cost the whole page -- keep the real records."""
    df = parse_nfl_pro_stats({"passers": [{"a": 1}, "junk", {"a": 2}]})
    assert df.height == 2


def test_leading_junk_does_not_hide_a_trailing_valid_record():
    """Checking only element 0 would reject ["junk", {"a": 1}] outright."""
    df = parse_nfl_pro_stats({"passers": ["junk", {"a": 1}]})
    assert df.height == 1


def test_nested_records_are_snake_cased_not_dotted():
    """json_normalize defaults to `a.b`; underscore() rewrites camel case, not dots."""
    df = parse_nfl_pro_stats({"passers": [{"outer": {"innerValue": 1}}]})
    assert not [c for c in df.columns if "." in c]
    assert "outer_inner_value" in df.columns


def test_colliding_column_spellings_do_not_raise():
    """Two spellings of one field collide after snake-casing; polars rejects dupes."""
    df = parse_nfl_pro_stats({"passers": [{"nflId": 1, "nfl_id": 2}]})
    assert df.height == 1
    assert len(set(df.columns)) == len(df.columns)


def test_return_as_pandas():
    df = parse_nfl_pro_stats(load("team_offense_overview_season"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5


class _Resp:
    def __init__(self, body):
        self._body = body
        self.text = "" if body is None else json.dumps(body)

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


def test_empty_200_raises_rather_than_returning_a_string(monkeypatch):
    """An unsupported param returns HTTP 200 with an EMPTY body and no error envelope."""
    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", lambda **kw: _Resp(None))
    with pytest.raises(ValueError, match="empty body"):
        _get("https://pro.nfl.com/api/secured/stats/x/season", {}, headers={"a": "b"})


def test_get_pages_until_it_has_every_row(monkeypatch):
    """The envelope reports `total` independently of what it returned."""
    total = 12
    rows = [{"nflId": i} for i in range(total)]
    calls: list = []

    def fake(**kw):
        offset = int(kw["params"].get("offset", 0))
        calls.append(offset)
        return _Resp({"passers": rows[offset : offset + 5], "total": total})

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", fake)
    body = _get("https://pro.nfl.com/x", {"limit": 5}, headers={"a": "b"})
    assert len(body["passers"]) == total
    assert calls == [0, 5, 10]


def test_pagination_honors_a_caller_supplied_offset(monkeypatch):
    """Offsets are absolute: paging from zero silently skips the head.

    The nastiest part of the original bug was that the result still ended up with
    len(rows) == total, so it passed its own completeness check while missing the
    first 200 rows and duplicating 100.
    """
    rows = [{"i": i} for i in range(1005)]
    seen: list = []

    def fake(**kw):
        off = int(kw["params"].get("offset", 0))
        lim = int(kw["params"].get("limit", 100))
        seen.append(off)
        return _Resp({"passers": rows[off : off + lim], "total": 1005})

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", fake)
    body = _get("https://pro.nfl.com/x", {"offset": 200, "limit": 100}, headers={"a": "b"})
    got = [r["i"] for r in body["passers"]]
    assert seen[:3] == [200, 300, 400]
    assert got[0] == 200
    assert len(got) == len(set(got)) == 805  # 1005 - 200, no duplicates


def test_server_ignoring_offset_raises_instead_of_duplicating(monkeypatch):
    """A param can be accepted and ignored; extending would pile up duplicates."""
    page = [{"i": i} for i in range(5)]
    monkeypatch.setattr(
        "sportsdataverse.nfl.nflpro_runtime.download",
        lambda **kw: _Resp({"passers": page, "total": 20}),
    )
    with pytest.raises(ValueError, match="ignored `offset`"):
        _get("https://pro.nfl.com/x", {"limit": 5}, headers={"a": "b"})


def test_http_error_surfaces_the_response_body(monkeypatch):
    """A missing required param (e.g. position_group) 500s with no field named
    in a bare requests HTTPError -- the body usually names the failed request."""

    class Failing(_Resp):
        def __init__(self):
            super().__init__({})
            self.text = '{"ok":false,"status":500,"statusText":"missing positionGroup"}'

        def raise_for_status(self):
            exc = Exception("500 Server Error")
            exc.response = self  # what requests.HTTPError carries in real life
            raise exc

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", lambda **kw: Failing())
    with pytest.raises(Exception, match="missing positionGroup") as exc_info:
        _get("https://pro.nfl.com/api/secured/stats/fantasy/game", {}, headers={"a": "b"})
    # A caller inspecting exc.response must still find it -- a fresh
    # type(exc)(...) instance would have silently dropped it.
    assert exc_info.value.response is not None


def test_missing_total_still_pages_on_a_full_page(monkeypatch):
    """With no `total`, a page whose size equals `limit` may still be truncated."""
    rows = [{"i": i} for i in range(12)]

    def fake(**kw):
        off = int(kw["params"].get("offset", 0))
        return _Resp({"passers": rows[off : off + 5]})  # no `total` key at all

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", fake)
    body = _get("https://pro.nfl.com/x", {"limit": 5}, headers={"a": "b"})
    assert len(body["passers"]) == 12


def test_truncation_is_flagged_not_silent(monkeypatch):
    """A partial collection must never look identical to a complete one."""
    rows = [{"i": i} for i in range(1000)]

    def fake(**kw):
        off = int(kw["params"].get("offset", 0))
        return _Resp({"passers": rows[off : off + 5], "total": 1000})

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", fake)
    body = _get("https://pro.nfl.com/x", {"limit": 5}, headers={"a": "b"}, max_pages=3)
    assert body["_truncated"] is True
    assert len(body["passers"]) < 1000


def test_does_not_mutate_the_payload_it_was_handed(monkeypatch):
    """Memory cache mode returns the body by reference; mutating it poisons the entry."""
    cached = {"passers": [{"i": 0}], "total": 2}

    def fake(**kw):
        off = int(kw["params"].get("offset", 0))
        return _Resp(cached if off == 0 else {"passers": [{"i": 1}], "total": 2})

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", fake)
    body = _get("https://pro.nfl.com/x", {}, headers={"a": "b"})
    assert len(body["passers"]) == 2
    assert len(cached["passers"]) == 1  # the "cached" object is untouched


def test_auth_failure_surfaces_as_an_auth_error(monkeypatch):
    class Denied(_Resp):
        status_code = 401

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime.download", lambda **kw: Denied({}))
    with pytest.raises(NFLProAuthError, match="NFL_PLUS"):
        _get("https://pro.nfl.com/x", {}, headers={"a": "b"})


def test_expired_plan_is_not_treated_as_entitlement():
    import base64

    def jwt(claims):
        pad = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return f"header.{pad}.sig"

    expired_plan = {"plans": [{"plan": "NFL_PLUS_PREMIUM", "status": "EXPIRED"}]}
    with pytest.raises(NFLProAuthError, match="NFL_PLUS"):
        nflpro_token(token=jwt(expired_plan))


def test_expired_token_is_rejected():
    import base64

    def jwt(claims):
        pad = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return f"header.{pad}.sig"

    stale = {"plans": [{"plan": "NFL_PLUS_PREMIUM", "status": "ACTIVE"}], "exp": 1}
    with pytest.raises(NFLProAuthError, match="expired"):
        nflpro_token(token=jwt(stale))


def test_paginate_false_returns_one_page(monkeypatch):
    monkeypatch.setattr(
        "sportsdataverse.nfl.nflpro_runtime.download",
        lambda **kw: _Resp({"passers": [{"nflId": 1}], "total": 99}),
    )
    body = _get("https://pro.nfl.com/x", {}, headers={"a": "b"}, paginate=False)
    assert len(body["passers"]) == 1


def test_client_credentials_token_is_rejected():
    """An anonymous token has claims identical to a UID-minted one but no NFL+ plan."""
    import base64

    def jwt(claims):
        pad = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return f"header.{pad}.sig"

    with pytest.raises(NFLProAuthError, match="NFL_PLUS"):
        nflpro_token(token=jwt({"plans": [{"plan": "free", "status": "ACTIVE"}]}))
    assert nflpro_token(token=jwt({"plans": [{"plan": "NFL_PLUS_PREMIUM"}], "exp": 4102444800}))


def test_missing_credentials_raise_a_useful_error(monkeypatch):
    for var in ("NFLPRO_TOKEN", "NFLPRO_EMAIL", "NFLPRO_PW"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime._token_cache", {})
    with pytest.raises(NFLProAuthError, match="NFLPRO_EMAIL"):
        nflpro_token()


def test_cached_token_is_not_reused_across_accounts(monkeypatch):
    """A single-slot cache would hand account B a token minted for account A."""
    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime._token_cache", {})
    logins: list = []

    def fake_login(email, password):
        logins.append(email)
        return f"token-for-{email}"

    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime._browser_login", fake_login)
    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime._entitled", lambda t: True)
    monkeypatch.setattr("sportsdataverse.nfl.nflpro_runtime._fresh", lambda t: True)

    tok_a = nflpro_token(email="a@example.com", password="pw-a")
    tok_b = nflpro_token(email="b@example.com", password="pw-b")
    assert tok_a == "token-for-a@example.com"
    assert tok_b == "token-for-b@example.com"
    assert logins == ["a@example.com", "b@example.com"]

    # A second call for account A hits the cache, not a third login.
    assert nflpro_token(email="a@example.com", password="pw-a") == tok_a
    assert logins == ["a@example.com", "b@example.com"]


def test_team_week_wrappers_do_not_expose_nfl_id():
    """nflId is silently accepted-and-ignored on team-scoped week routes (verified
    live: identical response with or without it), so it must not be a parameter."""
    import inspect

    from sportsdataverse.nfl.nflpro import (
        nfl_pro_team_defense_overview_week,
        nfl_pro_team_offense_overview_week,
    )

    for fn in (nfl_pro_team_offense_overview_week, nfl_pro_team_defense_overview_week):
        assert "nfl_id" not in inspect.signature(fn).parameters
