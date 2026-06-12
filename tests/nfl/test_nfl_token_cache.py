"""Offline tests for the ``api.nfl.com`` token cache + env overrides.

All tests mock :func:`sportsdataverse.nfl.nfl_games._mint_token` (the only thing
that touches the network), so nothing here hits ``/identity/v3/token``. They
cover:

- A minted token is cached and reused across calls (one mint, not N).
- ``force_refresh=True`` re-mints even when a cached token is still valid.
- A token whose JWT ``exp`` is within the renewal skew is re-minted next call.
- Switching credentials (different client key) invalidates the cache.
- ``NFL_ACCESS_TOKEN`` short-circuits minting entirely (and is ignored when
  explicit credentials are passed).
- ``nfl_clear_token_cache()`` forces a fresh mint.
- ``_jwt_exp`` reads the ``exp`` claim and tolerates malformed tokens.
"""

from __future__ import annotations

import base64
import json
import time

import pytest

from sportsdataverse.nfl import nfl_games


def _make_jwt(exp: float | None) -> str:
    """Build a throwaway ``header.payload.sig`` JWT carrying an ``exp`` claim."""
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    claims = {} if exp is None else {"exp": exp}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.sig"


@pytest.fixture(autouse=True)
def _clean_token_state(monkeypatch):
    """Clear the module token cache + NFL_* env vars around every test."""
    nfl_games.nfl_clear_token_cache()
    for var in ("NFL_ACCESS_TOKEN", "NFL_CLIENT_KEY", "NFL_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)
    yield
    nfl_games.nfl_clear_token_cache()


def _patch_mint(monkeypatch, exp_offset: float = 3600.0):
    """Patch ``_mint_token`` with a call-counting stub returning fresh JWTs.

    Returns the ``calls`` list whose length is the mint count; each minted token
    is distinct and expires ``exp_offset`` seconds from mint time.
    """
    calls: list[tuple[str, str]] = []

    def fake_mint(key: str, secret: str) -> str:
        calls.append((key, secret))
        return _make_jwt(time.time() + exp_offset) + f".{len(calls)}"

    monkeypatch.setattr(nfl_games, "_mint_token", fake_mint)
    return calls


def test_token_cached_across_calls(monkeypatch):
    calls = _patch_mint(monkeypatch)
    first = nfl_games.nfl_token_gen()
    second = nfl_games.nfl_token_gen()
    assert first == second
    assert len(calls) == 1  # minted once, served from cache thereafter


def test_force_refresh_remints(monkeypatch):
    calls = _patch_mint(monkeypatch)
    nfl_games.nfl_token_gen()
    nfl_games.nfl_token_gen(force_refresh=True)
    assert len(calls) == 2


def test_expiry_within_skew_triggers_remint(monkeypatch):
    # exp only 10s out -> inside the 120s renewal skew -> next call re-mints.
    calls = _patch_mint(monkeypatch, exp_offset=10.0)
    nfl_games.nfl_token_gen()
    nfl_games.nfl_token_gen()
    assert len(calls) == 2


def test_distinct_credentials_invalidate_cache(monkeypatch):
    calls = _patch_mint(monkeypatch)
    nfl_games.nfl_token_gen()  # bundled default key
    nfl_games.nfl_token_gen(client_key="other-key", client_secret="other-secret")
    assert len(calls) == 2
    assert calls[1] == ("other-key", "other-secret")


def test_changed_secret_same_key_invalidates_cache(monkeypatch):
    # Same key, different secret -> the cache identity must change and re-mint,
    # otherwise an explicit credential override would be silently ineffective.
    calls = _patch_mint(monkeypatch)
    nfl_games.nfl_token_gen(client_key="k", client_secret="s1")
    nfl_games.nfl_token_gen(client_key="k", client_secret="s2")
    assert len(calls) == 2
    assert calls[1] == ("k", "s2")


def test_env_access_token_short_circuits(monkeypatch):
    calls = _patch_mint(monkeypatch)
    monkeypatch.setenv("NFL_ACCESS_TOKEN", "user-supplied-token")
    assert nfl_games.nfl_token_gen() == "user-supplied-token"
    assert len(calls) == 0  # never minted


def test_env_token_ignored_when_explicit_credentials(monkeypatch):
    calls = _patch_mint(monkeypatch)
    monkeypatch.setenv("NFL_ACCESS_TOKEN", "user-supplied-token")
    token = nfl_games.nfl_token_gen(client_key="k", client_secret="s")
    assert token != "user-supplied-token"
    assert len(calls) == 1


def test_clear_token_cache_forces_remint(monkeypatch):
    calls = _patch_mint(monkeypatch)
    nfl_games.nfl_token_gen()
    nfl_games.nfl_clear_token_cache()
    nfl_games.nfl_token_gen()
    assert len(calls) == 2


def test_headers_gen_uses_cached_token(monkeypatch):
    calls = _patch_mint(monkeypatch)
    headers = nfl_games.nfl_headers_gen()
    again = nfl_games.nfl_headers_gen()
    assert headers["Authorization"].startswith("Bearer ")
    assert headers == again
    assert len(calls) == 1  # both header builds shared one minted token


def test_jwt_exp_reads_claim():
    exp = 1893456000.0  # 2030-01-01
    assert nfl_games._jwt_exp(_make_jwt(exp)) == exp


def test_jwt_exp_tolerates_malformed():
    assert nfl_games._jwt_exp("not-a-jwt") is None
    assert nfl_games._jwt_exp(_make_jwt(None)) is None  # payload has no exp
