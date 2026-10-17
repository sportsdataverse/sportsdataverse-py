"""One HTTP policy for the basketball runtimes (see the module docstring of ``basketball_runtime``).

* 500 is not retried where the provider uses it for a deterministic answer; the families are listed there and here.
* ``dl_utils.download(num_retries=...)`` counts retries, so ``ATTEMPTS`` attempts mean ``ATTEMPTS - 1`` retries.
* ``cba`` backs off exponentially and honours ``Retry-After``.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any, List

import pytest

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.cba import cba_runtime as cba
from sportsdataverse.basketball.dbl import sportality_runtime as dbl
from sportsdataverse.errors import AssetFetchError

NO_500 = {
    "cba": "cba.cba_runtime",
    "cebl": "cebl.cebl_runtime",
    "easl": "easl.easl_runtime",
    "genius": "genius.genius_runtime",
    "ibba": "ibba.ibba_runtime",
    "ibba_segev": "ibba.ibba_segev_runtime",
    "kbl": "kbl.kbl_runtime",
    "lba": "lba.lba_runtime",
    "dbl": "dbl.sportality_runtime",
    "plg": "plg.pleague_runtime",
    "plg_preciser": "plg.pleague_preciser_runtime",
    "tbf": "tbf.tbf_runtime",
}
WITH_500 = {
    "aba": "aba.aba_runtime",
    "acb": "acb.acb_runtime",
    "bleague": "bleague.bleague_runtime",
    "bnxt": "bnxt.bnxt_runtime",
    "feb": "feb.feb_runtime",
    "lkl": "lkl.lkl_runtime",
    "tpbl": "tpbl.tpbl_runtime",
}


def _statuses(module: str) -> frozenset:
    return importlib.import_module(f"sportsdataverse.basketball.{module}").RETRY_STATUSES


@pytest.mark.parametrize("name", sorted(NO_500))
def test_a_deterministic_500_is_not_retried(name):
    assert 500 not in _statuses(NO_500[name]) and 503 in _statuses(NO_500[name])


@pytest.mark.parametrize("name", sorted(WITH_500))
def test_a_transient_500_is_retried(name):
    assert 500 in _statuses(WITH_500[name])


def test_the_policy_is_written_down_where_the_families_are_listed():
    doc = bb.__doc__ or ""
    assert "HTTP 500 policy" in doc
    for stem in ("cba", "cebl", "genius", "ibba", "kbl", "tbf", "plg", "dbl", "aba", "acb", "feb", "lkl"):
        assert f"``{stem}``" in doc, stem


def test_sportality_asks_download_for_attempts_minus_one_retries(monkeypatch):
    seen: List[Any] = []

    def fake_download(url: str, **kwargs: Any) -> Any:
        seen.append(kwargs)
        return SimpleNamespace(status_code=200, text="[]", headers={})

    monkeypatch.setattr(dbl, "download", fake_download)
    dbl._transport("https://example.invalid/x")
    assert seen[0]["num_retries"] == dbl.ATTEMPTS - 1


def test_cba_backs_off_and_honours_retry_after(monkeypatch):
    sleeps: List[float] = []
    monkeypatch.setattr(cba.time, "sleep", sleeps.append)
    answers = [
        SimpleNamespace(status_code=503, headers={}, text=""),
        SimpleNamespace(status_code=429, headers={"Retry-After": "11"}, text=""),
        SimpleNamespace(status_code=503, headers={}, text=""),
    ]
    monkeypatch.setattr(cba.SESSION, "request", lambda *a, **k: answers.pop(0))
    with pytest.raises(AssetFetchError, match="503"):
        cba._fetch("x/y", {}, None)
    assert sleeps == [2.0, 11.0]  # attempt 0: 2 s backoff; attempt 1: Retry-After beats the 4 s backoff
