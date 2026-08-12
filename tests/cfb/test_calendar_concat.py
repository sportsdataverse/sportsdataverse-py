"""``espn_{cfb,nfl}_calendar`` must survive a payload-shaped column set.

Both calendar loaders flatten each season-type block with its own
``pandas.json_normalize(..., errors="ignore")`` call and concatenate the
results. That means the column set is decided by the payload, not fixed by
construction: a key absent from every entry in one block silently drops that
column from that block, and ``how="vertical"`` then raises ``ShapeError``.

**Honest scope note.** Unlike the NFL release parquets, this is defensive
hardening rather than a fix for an observed failure — probing 20 seasons
across both leagues (2002-2025) found the live calendar blocks uniform, so
there is no real drift span to capture. ``test_dropping_one_optional_key_*``
therefore deletes an optional key from one block of a REAL captured payload
(the mutation ``errors="ignore"`` exists to tolerate) and asserts the loader
survives it while the pre-fix ``vertical`` concat does not.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb import cfb_schedule as _cfb
from sportsdataverse.nfl import nfl_schedule as _nfl

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_schedule"

# The per-entry key most plausibly absent from a block: it is optional in the
# ESPN payload and `errors="ignore"` is what lets it go missing without raising.
_OPTIONAL_ENTRY_KEY = "detail"


def _payload(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _drop_optional_key_from_first_block(payload: dict) -> dict:
    """Delete an optional entry key from the FIRST block only -> column drift."""
    payload = copy.deepcopy(payload)
    blocks = [b for b in payload["leagues"][0]["calendar"] if b.get("entries")]
    for entry in blocks[0]["entries"]:
        entry.pop(_OPTIONAL_ENTRY_KEY, None)
    return payload


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


@pytest.fixture()
def leagues(monkeypatch):
    """(loader, module, fixture-name) for both calendars, patched offline."""

    def serve(module, payload):
        monkeypatch.setattr(module, "download", lambda *a, **k: _FakeResponse(payload))

    return serve


_CASES = [
    pytest.param(_cfb, "espn_cfb_calendar", "cfb_calendar_2024.json", id="cfb"),
    pytest.param(_nfl, "espn_nfl_calendar", "nfl_calendar_2024.json", id="nfl"),
]


@pytest.mark.parametrize(("module", "fn_name", "fixture"), _CASES)
def test_the_captured_payloads_are_uniform(module, fn_name, fixture):
    """Guard the honesty of the docstring above: no captured drift to lean on."""
    import pandas as pd

    sigs = {
        tuple(
            pd.json_normalize(
                data=block,
                record_path="entries",
                meta=["label", "value", "startDate", "endDate"],
                meta_prefix="season_type_",
                record_prefix="week_",
                errors="ignore",
                sep="_",
            ).columns
        )
        for block in _payload(fixture)["leagues"][0]["calendar"]
        if block.get("entries")
    }
    assert len(sigs) == 1, "captured blocks drifted -- update the fixture README"


@pytest.mark.parametrize(("module", "fn_name", "fixture"), _CASES)
def test_calendar_reads_the_real_payload(module, fn_name, fixture, leagues):
    leagues(module, _payload(fixture))
    df = getattr(module, fn_name)(season=2024)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert {"week", "season_type", "season"}.issubset(df.columns)


@pytest.mark.parametrize(("module", "fn_name", "fixture"), _CASES)
def test_dropping_one_optional_key_no_longer_breaks_the_concat(module, fn_name, fixture, leagues):
    drifted = _drop_optional_key_from_first_block(_payload(fixture))

    # Pre-fix behaviour: the very same frames blow up under `how="vertical"`.
    import pandas as pd

    frames = [
        pl.from_pandas(
            pd.json_normalize(
                data=block,
                record_path="entries",
                meta=["label", "value", "startDate", "endDate"],
                meta_prefix="season_type_",
                record_prefix="week_",
                errors="ignore",
                sep="_",
            )
        )
        for block in drifted["leagues"][0]["calendar"]
        if block.get("entries")
    ]
    assert len({tuple(f.columns) for f in frames}) > 1, "mutation did not actually drift"
    with pytest.raises((pl.exceptions.ShapeError, pl.exceptions.SchemaError)):
        pl.concat(frames, how="vertical")

    # Post-fix: the loader unions the columns and null-fills the gap.
    leagues(module, drifted)
    df = getattr(module, fn_name)(season=2024)
    assert df.height == sum(f.height for f in frames)
    assert "week_detail" in df.columns
    assert df["week_detail"].null_count() == frames[0].height


def test_no_vertical_concat_left_in_cfb_schedule():
    offenders = [
        line.strip()
        for line in Path(_cfb.__file__).read_text(encoding="utf-8").splitlines()
        if "pl.concat(" in line and 'how="vertical' in line
    ]
    assert offenders == []
