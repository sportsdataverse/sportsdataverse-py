"""Dataset contracts for the published CFB crosswalks — live validation.

The committed contracts were derived from live pulls (expectations from
history); their null ceilings on the provider-id columns ARE the
cross-provider match-rate floors. The live-gated checks reload each
crosswalk and validate non-strict: completeness-class violations (dtype
flips, row shrink beyond tolerance) fail; drift-class findings (match-rate
movement inside slack, id-value growth) warn by design. The offline check
just pins the committed files' structure.
"""

from __future__ import annotations

import warnings

import pytest

from sportsdataverse.modeling.integrity import read_contract, validate_frame
from tests.conftest import skip_if_no_live

CONTRACTS = {
    "cfb_teams_crosswalk": "tests/fixtures/contracts/dataset_cfb_teams_crosswalk.contract.json",
    "cfb_schedule_crosswalk": "tests/fixtures/contracts/dataset_cfb_schedule_crosswalk.contract.json",
    "cfb_rosters_crosswalk": "tests/fixtures/contracts/dataset_cfb_rosters_crosswalk.contract.json",
}


def _load(name: str):
    if name == "cfb_teams_crosswalk":
        from sportsdataverse.cfb.cfb_loaders import load_cfb_teams_crosswalk

        return load_cfb_teams_crosswalk([2023, 2024])
    if name == "cfb_schedule_crosswalk":
        from sportsdataverse.cfb.cfb_loaders import load_cfb_schedule_crosswalk

        return load_cfb_schedule_crosswalk([2023, 2024])
    from sportsdataverse.cfb.cfb_loaders_extra import load_cfb_rosters_crosswalk

    return load_cfb_rosters_crosswalk()


@pytest.mark.parametrize("name", sorted(CONTRACTS))
def test_committed_contract_structure(name: str) -> None:
    contract = read_contract(CONTRACTS[name])
    assert contract.name == name
    assert contract.min_rows and contract.min_rows > 1000
    id_cols = [c for c in contract.columns if c.endswith("_id")]
    assert id_cols, "a crosswalk contract must cover provider-id columns"
    for col in id_cols:
        cc = contract.columns[col]
        assert cc.null_rate_max is not None  # the match-rate floor
        assert cc.min_value is None and cc.max_value is None  # ids are labels


@skip_if_no_live
@pytest.mark.parametrize("name", sorted(CONTRACTS))
def test_live_crosswalk_meets_contract(name: str) -> None:
    contract = read_contract(CONTRACTS[name])
    frame = _load(name)
    report = validate_frame(frame, contract)
    for violation in report.warnings:
        warnings.warn(f"crosswalk drift [{name}] {violation.kind}[{violation.column}]: {violation.detail}")
    assert report.ok, [f"{v.kind}[{v.column}]: {v.detail}" for v in report.blocking]
