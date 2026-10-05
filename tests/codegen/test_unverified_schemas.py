"""Guards on ``unverified`` returns-schemas, for every family (nba/wnba/on3/...)."""

from pathlib import Path

import pytest
import yaml

SCHEMAS = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "schemas"
_UNVERIFIED = [
    (p, doc)
    for p in sorted(SCHEMAS.rglob("*.yaml"))
    if isinstance(doc := yaml.safe_load(p.read_text(encoding="utf-8")), dict) and "unverified" in doc
]


def _check(doc: dict) -> None:
    assert str(doc["unverified"]).strip(), "an unverified schema needs a non-empty reason"
    assert not doc.get("columns"), "an unverified schema must publish no columns"
    assert not any(f.get("columns") for f in doc.get("frames") or []), "...and no frames"


def test_some_schemas_are_unverified():
    assert _UNVERIFIED  # the parametrized guard below must not pass by collecting nothing


@pytest.mark.parametrize("path", [p for p, _ in _UNVERIFIED], ids=lambda p: p.relative_to(SCHEMAS).as_posix())
def test_unverified_schema_has_reason_and_no_columns(path):
    _check(yaml.safe_load(path.read_text(encoding="utf-8")))


@pytest.mark.parametrize(
    "bad",
    [
        {"unverified": "", "columns": []},
        {"unverified": "why", "columns": [{"name": "a"}]},
        {"unverified": "why", "frames": [{"section": "s", "columns": [{"name": "a"}]}]},
    ],
)
def test_guard_fails_on_a_bad_schema(bad):
    with pytest.raises(AssertionError):
        _check(bad)


def test_variant_frames_functions_are_not_described_as_returning_a_dict():
    """`frames_by` = one table per value of a request parameter; the function returns ONE DataFrame."""
    from sportsdataverse.nfl import pff_api

    by = {
        p.stem: yaml.safe_load(p.read_text(encoding="utf-8")).get("frames_by")
        for p in (SCHEMAS / "native" / "pff_api").glob("*.yaml")
        if yaml.safe_load(p.read_text(encoding="utf-8")).get("kind") == "frames"
    }
    variants = {short: param for short, param in by.items() if param}
    assert set(variants) == {"position_report", "team_stats", "team_leaders", "team_report"}
    # `kind: frames` WITHOUT frames_by is a true multi-table body (parse_pff_report returns a
    # dict, e.g. {defenders, receivers, versus}); pin the set so a new one is a deliberate choice.
    assert set(by) - set(variants) == {"receiving_coverage_stats"}
    for short in variants:
        doc = getattr(pff_api, f"pff_api_{short}").__doc__
        assert "dict of polars" not in doc and "dict of pandas" not in doc, short
    for fn in ("pff_api_facet_receiving_coverage", "pff_api_facet_defense_coverage_matchup"):
        assert "dict of polars" in getattr(pff_api, fn).__doc__, fn
