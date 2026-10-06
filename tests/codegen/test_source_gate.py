"""Every in-scope public function resolves to exactly one sources.yaml entry, and --check says so."""

from __future__ import annotations

from tools.codegen import generate, sources


def test_coverage_leagues_cover_the_previously_unscoped_leagues():
    for lg in ("soccer", "mch", "ufl", "college_baseball", "cbs", "yahoo", "fox"):
        assert lg in generate._COVERAGE_LEAGUES, f"{lg} must be in the sources/coverage scope"


def test_no_public_function_is_unmapped_or_ambiguous():
    gaps = generate._source_gaps()
    assert not gaps, "unmapped/ambiguous public functions:\n" + "\n".join(f"  {lg}.{n}: {why}" for lg, n, why in gaps)


def test_check_fails_on_an_unmapped_public_function(monkeypatch):
    """A new hand-written module with no registry rule must FAIL, naming the function."""
    monkeypatch.setattr(
        generate,
        "_coverage_scope_names",
        lambda: ({"nfl": {"brand_new_thing"}}, set()),
    )
    monkeypatch.setattr(
        generate,
        "_source_scope_objects",
        lambda: {("nfl", "brand_new_thing"): "sportsdataverse.nfl.brandnew"},
    )
    gaps = generate._source_gaps()
    assert [(lg, n) for lg, n, _ in gaps] == [("nfl", "brand_new_thing")]
    assert "matches no sources.yaml rule" in gaps[0][2]


def test_check_fails_on_an_ambiguous_public_function(monkeypatch):
    monkeypatch.setattr(
        generate,
        "_source_scope_objects",
        lambda: {("nfl", "two_rule_thing"): "sportsdataverse.nfl.fox_pbp.whatever"},
    )

    def _two(name, module, **kw):
        raise sources.AmbiguousSource(f"{name} ({module}) matches 2 sources.yaml rules: fox, cbs")

    monkeypatch.setattr(sources, "resolve", _two)
    gaps = generate._source_gaps()
    assert gaps == [
        (
            "nfl",
            "two_rule_thing",
            "two_rule_thing (sportsdataverse.nfl.fox_pbp.whatever) matches 2 sources.yaml rules: fox, cbs",
        )
    ]
