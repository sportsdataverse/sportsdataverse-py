"""The sources registry: shape, ordering, and uniqueness of every provider/category rule."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

SOURCES_FILE = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "sources.yaml"

_RULE_KEYS = {"label", "home", "auth", "espn_apis", "flat_apis", "release_bases", "modules", "functions"}


def _doc() -> dict:
    return yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8"))


def test_sources_yaml_exists():
    assert SOURCES_FILE.exists(), f"{SOURCES_FILE} is the registry the docs program is built on"


def test_top_level_is_providers_then_categories():
    doc = _doc()
    assert list(doc) == ["providers", "categories"]


def test_every_entry_has_a_label_and_only_known_keys():
    doc = _doc()
    for section in ("providers", "categories"):
        for key, rule in doc[section].items():
            assert rule.get("label"), f"{section}.{key} needs a label"
            assert set(rule) <= _RULE_KEYS, f"{section}.{key} has unknown keys: {set(rule) - _RULE_KEYS}"


def test_every_entry_carries_at_least_one_matching_rule():
    doc = _doc()
    for section in ("providers", "categories"):
        for key, rule in doc[section].items():
            matchers = ("espn_apis", "flat_apis", "release_bases", "modules", "functions")
            assert any(rule.get(m) for m in matchers), f"{section}.{key} matches nothing"


def test_provider_and_category_keys_do_not_collide():
    doc = _doc()
    assert not (set(doc["providers"]) & set(doc["categories"]))


@pytest.mark.parametrize("field", ["flat_apis", "release_bases", "espn_apis"])
def test_no_stem_is_claimed_by_two_providers(field):
    doc = _doc()
    seen: dict[str, str] = {}
    for key, rule in doc["providers"].items():
        for stem in rule.get(field) or []:
            assert stem not in seen, f"{field} {stem!r} claimed by both {seen[stem]} and {key}"
            seen[stem] = key


def test_every_flat_apis_stem_is_a_real_flat_api():
    from tools.codegen import generate

    stems = {stem for stem, _prefix in generate.FLAT_APIS}
    for key, rule in _doc()["providers"].items():
        for stem in rule.get("flat_apis") or []:
            assert stem in stems, f"providers.{key} names flat API {stem!r}, not in generate.FLAT_APIS"


def test_every_espn_apis_entry_is_a_real_espn_api():
    from tools.codegen import generate

    for key, rule in _doc()["providers"].items():
        for name in rule.get("espn_apis") or []:
            assert name in generate.ESPN_APIS, f"providers.{key} names ESPN API {name!r}"


def test_every_release_base_is_a_real_releases_yaml_base():
    from tools.codegen import generate, spec

    bases = set(spec.load_releases(generate.ENDPOINTS / "releases.yaml").bases)
    for key, rule in _doc()["providers"].items():
        for base in rule.get("release_bases") or []:
            assert base in bases, f"providers.{key} names release base {base!r}"


def test_every_flat_api_stem_is_claimed_by_some_provider():
    """A new flat-API family must be given a provider, or its league page has no section for it."""
    from tools.codegen import generate

    claimed = {stem for rule in _doc()["providers"].values() for stem in rule.get("flat_apis") or []}
    unclaimed = sorted({stem for stem, _prefix in generate.FLAT_APIS} - claimed)
    assert not unclaimed, f"flat APIs with no provider in sources.yaml: {unclaimed}"
