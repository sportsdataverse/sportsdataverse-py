"""Offline tests for the PFF per-league binder + the 4 one-line shim modules."""

import importlib
import inspect

import pytest

from sportsdataverse.nfl.pff_league import make_pff_league_module


def test_helper_binds_league_and_preserves_metadata():
    ns: dict = {}
    names = make_pff_league_module(ns, "ncaa")
    assert "pff_facet_passing_summary" in names
    bound = ns["pff_facet_passing_summary"]
    sig = inspect.signature(bound)
    # league is pre-bound -> either dropped from the signature or carries the slug default
    assert (
        sig.parameters.get("league") is None
        or sig.parameters["league"].default == "ncaa"
        or "league" not in sig.parameters
    )
    assert bound.__name__ == "pff_facet_passing_summary"
    assert bound.__doc__


def test_singletons_installed_unbound():
    ns: dict = {}
    make_pff_league_module(ns, "nfl")
    assert "pff_leagues" in ns  # singleton, no league param, installed as-is


def test_bound_call_injects_league(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_PREMIUM_KEY", "PK")
    ns: dict = {}
    make_pff_league_module(ns, "ufl")
    seen: dict = {}

    def fake(url, params, headers, cookies):
        seen["league"] = params.get("league")
        return 200, "{}"

    ns["pff_facet_passing_summary"](transport=fake)
    assert seen["league"] == "ufl"


@pytest.mark.parametrize(
    "mod,slug",
    [
        ("sportsdataverse.nfl.pff", "nfl"),
        ("sportsdataverse.cfb.pff", "ncaa"),
        ("sportsdataverse.football.aaf.pff", "aaf"),
        ("sportsdataverse.football.ufl.pff", "ufl"),
    ],
)
def test_shim_modules_prebind_league(mod, slug, monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_PREMIUM_KEY", "PK")
    m = importlib.import_module(mod)
    assert "pff_facet_passing_summary" in m.__all__
    seen: dict = {}

    def fake(url, params, headers, cookies):
        seen["league"] = params.get("league")
        return 200, "{}"

    m.pff_facet_passing_summary(transport=fake)
    assert seen["league"] == slug
