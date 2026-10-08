"""Every public function documents what it returns. No allowlist."""

from __future__ import annotations

import inspect

import pytest

from tools.codegen import generate

_SECTIONS = ("Returns:", "Returns\n", "Yields:", "Yields\n")


def test_returns_gaps_is_empty():
    gaps = generate._returns_gaps()
    assert not gaps, "public functions with no Returns/Yields section:\n" + "\n".join(f"  {lg}.{n}" for lg, n in gaps)


def test_the_gate_flags_an_undocumented_return_and_exempts_none_and_classes(monkeypatch):
    def none_fn() -> None:
        """Does a thing."""

    def int_fn() -> int:
        """Counts things."""

    def doc_fn() -> int:
        """Counts things.

        Returns:
            The count.
        """

    class Klass:
        """A class: napoleon documents it with Attributes:, not Returns:."""

    objs = {("global", f.__name__): f for f in (none_fn, int_fn, doc_fn, Klass)}
    monkeypatch.setattr(generate, "_source_scope_objects", lambda: dict.fromkeys(objs, "m"))
    monkeypatch.setattr(generate, "_scope_callable", lambda label, name: objs[(label, name)])
    assert generate._returns_gaps() == [("global", "int_fn")]


def test_the_gate_catches_a_missing_returns_section(monkeypatch):
    def f() -> int:
        """Does a thing with no Returns block."""
        return 1

    monkeypatch.setattr(generate, "_source_scope_objects", lambda: {("nfl", "f"): "tests.test_public_returns"})
    monkeypatch.setattr(generate, "_scope_callable", lambda label, name: f)
    assert generate._returns_gaps() == [("nfl", "f")]


def test_the_gate_accepts_a_yields_section(monkeypatch):
    def g():
        """Streams things.

        Yields:
            int: one at a time.
        """
        yield 1

    monkeypatch.setattr(generate, "_source_scope_objects", lambda: {("nfl", "g"): "tests.test_public_returns"})
    monkeypatch.setattr(generate, "_scope_callable", lambda label, name: g)
    assert generate._returns_gaps() == []


@pytest.mark.parametrize("section", _SECTIONS)
def test_every_accepted_section_spelling_is_recognised(section):
    assert generate._documents_a_return(f"Summary.\n\n{section}\n    x: y\n")


def test_prose_mentioning_the_word_return_is_not_enough():
    assert not generate._documents_a_return("Summary. It will return a frame eventually.\n")


def test_signature_introspection_still_works_on_every_in_scope_name():
    """A name the gate cannot introspect is a gate bug, not a pass."""
    bad = []
    for (label, name), _module in generate._source_scope_objects().items():
        obj = generate._scope_callable(label, name)
        if obj is None:
            bad.append(f"{label}.{name}")
            continue
        if inspect.isclass(obj):
            continue  # the gate exempts classes and never introspects them
        try:
            inspect.signature(obj)
        except (TypeError, ValueError) as e:
            bad.append(f"{label}.{name} ({type(e).__name__})")
    assert not bad, "unresolvable or un-introspectable in-scope names: " + ", ".join(bad)


def test_a_class_is_not_asked_for_a_returns_section(monkeypatch):
    """Calling a class returns an instance of it; napoleon documents a class with Attributes:,
    not Returns:. 146 in-scope names are dataclasses, enums and model classes."""

    class Event:
        """A shot event.

        Attributes:
            x: court x.
        """

    monkeypatch.setattr(generate, "_source_scope_objects", lambda: {("mbb", "Event"): "tests.test_public_returns"})
    monkeypatch.setattr(generate, "_scope_callable", lambda label, name: Event)
    assert generate._returns_gaps() == []


def _gap_names(label: str) -> list[str]:
    return [n for lg, n in generate._returns_gaps() if lg == label]


def test_pwhl_has_no_returns_gap():
    assert not _gap_names("pwhl")


def test_global_scope_has_no_returns_gap():
    assert not _gap_names("global")


def test_nfl_has_no_returns_gap():
    assert not _gap_names("nfl")


def test_mlb_has_no_returns_gap():
    assert not _gap_names("mlb")
