"""Codegen determinism + LF invariants.

The generator must be idempotent: rendering twice produces byte-identical
output, and that output is LF-only. Without this, re-running ``generate.py``
emits churn (notably CRLF phantom diffs on Windows) and "latent drift" slips
through until CI's unconditional ``--check``. These offline tests lock both
properties in (no network -- the renderers introspect the installed package).
"""

from __future__ import annotations

import pytest

from tools.codegen import generate


@pytest.mark.parametrize(
    "render",
    [
        generate._render_all,
        generate._render_docs_all,
        generate._render_loaders_all,
        generate._render_parsed_all,
        generate._render_flat_all,
    ],
)
@pytest.mark.xdist_group("codegen_render")
def test_render_is_deterministic(render, first_render) -> None:
    # Same inputs -> identical output on repeat (no dict-ordering / timestamp /
    # randomness leaking into generated artifacts). Still two INDEPENDENT
    # invocations; the second is the session-cached one the other codegen tests
    # reuse, so the property is unchanged and nothing renders a third time.
    assert render() == first_render(render)


@pytest.mark.xdist_group("codegen_render")
def test_rendered_content_is_lf_only(first_render) -> None:
    # Rendered source + docs must contain no carriage returns, so the write step
    # (now newline="\n" everywhere) yields the same bytes on every platform.
    # Reuses the session render: this asserts a property OF the corpus, not that
    # rendering repeats, so a fresh render buys nothing here.
    corpus = {**first_render(generate._render_all), **first_render(generate._render_docs_all)}
    offenders = [name for name, content in corpus.items() if "\r" in content]
    assert not offenders, f"rendered content has CR characters: {offenders}"
