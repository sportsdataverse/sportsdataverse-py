"""Shared render cache for the codegen tests.

Four tests dominated the suite -- 2,222s of a 2,755s run, 81% of the whole thing
-- and almost all of it was the SAME work repeated. ``_render_docs_all()`` ran
four to five times per session:

* ``test_render_is_deterministic[_render_docs_all]`` rendered twice (965s),
* ``test_rendered_content_is_lf_only`` rendered both corpora again (417s),
* ``test_generated_docs_tree_is_current`` rendered the tree again (320s).

Only the determinism test needs a second render -- that IS its assertion. The
other two just need *a* corpus, so they can share one.

``first_render`` renders each renderer once per session and hands back the cached
result. Determinism still compares two INDEPENDENT invocations (one cached, one
fresh), so the property under test is unchanged; it simply stops paying for a
render another test already did.

xdist caveat, and why the group marker matters: a session fixture is scoped to a
WORKER, not the run. Under ``-n auto`` these tests can land on three different
workers and each would render from scratch, silently undoing the saving. They are
marked ``xdist_group("codegen_render")`` and CI passes ``--dist loadgroup`` so they
share one worker -- and one cache -- while the rest of the suite still spreads out.
"""

from __future__ import annotations

from typing import Callable, Dict

import pytest


@pytest.fixture(scope="session")
def first_render() -> Callable[[Callable[[], Dict[str, str]]], Dict[str, str]]:
    """Return a getter that renders each renderer at most once per session.

    Returns:
        A callable taking a ``generate._render_*`` function and returning its
        rendered corpus, computed on first request and cached thereafter.
    """
    cache: Dict[Callable[[], Dict[str, str]], Dict[str, str]] = {}

    def get(render: Callable[[], Dict[str, str]]) -> Dict[str, str]:
        if render not in cache:
            cache[render] = render()
        return cache[render]

    return get
