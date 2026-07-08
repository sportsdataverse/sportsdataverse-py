"""Wave-0 F1: the pff + 247 live-test gates exist and name their env vars."""

from __future__ import annotations


def test_pff_and_247_live_gates_exist():
    from tests.conftest import skip_if_no_247_live, skip_if_no_pff_live

    for marker, env in [
        (skip_if_no_pff_live, "SDV_PY_PFF_LIVE"),
        (skip_if_no_247_live, "SDV_PY_247_LIVE"),
    ]:
        reason = marker.kwargs.get("reason", "")
        assert env in reason, f"{env} not named in gate reason: {reason!r}"
