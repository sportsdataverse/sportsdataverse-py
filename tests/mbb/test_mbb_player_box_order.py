"""Pin the WBB/MBB ``active``-column divergence in the player-box select order.

Both the men's and women's ESPN player-box producers share one canonical
column-order template (``wbb_player_box._FINAL_ORDER``), but the two real
release parquets order ONE column differently:

* **WBB** (``espn_womens_college_basketball_player_boxscores``): ``active``
  sits mid-list, immediately after ``did_not_play``/``reason``.
* **MBB** (``espn_mens_college_basketball_player_boxscores``): ``active`` is
  the LAST column.

Both positions were confirmed against the real R-released 2025 parquets
(``wehoop-wbb-data/wbb/player_box/parquet/player_box_2025.parquet`` at
``active`` index 30, and ``hoopR-mbb-data/mbb/player_box/parquet/
player_box_2025.parquet`` at ``active`` index 54 -- the last of 55 columns;
``reason`` is a conditionally-emitted column absent from both 2025 releases,
so the shared template's mid-list ``active`` lands at index 30 once ``reason``
is filtered out, matching the WBB oracle exactly).

This is an offline structural pin. The full-frame value/order parity against
the real MBB oracle lives in
``hoopR-mbb-data/python/tests/mbb_data_build/test_parity_player_box.py``
(``assert_parquet_parity(..., require_order=True)``); there is no equivalent
committed WBB-side build package, so the WBB order is guarded here at the
template level.
"""

from sportsdataverse.mbb.mbb_player_box import _MBB_FINAL_ORDER
from sportsdataverse.wbb.wbb_player_box import _FINAL_ORDER


def test_wbb_keeps_active_mid_list_after_did_not_play():
    # WBB oracle: active is NOT last; it follows did_not_play/reason.
    assert _FINAL_ORDER[-1] != "active"
    i = _FINAL_ORDER.index("active")
    assert _FINAL_ORDER[i - 2 : i] == ("did_not_play", "reason")


def test_mbb_moves_active_to_the_end():
    # MBB oracle: active is the final column.
    assert _MBB_FINAL_ORDER[-1] == "active"


def test_mbb_order_is_wbb_order_with_active_last():
    # Same column set, differing only in active's position.
    assert set(_MBB_FINAL_ORDER) == set(_FINAL_ORDER)
    assert _MBB_FINAL_ORDER == tuple(c for c in _FINAL_ORDER if c != "active") + ("active",)
