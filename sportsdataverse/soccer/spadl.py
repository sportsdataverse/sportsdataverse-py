"""SPADL actions from a kloppy event dataset (port of socceraction's converter).

Ported to polars from socceraction ``spadl/config.py``, ``spadl/kloppy.py`` and ``spadl/base.py``
(https://github.com/ML-KULeuven/socceraction, commit 93a1242).

MIT License. Copyright (c) 2019 KU Leuven Machine Learning Research Group.
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions: The above copyright notice and this
permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

One deliberate departure from the original: the dataset is transformed to kloppy's
``ACTION_EXECUTING_TEAM`` orientation, so every action already attacks left to right and no
``play_left_to_right`` step exists. socceraction's kloppy converter requests ``HOME_AWAY``, which
flips the home side each period in kloppy 3.19 and yields wrong second-half coordinates.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import polars as pl


if TYPE_CHECKING:  # pragma: no cover
    pass

__all__ = ["ACTIONTYPES", "BODYPARTS", "RESULTS", "SPADL_COLUMNS", "soccer_spadl"]

_log = logging.getLogger(__name__)

FIELD_LENGTH: float = 105.0
FIELD_WIDTH: float = 68.0

BODYPARTS: tuple[str, ...] = ("foot", "head", "other", "head/other", "foot_left", "foot_right")
RESULTS: tuple[str, ...] = ("fail", "success", "offside", "owngoal", "yellow_card", "red_card")
ACTIONTYPES: tuple[str, ...] = (
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "foul",
    "tackle",
    "interception",
    "shot",
    "shot_penalty",
    "shot_freekick",
    "keeper_save",
    "keeper_claim",
    "keeper_punch",
    "keeper_pick_up",
    "clearance",
    "bad_touch",
    "non_action",
    "dribble",
    "goalkick",
)
SPADL_COLUMNS: tuple[str, ...] = (
    "game_id",
    "original_event_id",
    "action_id",
    "period_id",
    "time_seconds",
    "team_id",
    "player_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "bodypart_id",
    "bodypart_name",
    "type_id",
    "type_name",
    "result_id",
    "result_name",
)
SPADL_SCHEMA: dict[str, Any] = {
    "game_id": pl.Utf8,
    "original_event_id": pl.Utf8,
    "action_id": pl.Int64,
    "period_id": pl.Int64,
    "time_seconds": pl.Float64,
    "team_id": pl.Utf8,
    "player_id": pl.Utf8,
    "start_x": pl.Float64,
    "start_y": pl.Float64,
    "end_x": pl.Float64,
    "end_y": pl.Float64,
    "bodypart_id": pl.Int64,
    "bodypart_name": pl.Utf8,
    "type_id": pl.Int64,
    "type_name": pl.Utf8,
    "result_id": pl.Int64,
    "result_name": pl.Utf8,
}

# socceraction spadl/base.py:33-35
MIN_DRIBBLE_LENGTH: float = 3.0
MAX_DRIBBLE_LENGTH: float = 60.0
MAX_DRIBBLE_DURATION: float = 10.0

_TYPE = {name: i for i, name in enumerate(ACTIONTYPES)}
_RESULT = {name: i for i, name in enumerate(RESULTS)}
_BODYPART = {name: i for i, name in enumerate(BODYPARTS)}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SPADL_SCHEMA)


def soccer_spadl() -> None:
    """Placeholder for SPADL conversion function (Task 3)."""
    pass
