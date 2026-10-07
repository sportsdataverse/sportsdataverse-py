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
from typing import TYPE_CHECKING, Any, Optional

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


def _qualifiers(event: Any) -> list[Any]:
    return [q.value for q in event.qualifiers] if event.qualifiers else []


def _bodypart(quals: list[Any], kd: Any, default: str = "foot") -> str:
    bp = kd.BodyPart
    if bp.HEAD in quals:
        return "head"
    if bp.RIGHT_FOOT in quals:
        return "foot_right"
    if bp.LEFT_FOOT in quals:
        return "foot_left"
    if bp.CHEST in quals or bp.OTHER in quals:
        return "other"
    if bp.HEAD_OTHER in quals:
        return "head/other"
    return default


_CROSSED = ("CHIPPED_PASS", "CROSS", "HIGH_PASS", "LONG_BALL")


def _parse_pass(event: Any, kd: Any) -> tuple[str, str, str]:
    quals = _qualifiers(event)
    b = _bodypart(quals, kd)
    sp, pt, pr = kd.SetPieceType, kd.PassType, kd.PassResult
    crossed = any(getattr(pt, name) in quals for name in _CROSSED)
    if sp.FREE_KICK in quals:
        a = "freekick_crossed" if crossed else "freekick_short"
    elif sp.CORNER_KICK in quals:
        a = "corner_crossed" if crossed else "corner_short"
    elif sp.GOAL_KICK in quals:
        a = "goalkick"
    elif sp.THROW_IN in quals:
        a, b = "throw_in", "other"
    elif pt.CROSS in quals:
        a = "cross"
    else:
        a = "pass"
    if kd.BodyPart.KEEPER_ARM in quals:
        b = "other"
    if event.result in (pr.INCOMPLETE, pr.OUT):
        r = "fail"
    elif event.result == pr.OFFSIDE:
        r = "offside"
    elif event.result == pr.COMPLETE:
        r = "success"
    else:  # interrupted passes are discarded, as upstream
        a, r = "non_action", "success"
    return a, r, b


def _parse_shot(event: Any, kd: Any) -> tuple[str, str, str]:
    quals = _qualifiers(event)
    b = _bodypart(quals, kd)
    sp, sr = kd.SetPieceType, kd.ShotResult
    a = "shot_freekick" if sp.FREE_KICK in quals else "shot_penalty" if sp.PENALTY in quals else "shot"
    if event.result == sr.GOAL:
        r = "success"
    elif event.result == sr.OWN_GOAL:
        a, r = "bad_touch", "owngoal"
    else:
        r = "fail"
    return a, r, b


def _parse_take_on(event: Any, kd: Any) -> tuple[str, str, str]:
    return "take_on", ("success" if event.result == kd.TakeOnResult.COMPLETE else "fail"), "foot"


def _parse_carry(event: Any, kd: Any) -> tuple[str, str, str]:
    return "dribble", "success", "foot"


def _parse_interception(event: Any, kd: Any) -> tuple[str, str, str]:
    ir = kd.InterceptionResult
    r = "fail" if event.result in (ir.LOST, ir.OUT) else "success"
    return "interception", r, _bodypart(_qualifiers(event), kd)


def _parse_foul(event: Any, kd: Any) -> tuple[str, str, str]:
    quals = _qualifiers(event)
    ct = kd.CardType
    r = "fail"
    if ct.FIRST_YELLOW in quals:
        r = "yellow_card"
    elif ct.SECOND_YELLOW in quals or ct.RED in quals:
        r = "red_card"
    return "foul", r, "foot"


def _parse_duel(event: Any, kd: Any) -> tuple[str, str, str]:
    quals = _qualifiers(event)
    dt = kd.DuelType
    a = "tackle" if (dt.GROUND in quals and dt.LOOSE_BALL not in quals) else "non_action"
    r = "fail" if event.result == kd.DuelResult.LOST else "success"
    return a, r, "foot"


def _parse_clearance(event: Any, kd: Any) -> tuple[str, str, str]:
    return "clearance", "success", _bodypart(_qualifiers(event), kd)


def _parse_miscontrol(event: Any, kd: Any) -> tuple[str, str, str]:
    return "bad_touch", "fail", "foot"


def _parse_goalkeeper(event: Any, kd: Any) -> tuple[str, str, str]:
    quals = _qualifiers(event)
    gk = kd.GoalkeeperActionType
    a = "non_action"
    b = _bodypart(quals, kd, default="other")
    if gk.SAVE in quals:
        a = "keeper_save"
    if gk.CLAIM in quals or gk.SMOTHER in quals:
        a = "keeper_claim"
    if gk.PUNCH in quals:
        a = "keeper_punch"
    if gk.PICK_UP in quals:
        a = "keeper_pick_up"
    return a, "success", b


def _non_action(event: Any, kd: Any) -> tuple[str, str, str]:
    return "non_action", "success", "foot"


def _parse_event(event: Any, kd: Any) -> tuple[int, int, int]:
    """Map one kloppy event to ``(type_id, result_id, bodypart_id)``."""
    et = kd.EventType
    parsers = {
        et.PASS: _parse_pass,
        et.SHOT: _parse_shot,
        et.TAKE_ON: _parse_take_on,
        et.CARRY: _parse_carry,
        et.FOUL_COMMITTED: _parse_foul,
        et.DUEL: _parse_duel,
        et.CLEARANCE: _parse_clearance,
        et.MISCONTROL: _parse_miscontrol,
        et.GOALKEEPER: _parse_goalkeeper,
        et.INTERCEPTION: _parse_interception,
    }
    a, r, b = parsers.get(event.event_type, _non_action)(event, kd)
    return _TYPE[a], _RESULT[r], _BODYPART[b]


def _end_location(event: Any, kd: Any) -> tuple[Optional[float], Optional[float]]:
    """End coordinates: pass -> receiver, carry -> end, shot -> result, otherwise the start."""
    et = event.event_type
    if et == kd.EventType.PASS and event.receiver_coordinates:
        return event.receiver_coordinates.x, event.receiver_coordinates.y
    if et == kd.EventType.CARRY and event.end_coordinates:
        return event.end_coordinates.x, event.end_coordinates.y
    if et == kd.EventType.SHOT and event.result_coordinates:
        return event.result_coordinates.x, event.result_coordinates.y
    if event.coordinates:
        return event.coordinates.x, event.coordinates.y
    return None, None


def soccer_spadl() -> None:
    """Placeholder for SPADL conversion function (Task 3)."""
    pass
