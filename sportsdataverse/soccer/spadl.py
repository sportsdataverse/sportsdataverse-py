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
from typing import TYPE_CHECKING, Any, Optional, Union

import polars as pl

from sportsdataverse.soccer.soccer_events import _kloppy


if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

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


def _fix_clearances(df: pl.DataFrame) -> pl.DataFrame:
    """A clearance ends where the next action starts (socceraction spadl/base.py:13-20)."""
    nxt_x = pl.col("start_x").shift(-1).over("game_id")
    nxt_y = pl.col("start_y").shift(-1).over("game_id")
    is_clear = pl.col("type_id") == _TYPE["clearance"]
    # coordinates are in each actor's attacking frame: the other team's next action must be mirrored
    flip = pl.col("team_id") != pl.col("team_id").shift(-1).over("game_id")
    nxt_x = pl.when(flip).then(FIELD_LENGTH - nxt_x).otherwise(nxt_x)
    nxt_y = pl.when(flip).then(FIELD_WIDTH - nxt_y).otherwise(nxt_y)
    return df.with_columns(
        pl.when(is_clear & nxt_x.is_not_null()).then(nxt_x).otherwise(pl.col("end_x")).alias("end_x"),
        pl.when(is_clear & nxt_y.is_not_null()).then(nxt_y).otherwise(pl.col("end_y")).alias("end_y"),
    )


def _add_dribbles(df: pl.DataFrame) -> pl.DataFrame:
    """Insert a synthetic dribble between consecutive actions (socceraction spadl/base.py:38-91)."""
    nxt = {
        c: pl.col(c).shift(-1).over("game_id")
        for c in ("team_id", "type_id", "bodypart_id", "start_x", "start_y", "time_seconds", "period_id", "player_id")
    }
    dx = pl.col("end_x") - nxt["start_x"]
    dy = pl.col("end_y") - nxt["start_y"]
    dist2 = dx * dx + dy * dy
    cond = (
        (pl.col("team_id") == nxt["team_id"])
        & (nxt["type_id"] != _TYPE["foul"])
        & (nxt["type_id"] != _TYPE["shot"])
        & (nxt["bodypart_id"] != _BODYPART["head"])
        & (dist2 >= MIN_DRIBBLE_LENGTH**2)
        & (dist2 <= MAX_DRIBBLE_LENGTH**2)
        & ((nxt["time_seconds"] - pl.col("time_seconds")) < MAX_DRIBBLE_DURATION)
        & (pl.col("period_id") == nxt["period_id"])
    ).fill_null(False)
    prev = df.with_columns(
        nxt["team_id"].alias("_n_team"),
        nxt["player_id"].alias("_n_player"),
        nxt["start_x"].alias("_n_x"),
        nxt["start_y"].alias("_n_y"),
        nxt["time_seconds"].alias("_n_t"),
        cond.alias("_dribble"),
    ).filter(pl.col("_dribble") == True)
    dribbles = prev.select(
        pl.col("game_id"),
        pl.lit(None, dtype=pl.Utf8).alias("original_event_id"),
        (pl.col("action_id").cast(pl.Float64) + 0.1).alias("_order"),
        pl.col("period_id"),
        ((pl.col("time_seconds") + pl.col("_n_t")) / 2).alias("time_seconds"),
        pl.col("_n_team").alias("team_id"),
        pl.col("_n_player").alias("player_id"),
        pl.col("end_x").alias("start_x"),
        pl.col("end_y").alias("start_y"),
        pl.col("_n_x").alias("end_x"),
        pl.col("_n_y").alias("end_y"),
        pl.lit(_BODYPART["foot"], dtype=pl.Int64).alias("bodypart_id"),
        pl.lit(_TYPE["dribble"], dtype=pl.Int64).alias("type_id"),
        pl.lit(_RESULT["success"], dtype=pl.Int64).alias("result_id"),
    )
    base = df.with_columns(pl.col("action_id").cast(pl.Float64).alias("_order")).select(dribbles.columns)
    out = pl.concat([base, dribbles]).sort(["game_id", "period_id", "_order"], maintain_order=True)
    return out.drop("_order").with_row_index("action_id").with_columns(pl.col("action_id").cast(pl.Int64))


def _pitch_scaler(meta: Any, kd: Any) -> Any:
    """Linear map from the dataset's native pitch to 105 x 68 meters, bottom-left origin.

    kloppy's own ``standardized`` pitch conversion is piecewise (it moves points by up to
    1.5 m near the markings); SPADL's reference converter scales linearly, so we do too.
    """
    dims = meta.pitch_dimensions
    if any(v is None for v in (dims.x_dim.min, dims.x_dim.max, dims.y_dim.min, dims.y_dim.max)):
        raise ValueError(
            f"provider {meta.provider} has no fixed pitch dimensions; SPADL needs a metric or native pitch"
        )
    x0, x1, y0, y1 = dims.x_dim.min, dims.x_dim.max, dims.y_dim.min, dims.y_dim.max
    top = meta.coordinate_system.vertical_orientation == kd.VerticalOrientation.TOP_TO_BOTTOM

    def scale(pt: Any) -> tuple[Optional[float], Optional[float]]:
        if pt is None or pt[0] is None:
            return None, None
        x = (pt[0] - x0) / (x1 - x0) * FIELD_LENGTH
        y = (pt[1] - y0) / (y1 - y0) * FIELD_WIDTH
        y = FIELD_WIDTH - y if top else y
        return min(max(x, 0.0), FIELD_LENGTH), min(max(y, 0.0), FIELD_WIDTH)  # socceraction statsbomb.py:210-211

    return scale


def soccer_spadl(
    dataset: Any, *, game_id: Optional[Union[int, str]] = None, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Convert a kloppy event dataset to SPADL actions on the 105 x 68 pitch.

    Every action attacks left to right (kloppy ``ACTION_EXECUTING_TEAM`` orientation), so a
    frame from any provider kloppy reads is comparable. StatsBomb is the tested path.

    Args:
        dataset: A kloppy ``EventDataset`` (e.g. from :func:`soccer_open_dataset`).
        game_id: Game identifier when the dataset's metadata carries none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per on-ball action with the SPADL columns (``type_name``, ``result_name``,
        ``bodypart_name``, start/end coordinates in meters, ``time_seconds`` from the period's kick-off).
        Empty dataset -> zero-row frame with the same schema.

    Raises:
        ImportError: kloppy is missing (``pip install "sportsdataverse[soccer]"``).
        ValueError: neither the dataset nor ``game_id`` names the game.

    Example:
        Quick start::

            from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl
            actions = soccer_spadl(soccer_open_dataset("statsbomb", 8658), game_id=8658)
            print(actions.shape)

        Pipeline next step (one line)::

            actions.filter(pl.col("type_name") == "shot").group_by("team_id").len()

    See Also:
        * `socceraction`_ -- the original SPADL definition and converters (MIT), ported here
        * `kloppy`_ -- reads the provider files this function consumes

    .. _socceraction: https://github.com/ML-KULeuven/socceraction
    .. _kloppy: https://kloppy.pysport.org
    """
    _kloppy()
    import kloppy.domain as kd

    meta = dataset.metadata
    gid: Optional[str] = (
        str(game_id) if game_id is not None else (str(meta.game_id) if getattr(meta, "game_id", None) else None)
    )
    if gid is None:
        raise ValueError("game_id is not in the dataset metadata; pass game_id=")
    if meta.provider != kd.Provider.STATSBOMB:
        _log.warning("soccer_spadl: provider %s is untested; StatsBomb is the verified path", meta.provider)
    ds = dataset.transform(to_orientation=kd.Orientation.ACTION_EXECUTING_TEAM, to_coordinate_system=meta.provider)
    scale = _pitch_scaler(ds.metadata, kd)
    rows = []
    for ev in ds.events:
        t, r, b = _parse_event(ev, kd)
        ex, ey = _end_location(ev, kd)
        sx, sy = scale((ev.coordinates.x, ev.coordinates.y) if ev.coordinates else None)
        ex, ey = scale((ex, ey))
        rows.append(
            {
                "game_id": gid,
                "original_event_id": str(ev.event_id),
                "period_id": int(ev.period.id),
                "time_seconds": ev.timestamp.total_seconds(),
                "team_id": str(ev.team.team_id) if ev.team else None,
                "player_id": str(ev.player.player_id) if ev.player else None,
                "start_x": sx,
                "start_y": sy,
                "end_x": ex,
                "end_y": ey,
                "type_id": t,
                "result_id": r,
                "bodypart_id": b,
            }
        )
    if not rows:
        out = _empty()
        return out.to_pandas() if return_as_pandas else out
    df = (
        pl.DataFrame(rows, schema={k: v for k, v in SPADL_SCHEMA.items() if k in rows[0]})
        .sort(["game_id", "period_id", "time_seconds"], maintain_order=True)
        .filter(pl.col("type_id") != _TYPE["non_action"])
    )
    df = _fix_clearances(df).with_row_index("action_id").with_columns(pl.col("action_id").cast(pl.Int64))
    df = _add_dribbles(df)
    names = {
        "bodypart_name": pl.col("bodypart_id").replace_strict(dict(enumerate(BODYPARTS)), return_dtype=pl.Utf8),
        "type_name": pl.col("type_id").replace_strict(dict(enumerate(ACTIONTYPES)), return_dtype=pl.Utf8),
        "result_name": pl.col("result_id").replace_strict(dict(enumerate(RESULTS)), return_dtype=pl.Utf8),
    }
    out = df.with_columns(**names).select(list(SPADL_COLUMNS))
    return out.to_pandas() if return_as_pandas else out
