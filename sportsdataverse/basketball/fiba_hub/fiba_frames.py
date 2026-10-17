"""Frame helpers shared by the FIBA parsers (``fiba_gdap_parsers``, ``fiba_gamepage_parsers``).

**Row building.** :func:`_flatten` turns a nested object into one flat row (``team_a`` +
``organisationId`` -> ``team_a_organisation_id``); a list cell becomes a JSON string, so a polars
frame accepts it. :func:`_frame` builds a frame from rows and refuses nothing: a malformed body is
an empty frame, never an exception.

**Dtypes are pinned, not sampled.** ``fiba_dtypes.DTYPES`` (written by ``tools/codegen/gen_fiba.py``
from the FULL fixture bodies plus the captures) maps every parser to its ordered ``{column: code}``
(``i`` Int64, ``f`` Float64, ``b`` Boolean, ``s`` String). :func:`_finish` gives that dtype to a
populated column, to an all-null column and to a zero-row frame, so two empty frames stack and an
empty frame stacks onto a populated one. A column no fixture carries is typed by its name
(:func:`_code_for`). A rate (``per_game``, ``percentage``, ``average``, ``pct``) is always ``Float64``:
the API sends ``2`` for a rate of 2.0.

**Id key spaces** (one dtype per id, ``Int64``; the parsers never stringify a float):

* ``player_id`` / ``person_id``: one id per person for life, across competitions and roles. The
  game page's ``P_<id>`` and the play-by-play ``pId`` are the same number.
* ``team_id``: one per team PER COMPETITION (Germany is 279569 at the 2023 World Cup and 281545 at
  Paris 2024). Join across seasons on ``organisation_id``.
* ``organisation_id``: the stable federation or club (64 = Germany). The game page's ``T_<id>`` and
  the play-by-play ``oId`` are organisation ids, not team ids.
* ``game_id`` (global), ``competition_id`` (one per edition), ``competition_marketing_name_id`` (one
  per competition family), ``round_id`` and ``venue_id`` (separate tables).
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.fiba_hub import fiba_dtypes
from sportsdataverse.dl_utils import underscore

Frame = Union[pl.DataFrame, pd.DataFrame]

_DTYPE = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
_RATE = re.compile(r"(per_game|percentage|average|_pct$|^pct_)")
_ID = re.compile(r"(^|_)(id|ids)$")
_BOOL = re.compile(r"^(is|has)_")


def _code_for(col: str) -> str:
    """The dtype code of a column no fixture carries, from its name."""
    if _RATE.search(col):
        return "f"
    if _ID.search(col):
        return "i"
    if _BOOL.search(col):
        return "b"
    return "s"


def _dtypes(parser: str) -> Dict[str, str]:
    return fiba_dtypes.DTYPES.get(parser, {})


def _flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    """One flat row: nested objects join their keys with ``_``, a list becomes a JSON string."""
    out: Dict[str, Any] = {}
    if not isinstance(obj, dict):
        return out
    for key, value in obj.items():
        name = f"{prefix}_{key}" if prefix else str(key)
        if isinstance(value, dict):
            out.update(_flatten(value, name))
        elif isinstance(value, list):
            out[name] = json.dumps(value, ensure_ascii=False, separators=(",", ":")) if value else None
        else:
            out[name] = value
    return out


def _snake(row: Mapping[str, Any]) -> Dict[str, Any]:
    """The row with snake_case keys (``dl_utils.underscore``); a later duplicate key never overwrites."""
    out: Dict[str, Any] = {}
    for key, value in row.items():
        out.setdefault(underscore(str(key)), value)
    return out


def _rows(raw: Any) -> List[Dict[str, Any]]:
    """The object rows of a body: a list is its objects, an object with one list key is that list, any other object is one row."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if isinstance(raw, dict) and raw:
        lists = [k for k, v in raw.items() if isinstance(v, list)]
        if len(lists) == 1 and all(not isinstance(v, (dict, list)) for k, v in raw.items() if k != lists[0]):
            return [r for r in raw[lists[0]] if isinstance(r, dict)]
        return [raw]
    return []


def _id(value: Any) -> Optional[int]:
    """An id as ``int``: an int, a digit string, or a ``P_123`` / ``T_64`` / ``O_9`` key; anything else is ``None``.

    A float is never an id (``"123.0"`` and ``123.0`` both fail), and a bool is not an int.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        tail = text.split("_", 1)[1] if re.match(r"^[A-Za-z]_\d+$", text) else text
        if tail.isdigit():
            return int(tail)
    return None


def _frame(rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    rows = [r for r in rows if r]
    if not rows:
        return pl.DataFrame()
    try:
        return pl.from_dicts(rows, infer_schema_length=None, strict=False)
    except Exception:  # noqa: BLE001 -- a malformed body parses to nothing, never raises
        return pl.DataFrame()


def _finish(
    df: pl.DataFrame, parser: str, columns: Optional[Sequence[str]], return_as_pandas: bool, *, order: bool = True
) -> Frame:
    """Apply the parser's pinned dtypes and the documented columns.

    A populated column takes its pinned dtype when the cast loses nothing (a value that does not
    cast would otherwise turn into a silent null); an all-null column takes it always. A zero-row
    frame carries every documented column, typed. A documented column the body lacks is added as nulls.
    """
    pinned = _dtypes(parser)
    names = list(columns) if columns else list(pinned)
    if df.width == 0:
        df = pl.DataFrame(schema={c: _DTYPE[pinned.get(c) or _code_for(c)] for c in names})
    else:
        casts = []
        for col in df.columns:
            series = df[col]
            allnull = series.null_count() == df.height
            code = pinned.get(col)
            if code is None:  # a column no fixture carries: keep what the body says, except rates and all-null columns
                guess = _code_for(col)
                if allnull or (guess == "f" and series.dtype.is_numeric()):
                    code = guess
                else:
                    continue
            want = _DTYPE[code]
            if df.schema[col] == want:
                continue
            if allnull:
                casts.append(pl.col(col).cast(want))
                continue
            try:
                ok = series.cast(want, strict=True)
            except Exception:  # noqa: BLE001 -- keep the column as parsed rather than null the values
                continue
            if code == "i" and not series.dtype.is_integer() and not ok.cast(series.dtype, strict=False).equals(series):
                continue  # a fractional number is not an id
            casts.append(pl.col(col).cast(want, strict=True))
        if casts:
            df = df.with_columns(casts)
        missing = [
            pl.lit(None, dtype=_DTYPE[pinned.get(c) or _code_for(c)]).alias(c) for c in names if c not in df.columns
        ]
        if missing:
            df = df.with_columns(missing)
        if order and names:
            df = df.select([*[c for c in names if c in df.columns], *[c for c in df.columns if c not in names]])
    return df.to_pandas() if return_as_pandas else df
