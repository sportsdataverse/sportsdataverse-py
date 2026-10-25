"""Private helpers shared by the pandas-hop parser modules (task 6.1, A-N15).

One copy of the bodies that were byte-identical across ``mlb/mlb_api_parsers.py``, the four
``nhl/nhl_*_parsers.py`` modules and ``_common_espn_parsers.py``. Each module still binds the old
private name, so ``module._empty_frame`` and friends keep working.

``_to_output`` is not here: since the dtype contract each module's copy differs (the id cast and its
warning label, ``text_ids``). ``_flatten_rows`` is not here either: it is AST-identical in the MLB and
NHL api-web modules but late-binds that module's own ``_to_output``.
"""

from __future__ import annotations

from typing import Any, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import _error_envelope_code

__all__: list[str] = []

# What ``pl.from_pandas`` raises on a mixed-type object column: pyarrow's ArrowInvalid (ValueError),
# ArrowTypeError (TypeError), ArrowNotImplementedError (NotImplementedError), or a polars error.
_FROM_PANDAS_ERRORS = (TypeError, ValueError, NotImplementedError, pl.exceptions.PolarsError)


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename ``df``'s columns in place to snake_case, dots to underscores, and return it."""
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    return df


def _empty_frame(return_as_pandas: bool = False) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return an appropriately-typed empty frame."""
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


def _as_dict(payload: Any) -> dict:
    """``payload`` when it is a data object; ``{}`` for a non-dict body or an error envelope."""
    return payload if isinstance(payload, dict) and _error_envelope_code(payload) is None else {}
