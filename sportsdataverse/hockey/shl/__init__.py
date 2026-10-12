"""SHL (Sweden) wrappers over www.shl.se/api (codegen flat-API family ``shl``).

Unofficial use of a public source: payloads are not redistributed as release assets.
Seasons are END years (2026 = 2025/26); the runtime maps them to the site's ssgt uuids.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES). The family's generated module is
# re-exported here once codegen renders it (Task 2 of the European hockey plan).
from sportsdataverse.hockey.shl import shl_parsers as _parsers
from sportsdataverse.hockey.shl.shl_parsers import *  # noqa: F401,F403
from sportsdataverse.hockey.shl.shl_runtime import shl_player_crosswalk, shl_ssgt_uuid

# Explicit, so the top-level ``from sportsdataverse.hockey.shl import *`` exports only these.
__all__ = [*_parsers.__all__, "shl_player_crosswalk", "shl_ssgt_uuid"]
