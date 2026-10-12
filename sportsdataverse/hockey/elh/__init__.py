"""Czech Extraliga wrappers over hokej.cz S3 bucket + eSports Media scoreboard (codegen flat-API family ``elh``).

Unofficial use of a public source: payloads are not redistributed as release assets.

Two id spaces: the play-by-play carries onlajny ids, the shots and the scoreboard hokej.cz
ids (see :mod:`sportsdataverse.hockey.elh.elh_parsers`). Seasons are END years (``2027`` =
2026-27). :func:`elh_game_ids` lists a season's game ids from the bucket, unfiltered: the
bucket holds every competition hokej.cz shows, not only the Extraliga.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.hockey.elh import elh as _api
from sportsdataverse.hockey.elh.elh import *  # noqa: F401,F403
from sportsdataverse.hockey.elh import elh_parsers as _parsers
from sportsdataverse.hockey.elh.elh_parsers import *  # noqa: F401,F403
from sportsdataverse.hockey.elh.elh_runtime import elh_game_ids

# Explicit, so ``from sportsdataverse.hockey.elh import *`` (the top-level package does it)
# exports the parsers and never the submodule objects.
__all__ = [*_api.__all__, *_parsers.__all__, "elh_game_ids"]
