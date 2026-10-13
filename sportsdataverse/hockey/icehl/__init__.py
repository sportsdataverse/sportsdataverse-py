"""ICE Hockey League (Austria) wrappers over eSports hokejovyzapis S3 bucket (codegen flat-API family ``icehl_esports``).

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
# The four eSports hokejovyzapis families (del_, icehl, eihl, metal) share one parser module,
# sportsdataverse.hockey.esports_parsers.
from sportsdataverse.hockey.icehl import icehl_esports as _api
from sportsdataverse.hockey.icehl.icehl_esports import *  # noqa: F401,F403
from sportsdataverse.hockey import esports_parsers as _parsers
from sportsdataverse.hockey.esports_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
