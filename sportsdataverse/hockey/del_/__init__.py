"""DEL (Germany) wrappers over eSports hokejovyzapis S3 bucket (codegen flat-API family ``del_esports``).

``del`` is a Python keyword, so this package is ``del_``; the docs prefix stays ``del``.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES). The four eSports hokejovyzapis families
# (del_, icehl, eihl, metal) share one parser module, sportsdataverse.hockey.esports_parsers.
# The generated module's import is added at integration, once codegen has rendered it.
from sportsdataverse.hockey import esports_parsers as _parsers
from sportsdataverse.hockey.esports_parsers import *  # noqa: F401,F403

__all__ = [*_parsers.__all__]
