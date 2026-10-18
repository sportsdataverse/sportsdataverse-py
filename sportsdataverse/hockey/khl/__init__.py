"""KHL wrappers over www.khl.ru/rest + khl.api.webcaster.pro (codegen flat-API family ``khl_rest`` + ``khl_webcaster``).

Unofficial use of a public source: payloads are not redistributed as release assets. Owner flag
(``sdv-internal-refs/khl/README.md``): the KHL and its webcaster platform are run by Russian operators,
some clubs by sanctioned state companies; any published data release, hosted mirror or redistribution
needs owner (sanctions / legal) sign-off first.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.hockey.khl import khl_rest as _khl_rest
from sportsdataverse.hockey.khl import khl_webcaster as _khl_webcaster
from sportsdataverse.hockey.khl import khl_rest_parsers as _khl_rest_parsers
from sportsdataverse.hockey.khl import khl_webcaster_parsers as _khl_webcaster_parsers
from sportsdataverse.hockey.khl.khl_crosswalk_runtime import khl_club_crosswalk, khl_club_crosswalk_from_frames
from sportsdataverse.hockey.khl.khl_rest import *  # noqa: F401,F403
from sportsdataverse.hockey.khl.khl_webcaster import *  # noqa: F401,F403
from sportsdataverse.hockey.khl.khl_rest_parsers import *  # noqa: F401,F403
from sportsdataverse.hockey.khl.khl_webcaster_parsers import *  # noqa: F401,F403

__all__ = [
    *_khl_rest.__all__,
    *_khl_webcaster.__all__,
    *_khl_rest_parsers.__all__,
    *_khl_webcaster_parsers.__all__,
    "khl_club_crosswalk",
    "khl_club_crosswalk_from_frames",
]
