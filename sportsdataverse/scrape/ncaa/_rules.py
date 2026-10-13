"""NCAA basketball era facts: the one place the WBB period-model tuples are written down.

Read them through :func:`sportsdataverse.scrape.ncaa.parse.wbb_period_model`, which picks
the model for a season; nothing else should spell the tuples out.

NCAA women's basketball moved from two 20-minute halves to four 10-minute quarters for the
**2015-16** season (``rules/wbb.yaml#wbb-2016-four-quarters`` in sdv-internal-refs). Seasons
are ending-year, so 2016 is the first quarters season and 2015 the last halves season. Men's
basketball never switched, which is why only the women's side needs an era split.

Applying the wrong model does NOT fail loudly -- it silently yields a frame with no rows. The
quarters model was applied to every WBB season until 2026-08-18, so every pre-2016 game parsed
to an EMPTY record while the parse stage reported success (it counted files written, not rows
extracted), and seasons 2010-2015 were published as ~0.6%-populated datasets. Measured on real
bundles: 2015 quarters=0 rows / halves=520; 2012 quarters=0 / halves=566; 2010 quarters=0 /
halves=527. The reverse also degrades -- 2016 quarters=550 / halves=262 -- so this is a real
discriminator, not a fallback.
"""

#: ``(regulation periods, regulation period seconds, overtime seconds)``, 2015-16 onward.
WBB_QUARTERS_MODEL: "tuple[int, int, int]" = (4, 600, 300)
#: The same triple through 2014-15 (two 20-minute halves).
WBB_HALVES_MODEL: "tuple[int, int, int]" = (2, 1200, 300)
#: First ending-year season played in quarters.
WBB_FIRST_QUARTERS_SEASON = 2016
