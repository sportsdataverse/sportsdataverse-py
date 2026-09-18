"""CBS Sports (NAPI) NFL play-by-play -> ESPN-summary adapter (private, experimental).

CBS's NFL feed is the NFL's own GSIS feed via Genius Sports
(``source: "genius.feed.football.nfl"``), so a CBS play ``id`` **is** the GSIS ``playId``
and the ESPN play id is ``{espn_event_id}{cbs play id}`` exactly -- the same identity the
Shield adapter relies on. This package owns the re-skin (ESPN's play-type vocabulary, the
absolute yard line, the PAT folded into its touchdown, synthesized admin rows, a renderable
header) and the one thing Shield never needs: **resolving CBS's own game id**, which no
offline source carries (:mod:`sportsdataverse.nfl.cbs_pbp.game_id`).

Nothing here is public API: the dispatcher reaches the adapter through
:mod:`sportsdataverse.football.sources.dispatch` (``source="cbs"``), so no symbol is
exported from :mod:`sportsdataverse.nfl` and no codegen output depends on this package.
"""
