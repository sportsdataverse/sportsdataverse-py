"""Adapt published NCAA ``possessions`` + ``team_rosters`` into RAPM input.

The RAPM engine (:mod:`sportsdataverse.mbb.mbb_rapm`, the hoop-explorer
``RapmUtils`` port) is complete and league-agnostic, but nothing has ever fed
it a season: it consumes typed ``PlayerOnOffStats`` / ``LineupStatSet`` records
while the published datasets are polars frames keyed by player NAME-CODE. This
module is that missing adapter's first half -- player identity.

**Why identity is the hard part.** ``ncaa_{mbb,wbb}_possessions`` stores each
of the ten on-floor slots as a name-code (``ANTONIA.BATES.``), not an id.
``team_rosters`` carries ``player_id`` (no nulls) beside ``player`` in the same
format, so ``(team, player) -> player_id`` bridges them. Measured on WBB 2024:
**97.4% of D-I (team, player) pairs resolve** (4,530/4,653).

The naive whole-corpus rate is only 58.4%, and that gap is NOT a normalizer
bug -- 619 teams appear on the floor while just 358 have rosters. The surplus
are non-Division-I exhibition opponents (Academy of Art, Adelphi, Agnes Scott).
90.9% of possessions have both teams D-I.

**Two entities look like absence and must be modelled as presence.** Both would
corrupt RAPM silently rather than loudly:

* ``TEAM`` occupies a player slot but denotes team rebounds/turnovers. Resolved
  like a person it becomes a phantom player with enormous minutes on every
  roster.
* Non-D-I opponents have no roster. Under ``non_di="pool"`` they map to ONE
  explicitly named pseudo-team (:data:`NON_DI_TEAM`) -- never to null, because
  a null opponent quietly takes whatever branch a join gives missing keys.
"""

from __future__ import annotations

import hashlib

from typing import Literal

import polars as pl

__all__ = [
    "NON_DI_PLAYER",
    "NON_DI_TEAM",
    "TEAM_PSEUDO_PLAYER",
    "build_person_keys",
    "build_player_xwalk",
    "expand_xwalk_aliases",
    "normalize_player_key",
    "observed_pairs",
    "resolve_possessions",
]

#: Slot value denoting team rebounds/turnovers rather than a person.
TEAM_PSEUDO_PLAYER = "TEAM"

#: The single pooled non-Division-I opponent under ``non_di="pool"``. Explicit
#: and non-null on purpose -- see the module docstring.
NON_DI_TEAM = "__NON_DI__"

#: The single replacement-level player every non-D-I opponent's slots collapse
#: to under ``non_di="pool"``.
#:
#: Pooling the TEAM without pooling its PLAYERS is a half-measure that makes
#: things worse, not better: the five opposing slots stay unresolvable, so the
#: possession can never reach ten ids and is dropped by the design-matrix build
#: anyway. Measured on WBB 2024, team-only pooling scored 79.3% fully-resolved
#: possessions against 87.3% for plain ``"drop"`` -- i.e. it retained 9% more
#: possessions and lost more of them. Pooling the players too is what makes the
#: mode mean "D-I players keep their minutes in these games".
NON_DI_PLAYER = "__NON_DI_PLAYER__"

_SLOTS = tuple(f"{side}_{i}" for side in ("home", "away") for i in range(1, 6))


def normalize_player_key(expr: pl.Expr) -> pl.Expr:
    """Normalize a player name-code so possessions and rosters agree.

    Three normalizations, each fixing an observed divergence between the two
    sources:

    * **Trailing dots** -- ``possessions`` writes ``ANTONIA.BATES.`` where
      ``team_rosters`` writes ``ANTONIA.BATES``. Interior dots are the
      ``FIRST.LAST`` separator and are load-bearing, so only trailing ones go.
    * **Diacritics** -- ``ANAELLE.DUTAT`` on the floor vs ``ANAËLLE.DUTAT`` on
      the roster. NFKD-decompose and drop combining marks.
    * **Case** -- belt and braces; both sources are upper today.

    Args:
        expr: A ``Utf8`` expression holding the raw name-code.

    Returns:
        The normalized key expression.
    """
    return expr.str.normalize("NFKD").str.replace_all(r"\p{M}", "").str.to_uppercase().str.strip_chars_end(".")


def build_player_xwalk(team_rosters: pl.DataFrame) -> pl.DataFrame:
    """Build the ``(team, player_key) -> player_id`` bridge from ``team_rosters``.

    Args:
        team_rosters: Published ``ncaa_{lg}_team_rosters`` frame; needs
            ``team``, ``player`` and ``player_id``.

    Returns:
        Frame of ``team``, ``player_key``, ``player_id`` (all ``Utf8``), unique
        on ``(team, player_key)``.

    The key is ``(team, player)``, never ``player`` alone: the same name-code
    can belong to different people on different teams, and collapsing them
    would merge two players' minutes into one RAPM coefficient.

    Example:
        Bridge possessions to ids::

            from sportsdataverse.mbb.mbb_ncaa_rapm_input import build_player_xwalk

            xwalk = build_player_xwalk(team_rosters)
    """
    base = (
        team_rosters.select(
            pl.col("team").cast(pl.Utf8),
            normalize_player_key(pl.col("player").cast(pl.Utf8)).alias("player_key"),
            pl.col("player_id").cast(pl.Utf8),
        )
        .filter(pl.col("player_key") != TEAM_PSEUDO_PLAYER)
        .unique()
    )
    # A normalized key that resolves to MORE THAN ONE player_id is two people
    # sharing a rendering. OMIT the key rather than keeping an arbitrary row:
    # an unresolved possession is recoverable, a silent wrong attribution is
    # not. Same gate as the sibling-code and same-full-name rejections.
    unambiguous = (
        base.group_by(["team", "player_key"])
        .agg(pl.col("player_id").n_unique().alias("_n_ids"))
        .filter(pl.col("_n_ids") == 1)
        .select(["team", "player_key"])
    )
    return base.join(unambiguous, on=["team", "player_key"], how="inner").unique(
        subset=["team", "player_key"], keep="first"
    )


def resolve_possessions(
    possessions: pl.DataFrame,
    xwalk: pl.DataFrame,
    *,
    non_di: Literal["drop", "pool"] = "drop",
) -> pl.DataFrame:
    """Attach a ``player_id`` to each of the ten on-floor slots.

    Args:
        possessions: Published ``ncaa_{lg}_possessions`` frame, carrying
            ``home``/``away`` and ``home_1..5``/``away_1..5``.
        xwalk: Output of :func:`build_player_xwalk`.
        non_di: How to treat possessions involving a team with no roster.
            ``"drop"`` (default, conservative) discards them; ``"pool"``
            rewrites the team to :data:`NON_DI_TEAM` so the D-I side keeps its
            minutes.

    Returns:
        ``possessions`` plus ten ``{slot}_id`` columns. ``TEAM`` slots and
        unresolved players carry a null id -- an explicit "not a rated player"
        that the design-matrix build must skip, never impute.

    Raises:
        ValueError: ``non_di`` is not ``"drop"`` or ``"pool"``.
    """
    if non_di not in ("drop", "pool"):
        raise ValueError(f"non_di must be 'drop' or 'pool', got {non_di!r}")

    di_teams = xwalk.select("team").unique()
    di = set(di_teams["team"].to_list())

    out = possessions
    if non_di == "drop":
        out = out.filter(pl.col("home").is_in(di) & pl.col("away").is_in(di))
    else:
        # Pool to an EXPLICIT entity. A null here would silently become
        # "missing key" in the slot joins below rather than a real opponent.
        out = out.with_columns(
            [
                pl.when(pl.col(side).is_in(di)).then(pl.col(side)).otherwise(pl.lit(NON_DI_TEAM)).alias(side)
                for side in ("home", "away")
            ]
        )

    lookup = xwalk.select("team", "player_key", "player_id")
    assert lookup.schema["team"] == out.schema["home"], (
        f"join-key dtype mismatch: xwalk.team={lookup.schema['team']} vs possessions.home={out.schema['home']}"
    )

    for slot in _SLOTS:
        side = "home" if slot.startswith("home") else "away"
        out = (
            out.with_columns(normalize_player_key(pl.col(slot).cast(pl.Utf8)).alias("_k"))
            .with_columns(
                # TEAM is not a person: never let it resolve to an id.
                pl.when(pl.col("_k") == TEAM_PSEUDO_PLAYER).then(None).otherwise(pl.col("_k")).alias("_k")
            )
            .join(
                lookup.rename({"team": "_t", "player_key": "_k", "player_id": slot + "_id"}),
                left_on=[side, "_k"],
                right_on=["_t", "_k"],
                how="left",
            )
            .drop("_k")
        )

    if non_di == "pool":
        # Collapse the pooled opponent's slots to ONE replacement-level player
        # so its possessions can actually reach ten ids. Without this the mode
        # keeps MORE possessions and yields FEWER usable ones (see
        # NON_DI_PLAYER). TEAM slots stay null even here -- still not a person.
        out = out.with_columns(
            [
                pl.when(
                    (pl.col("home" if s_.startswith("home") else "away") == NON_DI_TEAM)
                    & (normalize_player_key(pl.col(s_).cast(pl.Utf8)) != TEAM_PSEUDO_PLAYER)
                )
                .then(pl.lit(NON_DI_PLAYER))
                .otherwise(pl.col(s_ + "_id"))
                .alias(s_ + "_id")
                for s_ in _SLOTS
            ]
        )
    return out


def _split(key: str) -> "tuple[str, str]":
    """``FIRST.LAST.SUFFIX`` -> ``("FIRST", "LAST.SUFFIX")``."""
    first, _, rest = key.partition(".")
    return first, rest


def _prefix_either(a: str, b: str) -> bool:
    return bool(a) and bool(b) and (a.startswith(b) or b.startswith(a))


def expand_xwalk_aliases(
    xwalk: pl.DataFrame,
    observed: pl.DataFrame,
    *,
    name_changes: "pl.DataFrame | None" = None,
) -> pl.DataFrame:
    """Add alias rows for on-floor names the exact key misses.

    Args:
        xwalk: Output of :func:`build_player_xwalk`.
        observed: Frame with ``team`` and ``player`` -- the distinct
            (team, player) pairs seen in the possessions slots.
        name_changes: Optional frame with ``team``, ``name_game_time`` and
            ``name_current`` -- the id-bound crosswalk built from the
            ``box_score`` page (``dev/ncaa_rapm/build_name_changes.py``).
            Injected rather than loaded so this stays pure and testable.

    Returns:
        ``xwalk`` plus one row per confidently-resolved alias. Never fewer rows
        than the input, and idempotent.

    Possessions and rosters render the same person differently. Measured on
    real WBB data, three patterns cover nearly all of it -- diacritics
    (``ANAELLE.DUTAT`` / ``ANAËLLE.DUTAT``, already handled by
    :func:`normalize_player_key`), a truncated compound surname
    (``PAULA.REUS`` / ``PAULA.REUS.PIZA``), and a shortened first name
    (``MELANNIE.DALEY`` / ``MEL.DALEY``).

    **An alias is emitted only when exactly one roster entry matches.** Purdue
    2024 carries both ``MADISON.LAYDENZAY`` and ``MCKENNA.LAYDEN``; a loose
    surname match would bind possessions to the wrong sibling. A silent wrong
    match is worse than a drop -- it corrupts two players' coefficients at once,
    where a drop only forfeits one player's minutes. Ambiguity therefore
    resolves to nothing, deliberately.

    Measured effect on the D-I appearance-weighted resolve rate: 98.57% ->
    99.04% (2024), 96.44% -> 98.21% (2016), 97.21% -> 98.49% (2011). Older
    seasons gain most; ambiguous candidates were never guessed at (0 in every
    season measured).

    Example:
        Expand before resolving::

            from sportsdataverse.mbb.mbb_ncaa_rapm_input import (
                build_player_xwalk, expand_xwalk_aliases, observed_pairs,
            )

            xwalk = expand_xwalk_aliases(
                build_player_xwalk(team_rosters), observed_pairs(possessions)
            )
    """
    by_team: "dict[str, list[tuple[str, str]]]" = {}
    for team, key, pid in xwalk.select("team", "player_key", "player_id").rows():
        by_team.setdefault(team, []).append((key, pid))
    known = {(t, k) for t, k, _ in xwalk.select("team", "player_key", "player_id").rows()}

    obs = observed.select(
        pl.col("team").cast(pl.Utf8),
        normalize_player_key(pl.col("player").cast(pl.Utf8)).alias("player_key"),
    ).unique()

    # (team, game-time name) -> current name, from the id-bound crosswalk.
    # A game-time name mapping to MORE THAN ONE current name is two different
    # people sharing a rendering; drop it rather than pick one.
    renames: "dict[tuple[str, str], str]" = {}
    if name_changes is not None and name_changes.height:
        nc = name_changes.select(
            pl.col("team").cast(pl.Utf8),
            normalize_player_key(pl.col("name_game_time").cast(pl.Utf8)).alias("old"),
            normalize_player_key(pl.col("name_current").cast(pl.Utf8)).alias("new"),
        ).unique()
        seen: "dict[tuple[str, str], set[str]]" = {}
        for team, old, new in nc.rows():
            seen.setdefault((team, old), set()).add(new)
        renames = {k: next(iter(v)) for k, v in seen.items() if len(v) == 1}

    by_key = {(t, k): pid for t, k, pid in xwalk.select("team", "player_key", "player_id").rows()}

    rows: "list[dict[str, str]]" = []
    for team, key in obs.rows():
        if not key or key == TEAM_PSEUDO_PLAYER or (team, key) in known:
            continue

        # Tier 0: the id-bound name change. Authoritative -- it comes from the
        # provider binding both renderings to one player id -- so it outranks
        # the string-prefix heuristics below.
        cur = renames.get((team, key))
        if cur is not None:
            pid = by_key.get((team, cur))
            if pid is not None:
                rows.append({"team": team, "player_key": key, "player_id": pid})
            continue

        cands = by_team.get(team, [])
        first, sur = _split(key)

        # Tier 1: first name agrees exactly, surname is a prefix either way.
        hit = {pid for rk, pid in cands if _split(rk)[0] == first and _prefix_either(_split(rk)[1], sur)}
        # Tier 2: surname agrees exactly, first name is a prefix either way.
        if len(hit) != 1:
            hit = {pid for rk, pid in cands if sur and _split(rk)[1] == sur and _prefix_either(_split(rk)[0], first)}
        if len(hit) == 1:  # unique -> safe; 0 or >1 -> leave unresolved
            rows.append({"team": team, "player_key": key, "player_id": hit.pop()})

    if not rows:
        return xwalk
    return pl.concat([xwalk, pl.DataFrame(rows, schema=xwalk.schema)], how="vertical")


def observed_pairs(possessions: pl.DataFrame) -> pl.DataFrame:
    """Distinct ``(team, player)`` pairs across the ten on-floor slots.

    Args:
        possessions: Published ``ncaa_{lg}_possessions`` frame.

    Returns:
        Unique ``team``/``player`` frame, the input :func:`expand_xwalk_aliases`
        expects.

    Example:
        ::

            from sportsdataverse.mbb.mbb_ncaa_rapm_input import observed_pairs

            pairs = observed_pairs(possessions)
    """
    return pl.concat(
        [
            possessions.select(
                pl.col("home" if s.startswith("home") else "away").alias("team"),
                pl.col(s).alias("player"),
            )
            for s in _SLOTS
        ]
    ).unique()


_CLASS_ORDER = {"FR.": 1, "SO.": 2, "JR.": 3, "SR.": 4, "GR.": 5}
_HT_TOLERANCE_IN = 1


class _Union:
    """Minimal union-find over roster-row indices."""

    def __init__(self, n: int) -> None:
        self._p = list(range(n))

    def find(self, a: int) -> int:
        while self._p[a] != a:
            self._p[a] = self._p[self._p[a]]
            a = self._p[a]
        return a

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._p[max(ra, rb)] = min(ra, rb)


def build_person_keys(
    team_rosters: pl.DataFrame,
    *,
    name_changes: "pl.DataFrame | None" = None,
) -> pl.DataFrame:
    """Synthesize a cross-season ``person_id`` for roster rows.

    Args:
        team_rosters: Concatenated ``ncaa_{lg}_team_rosters`` across seasons;
            needs ``season``, ``team``, ``player``, ``player_id``,
            ``ht_inches`` and ``class``.
        name_changes: Optional crosswalk (see :func:`expand_xwalk_aliases`) so a
            player who changed their name is one person, not two.

    Returns:
        The input columns plus ``player_key`` and a ``Utf8`` ``person_id`` that
        is stable across seasons and teams. ``player_id`` is preserved -- the
        synthetic key ADDS to the provider's id, never replaces it.

    **Why this has to exist.** ``player_id`` is a per-season roster-ENTRY id.
    Across 17 seasons, 83,518 roster rows carry 83,518 distinct ids, none
    appearing in more than one season; of 23,302 ``(team, player)`` pairs
    spanning seasons, 23,286 receive a new id every season and 0 are stable.
    RAPM conventionally pools 2-3 seasons, so pooling on ``player_id`` would
    treat every player as a new person each year.

    **The rule, and the measurements behind it.** Rows are linked across
    ADJACENT seasons on the canonical name (after ``name_changes``), validated
    by height and gated on uniqueness:

    * ``name`` is 99.4% nationally unique within a season (0.59% collide), so
      it carries the link.
    * ``ht_inches`` is identical across 99.4% of consecutive-season links and
      within one inch across 99.5%, so it is the validator
      (:data:`_HT_TOLERANCE_IN`).
    * ``class`` is NOT required to advance. Only 88.8% of real links advance by
      +1; 10.5% stay flat (redshirt and medical years), so demanding +1 would
      break one link in ten. Only BACKWARDS movement (0.1%) disqualifies.
    * Two candidates in the following season -> no link. Same-season namesakes
      never merge, since linking is strictly across adjacent seasons.

    Example:
        ::

            from sportsdataverse.mbb.mbb_ncaa_rapm_input import build_person_keys

            keys = build_person_keys(all_season_rosters, name_changes=changes)
    """
    df = team_rosters.with_columns(
        pl.col("season").cast(pl.Utf8),
        pl.col("team").cast(pl.Utf8),
        normalize_player_key(pl.col("player").cast(pl.Utf8)).alias("player_key"),
    )

    # Fold renamed players onto their CURRENT name so both spellings land in
    # the same link group.
    if name_changes is not None and name_changes.height:
        nc = name_changes.select(
            pl.col("season").cast(pl.Utf8),
            pl.col("team").cast(pl.Utf8),
            normalize_player_key(pl.col("name_game_time").cast(pl.Utf8)).alias("old"),
            normalize_player_key(pl.col("name_current").cast(pl.Utf8)).alias("new"),
        ).unique()
        # Scope the fold by SEASON. A (team, game-time name) -> current name
        # mapping is evidence about ONE season's roster; applied to every
        # season it would rewrite an unrelated player who happens to share the
        # team and old rendering, merging two identities.
        seen: "dict[tuple[str, str, str], set[str]]" = {}
        for season_, team_, old_, new_ in nc.rows():
            seen.setdefault((season_, team_, old_), set()).add(new_)
        ren = {k: next(iter(v)) for k, v in seen.items() if len(v) == 1}
        if ren:
            df = df.with_columns(
                pl.struct(["season", "team", "player_key"])
                .map_elements(
                    lambda s: ren.get((s["season"], s["team"], s["player_key"]), s["player_key"]),
                    return_dtype=pl.Utf8,
                )
                .alias("canon_key")
            )
        else:
            df = df.with_columns(pl.col("player_key").alias("canon_key"))
    else:
        df = df.with_columns(pl.col("player_key").alias("canon_key"))

    rows = df.select("season", "canon_key", "ht_inches", "class", "team").rows()
    by_name: "dict[str, list[int]]" = {}
    for i, (_s, key, _h, _c, _t) in enumerate(rows):
        by_name.setdefault(key, []).append(i)

    uf = _Union(len(rows))
    for idxs in by_name.values():
        if len(idxs) < 2:
            continue
        by_season: "dict[int, list[int]]" = {}
        for i in idxs:
            try:
                by_season.setdefault(int(rows[i][0]), []).append(i)
            except (TypeError, ValueError):
                continue
        for season in sorted(by_season):
            nxt = by_season.get(season + 1)
            cur = by_season[season]
            if not nxt or len(cur) != 1 or len(nxt) != 1:
                continue  # ambiguous on either side -> do not link
            a, b = cur[0], nxt[0]
            ha, hb = rows[a][2], rows[b][2]
            if ha is not None and hb is not None and abs(ha - hb) > _HT_TOLERANCE_IN:
                continue
            ca = _CLASS_ORDER.get(str(rows[a][3]).upper())
            cb = _CLASS_ORDER.get(str(rows[b][3]).upper())
            if ca is not None and cb is not None and cb < ca:
                continue  # class cannot go backwards
            uf.union(a, b)

    # The person key must NOT depend on input row order -- a published
    # person_id has to survive a regenerated or differently-ordered roster
    # extract, or nothing can join to it later. Derive it from a canonical
    # property of the component instead of the union-find row index: every
    # member shares `canon_key`, so anchor on that plus the lexicographically
    # smallest "season:team" the component contains.
    components: "dict[int, list[int]]" = {}
    for i in range(len(rows)):
        components.setdefault(uf.find(i), []).append(i)
    person_of_root: "dict[int, str]" = {}
    for root, members in components.items():
        anchor = min(f"{rows[m][0]}:{rows[m][4]}" for m in members)
        ident = f"{rows[members[0]][1]}|{anchor}"
        person_of_root[root] = "p" + hashlib.sha1(ident.encode("utf-8")).hexdigest()[:12]
    return df.drop("canon_key").with_columns(
        pl.Series("person_id", [person_of_root[uf.find(i)] for i in range(len(rows))], dtype=pl.Utf8)
    )
