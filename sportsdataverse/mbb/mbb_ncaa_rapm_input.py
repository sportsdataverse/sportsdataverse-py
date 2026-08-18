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

from typing import Literal

import polars as pl

__all__ = [
    "NON_DI_PLAYER",
    "NON_DI_TEAM",
    "TEAM_PSEUDO_PLAYER",
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
    return (
        team_rosters.select(
            pl.col("team").cast(pl.Utf8),
            normalize_player_key(pl.col("player").cast(pl.Utf8)).alias("player_key"),
            pl.col("player_id").cast(pl.Utf8),
        )
        .filter(pl.col("player_key") != TEAM_PSEUDO_PLAYER)
        .unique(subset=["team", "player_key"], keep="first")
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


def expand_xwalk_aliases(xwalk: pl.DataFrame, observed: pl.DataFrame) -> pl.DataFrame:
    """Add alias rows for on-floor names the exact key misses.

    Args:
        xwalk: Output of :func:`build_player_xwalk`.
        observed: Frame with ``team`` and ``player`` -- the distinct
            (team, player) pairs seen in the possessions slots.

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

    rows: "list[dict[str, str]]" = []
    for team, key in obs.rows():
        if not key or key == TEAM_PSEUDO_PLAYER or (team, key) in known:
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
