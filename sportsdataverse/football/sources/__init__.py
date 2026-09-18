"""Source-switch contract for the football play processors (EXPERIMENTAL, private).

Stage 2 item 1 of the football sources program: the adapter contract, the dispatch
entry Game on Paper calls, the pre-kickoff id map and the ESPN-vs-alternate parity
harness. Spec: ``docs/superpowers/specs/2026-09-17-football-source-switch.md``.

Every name in this package is underscore-prefixed on purpose. Nothing here is a
public sdv-py surface yet (no codegen, no reference docs); the per-source adapters
land later and the surface is promoted once one of them ships.
"""

from __future__ import annotations
