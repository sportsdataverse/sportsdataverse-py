"""Read the published contract for a CFB model.

Each model in the ``cfb_model_artifacts`` bundle ships a ``<model>.card.json``
carrying the ordered ``features`` array it was trained with and, where the model
consumes one, an ``era_contract``. Reading that contract is what lets a caller's
frame be validated against the artifact itself instead of a list restated in
this package -- the duplication that produced cfbfastR-cfb-data#70, where both
consumers kept a private copy of the era cuts, both drifted to a 2017 boundary
the trainer never used, and 2018-2020 scored an era off the models trained with
them.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

#: Cards ship beside the boosters in the packaged model directory.
_MODEL_DIR = Path(__file__).resolve().parent / "models"


def _read_card_json(model: str) -> dict[str, Any]:
    """Read ``<model>.card.json`` from the packaged model directory."""
    path = _MODEL_DIR / f"{model}.card.json"
    if not path.exists():
        raise FileNotFoundError(f"no published card for {model!r} at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=None)
def load_model_card(model: str) -> dict[str, Any]:
    """Load and cache one model's published card.

    Args:
        model: Bundle stem, e.g. ``"ep_model"`` or ``"wp_spread"``.

    Returns:
        The parsed card.

    Raises:
        FileNotFoundError: When no card ships for that model.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_cards import load_model_card
            load_model_card("ep_model")["features"]
    """
    return _read_card_json(model)


def card_features(model: str) -> list[str]:
    """The model's feature names, in the order it was trained with.

    Order is load-bearing: XGBoost aligns a DMatrix by position, so a sorted or
    re-derived list scores against the wrong columns without ever erroring.

    Args:
        model: Bundle stem, e.g. ``"ep_model"``.

    Returns:
        Ordered feature names.

    Raises:
        ValueError: When the card declares no features -- such a card validates
            nothing and must not be treated as a contract.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_cards import card_features
            card_features("ep_model")
    """
    feats = load_model_card(model).get("features")
    if not feats:
        raise ValueError(f"card for {model!r} declares no features")
    return list(feats)


def card_era_contract(model: str) -> Optional[dict[str, Any]]:
    """The model's rule-era encoding, or ``None`` when it has no era feature.

    ``None`` is correct for ``ep_model``, ``wp_naive``, ``wp_spread`` and
    ``cfb_cp_model``; inventing a contract for them would tell a caller they
    take an era feature they have never had.

    Args:
        model: Bundle stem, e.g. ``"fg_model"``.

    Returns:
        The contract dict with ``encoding``, ``columns`` and ``cuts``, or
        ``None`` when the model consumes no era feature.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_cards import card_era_contract
            card_era_contract("fg_model")
    """
    return load_model_card(model).get("era_contract")
