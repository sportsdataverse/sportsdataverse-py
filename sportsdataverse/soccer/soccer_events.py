"""Soccer event data through `kloppy <https://kloppy.pysport.org>`_ (the optional ``soccer`` extra).

kloppy reads ~15 event / tracking providers (StatsBomb, Opta, Wyscout, Sportec, SkillCorner,
Second Spectrum, Metrica, ...) into one event model. This module does two small things on top:
load a provider's free open data by match id, and turn any kloppy dataset into the package's
polars / pandas frame convention. kloppy fetches its own files (it does not go through
``dl_utils.download``), so these helpers never rewrite a provider reader.

Install with ``pip install "sportsdataverse[soccer]"``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Union

from sportsdataverse.dl_utils import underscore

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports (PEP 563 defers eval)
    import pandas as pd
    import polars as pl

__all__ = ["soccer_events_to_frame", "soccer_open_dataset", "soccer_open_events"]

_INSTALL_HINT = 'pip install "sportsdataverse[soccer]"'


def _kloppy() -> Any:
    """Import kloppy lazily so the package imports without the ``soccer`` extra."""
    try:
        import kloppy
    except ImportError as exc:
        raise ImportError(f"soccer_open_events() needs kloppy: {_INSTALL_HINT}") from exc
    return kloppy


# provider -> the kloppy module attribute whose ``load_open_data(match_id=...)`` serves it.
# ponytail: statsbomb only; Metrica / SkillCorner open samples follow the same shape when asked for.
_OPEN_DATA_PROVIDERS: Dict[str, str] = {"statsbomb": "statsbomb"}


def soccer_events_to_frame(dataset: Any, *, return_as_pandas: bool = False) -> Union[pl.DataFrame, pd.DataFrame]:
    """Turn a kloppy dataset (any provider, any file) into a tidy frame.

    The one place the package's column convention is applied to kloppy output: one row per
    event, columns snake-cased (kloppy's own names -- ``event_id``, ``event_type``,
    ``period_id``, ``timestamp``, ``team_id``, ``player_id``, ``coordinates_x``,
    ``coordinates_y``, ... -- already are, so this is a no-op guard for any extra column).

    Args:
        dataset: A kloppy ``EventDataset`` (or any dataset with ``to_df``), e.g. from
            ``kloppy.statsbomb.load(event_data=..., lineup_data=...)`` or ``kloppy.opta.load(...)``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars DataFrame (pandas with ``return_as_pandas=True``), one row per event.
        Coordinates are in the dataset's coordinate system -- kloppy's default is a 0-1
        normalized pitch; pass ``coordinates="statsbomb"`` (etc.) to kloppy's loader to keep
        the provider's units.

    Raises:
        AttributeError: ``dataset`` has no ``to_df`` (it is not a kloppy dataset).

    Example:
        Quick start (a local StatsBomb match)::

            from kloppy import statsbomb
            from sportsdataverse.soccer import soccer_events_to_frame
            ds = statsbomb.load(event_data="8658.json", lineup_data="lineups_8658.json")
            df = soccer_events_to_frame(ds)
            print(df.shape)

        Useful parameter combination::

            df_pd = soccer_events_to_frame(ds, return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("event_type") == "SHOT").select("player_id", "coordinates_x", "coordinates_y")

    See Also:
        * `kloppy`_ -- the provider readers and the event model behind this frame
        * `sdvplot pitch_coords`_ -- puts the frame on the 105 x 68 pitch (``provider="statsbomb"``)
        * `sdvplotR sdv_pitch_coords`_ -- the R twin

    .. _kloppy: https://kloppy.pysport.org
    .. _sdvplot pitch_coords: https://github.com/sportsdataverse/sdvplot
    .. _sdvplotR sdv_pitch_coords: https://github.com/sportsdataverse/sdvplotR
    """
    df = dataset.to_df(engine="polars")
    renames = {c: underscore(c) for c in df.columns if underscore(c) != c}
    if renames:
        df = df.rename(renames)
    return df.to_pandas() if return_as_pandas else df


def soccer_open_dataset(provider: str, match_id: Union[int, str], **kwargs: Any) -> Any:
    """Load one match of a provider's free open event data as a kloppy ``EventDataset``.

    ``provider="statsbomb"`` reads StatsBomb open data (https://github.com/statsbomb/open-data)
    through ``kloppy.statsbomb.load_open_data(match_id=...)``. That data is free for research and
    non-commercial use only, under StatsBomb's open-data license -- read it before publishing
    anything built on it. This is the input :func:`soccer_spadl` expects;
    :func:`soccer_open_events` is the same load flattened to a frame.

    Args:
        provider: Open-data provider key; currently ``"statsbomb"``.
        match_id: The provider's match id (StatsBomb: e.g. ``8658`` -- France v Croatia, 2018
            World Cup final).
        **kwargs: Forwarded to the kloppy loader. ``coordinates`` defaults to the provider's own
            units (StatsBomb: 120 x 80 yards); pass ``coordinates="kloppy"`` for kloppy's 0-1
            normalized pitch. Others pass through, e.g. ``event_types=["shot", "pass"]``.

    Returns:
        A kloppy ``EventDataset``.

    Raises:
        ImportError: kloppy is not installed -- ``pip install "sportsdataverse[soccer]"``.
        ValueError: ``provider`` is not a supported open-data key.

    Example:
        Quick start::

            from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl
            dataset = soccer_open_dataset("statsbomb", 8658)
            actions = soccer_spadl(dataset, game_id=8658)

        Pipeline next step (one line)::

            frame = dataset.to_df(engine="polars")

    See Also:
        * `kloppy`_ -- the dataset model and every other provider loader

    .. _kloppy: https://kloppy.pysport.org
    """
    key = provider.lower()
    if key not in _OPEN_DATA_PROVIDERS:
        raise ValueError(f"unknown open-data provider {provider!r}; supported: {sorted(_OPEN_DATA_PROVIDERS)}")
    kloppy = _kloppy()
    kwargs.setdefault("coordinates", key)  # provider units (StatsBomb 120 x 80), what pitch_coords() expects
    return getattr(kloppy, _OPEN_DATA_PROVIDERS[key]).load_open_data(match_id=match_id, **kwargs)


def soccer_open_events(
    provider: str,
    match_id: Union[int, str],
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Load one match of a provider's free open event data as a tidy frame.

    ``provider="statsbomb"`` reads StatsBomb open data (https://github.com/statsbomb/open-data)
    through ``kloppy.statsbomb.load_open_data(match_id=...)``. That data is free for research and
    non-commercial use only, under StatsBomb's open-data license -- read it before publishing
    anything built on it. Other kloppy open samples (Metrica, SkillCorner) follow the same
    shape and are added on request.

    Args:
        provider: Open-data provider key; currently ``"statsbomb"``.
        match_id: The provider's match id (StatsBomb: e.g. ``8658`` -- France v Croatia, 2018
            World Cup final).
        return_as_pandas: Return a pandas DataFrame instead of polars.
        **kwargs: Forwarded to the kloppy loader. ``coordinates`` defaults to the provider's own
            units (StatsBomb: 120 x 80 yards, what sdvplot's ``pitch_coords`` expects); pass
            ``coordinates="kloppy"`` for kloppy's 0-1 normalized pitch. Others pass through, e.g.
            ``event_types=["shot", "pass"]``.

    Returns:
        A polars DataFrame (pandas with ``return_as_pandas=True``), one row per event; see
        :func:`soccer_events_to_frame` for the columns.

    Raises:
        ImportError: kloppy is not installed -- ``pip install "sportsdataverse[soccer]"``.
        ValueError: ``provider`` is not a supported open-data key.

    Example:
        Quick start::

            from sportsdataverse.soccer import soccer_open_events
            df = soccer_open_events("statsbomb", 8658)
            print(df.shape)

        Useful parameter combination::

            df_pd = soccer_open_events("statsbomb", 8658, coordinates="statsbomb", return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("event_type") == "SHOT").group_by("team_id").len()

    See Also:
        * `kloppy`_ -- every other provider / file goes through kloppy then :func:`soccer_events_to_frame`
        * `sdvplot pitch_coords`_ -- ``pitch_coords(df, provider="statsbomb")`` for plotting
        * `sdvplotR sdv_pitch_coords`_ -- the R twin

    .. _kloppy: https://kloppy.pysport.org
    .. _sdvplot pitch_coords: https://github.com/sportsdataverse/sdvplot
    .. _sdvplotR sdv_pitch_coords: https://github.com/sportsdataverse/sdvplotR
    """
    dataset = soccer_open_dataset(provider, match_id, **kwargs)
    return soccer_events_to_frame(dataset, return_as_pandas=return_as_pandas)
