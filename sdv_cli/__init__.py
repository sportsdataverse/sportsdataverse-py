"""The ``sdv`` command line.

A top-level package, beside ``sportsdataverse``, so that starting the CLI does not import the
library: ``import sportsdataverse`` loads every league (several seconds), and Python would run it
before ``main`` if the console script lived inside that package. Commands import the library
lazily, only when they need it. Never import ``sportsdataverse`` at module level here.
"""
