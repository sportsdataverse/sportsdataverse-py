<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Conda recipe — `sportsdataverse-py`](#conda-recipe--sportsdataverse-py)
  - [Local build (development)](#local-build-development)
  - [Build from a published PyPI release](#build-from-a-published-pypi-release)
  - [Submitting to conda-forge](#submitting-to-conda-forge)
  - [Why `noarch: python`?](#why-noarch-python)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Conda recipe — `sportsdataverse-py`

This directory contains the [conda-build](https://docs.conda.io/projects/conda-build/)
recipe used to produce the `sportsdataverse` conda package.

The recipe pulls runtime metadata from `pyproject.toml` (PEP 621) and runs the
PEP 517 build path under the hood — there is no `setup.py` involved.

## Local build (development)

Build a `noarch: python` package from the current working tree:

```sh
# One-time tooling install:
conda install -n base conda-build

# From the repo root (NOT inside recipe/):
conda build recipe/

# Install the freshly built package into the current environment:
conda install --use-local sportsdataverse
```

The output `.conda` file lands under
`$(conda info --base)/conda-bld/noarch/sportsdataverse-<version>-py_0.conda`.

To sanity-check before installing:

```sh
conda search --use-local sportsdataverse
```

## Build from a published PyPI release

`recipe/meta.yaml` ships with the local `path: ..` source enabled and the
PyPI-pinned source commented out. Switch the comment markers when building
from a released sdist (this is also the mode conda-forge expects):

```yaml
source:
  # path: ..
  url: https://pypi.io/packages/source/s/sportsdataverse/sportsdataverse-0.0.51.tar.gz
  sha256: <sha256 of the sdist>
```

Compute the `sha256`:

```sh
curl -sL https://pypi.io/packages/source/s/sportsdataverse/sportsdataverse-0.0.51.tar.gz | sha256sum
```

## Submitting to conda-forge

Once a version is published to PyPI, the recipe can be promoted to
[conda-forge](https://conda-forge.org/) so users get
`conda install -c conda-forge sportsdataverse` for free.

1. Fork [`conda-forge/staged-recipes`](https://github.com/conda-forge/staged-recipes).
2. Copy `recipe/meta.yaml` from this repo into
   `recipes/sportsdataverse/meta.yaml` in your fork.
3. Switch the `source:` block from the local `path:` form to the PyPI
   `url:` + `sha256:` form (see above).
4. Open a PR against `conda-forge/staged-recipes`.
5. Once the PR merges, conda-forge auto-creates a feedstock at
   `conda-forge/sportsdataverse-feedstock`. Future version bumps happen
   inside that feedstock (regro-cf-autotick-bot usually opens the bump PR
   automatically when a new PyPI release is detected).

After the feedstock is live, drop the recipe maintenance burden in this
repo — the `recipe/` directory is retained only as a reference / fallback
for local dev builds. Bumps in this directory do NOT propagate to
conda-forge automatically.

## Why `noarch: python`?

`sportsdataverse-py` is pure Python with no C extensions of its own
(xgboost ships its own wheels). `noarch: python` means a single build
serves every platform / Python version conda-forge currently maintains,
which is the usual choice for pure-Python wrappers around already-packaged
scientific stacks.
