# sportsdataverse-py <a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/master/sdv-py-logo.png' align="right"  width="20%" min-width="100px" /></a>
<!-- badges: start -->

![Lifecycle:experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg?style=for-the-badge&logo=github)
[![PyPI](https://img.shields.io/pypi/v/sportsdataverse?label=sportsdataverse&logo=python&style=for-the-badge)](https://pypi.org/project/sportsdataverse/)
![Contributors](https://img.shields.io/github/contributors/sportsdataverse/sportsdataverse-py?style=for-the-badge)
[![Twitter
Follow](https://img.shields.io/twitter/follow/sportsdataverse?color=blue&label=%40sportsdataverse&logo=twitter&style=for-the-badge)](https://twitter.com/sportsdataverse)

<!-- badges: end -->


See [CHANGELOG.md](https://py.sportsdataverse.org/CHANGELOG) for details.

The goal of [sportsdataverse-py](https://py.sportsdataverse.org) is to provide the community with a python package for working with sports data as a companion to the [cfbfastR](https://cfbfastR.sportsdataverse.org/), [hoopR](https://hoopR.sportsdataverse.org/), and [wehoop](https://wehoop.sportsdataverse.org/) R packages. Beyond data aggregation and tidying ease, one of the multitude of services that [sportsdataverse-py](https://py.sportsdataverse.org) provides is for benchmarking open-source expected points and win probability metrics for American Football.

## Installation

sportsdataverse-py can be installed via pip:

```bash
pip install sportsdataverse

# with full dependencies
pip install sportsdataverse[all]
```

or from the repo (which may at times be more up to date):

```bash
git clone https://github.com/sportsdataverse/sportsdataverse-py
cd sportsdataverse-py
pip install -e .[all]
```

## Development

This project uses [uv](https://docs.astral.sh/uv/) for fast, reliable Python package management. The minimum supported Python version is 3.9.

### Prerequisites

Install uv if you haven't already:

```bash
pip install uv
```

Or follow the [official installation guide](https://docs.astral.sh/uv/getting-started/installation/).

### Setting Up the Development Environment

Clone the repository and install all dependencies including development tools:

```bash
git clone https://github.com/sportsdataverse/sportsdataverse-py
cd sportsdataverse-py

# Install all dependencies (including test and doc dependencies)
uv sync --locked --all-extras --dev

# Install the package in editable mode
pip install -e .[all]
```

### Running Tests

```bash
# Run all tests
uv run pytest ./tests

# Run tests for a specific sport module
uv run pytest ./tests/cfb
uv run pytest ./tests/mbb
uv run pytest ./tests/nfl

# Run tests with coverage
uv run pytest --cov=sportsdataverse

# Run tests in parallel (much faster)
uv run pytest -n auto
```

### Code Quality

The project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Format all Python code
uv run ruff format .

# Check for linting issues
uv run ruff check .

# Type checking (requires mypy)
uv run mypy sportsdataverse
```

### Building the Package

```bash
# Build distribution packages (wheel and source)
uv build

# Run smoke tests on built distributions
uv run --isolated --no-project --with dist/*.whl tests/smoke_test.py
uv run --isolated --no-project --with dist/*.tar.gz tests/smoke_test.py
```

### Project Structure

The package is organized by sport, with each module providing data loading and processing functions:

- `sportsdataverse/cfb/` - College Football
- `sportsdataverse/nfl/` - NFL
- `sportsdataverse/mbb/` - Men's College Basketball
- `sportsdataverse/nba/` - NBA
- `sportsdataverse/wbb/` - Women's College Basketball
- `sportsdataverse/wnba/` - WNBA
- `sportsdataverse/nhl/` - NHL
- `sportsdataverse/mlb/` - MLB

Football modules include pre-trained XGBoost models for expected points (EP) and win probability (WP) calculations.

# **Our Authors**

-   [Saiem Gilani](https://twitter.com/saiemgilani)
<a href="https://twitter.com/saiemgilani" target="blank"><img src="https://img.shields.io/twitter/follow/saiemgilani?color=blue&label=%40saiemgilani&logo=twitter&style=for-the-badge" alt="@saiemgilani" /></a>
<a href="https://github.com/saiemgilani" target="blank"><img src="https://img.shields.io/github/followers/saiemgilani?color=eee&logo=Github&style=for-the-badge" alt="@saiemgilani" /></a>


## **Citations**

To cite the [**`sportsdataverse-py`**](https://py.sportsdataverse.org) Python package in publications, use:

BibTex Citation
```bibtex
@misc{gilani_sdvpy_2021,
  author = {Gilani, Saiem},
  title = {sportsdataverse-py: The SportsDataverse's Python Package for Sports Data.},
  url = {https://py.sportsdataverse.org},
  season = {2021}
}
```
