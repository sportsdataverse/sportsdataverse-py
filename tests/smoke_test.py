#!/usr/bin/env python3
"""Smoke test to verify distribution includes all necessary files."""

import sys
import os


def test_basic_imports():
    """Test that all main modules can be imported."""
    print("Testing basic imports...")

    try:
        import sportsdataverse
        from sportsdataverse import cfb
        from sportsdataverse import nfl
        from sportsdataverse import mbb
        from sportsdataverse import nba
        from sportsdataverse import nhl
        from sportsdataverse import wbb
        from sportsdataverse import wnba
        print("✓ All sport modules importable")
    except ImportError as e:
        print(f"✗ Import failed: {e}", file=sys.stderr)
        return False

    return True


def test_loader_functions():
    """Test that loader functions are accessible."""
    print("Testing loader functions...")

    try:
        from sportsdataverse.cfb.cfb_loaders import load_cfb_pbp, load_cfb_schedule
        from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp, load_nfl_schedule
        from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp, load_mbb_schedule
        print("✓ Loader functions accessible")
    except ImportError as e:
        print(f"✗ Loader import failed: {e}", file=sys.stderr)
        return False

    return True


def test_model_files():
    """Test that XGBoost model files are included in the distribution."""
    print("Testing model files...")

    try:
        from importlib.resources import files

        # Check CFB models
        cfb_ep_model = str(files("sportsdataverse").joinpath("cfb/models/ep_model.model"))
        cfb_wp_model = str(files("sportsdataverse").joinpath("cfb/models/wp_spread.model"))
        cfb_qbr_model = str(files("sportsdataverse").joinpath("cfb/models/qbr_model.model"))

        for model_path, name in [
            (cfb_ep_model, "CFB EP model"),
            (cfb_wp_model, "CFB WP model"),
            (cfb_qbr_model, "CFB QBR model"),
        ]:
            if not os.path.exists(model_path):
                print(f"✗ {name} not found at {model_path}", file=sys.stderr)
                return False
            print(f"  ✓ {name} found")

        # Check NFL models
        nfl_ep_model = str(files("sportsdataverse").joinpath("nfl/models/ep_model.model"))
        nfl_wp_model = str(files("sportsdataverse").joinpath("nfl/models/wp_spread.model"))
        nfl_qbr_model = str(files("sportsdataverse").joinpath("nfl/models/qbr_model.model"))

        for model_path, name in [
            (nfl_ep_model, "NFL EP model"),
            (nfl_wp_model, "NFL WP model"),
            (nfl_qbr_model, "NFL QBR model"),
        ]:
            if not os.path.exists(model_path):
                print(f"✗ {name} not found at {model_path}", file=sys.stderr)
                return False
            print(f"  ✓ {name} found")

        print("✓ All model files present")
    except Exception as e:
        print(f"✗ Model file check failed: {e}", file=sys.stderr)
        return False

    return True


def test_play_process_classes():
    """Test that PlayProcess classes can be instantiated."""
    print("Testing PlayProcess classes...")

    try:
        from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
        from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

        # Test instantiation (don't actually fetch data)
        cfb_proc = CFBPlayProcess(gameId=401301025)
        nfl_proc = NFLPlayProcess(gameId=401220403)

        print("✓ PlayProcess classes instantiable")
    except Exception as e:
        print(f"✗ PlayProcess instantiation failed: {e}", file=sys.stderr)
        return False

    return True


def main():
    """Run all smoke tests."""
    print("=" * 50)
    print("Running sportsdataverse smoke tests")
    print("=" * 50)
    print()

    tests = [
        test_basic_imports,
        test_loader_functions,
        test_model_files,
        test_play_process_classes,
    ]

    results = []
    for test in tests:
        result = test()
        results.append(result)
        print()

    print("=" * 50)
    if all(results):
        print("✓ ALL SMOKE TESTS PASSED")
        print("=" * 50)
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        print("=" * 50)
        return 1


if __name__ == "__main__":
    sys.exit(main())
