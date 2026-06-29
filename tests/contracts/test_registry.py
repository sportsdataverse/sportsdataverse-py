from tools.validation.registry import DatasetSpec, load_thresholds


def test_dataset_spec_defaults_are_not_shared():
    a = DatasetSpec(name="a", domain="nfl", parquet_glob="x", schema={})
    b = DatasetSpec(name="b", domain="nfl", parquet_glob="y", schema={})
    a.range_constraints["wp"] = (0.0, 1.0)
    assert b.range_constraints == {}  # default_factory, not a shared dict


def test_load_thresholds_returns_dict():
    t = load_thresholds("nfl")
    assert isinstance(t, dict)


def test_dataset_spec_has_leakage_fields_with_safe_defaults():
    a = DatasetSpec(name="a", domain="nfl", parquet_glob="x", schema={})
    assert a.lag_columns == () and a.cumulative_columns == ()
    assert a.group_key == "game_id"


def test_checkcontext_carries_leakage_fields():
    from tools.validation.findings import CheckContext

    ctx = CheckContext(
        domain="nfl",
        dataset="d",
        schema={},
        lag_columns=("prev_ep",),
        cumulative_columns=("cum_epa",),
        group_key="game_id",
    )
    assert ctx.lag_columns == ("prev_ep",)
    assert ctx.cumulative_columns == ("cum_epa",)
    assert ctx.group_key == "game_id"


def test_lint_target_is_frozen_and_registry_exists():
    from tools.validation.registry import LINT_TARGETS, LintTarget

    t = LintTarget(name="x", path="${ROOT}/src", language="python")
    assert t.language == "python"
    assert isinstance(LINT_TARGETS, dict)


def test_lint_targets_registered():
    from tools.validation.registry import LINT_TARGETS

    assert "nfl_native_pbp" in LINT_TARGETS
    assert "sdv_nfl_ep_wp" in LINT_TARGETS
    assert LINT_TARGETS["nfl_native_pbp"].language == "python"
    assert LINT_TARGETS["sdv_nfl_ep_wp"].language == "python"
    # sdv-py's own source is repo-relative; nfl-data is env-rooted
    assert "${SDV_VALIDATION_NFL_DATA_ROOT}" in LINT_TARGETS["nfl_native_pbp"].path
    assert LINT_TARGETS["sdv_nfl_ep_wp"].path == "sportsdataverse/nfl/ep_wp.py"


def test_nfl_model_pbp_registered_with_oracle():
    from tools.validation.oracles import ORACLES
    from tools.validation.registry import DATASETS

    spec = DATASETS["nfl_model_pbp"]
    assert spec.join_keys == ("game_id", "play_id")
    assert spec.oracle_domain == "nfl"
    assert "${SDV_VALIDATION_NFL_DATA_ROOT}" in spec.parquet_glob
    assert ORACLES["nfl"]._source_glob is not None  # oracle now wired
