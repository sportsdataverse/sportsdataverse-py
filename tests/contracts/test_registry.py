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
