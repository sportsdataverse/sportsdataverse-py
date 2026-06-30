from tools.validation.registry import DatasetSpec, load_thresholds


def test_dataset_spec_defaults_are_not_shared():
    a = DatasetSpec(name="a", domain="nfl", parquet_glob="x", schema={})
    b = DatasetSpec(name="b", domain="nfl", parquet_glob="y", schema={})
    a.range_constraints["wp"] = (0.0, 1.0)
    assert b.range_constraints == {}  # default_factory, not a shared dict


def test_load_thresholds_returns_dict():
    t = load_thresholds("nfl")
    assert isinstance(t, dict)
