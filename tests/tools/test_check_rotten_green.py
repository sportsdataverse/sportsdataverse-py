import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "check_rotten_green", Path(__file__).resolve().parents[2] / "tools" / "check_rotten_green.py"
)
rg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rg)

_SRC = """
import pytest

def test_weak_only(df):
    assert isinstance(df, object)
    assert df.height > 0

def test_value(df):
    assert df.height > 0
    assert df["x"].to_list() == [1]

def test_frame_equal(df, other):
    assert len(df) > 0
    assert_frame_equal(df, other)

def test_conditional(df):
    if df.height > 0:
        assert df["x"].to_list() == [1]

def test_skips(df):
    if df.is_empty():
        pytest.skip("empty")
    assert df["x"].to_list() == [1]

def test_xfails_itself(df):
    if df.is_empty():
        pytest.xfail("empty")
    assert df["x"].to_list() == [1]

class TestThings:
    def test_not_none(self, x):
        assert x is not None
"""


def test_flags_weak_conditional_and_skipping_tests(tmp_path):
    path = tmp_path / "test_sample.py"
    path.write_text(_SRC, encoding="utf-8")
    flagged = [line.split(": ")[1] for line in rg.check(path)]
    assert flagged == ["test_weak_only", "test_conditional", "test_skips", "test_xfails_itself", "test_not_none"]


def test_main_prints_the_count_and_exits_nonzero(tmp_path, capsys):
    path = tmp_path / "test_sample.py"
    path.write_text("def test_a(df):\n    assert df.height > 0\n", encoding="utf-8")
    assert rg.main([str(path)]) == 1
    assert capsys.readouterr().out.endswith("ROTTEN GREEN: 1\n")
    path.write_text("def test_a(df):\n    assert df['x'][0] == 1\n", encoding="utf-8")
    assert rg.main([str(path)]) == 0
