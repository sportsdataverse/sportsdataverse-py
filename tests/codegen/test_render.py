import ast
import types

from tools.codegen import generate, render


def test_type_hint_maps_union_and_scalars():
    assert render.type_hint("int") == "int"
    assert render.type_hint("str") == "str"
    assert render.type_hint("int|str") == "Union[int, str]"


def test_py_repr_quotes_strings_and_passes_numbers():
    assert render.py_repr("a") == "'a'"
    assert render.py_repr(500) == "500"
    assert render.py_repr(None) == "None"


def test_env_renders_a_trivial_template():
    out = render.ENV.from_string("hi {{ name }}").render(name="x")
    assert out == "hi x"


def test_generated_modules_are_valid_python_with_all_and_defs():
    for name, src in generate._render_all().items():
        tree = ast.parse(src)  # raises SyntaxError if malformed
        funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
        assert funcs, f"{name} has no functions"
        assert "__all__" in src
        for fn in funcs:
            assert f'"{fn}"' in src, f"{fn} missing from __all__ in {name}"


def test_returns_prose_recombines_colon_split_code_span():
    """docstring_parser splits a Returns body on the first colon; when that
    colon is INSIDE an inline code span (``col: dtype, ...``) it mis-splits,
    leaving type_name a dangling ``col`` fragment. The renderer must recombine
    so no unbalanced reST literal (a stray ``) survives. Regression for the
    load_fp_curve / mbb / wbb mangled Returns fragments."""
    ds = types.SimpleNamespace(
        type_name="``yardline_own",
        description="Int64 (1..99), ep: Float64`` -- monotone non-decreasing.",
    )
    out = generate._returns_prose(ds)
    assert "``" not in out  # no unbalanced reST double-backtick literal survives
    assert "`yardline_own: Int64 (1..99), ep: Float64`" in out
    assert "monotone non-decreasing" in out  # descriptive tail preserved


def test_returns_prose_leaves_plain_type_split_untouched():
    """A legit Google ``type: description`` (no backtick in the type) keeps the
    prior behavior — description only, no spurious recombination."""
    ds = types.SimpleNamespace(type_name="pl.DataFrame", description="A tidy frame.")
    assert generate._returns_prose(ds) == "A tidy frame."


def test_returns_prose_empty_inputs_yield_empty_string():
    assert generate._returns_prose(None) == ""
    assert generate._returns_prose(types.SimpleNamespace(type_name=None, description="")) == ""
