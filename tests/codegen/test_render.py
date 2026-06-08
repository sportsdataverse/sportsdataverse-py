import ast

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
