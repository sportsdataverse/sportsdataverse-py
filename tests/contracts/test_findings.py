from tools.validation.findings import Finding, Severity


def test_to_dict_serializes_severity_as_plain_string():
    f = Finding(check="schema_contract", severity=Severity.ERROR, domain="nfl", dataset="nfl_pbp", message="x")
    d = f.to_dict()
    assert d["severity"] == "error"  # JSON-friendly, not "Severity.ERROR"
    assert d["needs_judgment"] is False
    assert d["locator"] == {}  # default_factory, not shared mutable
