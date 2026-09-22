from updatis.config.redaction import redact


def test_recursive_redaction_does_not_expose_secret_canary() -> None:
    value = {"nested": {"password": "canary", "safe": "value"}, "authorization": "canary"}
    result = redact(value)
    assert "canary" not in repr(result)
    assert result["nested"]["safe"] == "value"


def test_secret_references_remain_exportable() -> None:
    reference = {"provider": "env", "name": "SIGNING_SECRET"}
    assert redact({"signing_secret": reference}) == {"signing_secret": reference}
