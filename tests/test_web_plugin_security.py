"""Security behavior for Brain web ingestion."""

from __future__ import annotations

import pytest
from contextunity.core.exceptions import SecurityError

from contextunity.brain.ingestion.rag.plugins.web import WebPlugin


def test_web_download_rejects_metadata_endpoint() -> None:
    with pytest.raises(SecurityError):
        WebPlugin._download_html(
            "http://169.254.169.254/latest/meta-data/",
            user_agent="ContextUnityTest/1.0",
            timeout_s=1.0,
        )


def test_web_download_uses_safe_fetch_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_safe_url_sync(
        url: object,
        *,
        timeout_s: float,
        headers: dict[str, str],
    ) -> bytes:
        captured["url"] = url
        captured["timeout_s"] = timeout_s
        captured["headers"] = headers
        return b"<html><title>ok</title><body>Hello</body></html>"

    monkeypatch.setattr(
        "contextunity.brain.ingestion.rag.plugins.web.fetch_safe_url_sync",
        fake_fetch_safe_url_sync,
    )

    html, final_url = WebPlugin._download_html(
        "https://example.com/docs/",
        user_agent="ContextUnityTest/1.0",
        timeout_s=3.0,
    )

    assert "Hello" in html
    assert final_url == "https://example.com/docs"
    assert captured["url"] == "https://example.com/docs/"
    assert captured["timeout_s"] == 3.0
    assert captured["headers"] == {
        "User-Agent": "ContextUnityTest/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
