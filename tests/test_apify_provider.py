from __future__ import annotations

from types import SimpleNamespace

from app.providers.apify_provider import ApifyProvider


class _MockResponse:
    def __init__(self, status_code: int, payload: object, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.url = "https://api.apify.com/v2/acts/clockworks~tiktok-scraper/run-sync-get-dataset-items"

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError("http error")


def test_build_actor_url_converts_slash_id_to_tilde():
    provider = ApifyProvider()
    url = provider._build_actor_url("clockworks/tiktok-scraper", "my-token")

    assert url == "https://api.apify.com/v2/acts/clockworks~tiktok-scraper/run-sync-get-dataset-items?token=my-token"


def test_build_actor_url_keeps_tilde_id():
    provider = ApifyProvider()
    url = provider._build_actor_url("clockworks~tiktok-scraper", "my-token")

    assert url == "https://api.apify.com/v2/acts/clockworks~tiktok-scraper/run-sync-get-dataset-items?token=my-token"


def test_build_actor_url_preserves_version_tag(monkeypatch):
    provider = ApifyProvider()
    url = provider._build_actor_url("clockworks~tiktok-scraper@1.2.3", "abc")

    assert "clockworks~tiktok-scraper@1.2.3" in url


def test_normalize_actor_id_rejects_empty():
    provider = ApifyProvider()

    try:
        provider._normalize_actor_id("   ")
    except ValueError as exc:
        assert str(exc) == "APIFY actor id is required"
    else:
        raise AssertionError("expected ValueError")


def test_run_actor_payload_bubbles_apify_404_with_url(monkeypatch):
    provider = ApifyProvider()

    def fake_post(url: str, *args, **kwargs):
        del args, kwargs
        return _MockResponse(
            404,
            {"type": "page-not-found", "message": "Did you specify it correctly?"},
        )

    monkeypatch.setattr("app.providers.apify_provider.requests.post", fake_post)

    try:
        provider._run_actor_payload({}, actor_id="clockworks/tiktok-scraper")
    except RuntimeError as exc:
        msg = str(exc)
        assert "Apify actor call failed (404)" in msg
        assert "clockworks~tiktok-scraper" in msg
        assert "page-not-found" in msg
    else:
        raise AssertionError("expected RuntimeError")


def test_run_actor_payload_extracts_items_from_dict_payload(monkeypatch):
    provider = ApifyProvider()

    def fake_post(url: str, *args, **kwargs):
        del url, args, kwargs
        return _MockResponse(
            200,
            {
                "items": [
                    {
                        "id": "123",
                        "webVideoUrl": "https://www.tiktok.com/@user/video/123",
                    }
                ],
                "nextCursor": "abc",
            },
        )

    monkeypatch.setattr("app.providers.apify_provider.requests.post", fake_post)

    items, raw = provider._run_actor_payload({}, actor_id="clockworks~tiktok-scraper")

    assert items == [{"id": "123", "webVideoUrl": "https://www.tiktok.com/@user/video/123"}]
    assert raw["nextCursor"] == "abc"


def test_run_actor_payload_includes_run_hint_for_apify_400(monkeypatch):
    provider = ApifyProvider()

    class _MockFailedResponse:
        def __init__(self):
            self.status_code = 400
            self.text = '{"error": {"type": "run-failed", "message": "run ID: a1b2c3"}}'
            self.url = "https://api.apify.com/v2/acts/clockworks~tiktok-scraper/run-sync-get-dataset-items"

        @property
        def ok(self):
            return False

        def json(self):
            return {"error": {"type": "run-failed", "message": "run ID: a1b2c3"}}

        def raise_for_status(self):
            raise RuntimeError("mocked http error")

    class _MockRunResponse:
        @property
        def ok(self):
            return True

        status_code = 200

        def json(self):
            return {"status": "FAILED", "statusMessage": "Actor failed", "exitCode": "1"}

        def raise_for_status(self):
            return None

    def fake_post(url: str, *args, **kwargs):
        del url, args, kwargs
        return _MockFailedResponse()

    def fake_get(url: str, *args, **kwargs):
        del url, args, kwargs
        return _MockRunResponse()

    monkeypatch.setattr("app.providers.apify_provider.requests.post", fake_post)
    monkeypatch.setattr("app.providers.apify_provider.requests.get", fake_get)

    try:
        provider._run_actor_payload({}, actor_id="clockworks~tiktok-scraper")
    except RuntimeError as exc:
        msg = str(exc)
        assert "Apify actor call failed (400)" in msg
        assert "run-failed" in msg
        assert "run_hint:" in msg
        assert "status=FAILED" in msg
    else:
        raise AssertionError("expected RuntimeError")


def test_fetch_metrics_parses_alternative_metric_keys(monkeypatch):
    provider = ApifyProvider()
    video = SimpleNamespace(url="https://www.tiktok.com/@user/video/1111111111111111111")

    def fake_run_actor(*args, **kwargs):
        del args, kwargs
        return [
            {
                "id": "1111111111111111111",
                "webVideoUrl": "https://www.tiktok.com/@user/video/1111111111111111111",
                "play_count": "12345",
                "like_count": "900",
                "commentCount": 77,
                "share_count": 12,
                "collect_count": 3,
            }
        ]

    monkeypatch.setattr(provider, "_run_actor", fake_run_actor)
    payload = provider.fetch_metrics(None, video)

    assert payload.view_count == 12345
    assert payload.like_count == 900
    assert payload.comment_count == 77
    assert payload.share_count == 12
    assert payload.bookmark_count == 3


def test_fetch_metrics_prefers_item_with_metrics_when_first_entry_has_error(monkeypatch):
    provider = ApifyProvider()
    video = SimpleNamespace(url="https://www.tiktok.com/@user/video/2222222222222222222")

    def fake_run_actor(*args, **kwargs):
        del args, kwargs
        return [
            {"error": "Post not found or private"},
            {
                "id": "2222222222222222222",
                "webVideoUrl": "https://www.tiktok.com/@user/video/2222222222222222222",
                "stats": {"playCount": "4321", "diggCount": 210},
            },
        ]

    monkeypatch.setattr(provider, "_run_actor", fake_run_actor)
    payload = provider.fetch_metrics(None, video)

    assert payload.view_count == 4321
    assert payload.like_count == 210


def test_fetch_metrics_marks_parse_warning_when_no_metrics_found(monkeypatch):
    provider = ApifyProvider()
    video = SimpleNamespace(url="https://www.tiktok.com/@user/video/3333333333333333333")

    def fake_run_actor(*args, **kwargs):
        del args, kwargs
        return [{"id": "3333333333333333333", "webVideoUrl": "https://www.tiktok.com/@user/video/3333333333333333333"}]

    monkeypatch.setattr(provider, "_run_actor", fake_run_actor)
    payload = provider.fetch_metrics(None, video)

    assert payload.view_count is None
    assert isinstance(payload.raw, dict)
    assert payload.raw.get("parse_warning") == "missing_numeric_metrics"
