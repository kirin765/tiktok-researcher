from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

os.environ.setdefault("DATABASE_URL", "sqlite:///./tests/test_db.sqlite")
os.environ.setdefault("WORKER_SYNC", "true")
os.environ.setdefault("APIFY_TOKEN", "test-token")

from app.main import app  # noqa: E402
from app.db.session import create_schema, get_db
from app.db.base import Base


@pytest.fixture(scope="session", autouse=True)
def prepare_db():
    db_url = os.environ["DATABASE_URL"]
    if db_url.startswith("sqlite:///"):
        Path(db_url.removeprefix("sqlite:///")).unlink(missing_ok=True)

    create_schema()
    with get_db() as db:
        # clear for deterministic tests
        for table in reversed(Base.metadata.sorted_tables):
            db.execute(text(f"DELETE FROM {table.name}"))
        db.commit()
    yield


class _FakeApifyResponse:
    def __init__(self, data: dict):
        self._data = data
        self.status_code = 200

    @property
    def ok(self) -> bool:
        return True

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._data


@pytest.fixture(autouse=True)
def mock_apify_requests(monkeypatch):
    def fake_post(url: str, *args, json=None, **kwargs):
        del args
        del kwargs
        payload = json if isinstance(json, dict) else None
        if not payload:
            payload = None

        post_urls = None
        if isinstance(payload, dict):
            post_urls = payload.get("postURLs")

        vid = "123456"
        if isinstance(post_urls, list) and post_urls:
            url = str(post_urls[0])
            if "/video/" in url:
                vid = url.split("/video/")[-1].split("?")[0]
        elif isinstance(payload, dict):
            token = (
                payload.get("searchTerms")
                or payload.get("searchKeyword")
                or payload.get("searchQueries")
                or payload.get("query")
                or payload.get("creatorId")
                or payload.get("profiles")
            )
            if token:
                if isinstance(token, (list, tuple)):
                    token = token[0] if token else None
                raw = str(token).strip()
                if raw:
                    vid = str(abs(hash(raw)) % 900000 + 100000)

        return _FakeApifyResponse(
            {
                "items": [
                    {
                        "id": vid,
                        "webVideoUrl": f"https://www.tiktok.com/@mock/video/{vid}",
                        "playCount": 1234,
                        "stats": {
                            "playCount": 1234,
                            "diggCount": 123,
                            "commentCount": 12,
                            "shareCount": 34,
                            "collectCount": 5,
                        },
                        "durationSec": 30,
                        "hashtags": ["tiktok"],
                        "caption_keywords": ["test"],
                    }
                ]
            }
        )

    monkeypatch.setattr("app.providers.apify_provider.requests.post", fake_post)


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c
