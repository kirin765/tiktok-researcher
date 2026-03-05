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

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._data


@pytest.fixture(autouse=True)
def mock_apify_requests(monkeypatch):
    def fake_post(url: str, *_, **__):
        return _FakeApifyResponse(
            {
                "items": [
                    {
                        "id": "123456",
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
