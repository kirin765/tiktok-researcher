from __future__ import annotations

from datetime import datetime, timedelta, timezone
import uuid

import pytest
from sqlalchemy import select

from app.core.ids import normalize_tiktok_url
from app.db.models import ScheduledTask, Video
from app.db.session import get_db
from app.providers.csv_provider import CsvProvider
from app.worker import tasks


def _url() -> str:
    return f"https://www.tiktok.com/@foo/video/{uuid.uuid4().int % 10_000_000}"


def test_duplicate_url_import_is_idempotent_with_fixed_schedule_base(monkeypatch, client):
    fixed_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(tasks, "_now", lambda: fixed_now)

    url = _url()
    first_csv = (
        "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt\n"
        f"{url},1,0,0,0,2026-01-01T00:00:00Z\n"
    )
    second_csv = (
        "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt\n"
        f"{url},2,0,0,0,2026-01-02T00:00:00Z\n"
    )

    first = client.post("/seeds/import-csv?provider=csv", files={"file": ("test.csv", first_csv.encode(), "text/csv")})
    assert first.status_code == 200
    assert first.json()["imported"] == 1
    assert first.json()["scheduled_snapshots"] == 5

    second = client.post("/seeds/import-csv?provider=csv", files={"file": ("test.csv", second_csv.encode(), "text/csv")})
    assert second.status_code == 200
    assert second.json()["imported"] == 1
    assert second.json()["scheduled_snapshots"] == 0

    with get_db() as db:
        video = db.execute(select(Video).where(Video.url == normalize_tiktok_url(url))).scalar_one_or_none()
        assert video is not None

        scheduled = db.execute(
            select(ScheduledTask)
            .where(ScheduledTask.video_id == video.id)
            .where(ScheduledTask.task_type == "metrics_snapshot")
            .order_by(ScheduledTask.due_at)
        ).scalars().all()
        assert len(scheduled) == 5


def test_scheduler_snapshot_offsets_match_spec(monkeypatch):
    fixed_now = datetime(2026, 1, 2, 10, 30)
    monkeypatch.setattr(tasks, "_now", lambda: fixed_now)

    video_id = None
    with get_db() as db:
        provider = CsvProvider()
        video = provider.upsert_video_from_url(db, normalize_tiktok_url(_url()), region="KR", language="ko")
        video_id = video.id
        count = tasks.schedule_snapshot_tasks(db, video)
        assert count == 5
        assert video_id is not None

    with get_db() as db:
        rows = (
            db.execute(
                select(ScheduledTask)
                .where(ScheduledTask.video_id == video_id)
                .where(ScheduledTask.task_type == "metrics_snapshot")
                .order_by(ScheduledTask.due_at)
            )
            .scalars()
            .all()
        )
        base = fixed_now if fixed_now.tzinfo is not None else fixed_now.replace(tzinfo=timezone.utc)
        deltas = []
        for row in rows:
            target = row.due_at
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            deltas.append(int((target - base).total_seconds()))
        deltas = sorted(deltas)
        assert deltas == [0, 3600, 21600, 86400, 259200]


def test_snapshot_retry_backoff_applies_exponential_and_cap(monkeypatch):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(tasks, "_now", lambda: base)
    monkeypatch.setenv("MAX_SNAPSHOT_ATTEMPTS", "5")
    monkeypatch.setenv("RETRY_BASE_SECONDS", "60")
    monkeypatch.setenv("RETRY_CAP_SECONDS", "120")

    first = ScheduledTask(task_type="metrics_snapshot", video_id=uuid.uuid4(), due_at=base, attempts=0)
    tasks._plan_next_retry(first, "boom")
    assert first.attempts == 1
    assert first.status == "pending"
    assert first.due_at == base + timedelta(seconds=60)

    second = ScheduledTask(task_type="metrics_snapshot", video_id=uuid.uuid4(), due_at=base, attempts=2)
    tasks._plan_next_retry(second, "boom")
    assert second.attempts == 3
    assert second.status == "pending"
    assert second.due_at == base + timedelta(seconds=120)

    final = ScheduledTask(task_type="metrics_snapshot", video_id=uuid.uuid4(), due_at=base, attempts=4)
    tasks._plan_next_retry(final, "boom")
    assert final.attempts == 5
    assert final.status == "failed"


def test_provider_default_is_apify(monkeypatch):
    from app.settings import get_settings

    monkeypatch.setenv("PROVIDER_DEFAULT", "apify")
    assert get_settings().provider_default == "apify"

    monkeypatch.setenv("PROVIDER_DEFAULT", "APIFY")
    assert get_settings().provider_default == "apify"

    monkeypatch.setenv("PROVIDER_DEFAULT", "csv")
    assert get_settings().provider_default == "csv"

    monkeypatch.delenv("PROVIDER_DEFAULT", raising=False)
    assert get_settings().provider_default == "apify"

    monkeypatch.setenv("PROVIDER_DEFAULT", "invalid")
    with pytest.raises(ValueError):
        get_settings()
