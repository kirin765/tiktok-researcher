from __future__ import annotations

import io
from datetime import datetime, timezone
import uuid

from sqlalchemy import select

from app.db.models import ContentToken, MetricSnapshot, Video
from app.db.session import get_db


def _random_url() -> str:
    return f"https://www.tiktok.com/@e2e/video/{uuid.uuid4().int % 10_000_000}"


def test_e2e_add_url_then_fetch_snapshot(client):
    url = _random_url()
    added = client.post("/seeds/add-url", json={"url": url, "region": "KR", "language": "ko"})
    assert added.status_code == 200
    video_id = added.json()["video_id"]
    assert added.json()["scheduled"] is True

    res = client.post("/jobs/fetch-snapshot", json={"video_id": video_id})
    assert res.status_code == 200
    job_id = res.json()["job_id"]

    job = client.get(f"/jobs/{job_id}")
    assert job.status_code == 200
    assert job.json()["status"] == "done"

    snapshots = client.get(f"/videos/{video_id}/snapshots")
    assert snapshots.status_code == 200
    rows = snapshots.json()
    assert len(rows) >= 1
    assert "source" in rows[0]
    assert rows[0]["source"] == "apify"


def test_e2e_generate_brief_job_creates_db_and_export_file(client):
    url = _random_url()
    csv = (
        "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt,viewCount\n"
        f"{url},3,1,0,1,2026-01-01T00:00:00Z,100\n"
    )
    import_res = client.post("/seeds/import-csv?provider=csv", files={"file": ("brief.csv", io.BytesIO(csv.encode()), "text/csv")})
    assert import_res.status_code == 200

    with get_db() as db:
        video = db.execute(select(Video).where(Video.url == url)).scalar_one_or_none()
        assert video is not None
        db.add(
            MetricSnapshot(
                video_id=video.id,
                captured_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                source="csv",
                view_count=240,
                like_count=20,
                comment_count=3,
                share_count=7,
                bookmark_count=9,
            )
        )
        db.add(
            ContentToken(
                video_id=video.id,
                schema_version="1.0",
                tokens_json={"content_type": "e2e"},
            )
        )
        db.flush()

    create = client.post(
        "/jobs/generate-brief",
        json={"region": "KR", "language": "ko", "niche": "test", "window_days": 7},
    )
    assert create.status_code == 200
    job_id = create.json()["job_id"]

    job = client.get(f"/jobs/{job_id}")
    assert job.status_code == 200
    assert job.json()["status"] == "done"

    briefs = client.get("/briefs").json()
    assert briefs
    brief_id = briefs[0]["id"]

    export = client.get(f"/briefs/{brief_id}/export")
    assert export.status_code == 200
    assert export.headers["content-type"].startswith("application/json")
