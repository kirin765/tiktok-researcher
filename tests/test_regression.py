from __future__ import annotations

import io
import json
from datetime import datetime, timezone
import uuid

from sqlalchemy import select

from app.db.models import ContentToken, MetricSnapshot, Video
from app.db.session import get_db


def _rand_id() -> str:
    return str(uuid.uuid4().int % 10_000_000)


def test_csv_import_with_mixed_extended_columns(client):
    first_id = _rand_id()
    second_id = _rand_id()

    csv = (
        "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt,viewCount,publishedAt,durationSec\n"
        f"https://www.tiktok.com/@foo/video/{first_id},10,2,1,1,2026-01-01T00:00:00Z,100,2026-01-01T00:00:00Z,30\n"
        f"https://www.tiktok.com/@foo/video/{second_id},5,0,0,0,2026-01-01T00:00:00Z,,,\n"
    )
    res = client.post("/seeds/import-csv?provider=csv", files={"file": ("test.csv", io.BytesIO(csv.encode()), "text/csv")})
    assert res.status_code == 200
    body = res.json()
    assert body["imported"] == 2
    assert body["skipped"] == 0

    with get_db() as db:
        row1 = db.execute(select(Video).where(Video.platform_video_id == first_id)).scalar_one_or_none()
        row2 = db.execute(select(Video).where(Video.platform_video_id == second_id)).scalar_one_or_none()
        assert row1 is not None
        assert row1.duration_sec == 30
        assert row1.published_at is not None
        assert row1.published_at.replace(tzinfo=timezone.utc) == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert row2 is not None


def test_tokens_endpoint_returns_404_when_missing(client):
    with get_db() as db:
        video = Video(url="https://www.tiktok.com/@foo/video/999999", platform="tiktok", platform_video_id="999999")
        db.add(video)
        db.flush()
        vid = str(video.id)

    res = client.get(f"/videos/{vid}/tokens")
    assert res.status_code == 404
    assert res.json()["detail"] == "content tokens not found"


def test_brief_export_schema_is_stable(client):
    with get_db() as db:
        video = Video(url=f"https://www.tiktok.com/@foo/video/{_rand_id()}", platform="tiktok", platform_video_id=_rand_id())
        db.add(video)
        db.flush()

        db.add(
            MetricSnapshot(
                video_id=video.id,
                captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                source="csv",
                view_count=100,
                like_count=10,
                comment_count=1,
                share_count=2,
                bookmark_count=3,
            )
        )
        db.add(
            MetricSnapshot(
                video_id=video.id,
                captured_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                source="csv",
                view_count=300,
                like_count=20,
                comment_count=4,
                share_count=6,
                bookmark_count=7,
            )
        )
        db.add(
            ContentToken(
                video_id=video.id,
                schema_version="1.0",
                tokens_json={"content_type": "test", "hooks": ["open_loop"]},
            )
        )
        db.flush()

    job = client.post(
        "/jobs/generate-brief",
        json={"region": "KR", "language": "ko", "niche": "general", "window_days": 7},
    )
    assert job.status_code == 200
    job_id = job.json()["job_id"]

    job_status = client.get(f"/jobs/{job_id}").json()
    assert job_status["status"] == "done"

    briefs = client.get("/briefs").json()
    assert briefs
    brief_id = briefs[0]["id"]

    res = client.get(f"/briefs/{brief_id}/export")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")

    payload = json.loads(res.text)
    assert set(payload.keys()) >= {"meta", "objective", "top_videos", "pattern_library", "generation_request"}
    assert payload["meta"]["region"] == "KR"
    assert payload["meta"]["window"]["start"] <= payload["meta"]["window"]["end"]
