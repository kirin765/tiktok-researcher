from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.brief_builder import build_brief_json
from app.db.models import ContentToken, MetricSnapshot, Video
from app.db.session import get_db


def test_brief_builder(client):
    with get_db() as db:
        video = Video(
            url="https://www.tiktok.com/@x/video/112233",
            platform="tiktok",
            platform_video_id="112233",
            region="KR",
            language="ko",
            caption_keywords=[],
            hashtags=[],
        )
        db.add(video)
        db.flush()

        db.add(
            MetricSnapshot(
                video_id=video.id,
                captured_at=datetime.now(tz=timezone.utc) - timedelta(hours=30),
                source="csv",
                view_count=120,
                like_count=10,
                comment_count=1,
                share_count=1,
                bookmark_count=2,
            )
        )
        db.add(
            MetricSnapshot(
                video_id=video.id,
                captured_at=datetime.now(tz=timezone.utc) - timedelta(hours=1),
                source="csv",
                view_count=300,
                like_count=20,
                comment_count=3,
                share_count=2,
                bookmark_count=5,
            )
        )
        db.add(ContentToken(video_id=video.id, schema_version="1.0", tokens_json={"hook_proxy": {"cuts_in_first_3s": 1}}))
        db.flush()

        payload = build_brief_json(db, region="KR", language="ko", niche="general", window_days=7)
        assert payload["meta"]["platform"] == "tiktok"
        assert len(payload["top_videos"]) >= 1
