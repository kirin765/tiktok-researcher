from __future__ import annotations

from datetime import date, datetime, timezone
import io
import uuid

from app.db.models import CreativeBrief, ContentToken, Job, MetricSnapshot, Video
from app.db.session import get_db
from app.settings import get_settings


def _video_url() -> str:
    return f"https://www.tiktok.com/@pytest/video/{uuid.uuid4().int % 10_000_000}"


class _BadApifyResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)
        self.url = "https://api.apify.com/v2/acts/clockworks~tiktok-scraper/run-sync-get-dataset-items"

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError("mocked http error")


def test_health_route_is_alive(client):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"ok": True}


def test_seed_add_url_is_idempotent_and_reuses_video(client):
    url = _video_url()
    first = client.post("/seeds/add-url", json={"url": url, "provider": "apify", "region": "KR", "language": "ko"})
    assert first.status_code == 200

    first_json = first.json()
    second = client.post("/seeds/add-url", json={"url": url, "provider": "apify", "region": "KR", "language": "ko"})
    assert second.status_code == 200

    second_json = second.json()
    assert second_json["video_id"] == first_json["video_id"]
    assert second_json["scheduled"] in (True, False)


def test_seed_add_url_rejects_non_video_url(client):
    res = client.post(
        "/seeds/add-url",
        json={"url": "https://www.tiktok.com/search/video/abc", "provider": "apify", "region": "KR", "language": "ko"},
    )
    assert res.status_code == 400
    assert "must be a TikTok video URL" in res.json()["detail"]


def test_seed_import_csv_returns_expected_schema(client):
    csv = "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt\n"
    csv += f"{_video_url()},3,1,0,0,2026-01-01T00:00:00Z\n"

    res = client.post("/seeds/import-csv?provider=csv", files={"file": ("seed.csv", io.BytesIO(csv.encode()), "text/csv")})
    assert res.status_code == 200
    body = res.json()
    assert body["imported"] == 1
    assert body["skipped"] == 0
    assert body["scheduled_snapshots"] == len(get_settings().snapshot_schedule_offsets_seconds)


def test_seed_discover_requires_query_like_condition(client):
    res = client.post("/seeds/discover", json={"provider": "apify", "max_results": 1, "region": "KR", "language": "ko"})
    assert res.status_code == 400
    assert res.json()["detail"] == "one of query, creator_handle, hashtag, challenge is required"


def test_seed_discover_runs_sync_for_small_request(client):
    res = client.post("/seeds/discover", json={"provider": "apify", "query": "trend", "max_results": 1, "region": "KR", "language": "ko"})
    assert res.status_code == 200
    body = res.json()
    assert body["mode"] == "sync"
    assert body["discovered"] == 1
    assert len(body["video_ids"]) == 1


def test_videos_list_and_detail_endpoints(client):
    url = _video_url()
    with get_db() as db:
        row = Video(
            url=url,
            platform="tiktok",
            platform_video_id="12345",
            region="KR",
            language="ko",
            caption_keywords=["test"],
            hashtags=["tag"],
        )
        db.add(row)
        db.flush()
        db.add(
            MetricSnapshot(
                video_id=row.id,
                captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                source="manual",
                view_count=100,
                like_count=10,
                comment_count=1,
                share_count=2,
                bookmark_count=3,
            )
        )
        db.flush()
        video_id = row.id

    res = client.get(f"/videos/{video_id}")
    assert res.status_code == 200
    payload = res.json()
    assert payload["id"] == str(video_id)
    assert payload["platform_video_id"] == "12345"
    assert payload["platform"] == "tiktok"

    snapshots = client.get(f"/videos/{video_id}/snapshots")
    assert snapshots.status_code == 200
    items = snapshots.json()
    assert isinstance(items, list)
    assert len(items) == 1
    assert items[0]["source"] == "manual"

    list_videos = client.get("/videos?region=KR&language=ko&limit=10")
    assert list_videos.status_code == 200
    assert isinstance(list_videos.json(), list)

    invalid_sort = client.get("/videos?sort=bad")
    assert invalid_sort.status_code == 400
    assert invalid_sort.json()["detail"] == "unsupported sort"


def test_videos_tokens_endpoint_returns_404_or_tokens(client):
    with get_db() as db:
        row = Video(
            url=_video_url(),
            platform="tiktok",
            platform_video_id="54321",
            region="KR",
            language="ko",
            caption_keywords=[],
            hashtags=[],
        )
        db.add(row)
        db.flush()
        db.add(ContentToken(video_id=row.id, schema_version="1.0", tokens_json={"topic": "mock"}))
        db.flush()
        video_id = row.id

    res = client.get(f"/videos/{video_id}/tokens")
    assert res.status_code == 200
    assert res.json() == {"topic": "mock"}


def test_jobs_contracts(client):
    with get_db() as db:
        row = Video(url=_video_url(), platform="tiktok", platform_video_id="11111", region="KR", language="ko", caption_keywords=[], hashtags=[])
        db.add(row)
        db.flush()
        video_id = row.id

    analyze = client.post("/jobs/analyze-content", json={"video_id": str(video_id)})
    assert analyze.status_code == 200
    analyze_id = analyze.json()["job_id"]

    compute = client.post("/jobs/compute-scores", json={"window_days": 7})
    assert compute.status_code == 200

    brief = client.post("/jobs/generate-brief", json={"region": "KR", "language": "ko", "niche": "trend", "window_days": 7})
    assert brief.status_code == 200

    snapshot = client.post("/jobs/fetch-snapshot", json={"video_id": str(video_id), "provider": "apify"})
    assert snapshot.status_code == 200

    jobs = client.get("/jobs")
    assert jobs.status_code == 200
    job_rows = jobs.json()
    assert isinstance(job_rows, list)
    ids = {row["id"] for row in job_rows}
    assert analyze_id in ids

    detail = client.get(f"/jobs/{analyze_id}")
    assert detail.status_code == 200
    assert detail.json()["id"] == analyze_id

    logs = client.get(f"/jobs/{analyze_id}/logs")
    assert logs.status_code == 200
    assert isinstance(logs.json(), list)


def test_jobs_list_type_filter(client):
    with get_db() as db:
        db.add_all(
            [
                Job(type="snapshot", status="queued"),
                Job(type="discover", status="queued"),
                Job(type="snapshot", status="done"),
            ]
        )

    filtered = client.get("/jobs?type=snapshot")
    assert filtered.status_code == 200
    rows = filtered.json()
    assert rows
    assert all(row["type"] == "snapshot" for row in rows)
    assert not any(row["type"] == "discover" for row in rows)


def test_snapshot_job_failure_surfaces_in_job_and_logs(client, monkeypatch):
    with get_db() as db:
        row = Video(url=_video_url(), platform="tiktok", platform_video_id="99999", region="KR", language="ko", caption_keywords=[], hashtags=[])
        db.add(row)
        db.flush()
        video_id = row.id

    def fake_post(url: str, *args, **kwargs):
        del url, args, kwargs
        return _BadApifyResponse(404, {"type": "page-not-found", "message": "actor not found"})

    monkeypatch.setattr("app.providers.apify_provider.requests.post", fake_post)

    snapshot = client.post("/jobs/fetch-snapshot", json={"video_id": str(video_id), "provider": "apify"})
    assert snapshot.status_code == 200
    job_id = snapshot.json()["job_id"]

    job = client.get(f"/jobs/{job_id}")
    assert job.status_code == 200
    payload = job.json()
    assert payload["status"] == "failed"
    assert "Apify actor call failed" in payload["error"]

    logs = client.get(f"/jobs/{job_id}/logs")
    assert logs.status_code == 200
    rows = logs.json()
    assert rows
    assert any(log["level"] == "error" and "Apify actor call failed" in log["message"] for log in rows)


def test_briefs_endpoints(client):
    with get_db() as db:
        video = Video(
            url=_video_url(),
            platform="tiktok",
            platform_video_id="88888",
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
                captured_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                source="manual",
                view_count=5,
                like_count=1,
                comment_count=0,
                share_count=0,
                bookmark_count=0,
            )
        )
        db.flush()
        brief = CreativeBrief(
            region="KR",
            language="ko",
            niche="trend",
            window_start=date(2026, 1, 1),
            window_end=date(2026, 1, 2),
            brief_json={"meta": {"window": {"start": "2026-01-01", "end": "2026-01-02"}}, "niche": "trend"},
        )
        db.add(brief)
        db.flush()
        brief_id = brief.id

    list_resp = client.get("/briefs?region=KR&language=ko&limit=5")
    assert list_resp.status_code == 200
    assert isinstance(list_resp.json(), list)
    assert any(r["id"] == str(brief_id) for r in list_resp.json())

    get_resp = client.get(f"/briefs/{brief_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["niche"] == "trend"

    export = client.get(f"/briefs/{brief_id}/export")
    assert export.status_code == 200
    assert export.headers["content-type"].startswith("application/json")

    generate_bad = client.post("/briefs/generate", json={"region": "KR", "niche": "trend", "window_days": 7})
    assert generate_bad.status_code == 400


def test_stats_videos_returns_video_snapshot_stats(client):
    with get_db() as db:
        row = Video(url=_video_url(), platform="tiktok", platform_video_id="77777", region="KR", language="ko", caption_keywords=[], hashtags=[])
        db.add(row)
        db.flush()
        db.add(
            MetricSnapshot(
                video_id=row.id,
                captured_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
                source="manual",
                view_count=10,
                like_count=1,
                comment_count=0,
                share_count=0,
                bookmark_count=0,
            )
        )
        db.flush()
        video_id = str(row.id)

    res = client.get("/stats/videos")
    assert res.status_code == 200
    rows = res.json()
    assert isinstance(rows, list)
    assert any(r["video_id"] == video_id for r in rows if isinstance(r, dict))
