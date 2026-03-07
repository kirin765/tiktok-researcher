from __future__ import annotations

from datetime import datetime, timezone
import io


def test_seed_import_csv_smoke(client):
    csv = (
        "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt\n"
        "https://www.tiktok.com/@foo/video/123456,10,2,1,1,2026-01-01T00:00:00Z\n"
        "https://www.tiktok.com/@foo/video/987654,3,1,0,0,2026-01-01T00:00:00Z\n"
    )
    res = client.post("/seeds/import-csv?provider=csv", files={"file": ("test.csv", io.BytesIO(csv.encode()), "text/csv")})
    assert res.status_code == 200
    data = res.json()
    assert data["imported"] == 2
    assert data["skipped"] == 0
    assert data["scheduled_snapshots"] >= 2


def test_seed_import_csv_skips_invalid_url(client):
    csv = (
        "url,likeCount,commentCount,bookmarkCount,shareCount,capturedAt\n"
        "https://www.tiktok.com/search/video/abc,10,2,1,1,2026-01-01T00:00:00Z\n"
        "https://www.tiktok.com/@foo/video/987654,3,1,0,0,2026-01-01T00:00:00Z\n"
    )
    res = client.post("/seeds/import-csv?provider=csv", files={"file": ("test.csv", io.BytesIO(csv.encode()), "text/csv")})
    assert res.status_code == 200
    data = res.json()
    assert data["imported"] == 1
    assert data["skipped"] == 1


def test_list_videos(client):
    res = client.get("/videos?limit=10")
    assert res.status_code == 200
    assert isinstance(res.json(), list)
