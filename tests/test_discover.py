from __future__ import annotations


def test_discover_seed_sync(client):
    res = client.post(
        "/seeds/discover",
        json={"provider": "apify", "query": "fun", "region": "KR", "language": "ko", "max_results": 1},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["mode"] == "sync"
    assert body["provider"] == "apify"
    assert body["discovered"] == 1
    assert body["imported"] == 1
    assert body["skipped"] == 0
    assert body["scheduled_snapshots"] == 5
    assert len(body["video_ids"]) == 1
    assert body["job_id"] is None


def test_discover_seed_async_threshold_switch(client, monkeypatch):
    monkeypatch.setenv("DISCOVERY_SYNC_MAX_RESULTS", "1")
    res = client.post(
        "/seeds/discover",
        json={"provider": "apify", "query": "fun-async", "region": "KR", "language": "ko", "max_results": 2},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["mode"] == "async"
    assert body["job_id"] is not None
    assert body["discovered"] == 0
    assert body["imported"] == 0
    assert body["scheduled_snapshots"] == 0


def test_discover_seed_official_not_available(client):
    res = client.post(
        "/seeds/discover",
        json={"provider": "official", "query": "fun"},
    )
    assert res.status_code == 501
    assert res.json()["detail"] == "provider 'official' is not enabled in phase-1"
