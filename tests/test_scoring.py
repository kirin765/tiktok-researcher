from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.scoring import compute_scores_for_videos, compute_score_from_snapshots
from app.db.models import MetricSnapshot


class Dummy:
    def __init__(self, v0, v24):
        self.video_id = 1
        self.captured_at = v0
        self.view_count = 100
        self.like_count = 10
        self.comment_count = 2
        self.share_count = 1
        self.bookmark_count = 0


class DummyNoSignal:
    def __init__(self, captured_at):
        self.video_id = 1
        self.captured_at = captured_at
        self.view_count = None
        self.like_count = None
        self.comment_count = None
        self.share_count = None
        self.bookmark_count = None


def test_compute_scores_formula():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t24 = datetime(2026, 1, 2, tzinfo=timezone.utc)

    row0 = Dummy(100, 200)
    row0.captured_at = t0
    row1 = Dummy(100, 200)
    row1.captured_at = t24
    row1.view_count = 300
    row1.like_count = 15
    row1.comment_count = 8
    row1.share_count = 4
    row1.bookmark_count = 3

    score = compute_score_from_snapshots([row0, row1])
    assert score is not None
    assert score.snapshot_0h["view"] == 100
    assert score.snapshot_24h["view"] == 300


def test_compute_scores_from_snapshots_rejects_no_metric_rows():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t24 = datetime(2026, 1, 2, tzinfo=timezone.utc)

    row0 = DummyNoSignal(t0)
    row1 = DummyNoSignal(t24)

    assert compute_score_from_snapshots([row0, row1]) is None
