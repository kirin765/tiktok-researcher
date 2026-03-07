from __future__ import annotations

from app.worker import tasks


def test_is_retriable_snapshot_error_retries_on_timeout():
    assert tasks._is_retriable_snapshot_error(
        "RuntimeError: Apify actor call failed (400): run-failed, timeout exceeded in actor run",
    )


def test_is_retriable_snapshot_error_retries_on_rate_limit():
    assert tasks._is_retriable_snapshot_error(
        "RuntimeError: Apify actor call failed (400): actor run: Too many requests from IP",
    )


def test_is_retriable_snapshot_error_retries_on_408():
    assert tasks._is_retriable_snapshot_error("Apify actor call failed (408): request timeout")


def test_is_retriable_snapshot_error_retries_on_502():
    assert tasks._is_retriable_snapshot_error("Apify actor call failed (502): service unavailable")


def test_is_retriable_snapshot_error_fails_on_run_failed():
    assert not tasks._is_retriable_snapshot_error(
        "RuntimeError: Apify actor call failed (400): run-failed",
    )


def test_is_retriable_snapshot_error_fails_on_invalid_url():
    assert not tasks._is_retriable_snapshot_error("invalid tiktok video url: foo")
