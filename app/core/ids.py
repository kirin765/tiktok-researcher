from __future__ import annotations

import re

VIDEO_RE = re.compile(r"/video/(\d+)")


def extract_tiktok_video_id(url: str) -> str | None:
    match = VIDEO_RE.search(url)
    if not match:
        return None
    return match.group(1)


def is_valid_tiktok_video_url(url: str) -> bool:
    return extract_tiktok_video_id(url) is not None


def coerce_tiktok_video_url(raw_url: str, fallback_platform_video_id: str | None = None) -> str | None:
    normalized = normalize_tiktok_url(raw_url)
    if extract_tiktok_video_id(normalized):
        return normalized
    if fallback_platform_video_id is not None:
        cleaned = str(fallback_platform_video_id).strip()
        if cleaned.isdigit():
            return f"https://www.tiktok.com/video/{cleaned}"
    return None


def normalize_tiktok_url(url: str) -> str:
    if not url:
        return url
    return url.strip().split("?")[0]
