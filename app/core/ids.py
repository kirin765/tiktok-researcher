from __future__ import annotations

import re

VIDEO_RE = re.compile(r"/video/(\d+)")


def extract_tiktok_video_id(url: str) -> str | None:
    match = VIDEO_RE.search(url)
    if not match:
        return None
    return match.group(1)


def normalize_tiktok_url(url: str) -> str:
    if not url:
        return url
    return url.strip().split("?")[0]
