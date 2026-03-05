from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class BriefRequest(BaseModel):
    region: str
    language: str
    niche: str
    window_days: int = 7


class BriefResponse(BaseModel):
    id: str
    region: str
    language: str
    niche: str


class BriefSchema(BaseModel):
    meta: dict[str, Any]
    objective: dict[str, Any]
    top_videos: list[dict[str, Any]]
    pattern_library: dict[str, Any]
    generation_request: dict[str, Any]
