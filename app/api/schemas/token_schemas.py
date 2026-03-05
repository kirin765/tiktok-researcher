from __future__ import annotations

from pydantic import BaseModel


class ContentTokenResponse(BaseModel):
    schema_version: str
    duration_sec: int
    resolution: dict
    hook_proxy: dict
    pacing_proxy: dict
    subtitle_proxy: dict
    audio_proxy: dict
    extensions: dict
