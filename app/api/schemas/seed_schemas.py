from __future__ import annotations

from pydantic import BaseModel


class AddUrlRequest(BaseModel):
    url: str
    region: str = "KR"
    language: str = "ko"
    provider: str | None = None


class ImportCsvResponse(BaseModel):
    batch_id: str
    imported: int
    skipped: int
    scheduled_snapshots: int
