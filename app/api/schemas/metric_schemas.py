from __future__ import annotations

from pydantic import BaseModel


class MetricItem(BaseModel):
    t0: int | None = None
    t24: int | None = None
