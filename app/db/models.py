from __future__ import annotations

import uuid
from datetime import datetime, date

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db.base import Base


class Video(Base):
    __tablename__ = "videos"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    platform = Column(String, default="tiktok", nullable=False)
    url = Column(Text, unique=True, nullable=False)
    platform_video_id = Column(String, nullable=True)
    author_id = Column(String, nullable=True)
    author_handle = Column(String, nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=True)
    duration_sec = Column(Integer, nullable=True)
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    has_audio = Column(Boolean, nullable=True)
    caption_keywords = Column(JSON().with_variant(JSON, "sqlite"), default=list)
    hashtags = Column(JSON().with_variant(JSON, "sqlite"), default=list)
    sound_id = Column(String, nullable=True)
    sound_title = Column(Text, nullable=True)
    sound_is_original = Column(Boolean, nullable=True)
    region = Column(String(8), nullable=True)
    language = Column(String(8), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    metric_snapshots = relationship("MetricSnapshot", back_populates="video", cascade="all, delete-orphan")
    content_token = relationship("ContentToken", back_populates="video", uselist=False)
    jobs = relationship("Job", back_populates="video")
    scheduled_tasks = relationship("ScheduledTask", back_populates="video")

    __table_args__ = (
        UniqueConstraint("platform", "platform_video_id", name="uq_videos_platform_platform_video_id"),
        Index("ix_videos_published_at", "published_at"),
        Index("ix_videos_region", "region"),
    )


class MetricSnapshot(Base):
    __tablename__ = "metric_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    video_id = Column(UUID(as_uuid=True), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)
    captured_at = Column(DateTime(timezone=True), nullable=False)
    view_count = Column(BigInteger, nullable=True)
    like_count = Column(BigInteger, nullable=True)
    comment_count = Column(BigInteger, nullable=True)
    share_count = Column(BigInteger, nullable=True)
    bookmark_count = Column(BigInteger, nullable=True)
    source = Column(String, nullable=False)
    raw = Column(JSON().with_variant(JSON, "sqlite"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    video = relationship("Video", back_populates="metric_snapshots")

    __table_args__ = (UniqueConstraint("video_id", "captured_at", name="uq_metric_video_captured_at"),)


class ContentToken(Base):
    __tablename__ = "content_tokens"

    video_id = Column(UUID(as_uuid=True), ForeignKey("videos.id", ondelete="CASCADE"), primary_key=True)
    schema_version = Column(String, default="1.0", nullable=False)
    tokens_json = Column(JSON().with_variant(JSON, "sqlite"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    video = relationship("Video", back_populates="content_token")


class CreativeBrief(Base):
    __tablename__ = "creative_briefs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    region = Column(String(16), nullable=False)
    language = Column(String(16), nullable=False)
    niche = Column(String(64), nullable=False)
    window_start = Column(Date, nullable=False)
    window_end = Column(Date, nullable=False)
    brief_json = Column(JSON().with_variant(JSON, "sqlite"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False, default="queued")
    progress = Column(Integer, nullable=False, default=0)
    video_id = Column(UUID(as_uuid=True), ForeignKey("videos.id", ondelete="SET NULL"), nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    video = relationship("Video", back_populates="jobs")
    logs = relationship("JobLog", back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (Index("ix_jobs_status_updated", "status", "updated_at"),)


class JobLog(Base):
    __tablename__ = "job_logs"

    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    level = Column(String(16), nullable=False)
    message = Column(Text, nullable=False)
    meta = Column(JSON().with_variant(JSON, "sqlite"), nullable=True)

    job = relationship("Job", back_populates="logs")


class ScheduledTask(Base):
    __tablename__ = "scheduled_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_type = Column(String(64), nullable=False)
    video_id = Column(UUID(as_uuid=True), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)
    due_at = Column(DateTime(timezone=True), nullable=False)
    status = Column(String(32), nullable=False, default="pending")
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    video = relationship("Video", back_populates="scheduled_tasks")

    __table_args__ = (Index("ix_scheduled_status_due_at", "status", "due_at"),)
