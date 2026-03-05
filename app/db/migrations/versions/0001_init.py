"""Initial schema."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "videos",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("platform", sa.String(), nullable=False, server_default="tiktok"),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("platform_video_id", sa.String(), nullable=True),
        sa.Column("author_id", sa.Text(), nullable=True),
        sa.Column("author_handle", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_sec", sa.Integer(), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("has_audio", sa.Boolean(), nullable=True),
        sa.Column("caption_keywords", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("hashtags", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("sound_id", sa.Text(), nullable=True),
        sa.Column("sound_title", sa.Text(), nullable=True),
        sa.Column("sound_is_original", sa.Boolean(), nullable=True),
        sa.Column("region", sa.String(8), nullable=True),
        sa.Column("language", sa.String(8), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("platform", "platform_video_id", name="uq_videos_platform_platform_video_id"),
        sa.UniqueConstraint("url", name="uq_videos_url"),
    )
    op.create_index("ix_videos_published_at", "videos", ["published_at"])
    op.create_index("ix_videos_region", "videos", ["region"])

    op.create_table(
        "metric_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("video_id", sa.UUID(), sa.ForeignKey("videos.id", ondelete="CASCADE"), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("view_count", sa.BigInteger(), nullable=True),
        sa.Column("like_count", sa.BigInteger(), nullable=True),
        sa.Column("comment_count", sa.BigInteger(), nullable=True),
        sa.Column("share_count", sa.BigInteger(), nullable=True),
        sa.Column("bookmark_count", sa.BigInteger(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("raw", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("video_id", "captured_at", name="uq_metric_video_captured_at"),
    )

    op.create_table(
        "content_tokens",
        sa.Column("video_id", sa.UUID(), sa.ForeignKey("videos.id", ondelete="CASCADE"), primary_key=True, nullable=False),
        sa.Column("schema_version", sa.String(), nullable=False, server_default="1.0"),
        sa.Column("tokens_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "creative_briefs",
        sa.Column("id", sa.UUID(), primary_key=True, nullable=False),
        sa.Column("region", sa.String(16), nullable=False),
        sa.Column("language", sa.String(16), nullable=False),
        sa.Column("niche", sa.String(64), nullable=False),
        sa.Column("window_start", sa.Date(), nullable=False),
        sa.Column("window_end", sa.Date(), nullable=False),
        sa.Column("brief_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.UUID(), primary_key=True, nullable=False),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="queued"),
        sa.Column("progress", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("video_id", sa.UUID(), sa.ForeignKey("videos.id", ondelete="SET NULL"), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_jobs_status_updated", "jobs", ["status", "updated_at"])

    op.create_table(
        "job_logs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.UUID(), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("level", sa.String(16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("meta", sa.JSON(), nullable=True),
    )

    op.create_table(
        "scheduled_tasks",
        sa.Column("id", sa.UUID(), primary_key=True, nullable=False),
        sa.Column("task_type", sa.String(64), nullable=False),
        sa.Column("video_id", sa.UUID(), sa.ForeignKey("videos.id", ondelete="CASCADE"), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_scheduled_status_due_at", "scheduled_tasks", ["status", "due_at"])


def downgrade() -> None:
    op.drop_index("ix_scheduled_status_due_at", table_name="scheduled_tasks")
    op.drop_table("scheduled_tasks")
    op.drop_table("job_logs")
    op.drop_index("ix_jobs_status_updated", table_name="jobs")
    op.drop_table("jobs")
    op.drop_table("creative_briefs")
    op.drop_table("content_tokens")
    op.drop_constraint("uq_metric_video_captured_at", "metric_snapshots", type_="unique")
    op.drop_index("ix_metric_snapshots_video_id", table_name="metric_snapshots", if_exists=True)
    op.drop_table("metric_snapshots")
    op.drop_index("ix_videos_region", table_name="videos")
    op.drop_index("ix_videos_published_at", table_name="videos")
    op.drop_table("videos")
