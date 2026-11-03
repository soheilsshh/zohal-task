"""Create the narrow audit table.

Revision ID: 0001_audit
Revises:
Create Date: 2026-11-03
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_audit"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_requests",
        sa.Column("trace_id", sa.String(length=128), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("provider", sa.String(length=16), nullable=True),
        sa.Column("final_status", sa.Integer(), nullable=False),
        sa.Column("latency_ms", sa.Float(), nullable=False),
        sa.Column("attempts", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error_class", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("audit_requests")
