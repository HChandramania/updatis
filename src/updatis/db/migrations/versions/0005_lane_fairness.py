"""Give new lanes a finite position and index active lease detection."""

from alembic import op

revision = "0005_lane_fairness"
down_revision = "0004_delivery_lanes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Backfilled and previously registered lanes retain a one-time priority,
    # but no new lane can repeatedly jump ahead of already waiting work.
    op.execute("""
        UPDATE delivery_lanes SET last_examined_at='2000-01-01 00:00:00+00'
        WHERE last_examined_at='-infinity'
    """)
    op.execute("""
        ALTER TABLE delivery_lanes ALTER COLUMN last_examined_at
        SET DEFAULT clock_timestamp()
    """)
    op.execute("""
        CREATE INDEX deliveries_lane_active_lease_idx
        ON deliveries(lane_id, lease_expires_at)
        WHERE lease_owner IS NOT NULL
    """)


def downgrade() -> None:
    raise RuntimeError("v0.1-c does not provide destructive automatic downgrades")
