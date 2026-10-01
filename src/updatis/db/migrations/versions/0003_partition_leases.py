"""Add fenced delivery scheduling leases.

Revision ID: 0003_partition_leases
Revises: 0002_durable_intake
"""

from alembic import op

revision = "0003_partition_leases"
down_revision = "0002_durable_intake"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE deliveries ADD COLUMN lease_owner text")
    op.execute("ALTER TABLE deliveries ADD COLUMN lease_expires_at timestamptz")
    op.execute("ALTER TABLE deliveries ADD COLUMN fencing_generation bigint NOT NULL DEFAULT 0")
    op.execute("""
        ALTER TABLE deliveries ADD CONSTRAINT deliveries_lease_pair
        CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
    """)
    op.execute("""
        ALTER TABLE deliveries ADD CONSTRAINT deliveries_lease_owner_length
        CHECK (lease_owner IS NULL OR length(lease_owner) BETWEEN 1 AND 128)
    """)
    op.execute("""
        ALTER TABLE deliveries ADD CONSTRAINT deliveries_fencing_generation_nonnegative
        CHECK (fencing_generation >= 0)
    """)
    op.execute("""
        ALTER TABLE deliveries ADD CONSTRAINT deliveries_owned_generation_positive
        CHECK (lease_owner IS NULL OR fencing_generation > 0)
    """)
    op.execute("""
        CREATE INDEX deliveries_unresolved_pipeline_event_idx
        ON deliveries (pipeline_id, event_id, id)
        WHERE state IN ('pending', 'attempting')
    """)


def downgrade() -> None:
    raise RuntimeError("v0.1-c does not provide destructive automatic downgrades")
