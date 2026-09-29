"""Add capture manifests, stream epochs, and durable intake state.

Revision ID: 0002_durable_intake
Revises: 0001_metadata
"""

from alembic import op

revision = "0002_durable_intake"
down_revision = "0001_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE ingest_records DROP CONSTRAINT ingest_records_pipeline_id_source_stream_epoch_fkey")
    op.execute("ALTER TABLE ingest_checkpoints DROP CONSTRAINT ingest_checkpoints_pipeline_id_source_stream_epoch_fkey")
    op.execute("ALTER TABLE pipelines DROP CONSTRAINT pipelines_id_source_stream_epoch_key")
    op.execute("ALTER TABLE pipelines ALTER COLUMN source_stream_epoch TYPE uuid USING source_stream_epoch::uuid")
    op.execute("ALTER TABLE pipelines ADD CONSTRAINT pipelines_id_source_stream_epoch_key UNIQUE (id, source_stream_epoch)")
    op.execute("""
        CREATE TABLE stream_epochs (
            pipeline_id uuid NOT NULL REFERENCES pipelines(id) ON DELETE RESTRICT,
            stream_epoch uuid NOT NULL,
            connector_name text NOT NULL UNIQUE,
            publication_name text NOT NULL,
            slot_name text NOT NULL UNIQUE,
            topic_prefix text NOT NULL UNIQUE,
            consumer_group text NOT NULL UNIQUE,
            snapshot_mode text NOT NULL CHECK (snapshot_mode = 'initial'),
            predecessor_epoch uuid,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (pipeline_id, stream_epoch),
            FOREIGN KEY (pipeline_id, predecessor_epoch)
                REFERENCES stream_epochs(pipeline_id, stream_epoch) ON DELETE RESTRICT
        )
    """)
    op.execute("""
        CREATE TABLE stream_epoch_transitions (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            stream_epoch uuid NOT NULL,
            transition text NOT NULL CHECK (transition IN (
                'provisioned', 'connector_registered', 'snapshot_started', 'streaming',
                'stopped', 'failed', 'retired', 'rebootstrap_declared')),
            reason text,
            details jsonb NOT NULL DEFAULT '{}'::jsonb,
            observed_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (pipeline_id, stream_epoch)
                REFERENCES stream_epochs(pipeline_id, stream_epoch) ON DELETE RESTRICT
        )
    """)
    op.execute("CREATE INDEX stream_epoch_transitions_order_idx ON stream_epoch_transitions (pipeline_id, stream_epoch, observed_at, id)")
    op.execute("CREATE UNIQUE INDEX stream_epoch_provisioned_once_idx ON stream_epoch_transitions (pipeline_id, stream_epoch, transition) WHERE transition='provisioned'")
    op.execute("CREATE UNIQUE INDEX stream_epoch_connector_registered_once_idx ON stream_epoch_transitions (pipeline_id, stream_epoch, transition) WHERE transition='connector_registered'")
    op.execute("""
        CREATE TABLE source_schema_manifests (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            stream_epoch uuid NOT NULL,
            schema_name text NOT NULL,
            table_name text NOT NULL,
            columns jsonb NOT NULL,
            schema_fingerprint text NOT NULL CHECK (schema_fingerprint ~ '^[0-9a-f]{64}$'),
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (pipeline_id, stream_epoch)
                REFERENCES stream_epochs(pipeline_id, stream_epoch) ON DELETE RESTRICT,
            UNIQUE (pipeline_id, stream_epoch, schema_name, table_name),
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("ALTER TABLE ingest_records ALTER COLUMN source_stream_epoch TYPE uuid USING source_stream_epoch::uuid")
    op.execute("ALTER TABLE ingest_records ADD CONSTRAINT ingest_records_pipeline_epoch_fkey FOREIGN KEY (pipeline_id, source_stream_epoch) REFERENCES pipelines(id, source_stream_epoch) ON DELETE RESTRICT")
    op.execute("ALTER TABLE ingest_records ADD COLUMN raw_key_prefix bytea")
    op.execute("ALTER TABLE ingest_records ADD COLUMN raw_value_prefix bytea")
    op.execute("ALTER TABLE ingest_records ADD COLUMN raw_key_length bigint NOT NULL DEFAULT 0 CHECK (raw_key_length >= 0)")
    op.execute("ALTER TABLE ingest_records ADD COLUMN raw_value_length bigint CHECK (raw_value_length IS NULL OR raw_value_length >= 0)")
    op.execute("ALTER TABLE ingest_records ADD COLUMN raw_key_truncated boolean NOT NULL DEFAULT false")
    op.execute("ALTER TABLE ingest_records ADD COLUMN raw_value_truncated boolean NOT NULL DEFAULT false")
    op.execute("ALTER TABLE ingest_records ADD CONSTRAINT ingest_raw_key_prefix_bound CHECK (octet_length(raw_key_prefix) <= 65536)")
    op.execute("ALTER TABLE ingest_records ADD CONSTRAINT ingest_raw_value_prefix_bound CHECK (octet_length(raw_value_prefix) <= 262144)")
    op.execute("ALTER TABLE ingest_records ADD CONSTRAINT ingest_raw_key_length_valid CHECK (raw_key_length >= COALESCE(octet_length(raw_key_prefix), 0))")
    op.execute("ALTER TABLE ingest_records ADD CONSTRAINT ingest_raw_value_length_valid CHECK (raw_value_length IS NULL OR raw_value_length >= COALESCE(octet_length(raw_value_prefix), 0))")
    op.execute("""
        CREATE TABLE intake_discontinuities (
            id text PRIMARY KEY CHECK (id ~ '^[0-9a-f]{64}$'),
            pipeline_id uuid NOT NULL,
            source_stream_epoch uuid NOT NULL,
            ingest_record_id uuid NOT NULL UNIQUE,
            topic text NOT NULL,
            partition integer NOT NULL CHECK (partition >= 0),
            offset_value bigint NOT NULL CHECK (offset_value >= 0),
            kind text NOT NULL CHECK (kind IN ('malformed', 'unsupported', 'oversized')),
            reason_code text NOT NULL,
            error_summary text NOT NULL CHECK (length(error_summary) <= 2048),
            raw_hash text NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
            raw_key_length bigint NOT NULL CHECK (raw_key_length >= 0),
            raw_value_length bigint CHECK (raw_value_length IS NULL OR raw_value_length >= 0),
            raw_key_truncated boolean NOT NULL,
            raw_value_truncated boolean NOT NULL,
            normalizer_version integer NOT NULL CHECK (normalizer_version = 1),
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (ingest_record_id, pipeline_id)
                REFERENCES ingest_records(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (pipeline_id, source_stream_epoch)
                REFERENCES stream_epochs(pipeline_id, stream_epoch) ON DELETE RESTRICT,
            UNIQUE (pipeline_id, source_stream_epoch, topic, partition, offset_value)
        )
    """)
    op.execute("ALTER TABLE ingest_checkpoints ALTER COLUMN source_stream_epoch TYPE uuid USING source_stream_epoch::uuid")
    op.execute("ALTER TABLE ingest_checkpoints ADD CONSTRAINT ingest_checkpoints_pipeline_epoch_fkey FOREIGN KEY (pipeline_id, source_stream_epoch) REFERENCES pipelines(id, source_stream_epoch) ON DELETE RESTRICT")
    op.execute("ALTER TABLE ingest_checkpoints ADD COLUMN initial_offset bigint CHECK (initial_offset >= 0)")
    op.execute("UPDATE ingest_checkpoints SET initial_offset = next_offset")
    op.execute("ALTER TABLE ingest_checkpoints ALTER COLUMN initial_offset SET NOT NULL")
    op.execute("ALTER TABLE ingest_checkpoints ADD CONSTRAINT checkpoint_not_before_initial CHECK (next_offset >= initial_offset)")
    op.execute("ALTER TABLE ordering_gaps ALTER COLUMN source_stream_epoch TYPE uuid USING source_stream_epoch::uuid")
    op.execute("""
        CREATE FUNCTION reject_intake_checkpoint_regression() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.initial_offset <> OLD.initial_offset OR NEW.next_offset < OLD.next_offset THEN
                RAISE EXCEPTION 'ingest checkpoint cannot move backward or change its initial offset';
            END IF;
            RETURN NEW;
        END; $$
    """)
    op.execute("""
        CREATE TRIGGER ingest_checkpoint_monotonic BEFORE UPDATE ON ingest_checkpoints
        FOR EACH ROW EXECUTE FUNCTION reject_intake_checkpoint_regression()
    """)
    for table in ("stream_epochs", "stream_epoch_transitions", "source_schema_manifests", "intake_discontinuities"):
        op.execute(f"""
            CREATE FUNCTION reject_{table}_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION '{table} rows are immutable'; END; $$
        """)
        op.execute(f"""
            CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION reject_{table}_mutation()
        """)
    op.execute("GRANT SELECT, INSERT, UPDATE ON stream_epochs, stream_epoch_transitions, source_schema_manifests, intake_discontinuities TO updatis_runtime")
    op.execute("REVOKE UPDATE ON stream_epochs, stream_epoch_transitions, source_schema_manifests, intake_discontinuities FROM updatis_runtime")


def downgrade() -> None:
    raise RuntimeError("v0.1-b does not provide destructive automatic downgrades")
