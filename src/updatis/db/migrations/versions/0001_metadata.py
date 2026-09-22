"""Create the v0.1-a metadata schema.

Revision ID: 0001_metadata
Revises: None
"""

from alembic import op

revision = "0001_metadata"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE pipelines (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            name text NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 64),
            source_instance_id text NOT NULL,
            source_stream_epoch text NOT NULL,
            desired_state text NOT NULL DEFAULT 'stopped' CHECK (desired_state IN ('stopped', 'started', 'paused')),
            created_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (id, source_stream_epoch)
        )
    """)
    op.execute("""
        CREATE TABLE configuration_revisions (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL REFERENCES pipelines(id) ON DELETE RESTRICT,
            revision integer NOT NULL CHECK (revision > 0),
            schema_version integer NOT NULL CHECK (schema_version = 1),
            document jsonb NOT NULL,
            content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
            created_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (pipeline_id, revision),
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("""
        CREATE TABLE ingest_records (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            source_stream_epoch text NOT NULL,
            topic text NOT NULL,
            partition integer NOT NULL CHECK (partition >= 0),
            offset_value bigint NOT NULL CHECK (offset_value >= 0),
            classification text NOT NULL CHECK (classification IN ('event', 'quarantine', 'control')),
            raw_hash text NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
            retained_payload jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (pipeline_id, source_stream_epoch)
                REFERENCES pipelines(id, source_stream_epoch) ON DELETE RESTRICT,
            UNIQUE (pipeline_id, source_stream_epoch, topic, partition, offset_value),
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("""
        CREATE TABLE events (
            id text PRIMARY KEY CHECK (id ~ '^[0-9a-f]{64}$'),
            pipeline_id uuid NOT NULL,
            ingest_record_id uuid NOT NULL UNIQUE,
            configuration_revision_id uuid NOT NULL,
            operation text NOT NULL CHECK (operation IN ('read', 'create', 'update', 'delete')),
            source_position jsonb NOT NULL,
            envelope jsonb NOT NULL,
            payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
            captured_at timestamptz,
            ingested_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (ingest_record_id, pipeline_id)
                REFERENCES ingest_records(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (configuration_revision_id, pipeline_id)
                REFERENCES configuration_revisions(id, pipeline_id) ON DELETE RESTRICT,
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("""
        CREATE TABLE ingest_checkpoints (
            pipeline_id uuid NOT NULL,
            source_stream_epoch text NOT NULL,
            topic text NOT NULL,
            partition integer NOT NULL CHECK (partition >= 0),
            consumer_group text NOT NULL,
            next_offset bigint NOT NULL CHECK (next_offset >= 0),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (pipeline_id, source_stream_epoch, topic, partition, consumer_group),
            FOREIGN KEY (pipeline_id, source_stream_epoch)
                REFERENCES pipelines(id, source_stream_epoch) ON DELETE RESTRICT
        )
    """)
    op.execute("""
        CREATE TABLE deliveries (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            event_id text NOT NULL,
            configuration_revision_id uuid NOT NULL,
            state text NOT NULL CHECK (state IN ('pending', 'attempting', 'acknowledged', 'terminal')),
            max_attempts integer NOT NULL CHECK (max_attempts > 0),
            max_age_seconds integer NOT NULL CHECK (max_age_seconds > 0),
            next_attempt_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (event_id, pipeline_id) REFERENCES events(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (configuration_revision_id, pipeline_id)
                REFERENCES configuration_revisions(id, pipeline_id) ON DELETE RESTRICT,
            UNIQUE (event_id, configuration_revision_id),
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("CREATE INDEX deliveries_state_due_idx ON deliveries (state, next_attempt_at)")
    op.execute("""
        CREATE TABLE delivery_attempts (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            delivery_id uuid NOT NULL,
            attempt_number integer NOT NULL CHECK (attempt_number > 0),
            started_at timestamptz NOT NULL DEFAULT now(),
            finished_at timestamptz,
            outcome text CHECK (outcome IN ('acknowledged', 'retryable', 'terminal', 'ambiguous')),
            response_status integer CHECK (response_status BETWEEN 100 AND 599),
            error_summary text CHECK (length(error_summary) <= 2048),
            FOREIGN KEY (delivery_id, pipeline_id) REFERENCES deliveries(id, pipeline_id) ON DELETE RESTRICT,
            UNIQUE (delivery_id, attempt_number),
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("""
        CREATE TABLE dead_letters (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            ingest_record_id uuid NOT NULL,
            event_id text,
            delivery_id uuid,
            configuration_revision_id uuid NOT NULL,
            reason_code text NOT NULL,
            stage text NOT NULL CHECK (stage IN ('intake', 'normalization', 'delivery')),
            error_summary text NOT NULL CHECK (length(error_summary) <= 2048),
            payload_available boolean NOT NULL DEFAULT true,
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (ingest_record_id, pipeline_id) REFERENCES ingest_records(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (event_id, pipeline_id) REFERENCES events(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (delivery_id, pipeline_id) REFERENCES deliveries(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (configuration_revision_id, pipeline_id)
                REFERENCES configuration_revisions(id, pipeline_id) ON DELETE RESTRICT,
            UNIQUE (id, pipeline_id)
        )
    """)
    op.execute("""
        CREATE TABLE ordering_gaps (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            dead_letter_id uuid NOT NULL UNIQUE,
            source_stream_epoch text NOT NULL,
            topic text NOT NULL,
            partition integer NOT NULL CHECK (partition >= 0),
            offset_value bigint NOT NULL CHECK (offset_value >= 0),
            configuration_revision_id uuid NOT NULL,
            reason_code text NOT NULL,
            attempt_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
            advanced_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (dead_letter_id, pipeline_id) REFERENCES dead_letters(id, pipeline_id) ON DELETE RESTRICT,
            FOREIGN KEY (configuration_revision_id, pipeline_id)
                REFERENCES configuration_revisions(id, pipeline_id) ON DELETE RESTRICT,
            UNIQUE (pipeline_id, source_stream_epoch, topic, partition, offset_value)
        )
    """)
    op.execute("""
        CREATE TABLE audit_records (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid REFERENCES pipelines(id) ON DELETE RESTRICT,
            configuration_revision_id uuid,
            actor text NOT NULL,
            action text NOT NULL,
            outcome text NOT NULL,
            reason text,
            details jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            CHECK (configuration_revision_id IS NULL OR pipeline_id IS NOT NULL),
            FOREIGN KEY (configuration_revision_id, pipeline_id)
                REFERENCES configuration_revisions(id, pipeline_id) ON DELETE RESTRICT
        )
    """)
    op.execute("CREATE INDEX audit_records_pipeline_time_idx ON audit_records (pipeline_id, created_at DESC)")
    for table in ("configuration_revisions", "ingest_records", "events"):
        op.execute(f"""
            CREATE FUNCTION reject_{table}_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION '{table} rows are immutable'; END; $$
        """)
        op.execute(f"""
            CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION reject_{table}_mutation()
        """)
    op.execute("GRANT USAGE ON SCHEMA public TO updatis_runtime")
    op.execute("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO updatis_runtime")
    op.execute("REVOKE DELETE ON ALL TABLES IN SCHEMA public FROM updatis_runtime")


def downgrade() -> None:
    raise RuntimeError("v0.1-a does not provide destructive automatic downgrades")
