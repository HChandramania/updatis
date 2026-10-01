"""Index-aligned, bounded delivery lane discovery (forward-only)."""

from alembic import op

revision = "0004_delivery_lanes"
down_revision = "0003_partition_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE delivery_lanes (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_id uuid NOT NULL,
            source_stream_epoch uuid NOT NULL,
            topic text NOT NULL,
            partition integer NOT NULL CHECK (partition >= 0),
            last_examined_at timestamptz NOT NULL DEFAULT '-infinity',
            UNIQUE (pipeline_id, source_stream_epoch, topic, partition),
            FOREIGN KEY (pipeline_id, source_stream_epoch)
                REFERENCES pipelines(id, source_stream_epoch) ON DELETE RESTRICT
        )
    """)
    op.execute("CREATE INDEX delivery_lanes_discovery_idx ON delivery_lanes(last_examined_at, id)")
    op.execute("ALTER TABLE deliveries ADD COLUMN lane_id uuid REFERENCES delivery_lanes(id) ON DELETE RESTRICT")
    op.execute("ALTER TABLE deliveries ADD COLUMN source_offset bigint CHECK (source_offset >= 0)")
    op.execute("""
        INSERT INTO delivery_lanes(pipeline_id, source_stream_epoch, topic, partition)
        SELECT DISTINCT d.pipeline_id, i.source_stream_epoch, i.topic, i.partition
        FROM deliveries d JOIN events e ON e.id=d.event_id AND e.pipeline_id=d.pipeline_id
        JOIN ingest_records i ON i.id=e.ingest_record_id AND i.pipeline_id=e.pipeline_id
    """)
    op.execute("""
        UPDATE deliveries d SET lane_id=l.id, source_offset=i.offset_value
        FROM events e JOIN ingest_records i ON i.id=e.ingest_record_id AND i.pipeline_id=e.pipeline_id
        JOIN delivery_lanes l ON l.pipeline_id=i.pipeline_id
          AND l.source_stream_epoch=i.source_stream_epoch AND l.topic=i.topic AND l.partition=i.partition
        WHERE d.event_id=e.id AND d.pipeline_id=e.pipeline_id
    """)
    op.execute("ALTER TABLE deliveries ALTER COLUMN lane_id SET NOT NULL")
    op.execute("ALTER TABLE deliveries ALTER COLUMN source_offset SET NOT NULL")
    op.execute("""
        CREATE INDEX deliveries_lane_head_idx ON deliveries(lane_id, source_offset, id)
        WHERE state IN ('pending', 'attempting')
    """)
    op.execute("DROP INDEX deliveries_unresolved_pipeline_event_idx")
    op.execute("""
        CREATE FUNCTION register_delivery_lane() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE coordinates record; registered uuid;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF ROW(NEW.pipeline_id, NEW.event_id, NEW.configuration_revision_id, NEW.lane_id, NEW.source_offset)
                   IS DISTINCT FROM ROW(OLD.pipeline_id, OLD.event_id, OLD.configuration_revision_id, OLD.lane_id, OLD.source_offset) THEN
                    RAISE EXCEPTION 'delivery ordering identity is immutable';
                END IF;
                RETURN NEW;
            END IF;
            SELECT i.* INTO STRICT coordinates FROM events e JOIN ingest_records i
              ON i.id=e.ingest_record_id AND i.pipeline_id=e.pipeline_id
              WHERE e.id=NEW.event_id AND e.pipeline_id=NEW.pipeline_id;
            INSERT INTO delivery_lanes(pipeline_id, source_stream_epoch, topic, partition)
              VALUES (NEW.pipeline_id, coordinates.source_stream_epoch, coordinates.topic, coordinates.partition)
              ON CONFLICT (pipeline_id, source_stream_epoch, topic, partition) DO NOTHING;
            SELECT id INTO STRICT registered FROM delivery_lanes
              WHERE pipeline_id=NEW.pipeline_id AND source_stream_epoch=coordinates.source_stream_epoch
                AND topic=coordinates.topic AND partition=coordinates.partition FOR UPDATE;
            IF (NEW.lane_id IS NOT NULL AND NEW.lane_id <> registered)
               OR (NEW.source_offset IS NOT NULL AND NEW.source_offset <> coordinates.offset_value) THEN
                RAISE EXCEPTION 'delivery ordering coordinates mismatch';
            END IF;
            NEW.lane_id := registered;
            NEW.source_offset := coordinates.offset_value;
            RETURN NEW;
        END; $$
    """)
    op.execute("""
        CREATE TRIGGER deliveries_lane BEFORE INSERT OR UPDATE ON deliveries
        FOR EACH ROW EXECUTE FUNCTION register_delivery_lane()
    """)
    op.execute("""
        CREATE FUNCTION protect_delivery_lane() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'delivery lanes cannot be deleted'; END IF;
            IF ROW(NEW.id, NEW.pipeline_id, NEW.source_stream_epoch, NEW.topic, NEW.partition)
               IS DISTINCT FROM ROW(OLD.id, OLD.pipeline_id, OLD.source_stream_epoch, OLD.topic, OLD.partition) THEN
                RAISE EXCEPTION 'delivery lane identity is immutable';
            END IF;
            RETURN NEW;
        END; $$
    """)
    op.execute("""
        CREATE TRIGGER delivery_lanes_identity BEFORE UPDATE OR DELETE ON delivery_lanes
        FOR EACH ROW EXECUTE FUNCTION protect_delivery_lane()
    """)
    op.execute("GRANT SELECT, INSERT, UPDATE ON delivery_lanes TO updatis_runtime")


def downgrade() -> None:
    raise RuntimeError("v0.1-c does not provide destructive automatic downgrades")
