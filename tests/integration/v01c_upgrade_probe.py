"""Disposable metadata-only upgrade proof with valid pre-0003 deliveries."""
from alembic import command
from sqlalchemy import text

from updatis.db.migrate import build_alembic_config
from updatis.config.loader import load_runtime_config
from updatis.config.secrets import SecretResolver
from updatis.db.connection import create_metadata_engine


def main() -> None:
    path = "/etc/updatis/runtime.json"
    runtime = load_runtime_config(path)
    engine = create_metadata_engine(runtime.metadata, SecretResolver(runtime.secrets_directory))
    config = build_alembic_config(path)
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0002_durable_intake")
        # No stream_epochs row is required by the valid 0002 delivery contract.
        connection.execute(text("""
            INSERT INTO pipelines(id,name,source_instance_id,source_stream_epoch)
            VALUES ('00000000-0000-0000-0000-000000000001','upgrade-proof','source',
                    '00000000-0000-0000-0000-000000000002');
            INSERT INTO configuration_revisions(id,pipeline_id,revision,schema_version,document,content_hash)
            VALUES ('00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000001',1,1,'{}',repeat('a',64));
            INSERT INTO ingest_records(id,pipeline_id,source_stream_epoch,topic,partition,offset_value,classification,raw_hash)
            VALUES ('00000000-0000-0000-0000-000000000004','00000000-0000-0000-0000-000000000001',
                    '00000000-0000-0000-0000-000000000002','upgrade.orders',0,7,'event',repeat('b',64));
            INSERT INTO events(id,pipeline_id,ingest_record_id,configuration_revision_id,operation,source_position,envelope,payload_hash)
            VALUES (repeat('a',64),'00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000004',
                    '00000000-0000-0000-0000-000000000003','create','{}','{}',repeat('c',64));
            INSERT INTO deliveries(pipeline_id,event_id,configuration_revision_id,state,max_attempts,max_age_seconds)
            VALUES ('00000000-0000-0000-0000-000000000001',repeat('a',64),
                    '00000000-0000-0000-0000-000000000003','pending',5,3600);
        """))
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "0004_delivery_lanes")
        assert connection.execute(text("SELECT last_examined_at::text FROM delivery_lanes")).scalar_one() == "-infinity"
        command.upgrade(config, "head")
        row = connection.execute(text("""
            SELECT d.source_offset,d.lease_owner,d.lease_expires_at,d.fencing_generation,
                   l.topic,l.partition,d.state
            FROM deliveries d JOIN delivery_lanes l ON l.id=d.lane_id
            WHERE d.event_id=repeat('a',64)
        """)).one()
        assert row == (7, None, None, 0, "upgrade.orders", 0, "pending")
        assert connection.execute(text("SELECT last_examined_at::text FROM delivery_lanes")).scalar_one() != "-infinity"
        assert connection.execute(text("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid='delivery_lanes'::regclass AND adnum=(SELECT attnum FROM pg_attribute WHERE attrelid='delivery_lanes'::regclass AND attname='last_examined_at')")).scalar_one() == "clock_timestamp()"
        # Only fixture state; no product terminal-transition behavior.
        connection.execute(text("UPDATE deliveries SET state='terminal' WHERE event_id=repeat('a',64)"))
    engine.dispose()
    print("0002 pre-existing delivery upgrade through 0003, 0004, and 0005 passed")


if __name__ == "__main__":
    main()
