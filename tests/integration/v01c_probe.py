from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
import json
import sys
from uuid import UUID, uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from updatis.delivery.models import MAX_FENCING_GENERATION, LeaseOutcome
from updatis.delivery.repository import DeliveryLeaseRepository, _ACTIVE_LANE_LEASE, _LANE_CANDIDATES, _LOCKED_HEAD

PASSWORD = Path("/run/secrets/metadata_runtime_password").read_text().strip()
engine = create_engine(
    f"postgresql+psycopg://updatis_runtime:{PASSWORD}@metadata-db:5432/updatis_metadata",
    pool_pre_ping=True,
)
repository = DeliveryLeaseRepository(engine)


def seed_pipeline() -> tuple[UUID, UUID, UUID]:
    pipeline, epoch, revision = uuid4(), uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO pipelines(id,name,source_instance_id,source_stream_epoch)
            VALUES (:p,:name,'source',:epoch)
        """), {"p": pipeline, "name": f"p-{pipeline.hex}", "epoch": epoch})
        connection.execute(text("""
            INSERT INTO configuration_revisions
                (id,pipeline_id,revision,schema_version,document,content_hash)
            VALUES (:r,:p,1,1,'{}',:hash)
        """), {"r": revision, "p": pipeline, "hash": "a" * 64})
        connection.execute(text("""
            INSERT INTO stream_epochs
                (pipeline_id,stream_epoch,connector_name,publication_name,slot_name,
                 topic_prefix,consumer_group,snapshot_mode)
            VALUES (:p,:epoch,:connector,:publication,:slot,:prefix,:consumer,'initial')
        """), {"p": pipeline, "epoch": epoch, "connector": f"c-{epoch.hex}",
                 "publication": f"pub_{epoch.hex}", "slot": f"slot_{epoch.hex}",
                 "prefix": f"prefix-{epoch.hex}", "consumer": f"group-{epoch.hex}"})
    return pipeline, epoch, revision


def seed_delivery(pipeline: UUID, epoch: UUID, revision: UUID, offset: int,
                  partition: int = 0, topic: str = "source.orders") -> UUID:
    ingest = uuid4()
    event = f"{pipeline.hex}{partition:08x}{offset:024x}"[:64]
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO ingest_records
                (id,pipeline_id,source_stream_epoch,topic,partition,offset_value,
                 classification,raw_hash)
            VALUES (:id,:p,:epoch,:topic,:partition,:offset,'event',:hash)
        """), {"id": ingest, "p": pipeline, "epoch": epoch, "topic": topic,
                 "partition": partition, "offset": offset, "hash": "b" * 64})
        connection.execute(text("""
            INSERT INTO events
                (id,pipeline_id,ingest_record_id,configuration_revision_id,operation,
                 source_position,envelope,payload_hash)
            VALUES (:event,:p,:ingest,:revision,'create','{}','{}',:hash)
        """), {"event": event, "p": pipeline, "ingest": ingest,
                 "revision": revision, "hash": "c" * 64})
    return repository.materialize_delivery(
        event_id=event, configuration_revision_id=revision,
        max_attempts=5, max_age_seconds=3600,
    ).delivery_id


def force_release(delivery: UUID) -> None:
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE deliveries SET lease_owner=NULL,lease_expires_at=NULL WHERE id=:id"
        ), {"id": delivery})


def check_constraints(delivery: UUID) -> None:
    invalid_updates = (
        ("lease_owner='invalid',lease_expires_at=NULL", "deliveries_lease_pair"),
        ("lease_owner=NULL,lease_expires_at=clock_timestamp()", "deliveries_lease_pair"),
        ("fencing_generation=-1", "deliveries_fencing_generation_nonnegative"),
        ("fencing_generation=NULL", None),
        ("fencing_generation='invalid'", None),
        ("lease_owner='invalid',lease_expires_at=clock_timestamp(),fencing_generation=0", "deliveries_owned_generation_positive"),
    )
    for assignment, constraint in invalid_updates:
        try:
            with engine.begin() as connection:
                connection.execute(text(f"UPDATE deliveries SET {assignment} WHERE id=:id"), {"id": delivery})
        except DBAPIError as exc:
            assert exc.orig.sqlstate in ("23502", "23514", "22P02")
            if constraint:
                assert exc.orig.diag.constraint_name == constraint
        else:
            raise AssertionError(f"database accepted invalid lease: {assignment}")
    print("lease pair/generation database constraints passed")
    for statement in (
        "UPDATE deliveries SET source_offset=source_offset+1 WHERE id=:id",
        "UPDATE deliveries SET lane_id=gen_random_uuid() WHERE id=:id",
        "UPDATE delivery_lanes SET topic='changed' WHERE id=(SELECT lane_id FROM deliveries WHERE id=:id)",
    ):
        try:
            with engine.begin() as connection:
                connection.execute(text(statement), {"id": delivery})
        except DBAPIError as exc:
            assert exc.orig.sqlstate == "P0001"
        else:
            raise AssertionError("database accepted mutated ordering identity")
    print("immutable ordering coordinates and lane identity passed")


def plan_nodes(node: dict):
    yield node
    for child in node.get("Plans", []):
        yield from plan_nodes(child)


def check_late_materialization() -> None:
    p, e, r = seed_pipeline()
    later = seed_delivery(p, e, r, 11)
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1990-01-01' WHERE pipeline_id=:p"), {"p": p})
    original = repository.acquire_heads(owner_id="late-first", limit=1, candidate_limit=1).claims[0]
    assert original.delivery_id == later
    earlier = seed_delivery(p, e, r, 10)
    other = seed_delivery(p, e, r, 1, partition=1)
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1990-01-01' WHERE pipeline_id=:p"), {"p": p})
    with engine.connect() as connection:
        transaction = connection.begin()
        result = repository.acquire_heads(owner_id="late-second", limit=2, candidate_limit=2, connection=connection)
        transaction.commit()
    assert {claim.delivery_id for claim in result.claims} == {other}
    with engine.connect() as connection:
        assert connection.execute(text("SELECT lease_owner,lease_expires_at,fencing_generation FROM deliveries WHERE id=:id"), {"id": later}).one() == (original.owner_id, original.lease_expires_at, original.fencing_generation)
        assert connection.execute(text("SELECT count(*) FROM deliveries WHERE lane_id=(SELECT lane_id FROM deliveries WHERE id=:id) AND lease_owner IS NOT NULL AND lease_expires_at>clock_timestamp()"), {"id": later}).scalar_one() == 1
    with engine.begin() as connection:
        connection.execute(text("UPDATE deliveries SET lease_expires_at=clock_timestamp()+interval '5 seconds' WHERE id=:id"), {"id": later})
    assert repository.renew_claim(later, "late-first", original.fencing_generation).outcome is LeaseOutcome.RENEWED
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1989-01-01' WHERE id=(SELECT lane_id FROM deliveries WHERE id=:id)"), {"id": earlier})
    assert not repository.acquire_heads(owner_id="late-renewed", limit=1, candidate_limit=1).claims
    assert repository.complete_claim(later, "late-first", original.fencing_generation).outcome is LeaseOutcome.COMPLETED
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1989-01-01' WHERE id=(SELECT lane_id FROM deliveries WHERE id=:id)"), {"id": earlier})
    assert repository.acquire_heads(owner_id="late-third", limit=1, candidate_limit=1).claims[0].delivery_id == earlier
    print("late materialization: one active lane lease; independent lane progresses")


def check_lane_pagination() -> None:
    p, e, r = seed_pipeline()
    old = seed_delivery(p, e, r, 1)
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1980-01-01' WHERE pipeline_id=:p"), {"p": p})
    for _ in range(4):
        pn, en, rn = seed_pipeline()
        seed_delivery(pn, en, rn, 1)
        result = DeliveryLeaseRepository(engine).acquire_heads(owner_id="flood", limit=1, candidate_limit=1)
        if result.claims and result.claims[0].delivery_id == old:
            break
    else:
        raise AssertionError("older lane starved by newly inserted lanes")
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM delivery_lanes WHERE last_examined_at='-infinity' AND pipeline_id=:p"), {"p": p}).scalar_one() == 0
    # Equal timestamps are ordered by immutable lane ID.
    p2, e2, r2 = seed_pipeline()
    tie = [seed_delivery(p2, e2, r2, 1, partition=n) for n in range(3)]
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1970-01-01' WHERE pipeline_id=:p"), {"p": p2})
        expected = connection.execute(text("SELECT d.id FROM deliveries d JOIN delivery_lanes l ON l.id=d.lane_id WHERE l.pipeline_id=:p ORDER BY l.id"), {"p": p2}).scalars().all()
    assert set(expected) == set(tie)
    observed = [repository.acquire_heads(owner_id="tie", limit=1, candidate_limit=1).claims[0].delivery_id for _ in range(3)]
    assert observed == expected
    print("new-lane flood and stable equal-timestamp tie pagination passed")


def check_locked_front_lanes() -> None:
    p, e, r = seed_pipeline()
    deliveries = [seed_delivery(p, e, r, 1, partition=n) for n in range(4)]
    with engine.begin() as connection:
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1960-01-01' WHERE pipeline_id=:p"), {"p": p})
        ordered = connection.execute(text("SELECT d.id,l.id FROM deliveries d JOIN delivery_lanes l ON l.id=d.lane_id WHERE l.pipeline_id=:p ORDER BY l.id"), {"p": p}).all()
    assert {row[0] for row in ordered} == set(deliveries)
    held, release = Event(), Event()

    def hold_lanes() -> None:
        with engine.begin() as connection:
            repository._bound(connection)
            connection.execute(text("SELECT id FROM delivery_lanes WHERE id IN (:a,:b) FOR UPDATE"), {"a": ordered[0][1], "b": ordered[1][1]}).all()
            held.set()
            assert release.wait(8)

    with ThreadPoolExecutor(max_workers=1) as pool:
        holder = pool.submit(hold_lanes)
        try:
            assert held.wait(5)
            with engine.begin() as connection:
                page_plan = connection.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _LANE_CANDIDATES), {"candidate_limit": 2}).scalar_one()[0]["Plan"]
            result = repository.acquire_heads(owner_id="unlocked", limit=2, candidate_limit=2)
            assert {claim.delivery_id for claim in result.claims} == {ordered[2][0], ordered[3][0]}
            assert not holder.done()
        finally:
            release.set()
        holder.result(timeout=5)
    assert {claim.delivery_id for claim in repository.acquire_heads(owner_id="released", limit=2, candidate_limit=2).claims} == {ordered[0][0], ordered[1][0]}
    nodes = list(plan_nodes(page_plan))
    assert page_plan["Node Type"] == "Limit"
    assert any(n["Node Type"] == "LockRows" for n in nodes)
    assert any(n.get("Index Name") == "delivery_lanes_discovery_idx" for n in nodes)
    print("locked-front EXPLAIN: " + json.dumps(page_plan))
    print("locked front lanes skipped before page limit; released lanes reconsidered")


def check_large_backlog() -> None:
    p, e, r = seed_pipeline()
    first = seed_delivery(p, e, r, 0)
    # Bulk fixtures keep setup bounded while creating a substantial unresolved tail.
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO ingest_records(pipeline_id,source_stream_epoch,topic,partition,offset_value,classification,raw_hash)
            SELECT :p,:e,'source.orders',0,n,'event',repeat('b',64) FROM generate_series(1,20000) n
        """), {"p": p, "e": e})
        connection.execute(text("""
            INSERT INTO events(id,pipeline_id,ingest_record_id,configuration_revision_id,operation,source_position,envelope,payload_hash)
            SELECT md5(i.id::text)||md5(i.id::text),:p,i.id,:r,'create','{}','{}',repeat('c',64)
            FROM ingest_records i WHERE pipeline_id=:p AND offset_value>0
        """), {"p": p, "r": r})
        connection.execute(text("""
            INSERT INTO deliveries(pipeline_id,event_id,configuration_revision_id,state,max_attempts,max_age_seconds)
            SELECT :p,id,:r,'pending',5,3600 FROM events WHERE pipeline_id=:p
            ON CONFLICT(event_id,configuration_revision_id) DO NOTHING
        """), {"p": p, "r": r})
        # A large registry proves lane selection uses its ordered index too.
        connection.execute(text("""
            INSERT INTO delivery_lanes(pipeline_id,source_stream_epoch,topic,partition,last_examined_at)
            SELECT :p,:e,'fixture.empty',n,clock_timestamp() FROM generate_series(1,2000) n
        """), {"p": p, "e": e})
        # The two oldest candidates are this backlog and an empty fixture lane.
        # Earlier test leases may expire during bulk setup; they must not make
        # this access-path assertion depend on setup timing or random UUID order.
        connection.execute(text("""
            UPDATE delivery_lanes SET last_examined_at='1950-01-01'
            WHERE pipeline_id=:p AND topic='fixture.empty' AND partition=1
        """), {"p": p})
        lane = connection.execute(text("SELECT lane_id FROM deliveries WHERE id=:id"), {"id": first}).scalar_one()
        connection.execute(text("UPDATE delivery_lanes SET last_examined_at='1940-01-01' WHERE id=:lane"), {"lane": lane})
        lane_plan = connection.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _LANE_CANDIDATES), {"candidate_limit": 2}).scalar_one()[0]["Plan"]
        head_plan = connection.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _LOCKED_HEAD), {"lane": lane}).scalar_one()[0]["Plan"]
        active_plan = connection.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _ACTIVE_LANE_LEASE), {"lane": lane}).scalar_one()[0]["Plan"]
        lane_nodes, head_nodes = list(plan_nodes(lane_plan)), list(plan_nodes(head_plan))
        assert any(n.get("Index Name") == "delivery_lanes_discovery_idx" for n in lane_nodes)
        assert any(n.get("Index Name") == "deliveries_lane_head_idx" for n in head_nodes)
        assert not any(n["Node Type"] in ("Sort", "WindowAgg") for n in head_nodes)
        assert not any(n["Node Type"] in ("WindowAgg", "Seq Scan") for n in lane_nodes)
        assert not any(n["Node Type"] == "Seq Scan" for n in head_nodes)
        discovery = next(n for n in lane_nodes if n.get("Index Name") == "delivery_lanes_discovery_idx")
        assert discovery["Actual Rows"] <= 2
        head_index = next(n for n in head_nodes if n.get("Index Name") == "deliveries_lane_head_idx")
        assert head_index["Actual Rows"] == 1
        assert lane_plan["Node Type"] == "Limit" and lane_plan["Actual Rows"] <= 2
        assert any(n["Node Type"] == "LockRows" for n in lane_nodes)
        assert any(n.get("Index Name") == "deliveries_lane_active_lease_idx" for n in plan_nodes(active_plan))
        assert head_plan["Actual Rows"] == 1
        print("large-backlog EXPLAIN: " + json.dumps({"lanes": lane_plan, "head": head_plan, "active_lease": active_plan}))
    result = repository.acquire_heads(owner_id="large-backlog", limit=1, candidate_limit=2)
    assert [claim.delivery_id for claim in result.claims] == [first]
    print("20,001 unresolved deliveries, 2,000 extra lanes, candidate limit 2/head limit 1 passed")


def main() -> None:
    # Lowest head, revision continuity, and partition/pipeline independence.
    p1, epoch1, r1 = seed_pipeline()
    d10 = seed_delivery(p1, epoch1, r1, 10)
    d11 = seed_delivery(p1, epoch1, r1, 11)
    r2 = uuid4()
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO configuration_revisions
                (id,pipeline_id,revision,schema_version,document,content_hash)
            VALUES (:r,:p,2,1,'{}',:hash)
        """), {"r": r2, "p": p1, "hash": "d" * 64})
    d12 = seed_delivery(p1, epoch1, r2, 12)
    d20 = seed_delivery(p1, epoch1, r1, 20, partition=1)
    p2, epoch2, rp2 = seed_pipeline()
    dp2 = seed_delivery(p2, epoch2, rp2, 1)
    first = repository.acquire_heads(owner_id="worker-a", limit=10)
    assert first.outcome is LeaseOutcome.ACQUIRED
    assert {claim.delivery_id for claim in first.claims} == {d10, d20, dp2}
    assert repository.acquire_heads(owner_id="worker-a", limit=10).outcome is LeaseOutcome.NO_ELIGIBLE_WORK

    # Renewal is threshold-bound, generation-preserving, and fenced.
    head = next(claim for claim in first.claims if claim.delivery_id == d10)
    assert repository.renew_claim(d10, "worker-a", head.fencing_generation).outcome is LeaseOutcome.INVALID_REQUEST
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE deliveries SET lease_expires_at=clock_timestamp()+interval '5 seconds' WHERE id=:id"
        ), {"id": d10})
    renewed = repository.renew_claim(d10, "worker-a", head.fencing_generation)
    assert renewed.outcome is LeaseOutcome.RENEWED
    assert renewed.claim and renewed.claim.fencing_generation == head.fencing_generation
    assert repository.renew_claim(d10, "other", head.fencing_generation).outcome is LeaseOutcome.STALE
    assert repository.renew_claim(d10, "worker-a", head.fencing_generation + 1).outcome is LeaseOutcome.STALE
    assert repository.complete_claim(d10, "other", head.fencing_generation).outcome is LeaseOutcome.STALE
    assert repository.complete_claim(d10, "worker-a", head.fencing_generation + 1).outcome is LeaseOutcome.STALE
    with engine.connect() as connection:
        unrelated_before = connection.execute(text("SELECT * FROM deliveries WHERE id=:id"), {"id": d20}).one()
    assert repository.complete_claim(d10, "worker-a", head.fencing_generation).outcome is LeaseOutcome.COMPLETED
    with engine.connect() as connection:
        assert connection.execute(text("SELECT * FROM deliveries WHERE id=:id"), {"id": d20}).one() == unrelated_before
    with engine.connect() as connection:
        state = connection.execute(text(
            "SELECT state,lease_owner,lease_expires_at FROM deliveries WHERE id=:id"
        ), {"id": d10}).one()
    assert state == ("pending", None, None)
    assert repository.acquire_heads(owner_id="worker-b", limit=1).claims[0].delivery_id == d10

    # Expiry permits reacquisition and increments the durable fence; expired owners lose mutation rights.
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=:id"
        ), {"id": d10})
    assert repository.renew_claim(d10, "worker-b", head.fencing_generation + 1).outcome is LeaseOutcome.EXPIRED
    assert repository.complete_claim(d10, "worker-b", head.fencing_generation + 1).outcome is LeaseOutcome.EXPIRED
    reacquired = repository.acquire_heads(owner_id="worker-b", limit=1).claims[0]
    assert reacquired.delivery_id == d10 and reacquired.fencing_generation == head.fencing_generation + 2

    # A terminal predecessor permits the next unresolved item; revisions do not create lanes.
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE deliveries SET state='terminal',lease_owner=NULL,lease_expires_at=NULL WHERE id=:id"
        ), {"id": d10})
    second = repository.acquire_heads(owner_id="worker-d", limit=1).claims[0]
    assert second.delivery_id == d11
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE deliveries SET state='terminal',lease_owner=NULL,lease_expires_at=NULL WHERE id IN (:a,:b)"
        ), {"a": d11, "b": d20})
    assert repository.acquire_heads(owner_id="worker-e", limit=1).claims[0].delivery_id == d12

    # Hold a real row lock through a synchronized second-connection acquisition.
    pc, ec, rc = seed_pipeline()
    contested = seed_delivery(pc, ec, rc, 1)
    successor = seed_delivery(pc, ec, rc, 2)
    independent = seed_delivery(pc, ec, rc, 1, partition=1)
    held, release = Event(), Event()

    def hold_head() -> None:
        with engine.begin() as connection:
            repository._bound(connection)
            connection.execute(text("SELECT id FROM deliveries WHERE id=:id FOR UPDATE"), {"id": contested})
            held.set()
            assert release.wait(8), "second worker did not finish before lock deadline"

    with ThreadPoolExecutor(max_workers=1) as pool:
        holder = pool.submit(hold_head)
        try:
            assert held.wait(5), "head lock was not acquired"
            result = repository.acquire_heads(owner_id="racer-2", limit=100)
            ids = {claim.delivery_id for claim in result.claims}
            assert contested not in ids and successor not in ids
            assert independent in ids
            assert not holder.done(), "transactions did not overlap"
        finally:
            release.set()
        holder.result(timeout=5)
    after_lock = repository.acquire_heads(owner_id="after-lock", limit=100)
    assert {claim.delivery_id for claim in after_lock.claims} == {contested}
    restart_view = DeliveryLeaseRepository(engine).acquire_heads(owner_id="restarted", limit=100)
    assert contested not in {claim.delivery_id for claim in restart_view.claims}
    print("locked-head concurrency: held Event, independent connection, release Event passed")

    # A caller-owned transaction grants nothing after rollback.
    pr, er, rr = seed_pipeline()
    rollback_delivery = seed_delivery(pr, er, rr, 1)
    with engine.connect() as connection:
        transaction = connection.begin()
        claim = repository.acquire_heads(owner_id="rollback", limit=1, connection=connection)
        assert claim.claims[0].delivery_id == rollback_delivery
        transaction.rollback()
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT lease_owner,fencing_generation FROM deliveries WHERE id=:id"
        ), {"id": rollback_delivery}).one() == (None, 0)
    restarted_repository = DeliveryLeaseRepository(engine)
    assert restarted_repository.acquire_heads(owner_id="after-restart", limit=1).claims[0].delivery_id == rollback_delivery

    # A correlatable earlier discontinuity blocks its lane and fabricates no delivery.
    pg, eg, rg = seed_pipeline()
    later = seed_delivery(pg, eg, rg, 8)
    ingest = uuid4()
    discontinuity_id = "e" * 64
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO ingest_records
                (id,pipeline_id,source_stream_epoch,topic,partition,offset_value,
                 classification,raw_hash,raw_key_length)
            VALUES (:id,:p,:epoch,'source.orders',0,7,'quarantine',:hash,0)
        """), {"id": ingest, "p": pg, "epoch": eg, "hash": "f" * 64})
        connection.execute(text("""
            INSERT INTO intake_discontinuities
                (id,pipeline_id,source_stream_epoch,ingest_record_id,topic,partition,
                 offset_value,kind,reason_code,error_summary,raw_hash,raw_key_length,
                 raw_value_length,raw_key_truncated,raw_value_truncated,normalizer_version)
            VALUES (:did,:p,:epoch,:ingest,'source.orders',0,7,'malformed','invalid','invalid',
                    :hash,0,NULL,false,false,1)
        """), {"did": discontinuity_id, "p": pg, "epoch": eg,
                 "ingest": ingest, "hash": "f" * 64})
    blocked = repository.acquire_heads(owner_id="blocked", limit=100)
    assert later not in {claim.delivery_id for claim in blocked.claims}
    with engine.connect() as connection:
        assert connection.execute(text("""
            SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id
            JOIN ingest_records i ON i.id=e.ingest_record_id
            WHERE i.classification <> 'event'
        """)).scalar_one() == 0

    # A positive batch limit is an exact upper bound even with more eligible lanes.
    for _ in range(3):
        pb, eb, rb = seed_pipeline()
        seed_delivery(pb, eb, rb, 1)
    assert len(repository.acquire_heads(owner_id="bounded", limit=2).claims) == 2

    # Attempting/retryable predecessor remains the true unresolved head.
    pt, et, rt = seed_pipeline()
    retry_head = seed_delivery(pt, et, rt, 1)
    retry_next = seed_delivery(pt, et, rt, 2)
    with engine.begin() as connection:
        connection.execute(text("UPDATE deliveries SET state='attempting',next_attempt_at=clock_timestamp()+interval '1 hour' WHERE id=:id"), {"id": retry_head})
        connection.execute(text("""
            INSERT INTO delivery_attempts(pipeline_id,delivery_id,attempt_number,outcome)
            VALUES (:p,:id,1,'retryable')
        """), {"p": pt, "id": retry_head})
    retry_claims = repository.acquire_heads(owner_id="retry-head", limit=100).claims
    assert retry_head in {claim.delivery_id for claim in retry_claims}
    assert retry_next not in {claim.delivery_id for claim in retry_claims}
    assert retry_next not in {claim.delivery_id for claim in repository.acquire_heads(owner_id="retry-next", limit=100).claims}

    for invalid in (0, -1, 101, True):
        assert repository.acquire_heads(owner_id="bounds", limit=invalid).outcome is LeaseOutcome.INVALID_REQUEST
        assert repository.acquire_heads(owner_id="bounds", limit=1, candidate_limit=invalid).outcome is LeaseOutcome.INVALID_REQUEST
    po, eo, ro = seed_pipeline()
    overflow = seed_delivery(po, eo, ro, 1)
    healthy = seed_delivery(po, eo, ro, 1, partition=1)
    with engine.begin() as connection:
        connection.execute(text("UPDATE deliveries SET fencing_generation=:maximum WHERE id=:id"),
                           {"maximum": MAX_FENCING_GENERATION, "id": overflow})
        before = connection.execute(text("SELECT * FROM deliveries WHERE id=:id"), {"id": overflow}).one()
    mixed = repository.acquire_heads(owner_id="overflow", limit=100)
    assert mixed.outcome is LeaseOutcome.PARTIAL
    assert healthy in {claim.delivery_id for claim in mixed.claims}
    assert overflow not in {claim.delivery_id for claim in mixed.claims}
    assert len(mixed.failures) == 1 and mixed.failures[0].delivery_id == overflow
    assert mixed.failures[0].pipeline_id == po and mixed.failures[0].partition == 0
    with engine.connect() as connection:
        assert connection.execute(text("SELECT * FROM deliveries WHERE id=:id"), {"id": overflow}).one() == before

    check_constraints(overflow)
    check_large_backlog()
    check_late_materialization()
    check_lane_pagination()
    check_locked_front_lanes()

    print("v0.1-c/01G PostgreSQL lease probe passed")


if __name__ == "__main__":
    if "--outage" in sys.argv:
        repository.acquire_heads(owner_id="outage", limit=1)
        raise AssertionError("metadata outage unexpectedly returned an acquisition result")
    else:
        main()
