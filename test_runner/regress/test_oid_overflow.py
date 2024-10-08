from fixtures.log_helper import log
from fixtures.neon_tenant import NeonTestTenant


def test_oid_overflow(neon_tenant: NeonTestTenant):
    endpoint = neon_tenant.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION neon_test_utils")

    cur.execute("CREATE TABLE t1(x integer)")
    cur.execute("INSERT INTO t1 values (1)")
    cur.execute("CREATE TABLE t2(x integer)")
    cur.execute("INSERT INTO t2 values (2)")

    cur.execute("SELECT x from t1")
    assert cur.fetchone() == (1,)
    cur.execute("SELECT x from t2")
    assert cur.fetchone() == (2,)

    cur.execute("VACUUM FULL t1")
    cur.execute("VACUUM FULL t1")
    cur.execute("vacuum pg_class")
    cur.execute("SELECT relfilenode FROM pg_class where relname='t1'")
    oid = cur.fetchall()[0][0]
    log.info(f"t1.relfilenode={oid}")

    cur.execute("set statement_timeout=0")
    cur.execute(f"select test_consume_oids({oid-1})")
    cur.execute("VACUUM FULL t2")

    cur.execute("SELECT relfilenode FROM pg_class where relname='t2'")
    oid = cur.fetchall()[0][0]
    log.info(f"t2.relfilenode={oid}")

    endpoint.clear_shared_buffers(cursor=cur)

    cur.execute("SELECT x from t1")
    assert cur.fetchone() == (1,)
    cur.execute("SELECT x from t2")
    assert cur.fetchone() == (2,)
