from __future__ import annotations

import time

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC


@pytest.mark.timeout(600)
@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_prefetch(neon_simple_env: NeonEnv):
    """
    Test resizing the Local File Cache
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "effective_io_concurrency=100",
            "shared_buffers=1MB",
            "max_parallel_workers_per_gather=0",
            "enable_bitmapscan=off",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("create extension neon")
    cur.execute("create table t(pk integer, sk integer, filler text default repeat('x',200))")
    cur.execute("insert into t values (generate_series(1,1000000),random()*1000000)")
    cur.execute("create index on t(sk)")
    cur.execute("vacuum t")

    # reset LFC
    cur.execute("alter system set neon.file_cache_size_limit=0")
    cur.execute("select pg_reload_conf()")
    time.sleep(1)
    cur.execute("alter system set neon.file_cache_size_limit='1GB'")
    cur.execute("select pg_reload_conf()")

    cur.execute(
        "explain (analyze,prefetch,format json) select sum(pk) from t where sk between 100000 and 100100"
    )
    prefetch_hits = cur.fetchall()[0][0][0]["Plan"]["Prefetch Hits"]
    log.info(f"Prefetch hists: {prefetch_hits}")

    cur.execute(
        "explain (analyze,prefetch,format json) select sum(pk) from t where sk between 100000 and 100100"
    )
    prefetch_hits = cur.fetchall()[0][0][0]["Plan"]["Prefetch Hits"]
    log.info(f"Prefetch hists: {prefetch_hits}")

    assert prefetch_hits > 0

    cur.execute("set neon.store_prefetch_result_in_lfc=on")

    cur.execute(
        "explain (analyze,prefetch,format json) select sum(pk) from t where sk between 100000 and 100100"
    )
    prefetch_hits = cur.fetchall()[0][0][0]["Plan"]["Prefetch Hits"]
    log.info(f"Prefetch hists: {prefetch_hits}")

    cur.execute(
        "explain (analyze,prefetch,format json) select sum(pk) from t where sk between 100000 and 100100"
    )
    prefetch_hits = cur.fetchall()[0][0][0]["Plan"]["Prefetch Hits"]
    log.info(f"Prefetch hists: {prefetch_hits}")

    assert prefetch_hits == 0
