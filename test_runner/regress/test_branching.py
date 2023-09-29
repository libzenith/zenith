import random
import threading
import time
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
)
from fixtures.pageserver.utils import wait_until_tenant_active
from fixtures.types import Lsn, TimelineId
from fixtures.utils import query_scalar
from performance.test_perf_pgbench import get_scales_matrix
from requests import ReadTimeout


# Test branch creation
#
# This test spawns pgbench in a thread in the background, and creates a branch while
# pgbench is running. Then it launches pgbench on the new branch, and creates another branch.
# Repeat `n_branches` times.
#
# If 'ty' == 'cascade', each branch is created from the previous branch, so that you end
# up with a branch of a branch of a branch ... of a branch. With 'ty' == 'flat',
# each branch is created from the root.
@pytest.mark.parametrize("n_branches", [10])
@pytest.mark.parametrize("scale", get_scales_matrix(1))
@pytest.mark.parametrize("ty", ["cascade", "flat"])
def test_branching_with_pgbench(
    neon_simple_env: NeonEnv, pg_bin: PgBin, n_branches: int, scale: int, ty: str
):
    env = neon_simple_env

    # Use aggressive GC and checkpoint settings, so that we also exercise GC during the test
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "gc_period": "5 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "5 s",
        }
    )

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")

        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", "-T15", connstr])

    env.neon_cli.create_branch("b0", tenant_id=tenant)
    endpoints: List[Endpoint] = []
    endpoints.append(env.endpoints.create_start("b0", tenant_id=tenant))

    threads: List[threading.Thread] = []
    threads.append(
        threading.Thread(target=run_pgbench, args=(endpoints[0].connstr(),), daemon=True)
    )
    threads[-1].start()

    thread_limit = 4

    for i in range(n_branches):
        # random a delay between [0, 5]
        delay = random.random() * 5
        time.sleep(delay)
        log.info(f"Sleep {delay}s")

        # If the number of concurrent threads exceeds a threshold, wait for
        # all the threads to finish before spawning a new one. Because the
        # regression tests in this directory are run concurrently in CI, we
        # want to avoid the situation that one test exhausts resources for
        # other tests.
        if len(threads) >= thread_limit:
            for thread in threads:
                thread.join()
            threads = []

        if ty == "cascade":
            env.neon_cli.create_branch("b{}".format(i + 1), "b{}".format(i), tenant_id=tenant)
        else:
            env.neon_cli.create_branch("b{}".format(i + 1), "b0", tenant_id=tenant)

        endpoints.append(env.endpoints.create_start("b{}".format(i + 1), tenant_id=tenant))

        threads.append(
            threading.Thread(target=run_pgbench, args=(endpoints[-1].connstr(),), daemon=True)
        )
        threads[-1].start()

    for thread in threads:
        thread.join()

    for ep in endpoints:
        res = ep.safe_psql("SELECT count(*) from pgbench_accounts")
        assert res[0] == (100000 * scale,)


# Test branching from an "unnormalized" LSN.
#
# Context:
# When doing basebackup for a newly created branch, pageserver generates
# 'pg_control' file to bootstrap WAL segment by specifying the redo position
# a "normalized" LSN based on the timeline's starting LSN:
#
# checkpoint.redo = normalize_lsn(self.lsn, pg_constants::WAL_SEGMENT_SIZE).0;
#
# This test checks if the pageserver is able to handle a "unnormalized" starting LSN.
#
# Related: see discussion in https://github.com/neondatabase/neon/pull/2143#issuecomment-1209092186
def test_branching_unnormalized_start_lsn(neon_simple_env: NeonEnv, pg_bin: PgBin):
    XLOG_BLCKSZ = 8192

    env = neon_simple_env

    env.neon_cli.create_branch("b0")
    endpoint0 = env.endpoints.create_start("b0")

    pg_bin.run_capture(["pgbench", "-i", endpoint0.connstr()])

    with endpoint0.cursor() as cur:
        curr_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # Specify the `start_lsn` as a number that is divided by `XLOG_BLCKSZ`
    # and is smaller than `curr_lsn`.
    start_lsn = Lsn((int(curr_lsn) - XLOG_BLCKSZ) // XLOG_BLCKSZ * XLOG_BLCKSZ)

    log.info(f"Branching b1 from b0 starting at lsn {start_lsn}...")
    env.neon_cli.create_branch("b1", "b0", ancestor_start_lsn=start_lsn)
    endpoint1 = env.endpoints.create_start("b1")

    pg_bin.run_capture(["pgbench", "-i", endpoint1.connstr()])


def test_cannot_create_endpoint_on_non_uploaded_timeline(neon_env_builder: NeonEnvBuilder):
    """
    Endpoint should not be possible to create because branch has not been uploaded.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.append(
        ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*"
    )
    ps_http = env.pageserver.http_client()

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    ps_http.tenant_create(env.initial_tenant)

    initial_branch = "initial_branch"

    with pytest.raises(ReadTimeout):
        ps_http.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline, timeout=2)

    env.neon_cli.map_branch(initial_branch, env.initial_tenant, env.initial_timeline)

    env.endpoints.create_start(initial_branch, tenant_id=env.initial_tenant)

    # FIXME: uploads bother shutdown
    env.pageserver.stop(immediate=True)

    raise RuntimeError(
        "above should had thrown, but of course did not because this is unimplemented"
    )


def test_cannot_branch_from_non_uploaded_branch(neon_env_builder: NeonEnvBuilder):
    """
    Branch should not be possible to create because ancestor has not been uploaded.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.append(
        ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*"
    )
    ps_http = env.pageserver.http_client()

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    ps_http.tenant_create(env.initial_tenant)

    with pytest.raises(ReadTimeout):
        ps_http.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline, timeout=2)

    branch_id = TimelineId.generate()

    with pytest.raises(ReadTimeout):
        # FIXME: this should be rejected somehow
        ps_http.timeline_create(
            env.pg_version,
            env.initial_tenant,
            branch_id,
            ancestor_timeline_id=env.initial_timeline,
            timeout=2
        )

    env.pageserver.stop(immediate=True)

    raise RuntimeError("was able to branch before ancestor has been uploaded")


def test_non_uploaded_branch_availability_after_restart(neon_env_builder: NeonEnvBuilder):
    """
    Currently before RFC#27 we keep and continue uploading branches which were not successfully uploaded before shutdown.

    This test likely duplicates some other test, but it's easier to write one than to make sure there will be a failing test when the rfc is implemented.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.append(
        ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*"
    )
    ps_http = env.pageserver.http_client()

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    ps_http.tenant_create(env.initial_tenant)

    with pytest.raises(ReadTimeout):
        ps_http.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline, timeout=2)

    env.pageserver.stop(immediate=True)

    # now without a failpoint
    env.pageserver.start()

    wait_until_tenant_active(ps_http, env.initial_tenant)

    # currently it lives on and will get eventually uploaded, but this will change
    detail = ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)
    assert detail is not None


def test_non_uploaded_branch_chain_availability_after_restart(neon_env_builder: NeonEnvBuilder):
    """
    Similar to test_non_uploaded_branch_availability_after_restart but create a chain of branches.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.append(
        ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*"
    )
    ps_http = env.pageserver.http_client()

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    ps_http.tenant_create(env.initial_tenant)

    with pytest.raises(ReadTimeout):
        ps_http.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline, timeout=2)

    second_id = TimelineId.generate()

    with pytest.raises(ReadTimeout):
        ps_http.timeline_create(env.pg_version, env.initial_tenant, second_id, ancestor_timeline_id=env.initial_timeline, timeout=2)

    # make sure it's readable now, the create just hangs
    detail = ps_http.timeline_detail(env.initial_tenant, second_id)

    env.pageserver.stop(immediate=True)

    # now without a failpoint
    env.pageserver.start()

    wait_until_tenant_active(ps_http, env.initial_tenant)

    # currently it lives on and will get eventually uploaded, but this will change to both returning 404
    for id in [second_id, env.initial_timeline]:
        detail = ps_http.timeline_detail(env.initial_tenant, id)
        assert detail is not None
