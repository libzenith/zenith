from __future__ import annotations

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fixtures.common_types import Lsn, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import wait_until_tenant_active
from fixtures.utils import query_scalar
from performance.test_perf_pgbench import get_scales_matrix
from requests import RequestException


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
    tenant, _ = env.create_tenant(
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

        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", "-T15", connstr])

    env.create_branch("b0", tenant_id=tenant)
    endpoints: list[Endpoint] = []
    endpoints.append(env.endpoints.create_start("b0", tenant_id=tenant))

    threads: list[threading.Thread] = []
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
            env.create_branch(f"b{i + 1}", ancestor_branch_name=f"b{i}", tenant_id=tenant)
        else:
            env.create_branch(f"b{i + 1}", ancestor_branch_name="b0", tenant_id=tenant)

        endpoints.append(env.endpoints.create_start(f"b{i + 1}", tenant_id=tenant))

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

    env.create_branch("b0")
    endpoint0 = env.endpoints.create_start("b0")

    pg_bin.run_capture(["pgbench", "-i", endpoint0.connstr()])

    with endpoint0.cursor() as cur:
        curr_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # Specify the `start_lsn` as a number that is divided by `XLOG_BLCKSZ`
    # and is smaller than `curr_lsn`.
    start_lsn = Lsn((int(curr_lsn) - XLOG_BLCKSZ) // XLOG_BLCKSZ * XLOG_BLCKSZ)

    log.info(f"Branching b1 from b0 starting at lsn {start_lsn}...")
    env.create_branch("b1", ancestor_branch_name="b0", ancestor_start_lsn=start_lsn)
    endpoint1 = env.endpoints.create_start("b1")

    pg_bin.run_capture(["pgbench", "-i", endpoint1.connstr()])


def test_cannot_create_endpoint_on_non_uploaded_timeline(neon_env_builder: NeonEnvBuilder):
    """
    Endpoint should not be possible to create because branch has not been uploaded.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.extend(
        [
            ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*",
            ".*page_service_conn_main.*: query handler for 'basebackup .* ERROR: Not found: Timeline",
        ]
    )
    ps_http = env.pageserver.http_client()

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    env.pageserver.tenant_create(env.initial_tenant)

    initial_branch = "initial_branch"

    def start_creating_timeline():
        with pytest.raises(RequestException):
            ps_http.timeline_create(
                env.pg_version, env.initial_tenant, env.initial_timeline, timeout=60
            )

    t = threading.Thread(target=start_creating_timeline)
    try:
        t.start()

        wait_until_paused(env, "before-upload-index-pausable")

        env.neon_cli.mappings_map_branch(initial_branch, env.initial_tenant, env.initial_timeline)

        with pytest.raises(RuntimeError, match="ERROR: Not found: Timeline"):
            env.endpoints.create_start(
                initial_branch, tenant_id=env.initial_tenant, basebackup_request_tries=2
            )
        ps_http.configure_failpoints(("before-upload-index-pausable", "off"))
    finally:
        env.pageserver.stop(immediate=True)

        t.join()


def test_cannot_branch_from_non_uploaded_branch(neon_env_builder: NeonEnvBuilder):
    """
    Branch should not be possible to create because ancestor has not been uploaded.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.extend(
        [
            ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*",
            ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: .*Cannot branch off the timeline that's not present in pageserver.*",
        ]
    )
    ps_http = env.pageserver.http_client()

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    env.pageserver.tenant_create(env.initial_tenant)

    def start_creating_timeline():
        with pytest.raises(RequestException):
            ps_http.timeline_create(
                env.pg_version, env.initial_tenant, env.initial_timeline, timeout=60
            )

    t = threading.Thread(target=start_creating_timeline)
    try:
        t.start()

        wait_until_paused(env, "before-upload-index-pausable")

        branch_id = TimelineId.generate()

        with pytest.raises(
            PageserverApiException,
            match="Cannot branch off the timeline that's not present in pageserver",
        ):
            ps_http.timeline_create(
                env.pg_version,
                env.initial_tenant,
                branch_id,
                ancestor_timeline_id=env.initial_timeline,
            )

        with pytest.raises(
            PageserverApiException,
            match=f"NotFound: Timeline {env.initial_tenant}/{branch_id} was not found",
        ):
            ps_http.timeline_detail(env.initial_tenant, branch_id)
            # important to note that a task might still be in progress to complete
            # the work, but will never get to that because we have the pause
            # failpoint
    finally:
        # FIXME: paused uploads bother shutdown
        env.pageserver.stop(immediate=True)

        t.join()


def test_non_uploaded_root_timeline_is_deleted_after_restart(neon_env_builder: NeonEnvBuilder):
    """
    Check that a timeline is deleted locally on subsequent restart if it never successfully uploaded during creation.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.extend(
        [
            ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*",
            ".*Failed to load index_part from remote storage.*",
            # On a fast restart, there may be an initdb still running in a basebackup...__temp directory
            ".*Failed to purge.*Directory not empty.*",
        ]
    )
    ps_http = env.pageserver.http_client()

    env.pageserver.tenant_create(env.initial_tenant)

    # Create a timeline whose creation will succeed.  The tenant will need at least one
    # timeline to be loadable.
    success_timeline = TimelineId.generate()
    log.info(f"Creating timeline {success_timeline}")
    ps_http.timeline_create(env.pg_version, env.initial_tenant, success_timeline, timeout=60)

    # Create a timeline whose upload to remote storage will be blocked
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))

    def start_creating_timeline():
        log.info(f"Creating (expect failure) timeline {env.initial_timeline}")
        with pytest.raises(RequestException):
            ps_http.timeline_create(
                env.pg_version, env.initial_tenant, env.initial_timeline, timeout=60
            )

    t = threading.Thread(target=start_creating_timeline)
    try:
        t.start()

        wait_until_paused(env, "before-upload-index-pausable")
    finally:
        # FIXME: paused uploads bother shutdown
        env.pageserver.stop(immediate=True)
        t.join()

    # now without a failpoint
    env.pageserver.start()

    wait_until_tenant_active(ps_http, env.initial_tenant)

    with pytest.raises(PageserverApiException, match="not found"):
        ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)

    # The one successfully created timeline should still be there.
    assert len(ps_http.timeline_list(tenant_id=env.initial_tenant)) == 1


def test_non_uploaded_branch_is_deleted_after_restart(neon_env_builder: NeonEnvBuilder):
    """
    Check that a timeline is deleted locally on subsequent restart if it never successfully uploaded during creation.
    """

    env = neon_env_builder.init_configs()
    env.start()

    env.pageserver.allowed_errors.append(
        ".*request{method=POST path=/v1/tenant/.*/timeline request_id=.*}: request was dropped before completing.*"
    )
    ps_http = env.pageserver.http_client()

    env.pageserver.tenant_create(env.initial_tenant)
    ps_http.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline)

    # pause all uploads
    ps_http.configure_failpoints(("before-upload-index-pausable", "pause"))
    branch_id = TimelineId.generate()

    def start_creating_timeline():
        with pytest.raises(RequestException):
            ps_http.timeline_create(
                env.pg_version,
                env.initial_tenant,
                branch_id,
                ancestor_timeline_id=env.initial_timeline,
                timeout=60,
            )

    t = threading.Thread(target=start_creating_timeline)
    try:
        t.start()

        wait_until_paused(env, "before-upload-index-pausable")
    finally:
        # FIXME: paused uploads bother shutdown
        env.pageserver.stop(immediate=True)
        t.join()

    # now without a failpoint
    env.pageserver.start()

    wait_until_tenant_active(ps_http, env.initial_tenant)

    ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)

    with pytest.raises(PageserverApiException, match="not found"):
        ps_http.timeline_detail(env.initial_tenant, branch_id)


def test_duplicate_creation(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_configs()
    env.start()
    env.pageserver.tenant_create(env.initial_tenant)

    success_timeline = TimelineId.generate()
    log.info(f"Creating timeline {success_timeline}")
    ps_http = env.pageserver.http_client()
    success_result = ps_http.timeline_create(
        env.pg_version, env.initial_tenant, success_timeline, timeout=60
    )

    ps_http.configure_failpoints(("timeline-creation-after-uninit", "pause"))

    def start_creating_timeline():
        log.info(f"Creating (expect failure) timeline {env.initial_timeline}")
        with pytest.raises(RequestException):
            ps_http.timeline_create(
                env.pg_version, env.initial_tenant, env.initial_timeline, timeout=60
            )

    t = threading.Thread(target=start_creating_timeline)
    try:
        t.start()

        wait_until_paused(env, "timeline-creation-after-uninit")

        # While timeline creation is in progress, trying to create a timeline
        # again with the same ID should return 409
        with pytest.raises(
            PageserverApiException, match="creation of timeline with the given ID is in progress"
        ):
            ps_http.timeline_create(
                env.pg_version, env.initial_tenant, env.initial_timeline, timeout=60
            )

        # Creation of a timeline already successfully created is idempotent, and is not impeded by some
        # other timeline creation with a different TimelineId being stuck.
        repeat_result = ps_http.timeline_create(
            env.pg_version, env.initial_tenant, success_timeline, timeout=60
        )
        # remote_consistent_lsn_visible will be published only after we've
        # confirmed the generation, which is not part of what we await during
        # timeline creation (uploads). mask it out here to avoid flakyness.
        del success_result["remote_consistent_lsn_visible"]
        del repeat_result["remote_consistent_lsn_visible"]
        assert repeat_result == success_result
    finally:
        env.pageserver.stop(immediate=True)
        t.join()

    # now without a failpoint
    env.pageserver.start()

    wait_until_tenant_active(ps_http, env.initial_tenant)

    with pytest.raises(PageserverApiException, match="not found"):
        ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)

    # The one successfully created timeline should still be there.
    assert len(ps_http.timeline_list(tenant_id=env.initial_tenant)) == 1


def test_branching_while_stuck_find_gc_cutoffs(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start(initial_tenant_conf={"lsn_lease_length": "0s"})

    client = env.pageserver.http_client()

    failpoint = "Timeline::find_gc_cutoffs-pausable"

    client.configure_failpoints((failpoint, "pause"))

    with ThreadPoolExecutor(max_workers=1) as exec:
        completion = exec.submit(client.timeline_gc, env.initial_tenant, env.initial_timeline, None)

        wait_until_paused(env, failpoint)

        env.create_branch("branch", ancestor_branch_name="main")

        client.configure_failpoints((failpoint, "off"))

        completion.result()


def wait_until_paused(env: NeonEnv, failpoint: str):
    found = False
    msg = f"at failpoint {failpoint}"
    for _ in range(20):
        time.sleep(1)
        found = env.pageserver.log_contains(msg) is not None
        if found:
            break
    assert found
