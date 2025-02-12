from __future__ import annotations

import concurrent.futures
import os
import threading
import time
from contextlib import closing
from datetime import datetime
from itertools import chain
from pathlib import Path

import pytest
import requests
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.metrics import (
    PAGESERVER_GLOBAL_METRICS,
    PAGESERVER_PER_TENANT_METRICS,
    parse_metrics,
)
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import timeline_delete_wait_completed, wait_until_tenant_active
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import RemoteStorageKind
from fixtures.utils import wait_until
from prometheus_client.samples import Sample


def test_tenant_creation_fails(neon_simple_env: NeonEnv):
    tenants_dir = neon_simple_env.pageserver.tenant_dir()
    initial_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.tenant_list().stdout.splitlines())
    )
    [d for d in tenants_dir.iterdir()]

    error_regexes = [".*tenant-config-before-write.*"]
    neon_simple_env.pageserver.allowed_errors.extend(error_regexes)
    neon_simple_env.storage_controller.allowed_errors.extend(error_regexes)

    pageserver_http = neon_simple_env.pageserver.http_client()

    # Failure to write a config to local disk makes the pageserver assume that local disk is bad and abort the process
    pageserver_http.configure_failpoints(("tenant-config-before-write", "return"))

    tenant_id = TenantId.generate()

    with pytest.raises(requests.exceptions.ConnectionError, match="Connection aborted"):
        neon_simple_env.pageserver.http_client().tenant_attach(tenant_id=tenant_id, generation=1)

    # Any files left behind on disk during failed creation do not prevent
    # a retry from succeeding.  Restart pageserver with no failpoints.
    neon_simple_env.pageserver.running = False
    neon_simple_env.pageserver.start()

    # The failed creation should not be present in list of tenants, as when we start up we'll see
    # an empty tenant dir with no config in it.
    neon_simple_env.pageserver.allowed_errors.append(".*Failed to load tenant config.*")
    new_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.tenant_list().stdout.splitlines())
    )
    assert initial_tenants == new_tenants, "should not create new tenants"

    neon_simple_env.create_tenant()


def test_tenants_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    """Tests tenants with and without wal acceptors"""
    tenant_1, _ = env.create_tenant()
    tenant_2, _ = env.create_tenant()

    env.create_timeline("test_tenants_normal_work", tenant_id=tenant_1)
    env.create_timeline("test_tenants_normal_work", tenant_id=tenant_2)

    endpoint_tenant1 = env.endpoints.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_1,
    )
    endpoint_tenant2 = env.endpoints.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_2,
    )

    for endpoint in [endpoint_tenant1, endpoint_tenant2]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)


def test_metrics_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.pageserver_config_override = "availability_zone='test_ps_az'"

    env = neon_env_builder.init_start()
    tenant_1, _ = env.create_tenant()
    tenant_2, _ = env.create_tenant()

    timeline_1 = env.create_timeline("test_metrics_normal_work", tenant_id=tenant_1)
    timeline_2 = env.create_timeline("test_metrics_normal_work", tenant_id=tenant_2)

    endpoint_tenant1 = env.endpoints.create_start("test_metrics_normal_work", tenant_id=tenant_1)
    endpoint_tenant2 = env.endpoints.create_start("test_metrics_normal_work", tenant_id=tenant_2)

    for endpoint in [endpoint_tenant1, endpoint_tenant2]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)

    collected_metrics = {
        "pageserver": env.pageserver.http_client().get_metrics_str(),
    }
    for sk in env.safekeepers:
        collected_metrics[f"safekeeper{sk.id}"] = sk.http_client().get_metrics_str()

    for name in collected_metrics:
        basepath = os.path.join(neon_env_builder.repo_dir, f"{name}.metrics")

        with open(basepath, "w") as stdout_f:
            print(collected_metrics[name], file=stdout_f, flush=True)

    all_metrics = [parse_metrics(m, name) for name, m in collected_metrics.items()]
    ps_metrics = all_metrics[0]
    sk_metrics = all_metrics[1:]

    # Find all metrics among all safekeepers, accepts the same arguments as query_all()
    def query_all_safekeepers(name, filter):
        return list(
            chain.from_iterable(
                map(
                    lambda sk: sk.query_all(name, filter),
                    sk_metrics,
                )
            )
        )

    ttids = [
        {"tenant_id": str(tenant_1), "timeline_id": str(timeline_1)},
        {"tenant_id": str(tenant_2), "timeline_id": str(timeline_2)},
    ]

    # Test metrics per timeline
    for tt in ttids:
        log.info(f"Checking metrics for {tt}")

        ps_lsn = Lsn(int(ps_metrics.query_one("pageserver_last_record_lsn", filter=tt).value))
        sk_lsns = [
            Lsn(int(sk.query_one("safekeeper_commit_lsn", filter=tt).value)) for sk in sk_metrics
        ]

        log.info(f"ps_lsn: {ps_lsn}")
        log.info(f"sk_lsns: {sk_lsns}")

        assert ps_lsn <= max(sk_lsns)
        assert ps_lsn > Lsn(0)

    # Test common metrics
    for metrics in all_metrics:
        log.info(f"Checking common metrics for {metrics.name}")

        log.info(
            f"process_cpu_seconds_total: {metrics.query_one('process_cpu_seconds_total').value}"
        )
        log.info(f"process_threads: {int(metrics.query_one('process_threads').value)}")
        log.info(
            f"process_resident_memory_bytes (MB): {metrics.query_one('process_resident_memory_bytes').value / 1024 / 1024}"
        )
        log.info(
            f"process_virtual_memory_bytes (MB): {metrics.query_one('process_virtual_memory_bytes').value / 1024 / 1024}"
        )
        log.info(f"process_open_fds: {int(metrics.query_one('process_open_fds').value)}")
        log.info(f"process_max_fds: {int(metrics.query_one('process_max_fds').value)}")
        log.info(
            f"process_start_time_seconds (UTC): {datetime.fromtimestamp(metrics.query_one('process_start_time_seconds').value)}"
        )

    for io_direction in ["read", "write"]:
        # Querying all metrics for number of bytes read/written by pageserver in another AZ
        io_metrics = query_all_safekeepers(
            "safekeeper_pg_io_bytes_total",
            {
                "app_name": f"pageserver-{env.pageserver.id}",
                "client_az": "test_ps_az",
                "dir": io_direction,
                "same_az": "false",
            },
        )
        total_bytes = sum(int(metric.value) for metric in io_metrics)
        log.info(f"Pageserver {io_direction} bytes from another AZ: {total_bytes}")
        # We expect some bytes to be read/written, to make sure metrics are working
        assert total_bytes > 0

    # Test (a subset of) safekeeper global metrics
    for sk_m in sk_metrics:
        # Test that every safekeeper has read some bytes
        assert any(
            map(
                lambda x: x.value > 0,
                sk_m.query_all("safekeeper_pg_io_bytes_total", {"dir": "read"}),
            )
        ), f"{sk_m.name} has not read bytes"

        # Test that every safekeeper has written some bytes
        assert any(
            map(
                lambda x: x.value > 0,
                sk_m.query_all("safekeeper_pg_io_bytes_total", {"dir": "write"}),
            )
        ), f"{sk_m.name} has not written bytes"

    # Test (a subset of) pageserver global metrics
    for metric in PAGESERVER_GLOBAL_METRICS:
        if metric.startswith("pageserver_remote"):
            continue

        ps_samples = ps_metrics.query_all(metric, {})
        assert len(ps_samples) > 0, f"expected at least one sample for {metric}"
        for sample in ps_samples:
            labels = ",".join([f'{key}="{value}"' for key, value in sample.labels.items()])
            log.info(f"{sample.name}{{{labels}}} {sample.value}")

    # Test that we gather tenant operations metrics
    storage_operation_metrics = [
        "pageserver_storage_operations_seconds_global_bucket",
        "pageserver_storage_operations_seconds_global_sum",
        "pageserver_storage_operations_seconds_global_count",
    ]
    for metric in storage_operation_metrics:
        value = ps_metrics.query_all(metric, filter={"operation": "layer flush"})
        assert value


def test_pageserver_metrics_removed_after_detach(neon_env_builder: NeonEnvBuilder):
    """Tests that when a tenant is detached, the tenant specific metrics are not left behind"""

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.create_tenant()
    tenant_2, _ = env.create_tenant()

    env.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_1)
    env.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_2)

    endpoint_tenant1 = env.endpoints.create_start(
        "test_metrics_removed_after_detach", tenant_id=tenant_1
    )
    endpoint_tenant2 = env.endpoints.create_start(
        "test_metrics_removed_after_detach", tenant_id=tenant_2
    )

    for endpoint in [endpoint_tenant1, endpoint_tenant2]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)
        endpoint.stop()

    def get_ps_metric_samples_for_tenant(tenant_id: TenantId) -> list[Sample]:
        ps_metrics = env.pageserver.http_client().get_metrics()
        samples = []
        for metric_name in ps_metrics.metrics:
            for sample in ps_metrics.query_all(
                name=metric_name, filter={"tenant_id": str(tenant_id)}
            ):
                samples.append(sample)
        return samples

    for tenant in [tenant_1, tenant_2]:
        pre_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        expected = set(PAGESERVER_PER_TENANT_METRICS)
        assert pre_detach_samples == expected

        env.pageserver.http_client().tenant_detach(tenant)

        post_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        assert post_detach_samples == set()


def test_pageserver_with_empty_tenants(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(
        [
            ".*load failed.*list timelines directory.*",
        ]
    )

    client = env.pageserver.http_client()

    tenant_with_empty_timelines = env.initial_tenant
    timeline_delete_wait_completed(client, tenant_with_empty_timelines, env.initial_timeline)

    files_in_timelines_dir = sum(
        1 for _p in Path.iterdir(env.pageserver.timeline_dir(tenant_with_empty_timelines))
    )
    assert (
        files_in_timelines_dir == 0
    ), f"Tenant {tenant_with_empty_timelines} should have an empty timelines/ directory"

    # Trigger timeline re-initialization after pageserver restart
    env.endpoints.stop_all()
    env.pageserver.stop()

    env.pageserver.start()

    client = env.pageserver.http_client()

    def not_attaching():
        tenants = client.tenant_list()
        assert len(tenants) == 1
        assert all(t["state"]["slug"] != "Attaching" for t in tenants)

    wait_until(not_attaching)

    tenants = client.tenant_list()

    [loaded_tenant] = [t for t in tenants if t["id"] == str(tenant_with_empty_timelines)]
    assert (
        loaded_tenant["state"]["slug"] == "Active"
    ), "Tenant {tenant_with_empty_timelines} with empty timelines dir should be active and ready for timeline creation"

    loaded_tenant_status = client.tenant_status(tenant_with_empty_timelines)
    assert (
        loaded_tenant_status["state"]["slug"] == "Active"
    ), f"Tenant {tenant_with_empty_timelines} without timelines dir should be active"

    time.sleep(1)  # to allow metrics propagation

    ps_metrics = client.get_metrics()
    active_tenants_metric_filter = {
        "state": "Active",
    }

    tenant_active_count = int(
        ps_metrics.query_one(
            "pageserver_tenant_states_count", filter=active_tenants_metric_filter
        ).value
    )

    assert (
        tenant_active_count == 1
    ), f"Tenant {tenant_with_empty_timelines} should have metric as active"


def test_create_churn_during_restart(neon_env_builder: NeonEnvBuilder):
    """
    Probabilistic stress test for the pageserver's handling of tenant requests
    across a restart. This is intended to catch things like:
    - Bad response codes during shutdown (e.g. returning 500 instead of 503)
    - Issues where a tenant is still starting up while we receive a request for it
    - Issues with interrupting/resuming tenant/timeline creation in shutdown
    - Issues with a timeline is not created successfully because of restart.
    """
    env = neon_env_builder.init_configs()
    env.start()
    tenant_id: TenantId = env.initial_tenant
    timeline_id = env.initial_timeline

    # At this point, the initial tenant/timeline might not have been created successfully,
    # and this is the case we want to test.

    # Multiple creation requests which race will generate this error on the pageserver
    # and storage controller respectively
    env.pageserver.allowed_errors.append(".*Conflict: Tenant is already being modified.*")
    env.storage_controller.allowed_errors.append(".*Conflict: Tenant is already being modified.*")

    # Tenant creation requests which arrive out of order will generate complaints about
    # generation nubmers out of order.
    env.pageserver.allowed_errors.append(".*Generation .+ is less than existing .+")

    # Timeline::flush_and_shutdown cannot tell if it is hitting a failure because of
    # an incomplete attach, or some other problem.  In the field this should be rare,
    # so we allow it to log at WARN, even if it is occasionally a false positive.
    env.pageserver.allowed_errors.append(".*failed to freeze and flush.*")

    def create_bg(delay_ms):
        time.sleep(delay_ms / 1000.0)
        try:
            env.pageserver.tenant_create(tenant_id=tenant_id)
            env.pageserver.http_client().timeline_create(
                PgVersion.NOT_SET, tenant_id, new_timeline_id=timeline_id
            )
        except PageserverApiException as e:
            if e.status_code == 409:
                log.info(f"delay_ms={delay_ms} 409")
                pass
            elif e.status_code == 429:
                log.info(f"delay_ms={delay_ms} 429")
                pass
            elif e.status_code == 400:
                if "is less than existing" in e.message:
                    # We send creation requests very close together in time: it is expected that these
                    # race, and sometimes chigher-generation'd requests arrive first.  The pageserver rightly
                    # rejects any attempt to make a generation number go backwards.
                    pass
                else:
                    raise
            else:
                raise
        except requests.exceptions.ConnectionError:
            # Our requests might arrive during shutdown and be cut off at the transport level
            pass

    for _ in range(0, 10):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futs = []
            for delay_ms in (0, 1, 10, 50, 100, 200, 500, 800):
                f = executor.submit(create_bg, delay_ms)
                futs.append(f)
            env.pageserver.stop()
            env.pageserver.start()

            for f in futs:
                f.result(timeout=30)

    # The tenant should end up active
    wait_until_tenant_active(env.pageserver.http_client(), tenant_id, iterations=10, period=1)


def test_pageserver_metrics_many_relations(neon_env_builder: NeonEnvBuilder):
    """Test for the directory_entries_count metric"""

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    endpoint_tenant = env.endpoints.create_start("main", tenant_id=env.initial_tenant)

    # Not sure why but this many tables creates more relations than our limit
    TABLE_COUNT = 1600
    COUNT_AT_LEAST_EXPECTED = 5500

    with endpoint_tenant.connect() as conn:
        with conn.cursor() as cur:
            # Wrapping begin; commit; around this and the loop below keeps the reproduction
            # but it also doesn't have a performance benefit
            cur.execute("CREATE TABLE template_tbl(key int primary key, value text);")
            for i in range(TABLE_COUNT):
                cur.execute(f"CREATE TABLE tbl_{i}(like template_tbl INCLUDING ALL);")
    wait_for_last_flush_lsn(env, endpoint_tenant, env.initial_tenant, env.initial_timeline)
    endpoint_tenant.stop()

    m = ps_http.get_metrics()
    directory_entries_count_metric = m.query_all(
        "pageserver_directory_entries_count", {"tenant_id": str(env.initial_tenant)}
    )

    def only_int(samples: list[Sample]) -> int:
        assert len(samples) == 1
        return int(samples[0].value)

    directory_entries_count = only_int(directory_entries_count_metric)

    log.info(f"pageserver_directory_entries_count metric value: {directory_entries_count}")

    assert directory_entries_count > COUNT_AT_LEAST_EXPECTED

    timeline_detail = ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)

    counts = timeline_detail["directory_entries_counts"]
    assert counts
    log.info(f"directory counts: {counts}")
    # We need to add up reldir v1 + v2 counts
    assert counts[2] + counts[7] > COUNT_AT_LEAST_EXPECTED


def test_timelines_parallel_endpoints(neon_simple_env: NeonEnv):
    """
    (Relaxed) regression test for issue that led to https://github.com/neondatabase/neon/pull/9268
    Create many endpoints in parallel and then restart them
    """
    env = neon_simple_env

    # This param needs to be 200+ to reproduce the limit issue
    n_threads = 16
    barrier = threading.Barrier(n_threads)

    def test_timeline(branch_name: str, timeline_id: TimelineId, endpoint: Endpoint):
        endpoint.start()
        endpoint.stop()
        # Use a barrier to make sure we restart endpoints at the same time
        barrier.wait()
        endpoint.start()

    workers = []

    for i in range(0, n_threads):
        branch_name = f"branch_{i}"
        timeline_id = env.create_branch(branch_name)
        endpoint = env.endpoints.create(branch_name)
        w = threading.Thread(target=test_timeline, args=[branch_name, timeline_id, endpoint])
        workers.append(w)

    # Only start the restarts once we're done creating all timelines & endpoints
    for w in workers:
        w.start()

    for w in workers:
        w.join()
