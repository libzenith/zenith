import os
import time
from collections import defaultdict
from typing import Dict, List, Optional, Union

import pytest
import requests
from fixtures.compute_reconfigure import ComputeReconfigure
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    StorageControllerApiException,
    tenant_get_shards,
)
from fixtures.remote_storage import s3_storage
from fixtures.types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.utils import wait_until
from fixtures.workload import Workload
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


def test_sharding_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basic lifecycle of a sharded tenant:
     - ingested data gets split up
     - page service reads
     - timeline creation and deletion
     - splits
    """

    shard_count = 4
    neon_env_builder.num_pageservers = shard_count

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 128

    # Use S3-compatible remote storage so that we can scrub: this test validates
    # that the scrubber doesn't barf when it sees a sharded tenant.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()

    neon_env_builder.preserve_database_files = True

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant

    pageservers = dict((int(p.id), p) for p in env.pageservers)
    shards = env.storage_controller.locate(tenant_id)

    def get_sizes():
        sizes = {}
        for shard in shards:
            node_id = int(shard["node_id"])
            pageserver = pageservers[node_id]
            sizes[node_id] = pageserver.http_client().tenant_status(shard["shard_id"])[
                "current_physical_size"
            ]
        log.info(f"sizes = {sizes}")
        return sizes

    # Test that timeline creation works on a sharded tenant
    timeline_b = env.neon_cli.create_branch("branch_b", tenant_id=tenant_id)

    # Test that we can write data to a sharded tenant
    workload = Workload(env, tenant_id, timeline_b, branch_name="branch_b")
    workload.init()

    sizes_before = get_sizes()
    workload.write_rows(256)

    # Test that we can read data back from a sharded tenant
    workload.validate()

    # Validate that the data is spread across pageservers
    sizes_after = get_sizes()
    # Our sizes increased when we wrote data
    assert sum(sizes_after.values()) > sum(sizes_before.values())
    # That increase is present on all shards
    assert all(sizes_after[ps.id] > sizes_before[ps.id] for ps in env.pageservers)

    # Validate that timeline list API works properly on all shards
    for shard in shards:
        node_id = int(shard["node_id"])
        pageserver = pageservers[node_id]
        timelines = set(
            TimelineId(tl["timeline_id"])
            for tl in pageserver.http_client().timeline_list(shard["shard_id"])
        )
        assert timelines == {env.initial_timeline, timeline_b}

    env.storage_controller.consistency_check()


def test_sharding_split_unsharded(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test that shard splitting works on a tenant created as unsharded (i.e. with
    ShardCount(0)).
    """
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Check that we created with an unsharded TenantShardId: this is the default,
    # but check it in case we change the default in future
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 0)) is not None

    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()
    workload.write_rows(256)
    workload.validate()

    # Split one shard into two
    env.storage_controller.tenant_shard_split(tenant_id, shard_count=2)

    # Check we got the shard IDs we expected
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 2)) is not None
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 1, 2)) is not None

    workload.validate()

    env.storage_controller.consistency_check()


def test_sharding_split_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basics of shard splitting:
    - The API results in more shards than we started with
    - The tenant's data remains readable

    """

    # We will start with 4 shards and split into 8, then migrate all those
    # 8 shards onto separate pageservers
    shard_count = 4
    split_shard_count = 8
    neon_env_builder.num_pageservers = split_shard_count

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 128

    # Use S3-compatible remote storage so that we can scrub: this test validates
    # that the scrubber doesn't barf when it sees a sharded tenant.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()

    neon_env_builder.preserve_database_files = True

    non_default_tenant_config = {"gc_horizon": 77 * 1024 * 1024}

    env = neon_env_builder.init_configs(True)
    neon_env_builder.start()
    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.neon_cli.create_tenant(
        tenant_id,
        timeline_id,
        shard_count=shard_count,
        shard_stripe_size=stripe_size,
        placement_policy='{"Attached": 1}',
        conf=non_default_tenant_config,
    )
    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()

    # Initial data
    workload.write_rows(256)

    # Note which pageservers initially hold a shard after tenant creation
    pre_split_pageserver_ids = [loc["node_id"] for loc in env.storage_controller.locate(tenant_id)]

    # For pageservers holding a shard, validate their ingest statistics
    # reflect a proper splitting of the WAL.
    for pageserver in env.pageservers:
        if pageserver.id not in pre_split_pageserver_ids:
            continue

        metrics = pageserver.http_client().get_metrics_values(
            [
                "pageserver_wal_ingest_records_received_total",
                "pageserver_wal_ingest_records_committed_total",
                "pageserver_wal_ingest_records_filtered_total",
            ]
        )

        log.info(f"Pageserver {pageserver.id} metrics: {metrics}")

        # Not everything received was committed
        assert (
            metrics["pageserver_wal_ingest_records_received_total"]
            > metrics["pageserver_wal_ingest_records_committed_total"]
        )

        # Something was committed
        assert metrics["pageserver_wal_ingest_records_committed_total"] > 0

        # Counts are self consistent
        assert (
            metrics["pageserver_wal_ingest_records_received_total"]
            == metrics["pageserver_wal_ingest_records_committed_total"]
            + metrics["pageserver_wal_ingest_records_filtered_total"]
        )

    # TODO: validate that shards have different sizes

    workload.validate()

    assert len(pre_split_pageserver_ids) == 4

    def shards_on_disk(shard_ids):
        for pageserver in env.pageservers:
            for shard_id in shard_ids:
                if pageserver.tenant_dir(shard_id).exists():
                    return True

        return False

    old_shard_ids = [TenantShardId(tenant_id, i, shard_count) for i in range(0, shard_count)]
    # Before split, old shards exist
    assert shards_on_disk(old_shard_ids)

    # Before split, we have done one reconcile for each shard
    assert (
        env.storage_controller.get_metric_value(
            "storage_controller_reconcile_complete_total", filter={"status": "ok"}
        )
        == shard_count
    )

    env.storage_controller.tenant_shard_split(tenant_id, shard_count=split_shard_count)

    post_split_pageserver_ids = [loc["node_id"] for loc in env.storage_controller.locate(tenant_id)]
    # We should have split into 8 shards, on the same 4 pageservers we started on.
    assert len(post_split_pageserver_ids) == split_shard_count
    assert len(set(post_split_pageserver_ids)) == shard_count
    assert set(post_split_pageserver_ids) == set(pre_split_pageserver_ids)

    # The old parent shards should no longer exist on disk
    assert not shards_on_disk(old_shard_ids)

    workload.validate()

    workload.churn_rows(256)

    workload.validate()

    # Run GC on all new shards, to check they don't barf or delete anything that breaks reads
    # (compaction was already run as part of churn_rows)
    all_shards = tenant_get_shards(env, tenant_id)
    for tenant_shard_id, pageserver in all_shards:
        pageserver.http_client().timeline_gc(tenant_shard_id, timeline_id, None)
    workload.validate()

    migrate_to_pageserver_ids = list(
        set(p.id for p in env.pageservers) - set(pre_split_pageserver_ids)
    )
    assert len(migrate_to_pageserver_ids) == split_shard_count - shard_count

    # Migrate shards away from the node where the split happened
    for ps_id in pre_split_pageserver_ids:
        shards_here = [
            tenant_shard_id
            for (tenant_shard_id, pageserver) in all_shards
            if pageserver.id == ps_id
        ]
        assert len(shards_here) == 2
        migrate_shard = shards_here[0]
        destination = migrate_to_pageserver_ids.pop()

        log.info(f"Migrating shard {migrate_shard} from {ps_id} to {destination}")
        env.storage_controller.tenant_shard_migrate(migrate_shard, destination)

    workload.validate()

    # Assert on how many reconciles happened during the process.  This is something of an
    # implementation detail, but it is useful to detect any bugs that might generate spurious
    # extra reconcile iterations.
    #
    # We'll have:
    # - shard_count reconciles for the original setup of the tenant
    # - shard_count reconciles for detaching the original secondary locations during split
    # - split_shard_count reconciles during shard splitting, for setting up secondaries.
    # - shard_count reconciles for the migrations we did to move child shards away from their split location
    expect_reconciles = shard_count * 2 + split_shard_count + shard_count
    reconcile_ok = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )
    assert reconcile_ok == expect_reconciles

    # Check that no cancelled or errored reconciliations occurred: this test does no
    # failure injection and should run clean.
    cancelled_reconciles = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "cancel"}
    )
    errored_reconciles = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "error"}
    )
    assert cancelled_reconciles is not None and int(cancelled_reconciles) == 0
    assert errored_reconciles is not None and int(errored_reconciles) == 0

    env.storage_controller.consistency_check()

    def get_node_shard_counts(env: NeonEnv, tenant_ids):
        total: defaultdict[int, int] = defaultdict(int)
        attached: defaultdict[int, int] = defaultdict(int)
        for tid in tenant_ids:
            for shard in env.storage_controller.tenant_describe(tid)["shards"]:
                log.info(
                    f"{shard['tenant_shard_id']}: attached={shard['node_attached']}, secondary={shard['node_secondary']} "
                )
                for node in shard["node_secondary"]:
                    total[int(node)] += 1
                attached[int(shard["node_attached"])] += 1
                total[int(shard["node_attached"])] += 1

        return total, attached

    def check_effective_tenant_config():
        # Expect our custom tenant configs to have survived the split
        for shard in env.storage_controller.tenant_describe(tenant_id)["shards"]:
            node = env.get_pageserver(int(shard["node_attached"]))
            config = node.http_client().tenant_config(TenantShardId.parse(shard["tenant_shard_id"]))
            for k, v in non_default_tenant_config.items():
                assert config.effective_config[k] == v

    # Validate pageserver state: expect every child shard to have an attached and secondary location
    (total, attached) = get_node_shard_counts(env, tenant_ids=[tenant_id])
    assert sum(attached.values()) == split_shard_count
    assert sum(total.values()) == split_shard_count * 2
    check_effective_tenant_config()

    # Ensure post-split pageserver locations survive a restart (i.e. the child shards
    # correctly wrote config to disk, and the storage controller responds correctly
    # to /re-attach)
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    # Validate pageserver state: expect every child shard to have an attached and secondary location
    (total, attached) = get_node_shard_counts(env, tenant_ids=[tenant_id])
    assert sum(attached.values()) == split_shard_count
    assert sum(total.values()) == split_shard_count * 2
    check_effective_tenant_config()

    workload.validate()


@pytest.mark.parametrize("initial_stripe_size", [None, 65536])
def test_sharding_split_stripe_size(
    neon_env_builder: NeonEnvBuilder,
    httpserver: HTTPServer,
    httpserver_listen_address,
    initial_stripe_size: int,
):
    """
    Check that modifying stripe size inline with a shard split works as expected
    """
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"
    neon_env_builder.num_pageservers = 1

    # Set up fake HTTP notify endpoint: we will use this to validate that we receive
    # the correct stripe size after split.
    notifications = []

    def handler(request: Request):
        log.info(f"Notify request: {request}")
        notifications.append(request.json)
        return Response(status=200)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=1, initial_tenant_shard_stripe_size=initial_stripe_size
    )
    tenant_id = env.initial_tenant

    assert len(notifications) == 1
    expect: Dict[str, Union[List[Dict[str, int]], str, None, int]] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
    }
    assert notifications[0] == expect

    new_stripe_size = 2048
    env.storage_controller.tenant_shard_split(
        tenant_id, shard_count=2, shard_stripe_size=new_stripe_size
    )

    # Check that we ended up with the stripe size that we expected, both on the pageserver
    # and in the notifications to compute
    assert len(notifications) == 2
    expect_after: Dict[str, Union[List[Dict[str, int]], str, None, int]] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": new_stripe_size,
        "shards": [
            {"node_id": int(env.pageservers[0].id), "shard_number": 0},
            {"node_id": int(env.pageservers[0].id), "shard_number": 1},
        ],
    }
    log.info(f"Got notification: {notifications[1]}")
    assert notifications[1] == expect_after

    # Inspect the stripe size on the pageserver
    shard_0_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 0, 2))
    )
    assert shard_0_loc["shard_stripe_size"] == new_stripe_size
    shard_1_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 1, 2))
    )
    assert shard_1_loc["shard_stripe_size"] == new_stripe_size

    # Ensure stripe size survives a pageserver restart
    env.pageservers[0].stop()
    env.pageservers[0].start()
    shard_0_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 0, 2))
    )
    assert shard_0_loc["shard_stripe_size"] == new_stripe_size
    shard_1_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 1, 2))
    )
    assert shard_1_loc["shard_stripe_size"] == new_stripe_size

    # Ensure stripe size survives a storage controller restart
    env.storage_controller.stop()
    env.storage_controller.start()

    def assert_restart_notification():
        assert len(notifications) == 3
        assert notifications[2] == expect_after

    wait_until(10, 1, assert_restart_notification)


@pytest.mark.skipif(
    # The quantity of data isn't huge, but debug can be _very_ slow, and the things we're
    # validating in this test don't benefit much from debug assertions.
    os.getenv("BUILD_TYPE") == "debug",
    reason="Avoid running bulkier ingest tests in debug mode",
)
def test_sharding_ingest(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Check behaviors related to ingest:
    - That we generate properly sized layers
    - TODO: that updates to remote_consistent_lsn are made correctly via safekeepers
    """

    # Set a small stripe size and checkpoint distance, so that we can exercise rolling logic
    # without writing a lot of data.
    expect_layer_size = 131072
    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{expect_layer_size}",
        "compaction_target_size": f"{expect_layer_size}",
    }
    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
        # A stripe size the same order of magnitude as layer size: this ensures that
        # within checkpoint_distance some shards will have no data to ingest, if LSN
        # contains sequential page writes.  This test checks that this kind of
        # scenario doesn't result in some shards emitting empty/tiny layers.
        initial_tenant_shard_stripe_size=expect_layer_size // 8192,
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.validate()

    small_layer_count = 0
    ok_layer_count = 0
    huge_layer_count = 0

    # Inspect the resulting layer map, count how many layers are undersized.
    for shard in env.storage_controller.locate(tenant_id):
        pageserver = env.get_pageserver(shard["node_id"])
        shard_id = shard["shard_id"]
        layer_map = pageserver.http_client().layer_map_info(shard_id, timeline_id)

        for layer in layer_map.historic_layers:
            assert layer.layer_file_size is not None
            if layer.layer_file_size < expect_layer_size // 2:
                classification = "Small"
                small_layer_count += 1
            elif layer.layer_file_size > expect_layer_size * 2:
                classification = "Huge "
                huge_layer_count += 1
            else:
                classification = "OK   "
                ok_layer_count += 1

            if layer.kind == "Delta":
                assert layer.lsn_end is not None
                lsn_size = Lsn(layer.lsn_end) - Lsn(layer.lsn_start)
            else:
                lsn_size = 0

            log.info(
                f"{classification} layer[{pageserver.id}]: {layer.layer_file_name} (size {layer.layer_file_size}, LSN distance {lsn_size})"
            )

    # Why an inexact check?
    # - Because we roll layers on checkpoint_distance * shard_count, we expect to obey the target
    #   layer size on average, but it is still possible to write some tiny layers.
    log.info(f"Totals: {small_layer_count} small layers, {ok_layer_count} ok layers")
    if small_layer_count <= shard_count:
        # If each shard has <= 1 small layer
        pass
    else:
        # General case:
        assert float(small_layer_count) / float(ok_layer_count) < 0.25

    # Each shard may emit up to one huge layer, because initdb ingest doesn't respect checkpoint_distance.
    assert huge_layer_count <= shard_count


class Failure:
    pageserver_id: Optional[int]

    def apply(self, env: NeonEnv):
        raise NotImplementedError()

    def clear(self, env: NeonEnv):
        """
        Clear the failure, in a way that should enable the system to proceed
        to a totally clean state (all nodes online and reconciled)
        """
        raise NotImplementedError()

    def expect_available(self):
        raise NotImplementedError()

    def can_mitigate(self):
        """Whether Self.mitigate is available for use"""
        return False

    def mitigate(self, env: NeonEnv):
        """
        Mitigate the failure in a way that should allow shard split to
        complete and service to resume, but does not guarantee to leave
        the whole world in a clean state (e.g. an Offline node might have
        junk LocationConfigs on it)
        """
        raise NotImplementedError()

    def fails_forward(self, env: NeonEnv):
        """
        If true, this failure results in a state that eventualy completes the split.
        """
        return False

    def expect_exception(self):
        """
        How do we expect a call to the split API to fail?
        """
        return StorageControllerApiException


class PageserverFailpoint(Failure):
    def __init__(self, failpoint, pageserver_id, mitigate):
        self.failpoint = failpoint
        self.pageserver_id = pageserver_id
        self._mitigate = mitigate

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.allowed_errors.extend(
            [".*failpoint.*", ".*Resetting.*after shard split failure.*"]
        )
        pageserver.http_client().configure_failpoints((self.failpoint, "return(1)"))

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "off"))
        if self._mitigate:
            env.storage_controller.node_configure(self.pageserver_id, {"availability": "Active"})

    def expect_available(self):
        return True

    def can_mitigate(self):
        return self._mitigate

    def mitigate(self, env):
        env.storage_controller.node_configure(self.pageserver_id, {"availability": "Offline"})


class StorageControllerFailpoint(Failure):
    def __init__(self, failpoint, action):
        self.failpoint = failpoint
        self.pageserver_id = None
        self.action = action

    def apply(self, env: NeonEnv):
        env.storage_controller.configure_failpoints((self.failpoint, self.action))

    def clear(self, env: NeonEnv):
        if "panic" in self.action:
            log.info("Restarting storage controller after panic")
            env.storage_controller.stop()
            env.storage_controller.start()
        else:
            env.storage_controller.configure_failpoints((self.failpoint, "off"))

    def expect_available(self):
        # Controller panics _do_ leave pageservers available, but our test code relies
        # on using the locate API to update configurations in Workload, so we must skip
        # these actions when the controller has been panicked.
        return "panic" not in self.action

    def can_mitigate(self):
        return False

    def fails_forward(self, env):
        # Edge case: the very last failpoint that simulates a DB connection error, where
        # the abort path will fail-forward and result in a complete split.
        fail_forward = self.failpoint == "shard-split-post-complete"

        # If the failure was a panic, then if we expect split to eventually (after restart)
        # complete, we must restart before checking that.
        if fail_forward and "panic" in self.action:
            log.info("Restarting storage controller after panic")
            env.storage_controller.stop()
            env.storage_controller.start()

        return fail_forward

    def expect_exception(self):
        if "panic" in self.action:
            return requests.exceptions.ConnectionError
        else:
            return StorageControllerApiException


class NodeKill(Failure):
    def __init__(self, pageserver_id, mitigate):
        self.pageserver_id = pageserver_id
        self._mitigate = mitigate

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.stop(immediate=True)

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.start()

    def expect_available(self):
        return False

    def mitigate(self, env):
        env.storage_controller.node_configure(self.pageserver_id, {"availability": "Offline"})


class CompositeFailure(Failure):
    """
    Wrapper for failures in multiple components (e.g. a failpoint in the storage controller, *and*
    stop a pageserver to interfere with rollback)
    """

    def __init__(self, failures: list[Failure]):
        self.failures = failures

        self.pageserver_id = None
        for f in failures:
            if f.pageserver_id is not None:
                self.pageserver_id = f.pageserver_id
                break

    def apply(self, env: NeonEnv):
        for f in self.failures:
            f.apply(env)

    def clear(self, env):
        for f in self.failures:
            f.clear(env)

    def expect_available(self):
        return all(f.expect_available() for f in self.failures)

    def mitigate(self, env):
        for f in self.failures:
            f.mitigate(env)

    def expect_exception(self):
        expect = set(f.expect_exception() for f in self.failures)

        # We can't give a sensible response if our failures have different expectations
        assert len(expect) == 1

        return list(expect)[0]


@pytest.mark.parametrize(
    "failure",
    [
        PageserverFailpoint("api-500", 1, False),
        NodeKill(1, False),
        PageserverFailpoint("api-500", 1, True),
        NodeKill(1, True),
        PageserverFailpoint("shard-split-pre-prepare", 1, False),
        PageserverFailpoint("shard-split-post-prepare", 1, False),
        PageserverFailpoint("shard-split-pre-hardlink", 1, False),
        PageserverFailpoint("shard-split-post-hardlink", 1, False),
        PageserverFailpoint("shard-split-post-child-conf", 1, False),
        PageserverFailpoint("shard-split-lsn-wait", 1, False),
        PageserverFailpoint("shard-split-pre-finish", 1, False),
        StorageControllerFailpoint("shard-split-validation", "return(1)"),
        StorageControllerFailpoint("shard-split-post-begin", "return(1)"),
        StorageControllerFailpoint("shard-split-post-remote", "return(1)"),
        StorageControllerFailpoint("shard-split-post-complete", "return(1)"),
        StorageControllerFailpoint("shard-split-validation", "panic(failpoint)"),
        StorageControllerFailpoint("shard-split-post-begin", "panic(failpoint)"),
        StorageControllerFailpoint("shard-split-post-remote", "panic(failpoint)"),
        StorageControllerFailpoint("shard-split-post-complete", "panic(failpoint)"),
        CompositeFailure(
            [NodeKill(1, True), StorageControllerFailpoint("shard-split-post-begin", "return(1)")]
        ),
        CompositeFailure(
            [NodeKill(1, False), StorageControllerFailpoint("shard-split-post-begin", "return(1)")]
        ),
    ],
)
def test_sharding_split_failures(
    neon_env_builder: NeonEnvBuilder,
    compute_reconfigure_listener: ComputeReconfigure,
    failure: Failure,
):
    neon_env_builder.num_pageservers = 4
    neon_env_builder.control_plane_compute_hook_api = (
        compute_reconfigure_listener.control_plane_compute_hook_api
    )
    initial_shard_count = 2
    split_shard_count = 4

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()

    # Create a tenant with secondary locations enabled
    env.neon_cli.create_tenant(
        tenant_id, timeline_id, shard_count=initial_shard_count, placement_policy='{"Attached":1}'
    )

    env.storage_controller.allowed_errors.extend(
        [
            # All split failures log a warning when then enqueue the abort operation
            ".*Enqueuing background abort.*",
            # We exercise failure cases where abort itself will also fail (node offline)
            ".*abort_tenant_shard_split.*",
            ".*Failed to abort.*",
            # Tolerate any error lots that mention a failpoint
            ".*failpoint.*",
            # Node offline cases will fail to send requests
            ".*Reconcile error: receive body: error sending request for url.*",
            # Node offline cases will fail inside reconciler when detaching secondaries
            ".*Reconcile error on shard.*: receive body: error sending request for url.*",
        ]
    )

    for ps in env.pageservers:
        # When we do node failures and abandon a shard, it will de-facto have old generation and
        # thereby be unable to publish remote consistent LSN updates
        ps.allowed_errors.append(".*Dropped remote consistent LSN updates.*")

        # If we're using a failure that will panic the storage controller, all background
        # upcalls from the pageserver can fail
        ps.allowed_errors.append(".*calling control plane generation validation API failed.*")

    # Make sure the node we're failing has a shard on it, otherwise the test isn't testing anything
    assert (
        failure.pageserver_id is None
        or len(
            env.get_pageserver(failure.pageserver_id)
            .http_client()
            .tenant_list_locations()["tenant_shards"]
        )
        > 0
    )

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(100)

    # Put the environment into a failing state (exact meaning depends on `failure`)
    failure.apply(env)

    with pytest.raises(failure.expect_exception()):
        env.storage_controller.tenant_shard_split(tenant_id, shard_count=4)

    # We expect that the overall operation will fail, but some split requests
    # will have succeeded: the net result should be to return to a clean state, including
    # detaching any child shards.
    def assert_rolled_back(exclude_ps_id=None) -> None:
        secondary_count = 0
        attached_count = 0
        for ps in env.pageservers:
            if exclude_ps_id is not None and ps.id == exclude_ps_id:
                continue

            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                tenant_shard_id = TenantShardId.parse(loc[0])
                log.info(f"Shard {tenant_shard_id} seen on node {ps.id} in mode {loc[1]['mode']}")
                assert tenant_shard_id.shard_count == initial_shard_count
                if loc[1]["mode"] == "Secondary":
                    secondary_count += 1
                else:
                    attached_count += 1

        if exclude_ps_id is not None:
            # For a node failure case, we expect there to be a secondary location
            # scheduled on the offline node, so expect one fewer secondary in total
            assert secondary_count == initial_shard_count - 1
        else:
            assert secondary_count == initial_shard_count

        assert attached_count == initial_shard_count

    def assert_split_done(exclude_ps_id=None) -> None:
        secondary_count = 0
        attached_count = 0
        for ps in env.pageservers:
            if exclude_ps_id is not None and ps.id == exclude_ps_id:
                continue

            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                tenant_shard_id = TenantShardId.parse(loc[0])
                log.info(f"Shard {tenant_shard_id} seen on node {ps.id} in mode {loc[1]['mode']}")
                assert tenant_shard_id.shard_count == split_shard_count
                if loc[1]["mode"] == "Secondary":
                    secondary_count += 1
                else:
                    attached_count += 1
        assert attached_count == split_shard_count
        assert secondary_count == split_shard_count

    def finish_split():
        # Having failed+rolled back, we should be able to split again
        # No failures this time; it will succeed
        env.storage_controller.tenant_shard_split(tenant_id, shard_count=split_shard_count)

        workload.churn_rows(10)
        workload.validate()

    if failure.expect_available():
        # Even though the split failed partway through, this should not have interrupted
        # clients.  Disable waiting for pageservers in the workload helper, because our
        # failpoints may prevent API access.
        # This only applies for failure modes that leave pageserver page_service API available.
        workload.churn_rows(10, upload=False, ingest=False)
        workload.validate()

    if failure.fails_forward(env):
        log.info("Fail-forward failure, checking split eventually completes...")
        # A failure type which results in eventual completion of the split
        wait_until(30, 1, assert_split_done)
    elif failure.can_mitigate():
        log.info("Mitigating failure...")
        # Mitigation phase: we expect to be able to proceed with a successful shard split
        failure.mitigate(env)

        # The split should appear to be rolled back from the point of view of all pageservers
        # apart from the one that is offline
        wait_until(30, 1, lambda: assert_rolled_back(exclude_ps_id=failure.pageserver_id))

        finish_split()
        wait_until(30, 1, lambda: assert_split_done(exclude_ps_id=failure.pageserver_id))

        # Having cleared the failure, everything should converge to a pristine state
        failure.clear(env)
        wait_until(30, 1, assert_split_done)
    else:
        # Once we restore the faulty pageserver's API to good health, rollback should
        # eventually complete.
        log.info("Clearing failure...")
        failure.clear(env)

        wait_until(30, 1, assert_rolled_back)

        # Having rolled back, the tenant should be working
        workload.churn_rows(10)
        workload.validate()

        # Splitting again should work, since we cleared the failure
        finish_split()
        assert_split_done()

    env.storage_controller.consistency_check()


def test_sharding_backpressure(neon_env_builder: NeonEnvBuilder):
    """
    Check a scenario when one of the shards is much slower than others.
    Without backpressure, this would lead to the slow shard falling behind
    and eventually causing WAL timeouts.
    """

    shard_count = 4
    neon_env_builder.num_pageservers = shard_count

    # 256KiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 32

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    pageservers = dict((int(p.id), p) for p in env.pageservers)
    shards = env.storage_controller.locate(tenant_id)

    # Slow down one of the shards, around ~1MB/s
    pageservers[4].http_client().configure_failpoints(("wal-ingest-record-sleep", "5%sleep(1)"))

    def shards_info():
        infos = []
        for shard in shards:
            node_id = int(shard["node_id"])
            pageserver = pageservers[node_id]
            shard_info = pageserver.http_client().timeline_detail(shard["shard_id"], timeline_id)
            infos.append(shard_info)
            last_record_lsn = shard_info["last_record_lsn"]
            current_physical_size = shard_info["current_physical_size"]
            log.info(
                f"Shard on pageserver {node_id}: lsn={last_record_lsn}, size={current_physical_size}"
            )
        return infos

    shards_info()

    workload = Workload(
        env,
        tenant_id,
        timeline_id,
        branch_name="main",
        endpoint_opts={
            "config_lines": [
                # Tip: set to 100MB to make the test fail
                "max_replication_write_lag=1MB",
            ],
        },
    )
    workload.init()

    endpoint = workload.endpoint()

    # on 2024-03-05, the default config on prod was [15MB, 10GB, null]
    res = endpoint.safe_psql_many(
        [
            "SHOW max_replication_write_lag",
            "SHOW max_replication_flush_lag",
            "SHOW max_replication_apply_lag",
        ]
    )
    log.info(f"backpressure config: {res}")

    last_flush_lsn = None
    last_timestamp = None

    def update_write_lsn():
        nonlocal last_flush_lsn
        nonlocal last_timestamp

        res = endpoint.safe_psql(
            """
            SELECT
                pg_wal_lsn_diff(pg_current_wal_flush_lsn(), received_lsn) as received_lsn_lag,
                received_lsn,
                pg_current_wal_flush_lsn() as flush_lsn,
                neon.backpressure_throttling_time() as throttling_time
            FROM neon.backpressure_lsns();
            """,
            dbname="postgres",
        )[0]
        log.info(
            f"received_lsn_lag = {res[0]}, received_lsn = {res[1]}, flush_lsn = {res[2]}, throttling_time = {res[3]}"
        )

        lsn = Lsn(res[2])
        now = time.time()

        if last_timestamp is not None:
            delta = now - last_timestamp
            delta_bytes = lsn - last_flush_lsn
            avg_speed = delta_bytes / delta / 1024 / 1024
            log.info(
                f"flush_lsn {lsn}, written {delta_bytes/1024}kb for {delta:.3f}s, avg_speed {avg_speed:.3f} MiB/s"
            )

        last_flush_lsn = lsn
        last_timestamp = now

    update_write_lsn()

    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.validate()

    update_write_lsn()
    shards_info()

    for _write_iter in range(30):
        # approximately 1MB of data
        workload.write_rows(8000, upload=False)
        update_write_lsn()
        infos = shards_info()
        min_lsn = min(Lsn(info["last_record_lsn"]) for info in infos)
        max_lsn = max(Lsn(info["last_record_lsn"]) for info in infos)
        diff = max_lsn - min_lsn
        assert diff < 2 * 1024 * 1024, f"LSN diff={diff}, expected diff < 2MB due to backpressure"
