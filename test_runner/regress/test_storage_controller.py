import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    StorageControllerApiException,
    TokenScope,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    enable_remote_storage_versioning,
    list_prefix,
    remote_storage_delete_key,
    tenant_delete_wait_completed,
    timeline_delete_wait_completed,
)
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import RemoteStorageKind, s3_storage
from fixtures.types import TenantId, TenantShardId, TimelineId
from fixtures.utils import run_pg_bench_small, subprocess_capture, wait_until
from mypy_boto3_s3.type_defs import (
    ObjectTypeDef,
)
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


def get_node_shard_counts(env: NeonEnv, tenant_ids):
    counts: defaultdict[str, int] = defaultdict(int)
    for tid in tenant_ids:
        for shard in env.storage_controller.locate(tid):
            counts[shard["node_id"]] += 1
    return counts


def test_storage_controller_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basic lifecycle of a storage controller:
    - Restarting
    - Restarting a pageserver
    - Creating and deleting tenants and timelines
    - Marking a pageserver offline
    """

    neon_env_builder.num_pageservers = 3
    env = neon_env_builder.init_configs()

    for pageserver in env.pageservers:
        # This test detaches tenants during migration, which can race with deletion queue operations,
        # during detach we only do an advisory flush, we don't wait for it.
        pageserver.allowed_errors.extend([".*Dropped remote consistent LSN updates.*"])

    # Start services by hand so that we can skip a pageserver (this will start + register later)
    env.broker.try_start()
    env.storage_controller.start()
    env.pageservers[0].start()
    env.pageservers[1].start()
    for sk in env.safekeepers:
        sk.start()

    # The pageservers we started should have registered with the sharding service on startup
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2
    assert set(n["id"] for n in nodes) == {env.pageservers[0].id, env.pageservers[1].id}

    # Starting an additional pageserver should register successfully
    env.pageservers[2].start()
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 3
    assert set(n["id"] for n in nodes) == {ps.id for ps in env.pageservers}

    # Use a multiple of pageservers to get nice even number of shards on each one
    tenant_shard_count = len(env.pageservers) * 4
    tenant_count = len(env.pageservers) * 2
    shards_per_tenant = tenant_shard_count // tenant_count
    tenant_ids = set(TenantId.generate() for i in range(0, tenant_count))

    # Creating several tenants should spread out across the pageservers
    for tid in tenant_ids:
        env.neon_cli.create_tenant(tid, shard_count=shards_per_tenant)

    # Repeating a creation should be idempotent (we are just testing it doesn't return an error)
    env.storage_controller.tenant_create(
        tenant_id=next(iter(tenant_ids)), shard_count=shards_per_tenant
    )

    for node_id, count in get_node_shard_counts(env, tenant_ids).items():
        # we used a multiple of pagservers for the total shard count,
        # so expect equal number on all pageservers
        assert count == tenant_shard_count / len(
            env.pageservers
        ), f"Node {node_id} has bad count {count}"

    # Creating and deleting timelines should work, using identical API to pageserver
    timeline_crud_tenant = next(iter(tenant_ids))
    timeline_id = TimelineId.generate()
    env.storage_controller.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=timeline_crud_tenant, new_timeline_id=timeline_id
    )
    timelines = env.storage_controller.pageserver_api().timeline_list(timeline_crud_tenant)
    assert len(timelines) == 2
    assert timeline_id in set(TimelineId(t["timeline_id"]) for t in timelines)
    #    virtual_ps_http.timeline_delete(tenant_id=timeline_crud_tenant, timeline_id=timeline_id)
    timeline_delete_wait_completed(
        env.storage_controller.pageserver_api(), timeline_crud_tenant, timeline_id
    )
    timelines = env.storage_controller.pageserver_api().timeline_list(timeline_crud_tenant)
    assert len(timelines) == 1
    assert timeline_id not in set(TimelineId(t["timeline_id"]) for t in timelines)

    # Marking a pageserver offline should migrate tenants away from it.
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    def node_evacuated(node_id: int) -> None:
        counts = get_node_shard_counts(env, tenant_ids)
        assert counts[node_id] == 0

    wait_until(10, 1, lambda: node_evacuated(env.pageservers[0].id))

    # Marking pageserver active should not migrate anything to it
    # immediately
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Active"})
    time.sleep(1)
    assert get_node_shard_counts(env, tenant_ids)[env.pageservers[0].id] == 0

    # Restarting a pageserver should not detach any tenants (i.e. /re-attach works)
    before_restart = env.pageservers[1].http_client().tenant_list_locations()
    env.pageservers[1].stop()
    env.pageservers[1].start()
    after_restart = env.pageservers[1].http_client().tenant_list_locations()
    assert len(after_restart) == len(before_restart)

    # Locations should be the same before & after restart, apart from generations
    for _shard_id, tenant in after_restart["tenant_shards"]:
        del tenant["generation"]
    for _shard_id, tenant in before_restart["tenant_shards"]:
        del tenant["generation"]
    assert before_restart == after_restart

    # Delete all the tenants
    for tid in tenant_ids:
        tenant_delete_wait_completed(env.storage_controller.pageserver_api(), tid, 10)

    env.storage_controller.consistency_check()

    # Set a scheduling policy on one node, create all the tenants, observe
    # that the scheduling policy is respected.
    env.storage_controller.node_configure(env.pageservers[1].id, {"scheduling": "Draining"})

    # Create some fresh tenants
    tenant_ids = set(TenantId.generate() for i in range(0, tenant_count))
    for tid in tenant_ids:
        env.neon_cli.create_tenant(tid, shard_count=shards_per_tenant)

    counts = get_node_shard_counts(env, tenant_ids)
    # Nothing should have been scheduled on the node in Draining
    assert counts[env.pageservers[1].id] == 0
    assert counts[env.pageservers[0].id] == tenant_shard_count // 2
    assert counts[env.pageservers[2].id] == tenant_shard_count // 2

    env.storage_controller.consistency_check()


def test_node_status_after_restart(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # Initially we have two online pageservers
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2

    env.pageservers[1].stop()
    env.storage_controller.allowed_errors.extend([".*Could not scan node"])

    env.storage_controller.stop()
    env.storage_controller.start()

    def is_ready():
        assert env.storage_controller.ready() is True

    wait_until(30, 1, is_ready)

    # We loaded nodes from database on restart
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2

    # We should still be able to create a tenant, because the pageserver which is still online
    # should have had its availabilty state set to Active.
    env.storage_controller.tenant_create(TenantId.generate())

    env.storage_controller.consistency_check()


def test_storage_controller_passthrough(
    neon_env_builder: NeonEnvBuilder,
):
    """
    For simple timeline/tenant GET APIs that don't require coordination across
    shards, the sharding service implements a proxy to shard zero.  This test
    calls those APIs.
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # We will talk to storage controller as if it was a pageserver, using the pageserver
    # HTTP client
    client = PageserverHttpClient(env.storage_controller_port, lambda: True)
    timelines = client.timeline_list(tenant_id=env.initial_tenant)
    assert len(timelines) == 1

    status = client.tenant_status(env.initial_tenant)
    assert TenantId(status["id"]) == env.initial_tenant
    assert set(TimelineId(t) for t in status["timelines"]) == {
        env.initial_timeline,
    }
    assert status["state"]["slug"] == "Active"

    env.storage_controller.consistency_check()


def test_storage_controller_restart(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    tenant_a = env.initial_tenant
    tenant_b = TenantId.generate()
    env.storage_controller.tenant_create(tenant_b)
    env.pageserver.tenant_detach(tenant_a)

    # TODO: extend this test to use multiple pageservers, and check that locations don't move around
    # on restart.

    # Storage controller restart
    env.storage_controller.stop()
    env.storage_controller.start()

    observed = set(TenantId(tenant["id"]) for tenant in env.pageserver.http_client().tenant_list())

    # Tenant A should still be attached
    assert tenant_a not in observed

    # Tenant B should remain detached
    assert tenant_b in observed

    # Pageserver restart
    env.pageserver.stop()
    env.pageserver.start()

    # Same assertions as above: restarting either service should not perturb things
    observed = set(TenantId(tenant["id"]) for tenant in env.pageserver.http_client().tenant_list())
    assert tenant_a not in observed
    assert tenant_b in observed

    env.storage_controller.consistency_check()


@pytest.mark.parametrize("warm_up", [True, False])
def test_storage_controller_onboarding(neon_env_builder: NeonEnvBuilder, warm_up: bool):
    """
    We onboard tenants to the sharding service by treating it as a 'virtual pageserver'
    which provides the /location_config API.  This is similar to creating a tenant,
    but imports the generation number.
    """

    neon_env_builder.num_pageservers = 2

    # Start services by hand so that we can skip registration on one of the pageservers
    env = neon_env_builder.init_configs()
    env.broker.try_start()
    env.storage_controller.start()

    # This is the pageserver where we'll initially create the tenant.  Run it in emergency
    # mode so that it doesn't talk to storage controller, and do not register it.
    env.pageservers[0].allowed_errors.append(".*Emergency mode!.*")
    env.pageservers[0].start(
        overrides=("--pageserver-config-override=control_plane_emergency_mode=true",),
    )
    origin_ps = env.pageservers[0]

    # This is the pageserver managed by the sharding service, where the tenant
    # will be attached after onboarding
    env.pageservers[1].start()
    dest_ps = env.pageservers[1]
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    for sk in env.safekeepers:
        sk.start()

    # Create a tenant directly via pageserver HTTP API, skipping the storage controller
    tenant_id = TenantId.generate()
    generation = 123
    origin_ps.http_client().tenant_create(tenant_id, generation=generation)

    # As if doing a live migration, first configure origin into stale mode
    r = origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedStale",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1

    if warm_up:
        origin_ps.http_client().tenant_heatmap_upload(tenant_id)

        # We expect to be called via live migration code, which may try to configure the tenant into secondary
        # mode before attaching it.
        virtual_ps_http.tenant_location_conf(
            tenant_id,
            {
                "mode": "Secondary",
                "secondary_conf": {"warm": True},
                "tenant_conf": {},
                "generation": None,
            },
        )

        virtual_ps_http.tenant_secondary_download(tenant_id)

    # Call into storage controller to onboard the tenant
    generation += 1
    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedMulti",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1

    # As if doing a live migration, detach the original pageserver
    origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    # As if doing a live migration, call into the storage controller to
    # set it to AttachedSingle: this is a no-op, but we test it because the
    # cloud control plane may call this for symmetry with live migration to
    # an individual pageserver
    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1

    # We should see the tenant is now attached to the pageserver managed
    # by the sharding service
    origin_tenants = origin_ps.http_client().tenant_list()
    assert len(origin_tenants) == 0
    dest_tenants = dest_ps.http_client().tenant_list()
    assert len(dest_tenants) == 1
    assert TenantId(dest_tenants[0]["id"]) == tenant_id

    # sharding service advances generation by 1 when it first attaches.  We started
    # with a nonzero generation so this equality also proves that the generation
    # was properly carried over during onboarding.
    assert dest_tenants[0]["generation"] == generation + 1

    # The onboarded tenant should survive a restart of sharding service
    env.storage_controller.stop()
    env.storage_controller.start()

    # The onboarded tenant should surviev a restart of pageserver
    dest_ps.stop()
    dest_ps.start()

    # Having onboarded via /location_config, we should also be able to update the
    # TenantConf part of LocationConf, without inadvertently resetting the generation
    modified_tenant_conf = {"max_lsn_wal_lag": 1024 * 1024 * 1024 * 100}
    dest_tenant_before_conf_change = dest_ps.http_client().tenant_status(tenant_id)

    # The generation has moved on since we onboarded
    assert generation != dest_tenant_before_conf_change["generation"]

    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": modified_tenant_conf,
            # This is intentionally a stale generation
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1
    dest_tenant_after_conf_change = dest_ps.http_client().tenant_status(tenant_id)
    assert (
        dest_tenant_after_conf_change["generation"] == dest_tenant_before_conf_change["generation"]
    )
    dest_tenant_conf_after = dest_ps.http_client().tenant_config(tenant_id)
    assert dest_tenant_conf_after.tenant_specific_overrides == modified_tenant_conf

    env.storage_controller.consistency_check()


def test_storage_controller_compute_hook(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address,
):
    """
    Test that the sharding service calls out to the configured HTTP endpoint on attachment changes
    """

    # We will run two pageserver to migrate and check that the storage controller sends notifications
    # when migrating.
    neon_env_builder.num_pageservers = 2
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"

    # Set up fake HTTP notify endpoint
    notifications = []

    handle_params = {"status": 200}

    def handler(request: Request):
        status = handle_params["status"]
        log.info(f"Notify request[{status}]: {request}")
        notifications.append(request.json)
        return Response(status=status)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    # Start running
    env = neon_env_builder.init_start()

    # We will to an unclean migration, which will result in deletion queue warnings
    env.pageservers[0].allowed_errors.append(".*Dropped remote consistent LSN updates for tenant.*")

    # Initial notification from tenant creation
    assert len(notifications) == 1
    expect: Dict[str, Union[List[Dict[str, int]], str, None, int]] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
    }
    assert notifications[0] == expect

    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    def node_evacuated(node_id: int) -> None:
        counts = get_node_shard_counts(env, [env.initial_tenant])
        assert counts[node_id] == 0

    wait_until(10, 1, lambda: node_evacuated(env.pageservers[0].id))

    # Additional notification from migration
    log.info(f"notifications: {notifications}")
    expect = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[1].id), "shard_number": 0}],
    }

    def received_migration_notification():
        assert len(notifications) == 2
        assert notifications[1] == expect

    wait_until(20, 0.25, received_migration_notification)

    # When we restart, we should re-emit notifications for all tenants
    env.storage_controller.stop()
    env.storage_controller.start()

    def received_restart_notification():
        assert len(notifications) == 3
        assert notifications[2] == expect

    wait_until(10, 1, received_restart_notification)

    # Splitting a tenant should cause its stripe size to become visible in the compute notification
    env.storage_controller.tenant_shard_split(env.initial_tenant, shard_count=2)
    expect = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": 32768,
        "shards": [
            {"node_id": int(env.pageservers[1].id), "shard_number": 0},
            {"node_id": int(env.pageservers[1].id), "shard_number": 1},
        ],
    }

    def received_split_notification():
        assert len(notifications) == 4
        assert notifications[3] == expect

    wait_until(10, 1, received_split_notification)

    # If the compute hook is unavailable, that should not block creating a tenant and
    # creating a timeline.  This simulates a control plane refusing to accept notifications
    handle_params["status"] = 423
    degraded_tenant_id = TenantId.generate()
    degraded_timeline_id = TimelineId.generate()
    env.storage_controller.tenant_create(degraded_tenant_id)
    env.storage_controller.pageserver_api().timeline_create(
        PgVersion.NOT_SET, degraded_tenant_id, degraded_timeline_id
    )

    # Ensure we hit the handler error path
    env.storage_controller.allowed_errors.append(
        ".*Failed to notify compute of attached pageserver.*tenant busy.*"
    )
    env.storage_controller.allowed_errors.append(".*Reconcile error.*tenant busy.*")
    assert notifications[-1] is not None
    assert notifications[-1]["tenant_id"] == str(degraded_tenant_id)

    env.storage_controller.consistency_check()


def test_storage_controller_debug_apis(neon_env_builder: NeonEnvBuilder):
    """
    Verify that occasional-use debug APIs work as expected.  This is a lightweight test
    that just hits the endpoints to check that they don't bitrot.
    """

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(tenant_id, shard_count=2, shard_stripe_size=8192)

    # Check that the consistency check passes on a freshly setup system
    env.storage_controller.consistency_check()

    # These APIs are intentionally not implemented as methods on NeonStorageController, as
    # they're just for use in unanticipated circumstances.

    # Initial tenant (1 shard) and the one we just created (2 shards) should be visible
    response = env.storage_controller.request(
        "GET",
        f"{env.storage_controller_api}/debug/v1/tenant",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )
    assert len(response.json()) == 3

    # Scheduler should report the expected nodes and shard counts
    response = env.storage_controller.request(
        "GET", f"{env.storage_controller_api}/debug/v1/scheduler"
    )
    # Two nodes, in a dict of node_id->node
    assert len(response.json()["nodes"]) == 2
    assert sum(v["shard_count"] for v in response.json()["nodes"].values()) == 3
    assert all(v["may_schedule"] for v in response.json()["nodes"].values())

    response = env.storage_controller.request(
        "POST",
        f"{env.storage_controller_api}/debug/v1/node/{env.pageservers[1].id}/drop",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )
    assert len(env.storage_controller.node_list()) == 1

    response = env.storage_controller.request(
        "POST",
        f"{env.storage_controller_api}/debug/v1/tenant/{tenant_id}/drop",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )

    # Tenant drop should be reflected in dump output
    response = env.storage_controller.request(
        "GET",
        f"{env.storage_controller_api}/debug/v1/tenant",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )
    assert len(response.json()) == 1

    # Check that the 'drop' APIs didn't leave things in a state that would fail a consistency check: they're
    # meant to be unclean wrt the pageserver state, but not leave a broken storage controller behind.
    env.storage_controller.consistency_check()


def test_storage_controller_s3_time_travel_recovery(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    """
    Test for S3 time travel
    """

    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # Mock S3 doesn't have versioning enabled by default, enable it
    # (also do it before there is any writes to the bucket)
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        remote_storage = neon_env_builder.pageserver_remote_storage
        assert remote_storage, "remote storage not configured"
        enable_remote_storage_versioning(remote_storage)

    neon_env_builder.num_pageservers = 1

    env = neon_env_builder.init_start()
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id,
        shard_count=2,
        shard_stripe_size=8192,
        tenant_config=MANY_SMALL_LAYERS_TENANT_CONFIG,
    )

    # Check that the consistency check passes
    env.storage_controller.consistency_check()

    branch_name = "main"
    timeline_id = env.neon_cli.create_timeline(
        branch_name,
        tenant_id=tenant_id,
    )
    # Write some nontrivial amount of data into the endpoint and wait until it is uploaded
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        endpoint.safe_psql("CREATE TABLE created_foo(id integer);")
        # last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

    # Give the data time to be uploaded
    time.sleep(4)

    # Detach the tenant
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    time.sleep(4)
    ts_before_disaster = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    # Simulate a "disaster": delete some random files from remote storage for one of the shards
    assert env.pageserver_remote_storage
    shard_id_for_list = "0002"
    objects: List[ObjectTypeDef] = list_prefix(
        env.pageserver_remote_storage,
        f"tenants/{tenant_id}-{shard_id_for_list}/timelines/{timeline_id}/",
    ).get("Contents", [])
    assert len(objects) > 1
    log.info(f"Found {len(objects)} objects in remote storage")
    should_delete = False
    for obj in objects:
        obj_key = obj["Key"]
        should_delete = not should_delete
        if not should_delete:
            log.info(f"Keeping key on remote storage: {obj_key}")
            continue
        log.info(f"Deleting key from remote storage: {obj_key}")
        remote_storage_delete_key(env.pageserver_remote_storage, obj_key)
        pass

    time.sleep(4)
    ts_after_disaster = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    # Do time travel recovery
    virtual_ps_http.tenant_time_travel_remote_storage(
        tenant_id, ts_before_disaster, ts_after_disaster, shard_counts=[2]
    )
    time.sleep(4)

    # Attach the tenant again
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": 100,
        },
    )

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        endpoint.safe_psql("SELECT * FROM created_foo;")

    env.storage_controller.consistency_check()


def test_storage_controller_auth(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()
    svc = env.storage_controller
    api = env.storage_controller_api

    tenant_id = TenantId.generate()
    body: Dict[str, Any] = {"new_tenant_id": str(tenant_id)}

    env.storage_controller.allowed_errors.append(".*Unauthorized.*")
    env.storage_controller.allowed_errors.append(".*Forbidden.*")

    # No token
    with pytest.raises(
        StorageControllerApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("POST", f"{env.storage_controller_api}/v1/tenant", json=body)

    # Token with incorrect scope
    with pytest.raises(
        StorageControllerApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.SAFEKEEPER_DATA)
        )

    # Token with correct scope
    svc.request(
        "POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.PAGE_SERVER_API)
    )

    # Token with admin scope should also be permitted
    svc.request("POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.ADMIN))

    # No token
    with pytest.raises(
        StorageControllerApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("GET", f"{api}/debug/v1/tenant")

    # Token with incorrect scope
    with pytest.raises(
        StorageControllerApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "GET", f"{api}/debug/v1/tenant", headers=svc.headers(TokenScope.GENERATIONS_API)
        )

    # No token
    with pytest.raises(
        StorageControllerApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("POST", f"{api}/upcall/v1/re-attach")

    # Token with incorrect scope
    with pytest.raises(
        StorageControllerApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "POST", f"{api}/upcall/v1/re-attach", headers=svc.headers(TokenScope.PAGE_SERVER_API)
        )


def test_storage_controller_tenant_conf(neon_env_builder: NeonEnvBuilder):
    """
    Validate the pageserver-compatible API endpoints for setting and getting tenant conf, without
    supplying the whole LocationConf.
    """

    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant

    http = env.storage_controller.pageserver_api()

    default_value = "7days"
    new_value = "1h"
    http.set_tenant_config(tenant_id, {"pitr_interval": new_value})

    # Ensure the change landed on the storage controller
    readback_controller = http.tenant_config(tenant_id)
    assert readback_controller.effective_config["pitr_interval"] == new_value
    assert readback_controller.tenant_specific_overrides["pitr_interval"] == new_value

    # Ensure the change made it down to the pageserver
    readback_ps = env.pageservers[0].http_client().tenant_config(tenant_id)
    assert readback_ps.effective_config["pitr_interval"] == new_value
    assert readback_ps.tenant_specific_overrides["pitr_interval"] == new_value

    # Omitting a value clears it.  This looks different in storage controller
    # vs. pageserver API calls, because pageserver has defaults.
    http.set_tenant_config(tenant_id, {})
    readback_controller = http.tenant_config(tenant_id)
    assert readback_controller.effective_config["pitr_interval"] is None
    assert readback_controller.tenant_specific_overrides["pitr_interval"] is None
    readback_ps = env.pageservers[0].http_client().tenant_config(tenant_id)
    assert readback_ps.effective_config["pitr_interval"] == default_value
    assert "pitr_interval" not in readback_ps.tenant_specific_overrides

    env.storage_controller.consistency_check()


class Failure:
    pageserver_id: int

    def apply(self, env: NeonEnv):
        raise NotImplementedError()

    def clear(self, env: NeonEnv):
        raise NotImplementedError()


class NodeStop(Failure):
    def __init__(self, pageserver_id, immediate):
        self.pageserver_id = pageserver_id
        self.immediate = immediate

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.stop(immediate=self.immediate)

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.start()


class PageserverFailpoint(Failure):
    def __init__(self, failpoint, pageserver_id):
        self.failpoint = failpoint
        self.pageserver_id = pageserver_id

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "return(1)"))

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "off"))


def build_node_to_tenants_map(env: NeonEnv) -> dict[int, list[TenantId]]:
    tenants = env.storage_controller.tenant_list()

    node_to_tenants: dict[int, list[TenantId]] = {}
    for t in tenants:
        for node_id, loc_state in t["observed"]["locations"].items():
            if (
                loc_state is not None
                and "conf" in loc_state
                and loc_state["conf"] is not None
                and loc_state["conf"]["mode"] == "AttachedSingle"
            ):
                crnt = node_to_tenants.get(int(node_id), [])
                crnt.append(TenantId(t["tenant_shard_id"]))
                node_to_tenants[int(node_id)] = crnt

    return node_to_tenants


@pytest.mark.parametrize(
    "failure",
    [
        NodeStop(pageserver_id=1, immediate=False),
        NodeStop(pageserver_id=1, immediate=True),
        PageserverFailpoint(pageserver_id=1, failpoint="get-utilization-http-handler"),
    ],
)
def test_storage_controller_heartbeats(
    neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, failure: Failure
):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    # Default log allow list permits connection errors, but this test will use error responses on
    # the utilization endpoint.
    env.storage_controller.allowed_errors.append(
        ".*Call to node.*management API.*failed.*failpoint.*"
    )

    # Initially we have two online pageservers
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2
    assert all([n["availability"] == "Active" for n in nodes])

    # ... then we create two tenants and write some data into them
    def create_tenant(tid: TenantId):
        env.storage_controller.tenant_create(tid)

        branch_name = "main"
        env.neon_cli.create_timeline(
            branch_name,
            tenant_id=tid,
        )

        with env.endpoints.create_start("main", tenant_id=tid) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            endpoint.safe_psql("CREATE TABLE created_foo(id integer);")

    tenant_ids = [TenantId.generate(), TenantId.generate()]
    for tid in tenant_ids:
        create_tenant(tid)

    # ... expecting that each tenant will be placed on a different node
    def tenants_placed():
        node_to_tenants = build_node_to_tenants_map(env)
        log.info(f"{node_to_tenants=}")

        # Check that all the tenants have been attached
        assert sum((len(ts) for ts in node_to_tenants.values())) == len(tenant_ids)
        # Check that each node got one tenant
        assert all((len(ts) == 1 for ts in node_to_tenants.values()))

    wait_until(10, 1, tenants_placed)

    # ... then we apply the failure
    offline_node_id = failure.pageserver_id
    online_node_id = (set(range(1, len(env.pageservers) + 1)) - {offline_node_id}).pop()
    env.get_pageserver(offline_node_id).allowed_errors.append(
        # In the case of the failpoint failure, the impacted pageserver
        # still believes it has the tenant attached since location
        # config calls into it will fail due to being marked offline.
        ".*Dropped remote consistent LSN updates.*",
    )

    failure.apply(env)

    # ... expecting the heartbeats to mark it offline
    def node_offline():
        nodes = env.storage_controller.node_list()
        log.info(f"{nodes=}")
        target = next(n for n in nodes if n["id"] == offline_node_id)
        assert target["availability"] == "Offline"

    # A node is considered offline if the last successful heartbeat
    # was more than 10 seconds ago (hardcoded in the storage controller).
    wait_until(20, 1, node_offline)

    # .. expecting the tenant on the offline node to be migrated
    def tenant_migrated():
        node_to_tenants = build_node_to_tenants_map(env)
        log.info(f"{node_to_tenants=}")
        assert set(node_to_tenants[online_node_id]) == set(tenant_ids)

    wait_until(10, 1, tenant_migrated)

    # ... then we clear the failure
    failure.clear(env)

    # ... expecting the offline node to become active again
    def node_online():
        nodes = env.storage_controller.node_list()
        target = next(n for n in nodes if n["id"] == offline_node_id)
        assert target["availability"] == "Active"

    wait_until(10, 1, node_online)

    time.sleep(5)

    # ... then we create a new tenant
    tid = TenantId.generate()
    env.storage_controller.tenant_create(tid)

    # ... expecting it to be placed on the node that just came back online
    tenants = env.storage_controller.tenant_list()
    newest_tenant = next(t for t in tenants if t["tenant_shard_id"] == str(tid))
    locations = list(newest_tenant["observed"]["locations"].keys())
    locations = [int(node_id) for node_id in locations]
    assert locations == [offline_node_id]

    # ... expecting the storage controller to reach a consistent state
    def storage_controller_consistent():
        env.storage_controller.consistency_check()

    wait_until(10, 1, storage_controller_consistent)


def test_storage_controller_re_attach(neon_env_builder: NeonEnvBuilder):
    """
    Exercise the behavior of the /re-attach endpoint on pageserver startup when
    pageservers have a mixture of attached and secondary locations
    """

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    # We'll have two tenants.
    tenant_a = TenantId.generate()
    env.neon_cli.create_tenant(tenant_a, placement_policy='{"Attached":1}')
    tenant_b = TenantId.generate()
    env.neon_cli.create_tenant(tenant_b, placement_policy='{"Attached":1}')

    # Each pageserver will have one attached and one secondary location
    env.storage_controller.tenant_shard_migrate(
        TenantShardId(tenant_a, 0, 0), env.pageservers[0].id
    )
    env.storage_controller.tenant_shard_migrate(
        TenantShardId(tenant_b, 0, 0), env.pageservers[1].id
    )

    # Hard-fail a pageserver
    victim_ps = env.pageservers[1]
    survivor_ps = env.pageservers[0]
    victim_ps.stop(immediate=True)

    # Heatbeater will notice it's offline, and consequently attachments move to the other pageserver
    def failed_over():
        locations = survivor_ps.http_client().tenant_list_locations()["tenant_shards"]
        log.info(f"locations: {locations}")
        assert len(locations) == 2
        assert all(loc[1]["mode"] == "AttachedSingle" for loc in locations)

    # We could pre-empty this by configuring the node to Offline, but it's preferable to test
    # the realistic path we would take when a node restarts uncleanly.
    # The delay here will be ~NEON_LOCAL_MAX_UNAVAILABLE_INTERVAL in neon_local
    wait_until(30, 1, failed_over)

    reconciles_before_restart = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )

    # Restart the failed pageserver
    victim_ps.start()

    # We expect that the re-attach call correctly tipped off the pageserver that its locations
    # are all secondaries now.
    locations = victim_ps.http_client().tenant_list_locations()["tenant_shards"]
    assert len(locations) == 2
    assert all(loc[1]["mode"] == "Secondary" for loc in locations)

    # We expect that this situation resulted from the re_attach call, and not any explicit
    # Reconciler runs: assert that the reconciliation count has not gone up since we restarted.
    reconciles_after_restart = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )
    assert reconciles_after_restart == reconciles_before_restart


def test_storage_controller_shard_scheduling_policy(neon_env_builder: NeonEnvBuilder):
    """
    Check that emergency hooks for disabling rogue tenants' reconcilers work as expected.
    """
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()

    env.storage_controller.allowed_errors.extend(
        [
            # We will intentionally cause reconcile errors
            ".*Reconcile error.*",
            # Message from using a scheduling policy
            ".*Scheduling is disabled by policy.*",
            ".*Skipping reconcile for policy.*",
            # Message from a node being offline
            ".*Call to node .* management API .* failed",
        ]
    )

    # Stop pageserver so that reconcile cannot complete
    env.pageserver.stop()

    env.storage_controller.tenant_create(tenant_id, placement_policy="Detached")

    # Try attaching it: we should see reconciles failing
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "placement": {"Attached": 0},
        },
    )

    def reconcile_errors() -> int:
        return int(
            env.storage_controller.get_metric_value(
                "storage_controller_reconcile_complete_total", filter={"status": "error"}
            )
            or 0
        )

    def reconcile_ok() -> int:
        return int(
            env.storage_controller.get_metric_value(
                "storage_controller_reconcile_complete_total", filter={"status": "ok"}
            )
            or 0
        )

    def assert_errors_gt(n) -> int:
        e = reconcile_errors()
        assert e > n
        return e

    errs = wait_until(10, 1, lambda: assert_errors_gt(0))

    # Try reconciling again, it should fail again
    with pytest.raises(StorageControllerApiException):
        env.storage_controller.reconcile_all()
    errs = wait_until(10, 1, lambda: assert_errors_gt(errs))

    # Configure the tenant to disable reconciles
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "scheduling": "Stop",
        },
    )

    # Try reconciling again, it should not cause an error (silently skip)
    env.storage_controller.reconcile_all()
    assert reconcile_errors() == errs

    # Start the pageserver and re-enable reconciles
    env.pageserver.start()
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "scheduling": "Active",
        },
    )

    def assert_ok_gt(n) -> int:
        o = reconcile_ok()
        assert o > n
        return o

    # We should see a successful reconciliation
    wait_until(10, 1, lambda: assert_ok_gt(0))

    # And indeed the tenant should be attached
    assert len(env.pageserver.http_client().tenant_list_locations()["tenant_shards"]) == 1


def test_storcon_cli(neon_env_builder: NeonEnvBuilder):
    """
    The storage controller command line interface (storcon-cli) is an internal tool.  Most tests
    just use the APIs directly: this test exercises some basics of the CLI as a regression test
    that the client remains usable as the server evolves.
    """
    output_dir = neon_env_builder.test_output_dir
    shard_count = 4
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
    base_args = [env.neon_binpath / "storcon_cli", "--api", env.storage_controller_api]

    def storcon_cli(args):
        """
        CLI wrapper: returns stdout split into a list of non-empty strings
        """
        (output_path, stdout, status_code) = subprocess_capture(
            output_dir,
            [str(s) for s in base_args + args],
            echo_stderr=True,
            echo_stdout=True,
            env={},
            check=False,
            capture_stdout=True,
            timeout=10,
        )
        if status_code:
            log.warning(f"Command {args} failed")
            log.warning(f"Output at: {output_path}")

            raise RuntimeError("CLI failure (check logs for stderr)")

        assert stdout is not None
        return [line.strip() for line in stdout.split("\n") if line.strip()]

    # List nodes
    node_lines = storcon_cli(["nodes"])
    # Table header, footer, and one line of data
    assert len(node_lines) == 5
    assert "localhost" in node_lines[3]

    # Pause scheduling onto a node
    storcon_cli(["node-configure", "--node-id", "1", "--scheduling", "pause"])
    assert "Pause" in storcon_cli(["nodes"])[3]

    # We will simulate a node death and then marking it offline
    env.pageservers[0].stop(immediate=True)
    # Sleep to make it unlikely that the controller's heartbeater will race handling
    # a /utilization response internally, such that it marks the node back online.  IRL
    # there would always be a longer delay than this before a node failing and a human
    # intervening.
    time.sleep(2)

    storcon_cli(["node-configure", "--node-id", "1", "--availability", "offline"])
    assert "Offline" in storcon_cli(["nodes"])[3]

    # List tenants
    tenant_lines = storcon_cli(["tenants"])
    assert len(tenant_lines) == 5
    assert str(env.initial_tenant) in tenant_lines[3]

    # Setting scheduling policies intentionally result in warnings, they're for rare use.
    env.storage_controller.allowed_errors.extend(
        [".*Skipping reconcile for policy.*", ".*Scheduling is disabled by policy.*"]
    )

    # Describe a tenant
    tenant_lines = storcon_cli(["tenant-describe", "--tenant-id", str(env.initial_tenant)])
    assert len(tenant_lines) == 3 + shard_count * 2
    assert str(env.initial_tenant) in tenant_lines[3]

    # Pause changes on a tenant
    storcon_cli(["tenant-policy", "--tenant-id", str(env.initial_tenant), "--scheduling", "stop"])
    assert "Stop" in storcon_cli(["tenants"])[3]

    # Change a tenant's placement
    storcon_cli(
        ["tenant-policy", "--tenant-id", str(env.initial_tenant), "--placement", "secondary"]
    )
    assert "Secondary" in storcon_cli(["tenants"])[3]

    # Modify a tenant's config
    storcon_cli(
        [
            "tenant-config",
            "--tenant-id",
            str(env.initial_tenant),
            "--config",
            json.dumps({"pitr_interval": "1m"}),
        ]
    )

    # Quiesce any background reconciliation before doing consistency check
    env.storage_controller.reconcile_until_idle(timeout_secs=10)
    env.storage_controller.consistency_check()
