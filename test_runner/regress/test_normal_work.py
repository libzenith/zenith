from __future__ import annotations

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder
from fixtures.pageserver.http import PageserverHttpClient


def check_tenant(
    env: NeonEnv, pageserver_http: PageserverHttpClient, safekeeper_proto_version: int
):
    tenant_id, timeline_id = env.create_tenant()
    config_lines = [
        f"neon.safekeeper_proto_version = {safekeeper_proto_version}",
    ]
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id, config_lines=config_lines)
    # we rely upon autocommit after each statement
    res_1 = endpoint.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
            "SELECT sum(key) FROM t",
        ]
    )

    assert res_1[-1][0] == (5000050000,)
    # TODO check detach on live instance
    log.info("stopping compute")
    endpoint.stop()
    log.info("compute stopped")

    endpoint.start()
    res_2 = endpoint.safe_psql("SELECT sum(key) FROM t")
    assert res_2[0] == (5000050000,)

    endpoint.stop()
    pageserver_http.tenant_detach(tenant_id)


@pytest.mark.parametrize("num_timelines,num_safekeepers", [(3, 1)])
# Test both proto versions until we fully migrate.
@pytest.mark.parametrize("safekeeper_proto_version", [2, 3])
def test_normal_work(
    neon_env_builder: NeonEnvBuilder,
    num_timelines: int,
    num_safekeepers: int,
    safekeeper_proto_version: int,
):
    """
    Basic test:
    * create new tenant with a timeline
    * write some data
    * ensure that it was successfully written
    * restart compute
    * check that the data is there
    * stop compute
    * detach tenant

    Repeat check for several tenants/timelines.
    """

    neon_env_builder.num_safekeepers = num_safekeepers
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    for _ in range(num_timelines):
        check_tenant(env, pageserver_http, safekeeper_proto_version)
