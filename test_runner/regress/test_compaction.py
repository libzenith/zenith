import json
import os
from typing import Optional

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.workload import Workload

AGGRESIVE_COMPACTION_TENANT_CONF = {
    # Disable gc and compaction. The test runs compaction manually.
    "gc_period": "0s",
    "compaction_period": "0s",
    # Small checkpoint distance to create many layers
    "checkpoint_distance": 1024**2,
    # Compact small layers
    "compaction_target_size": 1024**2,
    "image_creation_threshold": 2,
}


@pytest.mark.skipif(os.environ.get("BUILD_TYPE") == "debug", reason="only run with release build")
def test_pageserver_compaction_smoke(neon_env_builder: NeonEnvBuilder):
    """
    This is a smoke test that compaction kicks in. The workload repeatedly churns
    a small number of rows and manually instructs the pageserver to run compaction
    between iterations. At the end of the test validate that the average number of
    layers visited to gather reconstruct data for a given key is within the empirically
    observed bounds.
    """

    # Effectively disable the page cache to rely only on image layers
    # to shorten reads.
    neon_env_builder.pageserver_config_override = """
page_cache_size=10
"""

    env = neon_env_builder.init_start(initial_tenant_conf=AGGRESIVE_COMPACTION_TENANT_CONF)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    row_count = 10000
    churn_rounds = 100

    ps_http = env.pageserver.http_client()

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageserver.id)

    log.info("Writing initial data ...")
    workload.write_rows(row_count, env.pageserver.id)

    for i in range(1, churn_rounds + 1):
        if i % 10 == 0:
            log.info(f"Running churn round {i}/{churn_rounds} ...")

        workload.churn_rows(row_count, env.pageserver.id)
        ps_http.timeline_compact(tenant_id, timeline_id)

    log.info("Validating at workload end ...")
    workload.validate(env.pageserver.id)

    log.info("Checking layer access metrics ...")

    layer_access_metric_names = [
        "pageserver_layers_visited_per_read_global_sum",
        "pageserver_layers_visited_per_read_global_count",
        "pageserver_layers_visited_per_read_global_bucket",
        "pageserver_layers_visited_per_vectored_read_global_sum",
        "pageserver_layers_visited_per_vectored_read_global_count",
        "pageserver_layers_visited_per_vectored_read_global_bucket",
    ]

    metrics = env.pageserver.http_client().get_metrics()
    for name in layer_access_metric_names:
        layer_access_metrics = metrics.query_all(name)
        log.info(f"Got metrics: {layer_access_metrics}")

    non_vectored_sum = metrics.query_one("pageserver_layers_visited_per_read_global_sum")
    non_vectored_count = metrics.query_one("pageserver_layers_visited_per_read_global_count")
    non_vectored_average = non_vectored_sum.value / non_vectored_count.value

    vectored_sum = metrics.query_one("pageserver_layers_visited_per_vectored_read_global_sum")
    vectored_count = metrics.query_one("pageserver_layers_visited_per_vectored_read_global_count")
    vectored_average = vectored_sum.value / vectored_count.value

    log.info(f"{non_vectored_average=} {vectored_average=}")

    # The upper bound for average number of layer visits below (8)
    # was chosen empirically for this workload.
    assert non_vectored_average < 8
    assert vectored_average < 8


# Stripe sizes in number of pages.
TINY_STRIPES = 16
LARGE_STRIPES = 32768


@pytest.mark.parametrize(
    "shard_count,stripe_size", [(None, None), (4, TINY_STRIPES), (4, LARGE_STRIPES)]
)
def test_sharding_compaction(
    neon_env_builder: NeonEnvBuilder, stripe_size: int, shard_count: Optional[int]
):
    """
    Use small stripes, small layers, and small compaction thresholds to exercise how compaction
    and image layer generation interacts with sharding.

    We are looking for bugs that might emerge from the way sharding uses sparse layer files that
    only contain some of the keys in the key range covered by the layer, such as errors estimating
    the size of layers that might result in too-small layer files.
    """

    compaction_target_size = 128 * 1024

    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{128 * 1024}",
        "compaction_threshold": "1",
        "compaction_target_size": f"{compaction_target_size}",
        # no PITR horizon, we specify the horizon when we request on-demand GC
        "pitr_interval": "0s",
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
        # create image layers eagerly: we want to exercise image layer creation in this test.
        "image_creation_threshold": "1",
        "image_layer_creation_check_threshold": 0,
    }

    neon_env_builder.num_pageservers = 1 if shard_count is None else shard_count
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
        initial_tenant_shard_stripe_size=stripe_size,
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(64)
    for _i in range(0, 10):
        # Each of these does some writes then a checkpoint: because we set image_creation_threshold to 1,
        # these should result in image layers each time we write some data into a shard, and also shards
        # recieving less data hitting their "empty image layer" path (wherre they should skip writing the layer,
        # rather than asserting)
        workload.churn_rows(64)

    # Assert that we got some image layers: this is important because this test's purpose is to exercise the sharding changes
    # to Timeline::create_image_layers, so if we weren't creating any image layers we wouldn't be doing our job.
    shard_has_image_layers = []
    for shard in env.storage_controller.locate(tenant_id):
        pageserver = env.get_pageserver(shard["node_id"])
        shard_id = shard["shard_id"]
        layer_map = pageserver.http_client().layer_map_info(shard_id, timeline_id)
        image_layer_sizes = {}
        for layer in layer_map.historic_layers:
            if layer.kind == "Image":
                image_layer_sizes[layer.layer_file_name] = layer.layer_file_size

                # Pageserver should assert rather than emit an empty layer file, but double check here
                assert layer.layer_file_size is not None
                assert layer.layer_file_size > 0

        shard_has_image_layers.append(len(image_layer_sizes) > 1)
        log.info(f"Shard {shard_id} image layer sizes: {json.dumps(image_layer_sizes, indent=2)}")

        if stripe_size == TINY_STRIPES:
            # Checking the average size validates that our keyspace partitioning is  properly respecting sharding: if
            # it was not, we would tend to get undersized layers because the partitioning would overestimate the physical
            # data in a keyrange.
            #
            # We only do this check with tiny stripes, because large stripes may not give all shards enough
            # data to have statistically significant image layers
            avg_size = sum(v for v in image_layer_sizes.values()) / len(image_layer_sizes)  # type: ignore
            log.info(f"Shard {shard_id} average image layer size: {avg_size}")
            assert avg_size > compaction_target_size / 2

    if stripe_size == TINY_STRIPES:
        # Expect writes were scattered across all pageservers: they should all have compacted some image layers
        assert all(shard_has_image_layers)
    else:
        # With large stripes, it is expected that most of our writes went to one pageserver, so we just require
        # that at least one of them has some image layers.
        assert any(shard_has_image_layers)

    # Assert that everything is still readable
    workload.validate()
