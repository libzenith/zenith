from typing import List
import threading
import pytest
from fixtures.neon_fixtures import NeonEnv, PgBin, Postgres
import time
import random
from fixtures.log_helper import log
from performance.test_perf_pgbench import get_scales_matrix


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
def test_branching_with_pgbench(neon_simple_env: NeonEnv,
                                pg_bin: PgBin,
                                n_branches: int,
                                scale: int,
                                ty: str):
    env = neon_simple_env

    # Use aggressive GC and checkpoint settings, so that we also exercise GC during the test
    tenant, _ = env.neon_cli.create_tenant(
         conf={
             'gc_period': '5 s',
             'gc_horizon': f'{1024 ** 2}',
             'checkpoint_distance': f'{1024 ** 2}',
             'compaction_target_size': f'{1024 ** 2}',
             # set PITR interval to be small, so we can do GC
             'pitr_interval': '5 s'
         })

    def run_pgbench(pg: Postgres):
        connstr = pg.connstr()

        log.info(f"Start a pgbench workload on pg {connstr}")

        pg_bin.run_capture(['pgbench', '-i', f'-s{scale}', connstr])
        pg_bin.run_capture(['pgbench', '-T15', connstr])

    env.neon_cli.create_branch('b0', tenant_id=tenant)
    pgs: List[Postgres] = []
    pgs.append(env.postgres.create_start('b0', tenant_id=tenant))

    threads: List[threading.Thread] = []
    threads.append(threading.Thread(target=run_pgbench, args=(pgs[0], ), daemon=True))
    threads[-1].start()

    thread_limit = 4

    for i in range(n_branches):
        # random a delay between [0, 5]
        delay = random.random() * 5
        time.sleep(delay)
        log.info(f"Sleep {delay}s")

        # If the number of concurrent threads exceeds a threshold,
        # wait for all the threads to finish before spawning a new one.
        # Because tests defined in `batch_others` are run concurrently in CI,
        # we want to avoid the situation that one test exhausts resources for other tests.
        if len(threads) >= thread_limit:
            for thread in threads:
                thread.join()
            threads = []

        if ty == "cascade":
            env.neon_cli.create_branch('b{}'.format(i + 1), 'b{}'.format(i), tenant_id=tenant)
        else:
            env.neon_cli.create_branch('b{}'.format(i + 1), 'b0', tenant_id=tenant)

        pgs.append(env.postgres.create_start('b{}'.format(i + 1), tenant_id=tenant))

        threads.append(threading.Thread(target=run_pgbench, args=(pgs[-1], ), daemon=True))
        threads[-1].start()

    for thread in threads:
        thread.join()

    for pg in pgs:
        res = pg.safe_psql('SELECT count(*) from pgbench_accounts')
        assert res[0] == (100000 * scale, )
