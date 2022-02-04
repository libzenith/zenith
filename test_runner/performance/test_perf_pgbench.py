from contextlib import closing
from fixtures.zenith_fixtures import PgBin, VanillaPostgres, ZenithEnv

from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


def pgbench_init(pg_bin: PgBin, connstr: str):
    pg_bin.run_capture(['pgbench', '-s5', '-i', connstr])


def pgbench_run_5000_transactions(pg_bin: PgBin, connstr: str):
    pg_bin.run_capture(['pgbench', '-c1', '-t5000', connstr])


#
# Run a very short pgbench test.
#
# Collects three metrics:
#
# 1. Time to initialize the pgbench database (pgbench -s5 -i)
# 2. Time to run 5000 pgbench transactions
# 3. Disk space used
#
def test_pgbench(zenith_simple_env: ZenithEnv, pg_bin: PgBin, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_pgbench_perf", "empty"])

    pg = env.postgres.create_start('test_pgbench_perf')
    log.info("postgres is running on 'test_pgbench_perf' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

    connstr = pg.connstr()

    # Initialize pgbench database, recording the time and I/O it takes
    with zenbenchmark.record_pageserver_writes(env.pageserver, 'pageserver_writes'):
        with zenbenchmark.record_duration('init'):
            pgbench_init(pg_bin, connstr)

            # Flush the layers from memory to disk. This is included in the reported
            # time and I/O
            pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")

    # Run pgbench for 5000 transactions
    with zenbenchmark.record_duration('5000_xacts'):
        pgbench_run_5000_transactions(pg_bin, connstr)

    # Flush the layers to disk again. This is *not' included in the reported time,
    # though.
    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")

    # Report disk space used by the repository
    timeline_size = zenbenchmark.get_timeline_size(env.repo_dir, env.initial_tenant, timeline)
    zenbenchmark.record('size',
                        timeline_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)


def test_pgbench_baseline(vanilla_pg: VanillaPostgres, zenbenchmark: ZenithBenchmarker):
    vanilla_pg.configure(['shared_buffers=1MB'])
    vanilla_pg.start()

    pg_bin = vanilla_pg.pg_bin
    connstr = vanilla_pg.connstr()
    conn = vanilla_pg.connect()
    cur = conn.cursor()

    with zenbenchmark.record_duration('init'):
        pgbench_init(pg_bin, connstr)

        # This is roughly equivalent to flushing the layers from memory to disk with Zenith.
        cur.execute(f"checkpoint")

    # Run pgbench for 5000 transactions
    with zenbenchmark.record_duration('5000_xacts'):
        pgbench_run_5000_transactions(pg_bin, connstr)

    # This is roughly equivalent to flush the layers from memory to disk with Zenith.
    cur.execute(f"checkpoint")

    # Report disk space used by the repository
    data_size = vanilla_pg.get_subdir_size('base')
    zenbenchmark.record('data_size',
                        data_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)
    wal_size = vanilla_pg.get_subdir_size('pg_wal')
    zenbenchmark.record('wal_size',
                        wal_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)
