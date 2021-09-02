import os
from contextlib import closing
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")

def get_timeline_size(repo_dir: str, tenantid: str, timelineid: str):
    path = "{}/tenants/{}/timelines/{}".format(repo_dir, tenantid, timelineid)

    totalbytes = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            totalbytes += os.path.getsize(os.path.join(root, name))

        if 'wal' in dirs:
            dirs.remove('wal')  # don't visit 'wal' subdirectory

    return totalbytes

def get_dir_size(path: str):

    totalbytes = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            totalbytes += os.path.getsize(os.path.join(root, name))

    return totalbytes

#
# Run a very short pgbench test.
#
# Collects three metrics:
#
# 1. Time to initialize the pgbench database (pgbench -s5 -i)
# 2. Time to run 5000 pgbench transactions
# 3. Disk space used
#
def test_pgbench(postgres: PostgresFactory, pageserver: ZenithPageserver, pg_bin, zenith_cli, zenbenchmark, repo_dir: str):
    # Create a branch for us
    zenith_cli.run(["branch", "test_pgbench_perf", "empty"])

    pg = postgres.create_start('test_pgbench_perf')
    print("postgres is running on 'test_pgbench_perf' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = pageserver.connect();
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

    connstr = pg.connstr()

    # Initialize pgbench database, recording the time and I/O it takes
    with zenbenchmark.record_pageserver_writes(pageserver, 'pageserver_writes'):
        with zenbenchmark.record_duration('init'):
            pg_bin.run_capture(['pgbench', '-s5', '-i', connstr])

            # Flush the layers from memory to disk. This is included in the reported
            # time and I/O
            pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")

    # Run pgbench for 5000 transactions
    with zenbenchmark.record_duration('5000_xacts'):
        pg_bin.run_capture(['pgbench', '-c1', '-t5000', connstr])

    # Flush the layers to disk again. This is *not' included in the reported time,
    # though.
    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")

    # Report disk space used by the repository
    timeline_size = get_timeline_size(repo_dir, pageserver.initial_tenant, timeline)
    zenbenchmark.record('size', timeline_size / (1024*1024), 'MB')


def test_baseline_pgbench(test_output_dir, pg_bin, zenbenchmark):
    print("test_output_dir: " + test_output_dir)
    pgdatadir = os.path.join(test_output_dir, 'pgdata-vanilla')
    print("pgdatadir: " + pgdatadir)
    pg_bin.run_capture(['initdb', '-D', pgdatadir])

    conf = open(os.path.join(pgdatadir, 'postgresql.conf'), 'a')
    conf.write('shared_buffers=1MB\n')
    conf.close()

    pg_bin.run_capture(['pg_ctl', '-D', pgdatadir, 'start'])

    connstr = 'host=localhost port=5432 dbname=postgres'
    conn = psycopg2.connect(connstr)
    cur = conn.cursor()

    with zenbenchmark.record_duration('init'):
        pg_bin.run_capture(['pgbench', '-s5', '-i', connstr])

        # This is roughly equivalent to flushing the layers from memory to disk with Zenith.
        cur.execute(f"checkpoint")

    # Run pgbench for 5000 transactions
    with zenbenchmark.record_duration('5000_xacts'):
        pg_bin.run_capture(['pgbench', '-c1', '-t5000', connstr])

    # This is roughly equivalent to flush the layers from memory to disk with Zenith.
    cur.execute(f"checkpoint")

    # Report disk space used by the repository
    data_size = get_dir_size(os.path.join(pgdatadir, 'base'))
    zenbenchmark.record('data_size', data_size / (1024*1024), 'MB')
    wal_size = get_dir_size(os.path.join(pgdatadir, 'pg_wal'))
    zenbenchmark.record('wal_size', wal_size / (1024*1024), 'MB')
