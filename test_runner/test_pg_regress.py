import pytest
from fixtures.utils import mkdir_if_needed
import getpass
import os
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

# FIXME: put host + port in a fixture
HOST = 'localhost'
PORT = 55432


def test_pg_regress(zen_simple, test_output_dir, pg_distrib_dir, base_dir):

    # Connect to postgres and create a database called "regression".
    username = getpass.getuser()
    conn_str = 'host={} port={} dbname=postgres user={}'.format(
        HOST, PORT, username)
    print('conn_str is', conn_str)
    pg_conn = psycopg2.connect(conn_str)
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()
    cur.execute('CREATE DATABASE regression')
    pg_conn.close()

    # Create some local directories for pg_regress to run in.
    runpath = os.path.join(test_output_dir, 'regress')
    mkdir_if_needed(runpath)
    mkdir_if_needed(os.path.join(runpath, 'testtablespace'))

    # Compute all the file locations that pg_regress will need.
    build_path = os.path.join(
        pg_distrib_dir, 'build/src/test/regress')
    src_path = os.path.join(
        base_dir, 'vendor/postgres/src/test/regress')
    bindir = os.path.join(pg_distrib_dir, 'bin')
    schedule = os.path.join(src_path, 'parallel_schedule')
    pg_regress = os.path.join(build_path, 'pg_regress')

    pg_regress_command = [
        pg_regress,
        '--bindir=""',
        '--use-existing',
        '--bindir={}'.format(bindir),
        '--dlpath={}'.format(build_path),
        '--schedule={}'.format(schedule),
        '--inputdir={}'.format(src_path),
    ]

    env = {
        'PGPORT': str(PORT),
        'PGUSER': username,
        'PGHOST': HOST,
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.

    zen_simple.pg_bin.run(pg_regress_command, env=env, cwd=runpath)
