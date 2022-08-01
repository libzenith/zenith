import os
from pathlib import Path
import pytest
from fixtures.neon_fixtures import NeonEnv, base_dir, pg_distrib_dir


# The isolation tests run for a long time, especially in debug mode,
# so use a larger-than-default timeout.
@pytest.mark.timeout(1800)
def test_isolation(neon_simple_env: NeonEnv, test_output_dir: Path, pg_bin, capsys):
    env = neon_simple_env

    env.neon_cli.create_branch("test_isolation", "empty")
    # Connect to postgres and create a database called "regression".
    # isolation tests use prepared transactions, so enable them
    pg = env.postgres.create_start('test_isolation', config_lines=['max_prepared_transactions=100'])
    pg.safe_psql('CREATE DATABASE isolation_regression')

    # Create some local directories for pg_isolation_regress to run in.
    runpath = test_output_dir / 'regress'
    (runpath / 'testtablespace').mkdir(parents=True)

    # Find the pg_isolation_regress binary
    proc = pg_bin.run(['pg_config', '--libdir'], capture_output=True)
    libdir = proc.stdout.decode().strip()
    proc = pg_bin.run(['pg_config', '--bindir'], capture_output=True)
    bindir = proc.stdout.decode().strip()
    pg_isolation_regress = os.path.join(libdir,
                                        'postgresql/pgxs/src/test/isolation/pg_isolation_regress')

    # Compute all the file locations that pg_isolation_regress will need.
    src_path = os.path.join(base_dir, 'vendor/postgres/src/test/isolation')
    schedule = os.path.join(src_path, 'isolation_schedule')

    pg_isolation_regress_command = [
        pg_isolation_regress,
        '--use-existing',
        '--bindir={}'.format(bindir),
        '--inputdir={}'.format(src_path),
        '--schedule={}'.format(schedule),
    ]

    env_vars = {
        'PGPORT': str(pg.default_options['port']),
        'PGUSER': pg.default_options['user'],
        'PGHOST': pg.default_options['host'],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_isolation_regress_command, env=env_vars, cwd=runpath)
