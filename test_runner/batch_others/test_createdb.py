from contextlib import closing
from fixtures.zenith_fixtures import ZenithPageserver, PostgresFactory, ZenithCli

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test CREATE DATABASE when there have been relmapper changes
#
def test_createdb(
    zenith_cli: ZenithCli,
    pageserver: ZenithPageserver,
    postgres: PostgresFactory,
    pg_bin,
):
    zenith_cli.run(["branch", "test_createdb", "empty"])

    pg = postgres.create_start('test_createdb')
    print("postgres is running on 'test_createdb' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # Cause a 'relmapper' change in the original branch
            cur.execute('VACUUM FULL pg_class')

            cur.execute('CREATE DATABASE foodb')

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]

    # Create a branch
    zenith_cli.run(["branch", "test_createdb2", "test_createdb@" + lsn])

    pg2 = postgres.create_start('test_createdb2')

    # Test that you can connect to the new database on both branches
    for db in (pg, pg2):
        with closing(db.connect(dbname='foodb')) as conn:
            with conn.cursor() as cur:
                # Check database size in both branches
                cur.execute('select pg_size_pretty(pg_database_size(%s)), pg_size_pretty(sum(pg_relation_size(oid))) from pg_class where relisshared is false;', ('foodb',))
                res = cur.fetchone()
                # check that dbsize equals sum of all relation sizes, excluding shared ones
                # This is how we define dbsize in zenith for now
                assert res[0] == res[1]
