pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test branching, when a transaction is in prepared state
#
def test_twophase(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_twophase", "empty"])

    pg = postgres.create_start('test_twophase', config_lines=['max_prepared_transactions=5'])
    print("postgres is running on 'test_twophase' branch")

    conn = pg.connect()
    cur = conn.cursor()

    cur.execute('CREATE TABLE foo (t text)')

    # Prepare a transaction that will insert a row
    cur.execute('BEGIN')
    cur.execute("INSERT INTO foo VALUES ('one')")
    cur.execute("PREPARE TRANSACTION 'insert_one'")

    # Prepare another transaction that will insert a row
    cur.execute('BEGIN')
    cur.execute("INSERT INTO foo VALUES ('two')")
    cur.execute("PREPARE TRANSACTION 'insert_two'")

    # Create a branch with the transaction in prepared state
    zenith_cli.run(["branch", "test_twophase_prepared", "test_twophase"])

    pg2 = postgres.create_start('test_twophase_prepared',
                                config_lines=['max_prepared_transactions=5'])
    conn2 = pg2.connect()
    cur2 = conn2.cursor()

    # On the new branch, commit one of the prepared transactions, abort the other one.
    cur2.execute("COMMIT PREPARED 'insert_one'")
    cur2.execute("ROLLBACK PREPARED 'insert_two'")

    cur2.execute('SELECT * FROM foo')
    assert cur2.fetchall() == [('one', )]

    # Neither insert is visible on the original branch, the transactions are still
    # in prepared state there.
    cur.execute('SELECT * FROM foo')
    assert cur.fetchall() == []
