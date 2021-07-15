pytest_plugins = ("fixtures.zenith_fixtures")


#
# Create a couple of branches off the main branch, at a historical point in time.
#
def test_max_timeline_size(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_max_timeline_size", "main"])

    pgmain = postgres.create_start('test_max_timeline_size', config_lines=['zenith.max_timeline_size=30MB'])
    print("postgres is running on 'test_max_timeline_size' branch")

    main_pg_conn = pgmain.connect()
    main_cur = main_pg_conn.cursor()

    # Create table, and insert the first 100 rows
    main_cur.execute('CREATE TABLE foo (t text)')
    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100) g
    ''')

    main_cur.execute('SELECT pg_size_pretty(pg_table_size(%s))', ("foo",))

    table_size = main_cur.fetchone()[0]
    print('table_size after 100 rows: ' + table_size)

    # Insert some more rows. This query should fail because of space limit
    try:
        main_cur.execute('''
            INSERT INTO foo
                SELECT 'long string to consume some space' || g
                FROM generate_series(1, 10000) g
        ''')
    except Exception as err:
        print ("Query failed with:", err)
        # 53100 is a disk_full errcode
        assert err.pgcode == 53100