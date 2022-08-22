import json
import subprocess
from urllib.parse import urlparse

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import PSQL


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1", options="project=generic-project-name")


def test_password_hack(static_proxy):
    user = "borat"
    password = "password"
    static_proxy.safe_psql(
        f"create role {user} with login password '{password}'",
        options="project=irrelevant")

    # Note the format of `magic`!
    magic = f"project=irrelevant;{password}"
    static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.errors.OperationalError):
        magic = "broken"
        static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)


async def get_session_id_from_welcome_message(local_link_proxy, proc):
    # Read line by line output of psql.
    # We expect to see result in first 3 lines. [this is flexible, change if need be]
    # Expected output:
    #   A notice that contains a welcome to neon message that contains a http url in the form of
    #   "http://dummy-uri/<psql_session_id>"
    #   (here dummy-uri was passed from the neon_fixture that created the link_proxy
    #   The url has to be on its own line.
    #   Example:
    #   "
    #   NOTICE:  Welcome to Neon!
    #   Authenticate by visiting:
    #      http://dummy-uri/<psql_session_id>
    #   "

    psql_session_id = None

    max_num_lines_of_welcome_message = 15
    line_id = 0
    for line_id in range(max_num_lines_of_welcome_message):
        raw_line = await proc.stderr.readline()
        if not raw_line:
            # output ended
            break
        line = raw_line.decode("utf-8").strip()
        if line.startswith("http"):
            url_parts = urlparse(line)
            psql_session_id = url_parts.path[1:]
            link_auth_uri = line[:-len(url_parts.path)]
            assert link_auth_uri == local_link_proxy.link_auth_uri, \
                f"Line='{line}' should contain a http auth link of form '{local_link_proxy.link_auth_uri}/<psql_session_id>'."
            break
        log.debug("line %d does not contain expected result: %s", line_id,
                  line)

    assert line_id <= max_num_lines_of_welcome_message, "exhausted given attempts, did not get the result"
    assert psql_session_id is not None, "psql_session_id not found from output of proc.stderr.readline()"
    log.info(f"Got psql_session_id={psql_session_id=}")

    return psql_session_id


def create_and_send_db_info(local_vanilla_pg, psql_session_id, mgmt_port):
    pg_user = "proxy"
    pg_password = "password"

    local_vanilla_pg.start()
    query = "create user " + pg_user + " with login superuser password '" + pg_password + "'"
    local_vanilla_pg.safe_psql(query)

    port = local_vanilla_pg.default_options['port']
    host = local_vanilla_pg.default_options['host']
    dbname = local_vanilla_pg.default_options['dbname']

    db_info_dict = {
        "session_id": psql_session_id,
        "result": {
            "Success": {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": pg_user,
                "password": pg_password
            }
        }
    }
    db_info_str = json.dumps(db_info_dict)
    cmd_line_args__to__mgmt = [
        "psql",
        "-h",
        "127.0.0.1",  # localhost
        "-p",
        f"{mgmt_port}",
        '-c',
        db_info_str
    ]

    log.info(
        f"Sending to proxy the user and db info: {cmd_line_args__to__mgmt}")
    p = subprocess.Popen(cmd_line_args__to__mgmt, stdout=subprocess.PIPE)
    out, err = p.communicate()
    assert "ok" in str(out)


@pytest.mark.asyncio
async def test_psql_session_id(vanilla_pg, link_proxy):
    """
    Test copied and modified from: test_project_psql_link_auth test from cloud/tests_e2e/tests/test_project.py
     Step 1. establish connection to the proxy
     Step 2. retrieves session_id
     Step 3. create a vanilla_pg and send user and db info via command line (using Popen) a psql query via mgmt port to proxy.
     Step 4. assert that select 1 has been executed correctly.
    """

    # Step 1.
    psql = PSQL(
        host=link_proxy.host,
        port=link_proxy.proxy_port,
    )
    proc = await psql.run("select 1")

    # Step 2.
    psql_session_id = await get_session_id_from_welcome_message(
        link_proxy, proc)

    # Step 3.
    create_and_send_db_info(vanilla_pg, psql_session_id, link_proxy.mgmt_port)

    # Step 4.
    # Expecting proxy output::
    # b' ?column? \n'
    # b'----------\n'
    # b'        1\n'
    # b'(1 row)\n'
    out_bytes = await proc.stdout.read()
    expected_out_bytes = b" ?column? \n----------\n        1\n(1 row)\n\n"
    assert out_bytes == expected_out_bytes


# Pass extra options to the server.
#
# Currently, proxy eats the extra connection options, so this fails.
# See https://github.com/neondatabase/neon/issues/1287
@pytest.mark.xfail
def test_proxy_options(static_proxy):
    with static_proxy.connect(options="-cproxytest.option=value") as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW proxytest.option")
            value = cur.fetchall()[0][0]
            assert value == "value"
