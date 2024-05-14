import requests
from contextlib import closing

from fixtures.neon_fixtures import NeonEnv

def test_compute_catalog(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_config", "empty")

    endpoint = env.endpoints.create_start("test_config", config_lines=["log_min_messages=debug1"])
    client = endpoint.http_client()

    objects = client.catalog_objects()

    # Assert that 'cloud_admin' role exists in the 'roles' list
    assert any(role['name'] == 'cloud_admin' for role in objects['roles']), "The 'cloud_admin' role is missing"

    # Assert that 'postgres' database exists in the 'databases' list
    assert any(db['name'] == 'postgres' for db in objects['databases']), "The 'postgres' database is missing"

    ddl = client.catalog_schema_ddl(database='postgres')

    assert "-- PostgreSQL database dump" in ddl

    try:
        client.catalog_schema_ddl(database='nonexistentdb')
        assert False, "Expected HTTPError was not raised"
    except requests.exceptions.HTTPError as e:
        assert e.response.status_code == 404, f"Expected 404 status code, but got {e.response.status_code}"
