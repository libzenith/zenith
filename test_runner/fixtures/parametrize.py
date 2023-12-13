import os
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest
from _pytest.python import Metafunc

from fixtures.pg_version import PgVersion

"""
Dynamically parametrize tests by Postgres version, build type (debug/release/remote), and possibly by other parameters
"""


@pytest.fixture(scope="function", autouse=True)
def pg_version(request: FixtureRequest) -> Optional[PgVersion]:
    # Do not parametrize performance tests yet, we need to prepare grafana charts first
    if "test_runner/performance" in str(request.node.path):
        v = os.environ.get("DEFAULT_PG_VERSION")
        return PgVersion(v)

    return None


@pytest.fixture(scope="function", autouse=True)
def build_type(request: FixtureRequest) -> Optional[str]:
    # Do not parametrize performance tests yet, we need to prepare grafana charts first
    if "test_runner/performance" in str(request.node.path):
        return os.environ.get("BUILD_TYPE", "").lower()

    return None


@pytest.fixture(scope="function", autouse=True)
def pageserver_virtual_file_io_engine(request: FixtureRequest) -> Optional[str]:
    # Do not parametrize performance tests yet, we need to prepare grafana charts first
    if "test_runner/performance" in str(request.node.path):
        return os.environ.get("PAGESERVER_VIRTUAL_FILE_IO_ENGINE", "").lower()

    return None


def pytest_generate_tests(metafunc: Metafunc):
    # Do not parametrize performance tests yet, we need to prepare grafana charts first
    if "test_runner/performance" in metafunc.definition._nodeid:
        return

    if (v := os.environ.get("DEFAULT_PG_VERSION")) is None:
        pg_versions = [version for version in PgVersion if version != PgVersion.NOT_SET]
    else:
        pg_versions = [PgVersion(v)]

    if (bt := os.environ.get("BUILD_TYPE")) is None:
        build_types = ["debug", "release"]
    else:
        build_types = [bt.lower()]

    if (io_engine := os.environ.get("PAGESERVER_VIRTUAL_FILE_IO_ENGINE")) is None:
        pageserver_virtual_file_io_engines = ["std-fs", "tokio-epoll-uring"]
    else:
        pageserver_virtual_file_io_engines = [io_engine.lower()]

    metafunc.parametrize("build_type", build_types)
    metafunc.parametrize("pg_version", pg_versions, ids=map(lambda v: f"pg{v}", pg_versions))
    metafunc.parametrize("pageserver_virtual_file_io_engine", pageserver_virtual_file_io_engines)
