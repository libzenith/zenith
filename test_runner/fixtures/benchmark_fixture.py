import dataclasses
import json
import os
from pathlib import Path
import re
import subprocess
import timeit
import pytest
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
import warnings

from contextlib import contextmanager

# Type-related stuff
from typing import Iterator, Optional


"""
This file contains fixtures for micro-benchmarks.

To use, declare the 'zenbenchmark' fixture in the test function. Run the
bencmark, and then record the result by calling zenbenchmark.record. For example:

import timeit
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")

def test_mybench(postgres: PostgresFactory, pageserver: ZenithPageserver, zenbenchmark):

    # Initialize the test
    ...

    # Run the test, timing how long it takes
    with zenbenchmark.record_duration('test_query'):
        cur.execute('SELECT test_query(...)')

    # Record another measurement
    zenbenchmark.record('speed_of_light', 300000, 'km/s')


You can measure multiple things in one test, and record each one with a separate
call to zenbenchmark. For example, you could time the bulk loading that happens
in the test initialization, or measure disk usage after the test query.

"""


def strip_float(f: str):
    # strip float to two decimals
    return "{:.2f}".format(float(f))


@dataclasses.dataclass
class PgBenchRunResult:
    scale: int
    number_of_clients: int
    number_of_threads: int
    number_of_transactions_actually_processed: int
    latency_average: str
    latency_stddev: str
    tps_including_connection_time: str
    tps_excluding_connection_time: str
    init_duration: str
    init_start_timestamp: int
    init_end_timestamp: int
    run_duration: int
    run_start_timestamp: int
    run_end_timestamp: int

    # TODO progress

    @classmethod
    def parse_from_output(
        cls,
        out: subprocess.CompletedProcess,
        init_duration: float,
        init_start_timestamp: int,
        init_end_timestamp: int,
        run_duration: float,
        run_start_timestamp: int,
        run_end_timestamp: int,
    ):
        stdout_lines = out.stdout.decode().splitlines()
        # we know significant parts of these values from test input
        # but to be precise take them from output
        # scaling factor: 5
        assert "scaling factor" in stdout_lines[1]
        scale = int(stdout_lines[1].split()[-1])
        # number of clients: 1
        assert "number of clients" in stdout_lines[3]
        number_of_clients = int(stdout_lines[3].split()[-1])
        # number of threads: 1
        assert "number of threads" in stdout_lines[4]
        number_of_threads = int(stdout_lines[4].split()[-1])
        # number of transactions actually processed: 1000/1000
        assert "number of transactions actually processed" in stdout_lines[6]
        number_of_transactions_actually_processed = int(stdout_lines[6].split("/")[1])
        # latency average = 19.894 ms
        assert "latency average" in stdout_lines[7]
        latency_average = strip_float(stdout_lines[7].split()[-2])
        # latency stddev = 3.387 ms
        assert "latency stddev" in stdout_lines[8]
        latency_stddev = strip_float(stdout_lines[8].split()[-2])
        # tps = 50.219689 (including connections establishing)
        assert "(including connections establishing)" in stdout_lines[9]
        tps_including_connection_time = strip_float(stdout_lines[9].split()[2])
        # tps = 50.264435 (excluding connections establishing)
        assert "(excluding connections establishing)" in stdout_lines[10]
        tps_excluding_connection_time = strip_float(stdout_lines[10].split()[2])

        return cls(
            scale=scale,
            number_of_clients=number_of_clients,
            number_of_threads=number_of_threads,
            number_of_transactions_actually_processed=number_of_transactions_actually_processed,
            latency_average=latency_average,
            latency_stddev=latency_stddev,
            tps_including_connection_time=tps_including_connection_time,
            tps_excluding_connection_time=tps_excluding_connection_time,
            init_duration=init_duration,
            init_start_timestamp=init_start_timestamp,
            init_end_timestamp=init_end_timestamp,
            run_duration=run_duration,
            run_start_timestamp=run_start_timestamp,
            run_end_timestamp=run_end_timestamp,
        )

class ZenithBenchmarker:
    """
    An object for recording benchmark results. This is created for each test
    function by the zenbenchmark fixture
    """

    def __init__(self, property_recorder, test_name: str):
        # property recorder here is a pytest fixture provided by junitxml module
        # https://docs.pytest.org/en/6.2.x/reference.html#pytest.junitxml.record_property
        self.property_recorder = property_recorder
        # self.test_name = test_name # TODO not needed

    def record(
        self,
        metric_name: str,
        metric_value: float,
        unit: str = "",
        higher_is_better: Optional[bool] = None,
    ):
        """
        Record a benchmark result.
        """
        # just to namespace the value
        name = f"zenith_benchmarker_{metric_name}"
        self.property_recorder(
            name,
            {
                "name": metric_name,
                "value": metric_value,
                "unit": unit,
                "higher_is_better": higher_is_better,
            },
        )

    @contextmanager
    def record_duration(self, metric_name: str):
        """
        Record a duration. Usage:

        with zenbenchmark.record_duration('foobar_runtime'):
            foobar()   # measure this
        """
        start = timeit.default_timer()
        yield
        end = timeit.default_timer()

        self.record(
            metric_name=metric_name,
            metric_value=end - start,
            unit="s",
            higher_is_better=False,
        )

    def record_pg_bench_result(self, pg_bench_result: PgBenchRunResult):
        self.record("scale", pg_bench_result.scale)
        self.record("number_of_clients", pg_bench_result.number_of_clients)
        self.record("number_of_threads", pg_bench_result.number_of_threads)
        self.record(
            "number_of_transactions_actually_processed",
            pg_bench_result.number_of_transactions_actually_processed,
            higher_is_better=True,
        )
        self.record("latency_average", pg_bench_result.latency_average, unit="ms", higher_is_better=False)
        self.record("latency_stddev", pg_bench_result.latency_stddev, unit="ms", higher_is_better=False)
        self.record(
            "tps_including_connection_time",
            pg_bench_result.tps_including_connection_time,
            higher_is_better=True
        )
        self.record(
            "tps_excluding_connection_time",
            pg_bench_result.tps_excluding_connection_time,
            higher_is_better=True
        )
        self.record("init_duration", pg_bench_result.init_duration, unit="s", higher_is_better=False)
        self.record("init_start_timestamp", pg_bench_result.init_start_timestamp)
        self.record("init_end_timestamp", pg_bench_result.init_end_timestamp)
        self.record("run_duration", pg_bench_result.run_duration, unit="s", higher_is_better=False)
        self.record("run_start_timestamp", pg_bench_result.run_start_timestamp)
        self.record("run_end_timestamp", pg_bench_result.run_end_timestamp)

    def get_io_writes(self, pageserver) -> int:
        """
        Fetch the "cumulative # of bytes written" metric from the pageserver
        """
        # Fetch all the exposed prometheus metrics from page server
        all_metrics = pageserver.http_client().get_metrics()
        # Use a regular expression to extract the one we're interested in
        #
        # TODO: If we start to collect more of the prometheus metrics in the
        # performance test suite like this, we should refactor this to load and
        # parse all the metrics into a more convenient structure in one go.
        #
        # The metric should be an integer, as it's a number of bytes. But in general
        # all prometheus metrics are floats. So to be pedantic, read it as a float
        # and round to integer.
        matches = re.search(
            r'pageserver_disk_io_bytes{io_operation="write"} (\S+)', all_metrics
        )
        return int(round(float(matches.group(1))))

    @contextmanager
    def record_pageserver_writes(self, pageserver, metric_name):
        """
        Record bytes written by the pageserver during a test.
        """
        before = self.get_io_writes(pageserver)
        yield
        after = self.get_io_writes(pageserver)

        self.record(metric_name, round((after - before) / (1024 * 1024)), "MB", higher_is_better=False)


@pytest.fixture(scope="function")
def zenbenchmark(record_property, request) -> Iterator[ZenithBenchmarker]:
    """
    This is a python decorator for benchmark fixtures. It contains functions for
    recording measurements, and prints them out at the end.
    """
    benchmarker = ZenithBenchmarker(record_property, request.node.name)
    yield benchmarker


def get_out_path(target_dir: Path, revision: str):
    """
    get output file path
    if running in the CI uses commit revision
    to avoid duplicates uses counter
    """
    return target_dir / f"{len(list(target_dir.iterdir())) + 1}_{revision}.json"


# Hook to print the results at the end
@pytest.hookimpl(hookwrapper=True)
def pytest_terminal_summary(
    terminalreporter: TerminalReporter, exitstatus: int, config: Config
):
    yield
    revision = os.getenv("GITHUB_SHA", "local")
    # TODO maybe extract platform info on per test case basis passed via parametriation
    platform = os.getenv("PLATFORM", "local")

    terminalreporter.section("Benchmark results", "-")

    result = []
    for test_report in terminalreporter.stats.get("passed", []):
        result_entry = []

        for _, recorded_property in test_report.user_properties:
            terminalreporter.write(
                "{}.{}: ".format(test_report.head_line, recorded_property["name"])
            )
            unit = recorded_property["unit"]
            value = recorded_property["value"]
            if unit == "MB":
                terminalreporter.write("{0:,.0f}".format(value), green=True)
            elif (unit == "s" or unit == "ms") and isinstance(value, float):
                terminalreporter.write("{0:,.3f}".format(value), green=True)
            elif isinstance(value, float):
                terminalreporter.write("{0:,.4f}".format(value), green=True)
            else:
                terminalreporter.write(str(value), green=True)
            terminalreporter.line(" {}".format(unit))

            result_entry.append(recorded_property)

        result.append(
            {
                "suit": test_report.nodeid,
                "total_duration": test_report.duration,
                "data": result_entry,
            }
        )

    out_dir = config.getoption("out_dir")
    if out_dir is None:
        warnings.warn("no out dir provided to store performance test results")
        return

    get_out_path(Path(out_dir), revision=revision).write_text(
        json.dumps(
            {"revision": revision, "platform": platform, "result": result}, indent=4
        )
    )
