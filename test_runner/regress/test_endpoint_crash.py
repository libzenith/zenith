import pytest
from fixtures.neon_fixtures import Endpoint


@pytest.mark.parametrize(
    "sql_func",
    [
        "trigger_panic",
        "trigger_segfault",
        "💣",  # calls `trigger_segfault` internally
    ],
)
def test_endpoint_crash(neon_endpoint: Endpoint, sql_func: str):
    """
    Test that triggering crash from neon_test_utils crashes the endpoint
    """
    endpoint = neon_endpoint
    endpoint.safe_psql("CREATE EXTENSION neon_test_utils;")
    with pytest.raises(Exception, match="This probably means the server terminated abnormally"):
        endpoint.safe_psql(f"SELECT {sql_func}();")
