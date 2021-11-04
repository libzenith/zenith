# pytest some has quirks with discovering plugins, so having it there just works
# probably we should create custom plugin and add it to pytest config to always have needed things at hand
def pytest_addoption(parser):
    parser.addoption(
        "--out-dir",
        dest="out_dir",
        help="Directory to ouput performance tests results to.",
    )
