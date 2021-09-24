# Zenith

Zenith substitutes PostgreSQL storage layer and redistributes data across a cluster of nodes

## Architecture overview

A Zenith installation consists of Compute nodes and Storage engine.

Compute nodes are stateless PostgreSQL nodes, backed by zenith storage.

Zenith storage engine consists of two major components:
- Pageserver. Scalable storage backend for compute nodes.
- WAL service. The service that receives WAL from compute node and ensures that it is stored durably.

Pageserver consists of:
- Repository - Zenith storage implementation.
- WAL receiver - service that receives WAL from WAL service and stores it in the repository.
- Page service - service that communicates with compute nodes and responds with pages from the repository.
- WAL redo - service that builds pages from base images and WAL records on Page service request.

## Running local installation

1. Install build dependencies and other useful packages

On Ubuntu or Debian this set of packages should be sufficient to build the code:
```text
apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang pkg-config libpq-dev
```

[Rust] 1.52 or later is also required.

To run the `psql` client, install the `postgresql-client` package or modify `PATH` and `LD_LIBRARY_PATH` to include `tmp_install/bin` and `tmp_install/lib`, respectively.

To run the integration tests (not required to use the code), install
Python (3.6 or higher), and install python3 packages with `pipenv` using `pipenv install` in the project directory.

2. Build zenith and patched postgres
```sh
git clone --recursive https://github.com/zenithdb/zenith.git
cd zenith
make -j5
```

3. Start pageserver and postgres on top of it (should be called from repo root):
```sh
# Create repository in .zenith with proper paths to binaries and data
# Later that would be responsibility of a package install script
> ./target/debug/zenith init
pageserver init succeeded

# start pageserver
> ./target/debug/zenith start
Starting pageserver at '127.0.0.1:64000' in .zenith
Pageserver started

# start postgres on top on the pageserver
> ./target/debug/zenith pg start main
Starting postgres node at 'host=127.0.0.1 port=55432 user=stas'
waiting for server to start.... done

# check list of running postgres instances
> ./target/debug/zenith pg list
BRANCH	ADDRESS		LSN		STATUS
main	127.0.0.1:55432	0/1609610	running
```

4. Now it is possible to connect to postgres and run some queries:
```text
> psql -p55432 -h 127.0.0.1 -U zenith_admin postgres
postgres=# CREATE TABLE t(key int primary key, value text);
CREATE TABLE
postgres=# insert into t values(1,1);
INSERT 0 1
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)
```

5. And create branches and run postgres on them:
```sh
# create branch named migration_check
> ./target/debug/zenith branch migration_check main
Created branch 'migration_check' at 0/1609610

# check branches tree
> ./target/debug/zenith branch
 main
 ┗━ @0/1609610: migration_check

# start postgres on that branch
> ./target/debug/zenith pg start migration_check
Starting postgres node at 'host=127.0.0.1 port=55433 user=stas'
waiting for server to start.... done

# this new postgres instance will have all the data from 'main' postgres,
# but all modifications would not affect data in original postgres
> psql -p55433 -h 127.0.0.1 -U zenith_admin postgres
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

postgres=# insert into t values(2,2);
INSERT 0 1
```

## Running tests

```sh
git clone --recursive https://github.com/zenithdb/zenith.git
make # builds also postgres and installs it to ./tmp_install
cd test_runner
pytest
```

If you see a `Found interfering processes running` error, stop pageserver and postgres nodes for all branches:

```sh
./target/debug/zenith pg stop migration_check
./target/debug/zenith pg stop main
./target/debug/zenith stop
```

## Documentation

Now we use README files to cover design ideas and overall architecture for each module and `rustdoc` style documentation comments. See also [/docs/](/docs/) a top-level overview of all available markdown documentation.

- [/docs/sourcetree.md](/docs/sourcetree.md) contains overview of source tree layout.

To view your `rustdoc` documentation in a browser, try running `cargo doc --no-deps --open`

### Postgres-specific terms

Due to Zenith's very close relation with PostgreSQL internals, there are numerous specific terms used.
Same applies to certain spelling: i.e. we use MB to denote 1024 * 1024 bytes, while MiB would be technically more correct, it's inconsistent with what PostgreSQL code and its documentation use.

To get more familiar with this aspect, refer to:

- [Zenith glossary](/docs/glossary.md)
- [PostgreSQL glossary](https://www.postgresql.org/docs/13/glossary.html)
- Other PostgreSQL documentation and sources (Zenith fork sources can be found [here](https://github.com/zenithdb/postgres))

## Join the development

- Read `CONTRIBUTING.md` to learn about project code style and practices.
- To get familiar with a source tree layout, use [/docs/sourcetree.md](/docs/sourcetree.md).
- To learn more about PostgreSQL internals, check http://www.interdb.jp/pg/index.html
