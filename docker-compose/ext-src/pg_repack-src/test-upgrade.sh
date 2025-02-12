#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./regress --bindir='/usr/local/pgsql/bin' --dbname=contrib_regression repack-setup repack-run error-on-invalid-idx no-error-on-invalid-idx after-schema repack-check nosuper get_order_by trigger
