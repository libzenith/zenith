#!/bin/sh
set -ex
cd "$(dirname ${0})"
PG_REGRESS=$(dirname "$(pg_config --pgxs)")/../test/regress/pg_regress
${PG_REGRESS} --use-existing --inputdir=./ --bindir='/usr/local/pgsql/bin'    --dbname=contrib_regression add_agg agg_oob auto_sparse card_op cast_shape copy_binary cumulative_add_cardinality_correction cumulative_add_comprehensive_promotion cumulative_add_sparse_edge cumulative_add_sparse_random cumulative_add_sparse_step cumulative_union_comprehensive cumulative_union_explicit_explicit cumulative_union_explicit_promotion cumulative_union_probabilistic_probabilistic cumulative_union_sparse_full_representation cumulative_union_sparse_promotion cumulative_union_sparse_sparse disable_hashagg equal explicit_thresh hash hash_any meta_func murmur_bigint murmur_bytea nosparse notequal scalar_oob storedproc transaction typmod typmod_insert union_op