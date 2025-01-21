#!/bin/bash
set -x

cd /ext-src || exit 2
export FAILED_FILE=/tmp/failed
touch ${FAILED_FILE}
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls -d -- *-src) | sort | uniq -u)
parallel -j3 '[ -d {} ] || exit 0; if ! psql -c "select 1" >/dev/null; then {echo {} >> $FAILED_FILE; exit 1; fi ; USE_PGXS=1 PGHOST=pcompute{%} make -C {} installcheck || echo {} >> $FAILED_FILE;' ::: ${LIST}
FAILED=$(cat $FAILED_FILE)
[ -z "${FAILED}" ] && exit 0
echo ${FAILED}
exit 1