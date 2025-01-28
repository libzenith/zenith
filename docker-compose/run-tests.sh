#!/bin/bash
set -x

extdir=${1}

cd "${extdir}" || exit 2
FAILED=
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls) | sort | uniq -u)
for d in ${LIST}
do
       [ -d "${d}" ] || continue
       if ! psql -w -c "select 1" >/dev/null; then
          FAILED="${d} ${FAILED}"
          break
       fi
       USE_PGXS=1 make -C "${d}" installcheck || FAILED="${d} ${FAILED}"
done
[ -z "${FAILED}" ] && exit 0
echo "${FAILED}"
exit 1