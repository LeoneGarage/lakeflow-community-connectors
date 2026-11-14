#!/bin/bash
# Runs the pipelines of the orders test: a bronze full refresh while the workload runs,
# triggered bronze updates until the workload has ended and drained, then a silver full
# refresh. Usage: orders_run.sh <bronze pipeline id> <silver pipeline id> <workload end, UTC HHMMSS> [profile]
B=$1; SV=$2; END=$3; PROFILE=${4:-DEFAULT}
update() {  # pipeline, extra flag, label
  local u s
  u=$(databricks pipelines start-update $1 $2 --profile "$PROFILE" -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["update_id"])')
  for i in $(seq 1 180); do
    s=$(databricks pipelines get-update $1 $u --profile "$PROFILE" -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["update"]["state"])')
    case $s in COMPLETED|FAILED|CANCELED) break;; esac; sleep 10
  done
  echo "$(date -u +%T) ${3} ${u:0:8} $s"
  [ "$s" = COMPLETED ]
}
update $B --full-refresh "bronze full refresh" || exit 1
while [ "$(date -u +%H%M%S)" \< "$END" ]; do update $B "" "bronze update" || exit 1; done
update $B "" "bronze drain 1" || exit 1
update $B "" "bronze drain 2" || exit 1
update $SV --full-refresh "silver full refresh" || exit 1
echo done
