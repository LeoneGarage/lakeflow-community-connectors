#!/bin/bash
# Runs the pipelines of the orders test, a silver run after every bronze run: a bronze full
# refresh while the workload runs and a silver full refresh after it, then a triggered
# bronze update and a silver update in turn until the workload has ended, and two more
# bronze updates, each followed by a silver update, to drain what is left. Silver therefore
# ends incremental: the materialized views refresh from their previous state and the
# foreachBatch sink rewrites only the orders each batch touches.
# The silver full refresh comes while its Delta table still holds the previous run's rows,
# which the sink's first batch must clear.
# Usage: orders_run.sh <bronze pipeline id> <silver pipeline id> <workload end, UTC HHMMSS> [profile]
B=$1; SV=$2; END=$3; PROFILE=${4:-DEFAULT}
update() {  # pipeline, extra flag, label
  local u s err
  # A refused start (auth, a wrong id, an update still settling) gave no id, and the loop
  # below then polled an empty one for half an hour: say why and stop.
  err=$(mktemp)
  u=$(databricks pipelines start-update $1 $2 --profile "$PROFILE" -o json 2>"$err" | python3 -c 'import json,sys; print(json.load(sys.stdin)["update_id"])' 2>/dev/null)
  if [ -z "$u" ]; then
    echo "$(date -u +%T) ${3} start-update failed: $(head -c 500 "$err")"
    rm -f "$err"
    return 1
  fi
  rm -f "$err"
  for i in $(seq 1 180); do
    s=$(databricks pipelines get-update $1 $u --profile "$PROFILE" -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["update"]["state"])')
    case $s in COMPLETED|FAILED|CANCELED) break;; esac; sleep 10
  done
  echo "$(date -u +%T) ${3} ${u:0:8} $s"
  [ "$s" = COMPLETED ]
}
pair() {  # flag for both, bronze label, silver label: a bronze run, then a silver run
  update $B "$1" "$2" && update $SV "$1" "$3"
}
pair --full-refresh "bronze full refresh" "silver full refresh" || exit 1
# The end as a moment, not a time of day: the one nearest the clock, so a run that crosses
# midnight UTC still alternates until the workload ends, and one started just after an
# end before midnight does not wait a day.
END_AT=$(python3 -c '
import datetime, sys
now = datetime.datetime.now(datetime.timezone.utc)
end = now.replace(hour=int(sys.argv[1][:2]), minute=int(sys.argv[1][2:4]), second=int(sys.argv[1][4:6]), microsecond=0)
if now - end > datetime.timedelta(hours=12):
    end += datetime.timedelta(days=1)
elif end - now > datetime.timedelta(hours=12):
    end -= datetime.timedelta(days=1)
print(int(end.timestamp()))' "$END") || exit 1
while [ "$(date -u +%s)" -lt "$END_AT" ]; do
  pair "" "bronze update" "silver update" || exit 1
done
pair "" "bronze drain 1" "silver update" || exit 1
pair "" "bronze drain 2" "silver update" || exit 1
echo done
