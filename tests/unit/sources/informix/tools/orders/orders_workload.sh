#!/bin/bash
# order_items workload: "delete all of an order's items, then insert new ones", "insert without deleting", and
# updates of an order's items by order_id (all of them, some of them, or only their updated_at).
# Every insert and update stamps updated_at, as an application keeping a last-update time does, and no
# UPDATE or DELETE matches rows on it: they match on order_id (and sku) as before. Change records carry
# whole rows, so the d row of a delete or of an update's old values holds the updated_at it replaced.
# No update touches order_id, the column the bronze snapshot pages by. The connector takes such an update during
# the snapshot (the stream emits the half of it the pages lack), but this workload does not exercise that.
# Usage (inside the Informix container, next to orders_lib.sh): orders_workload.sh [minutes, default 15]
export INFORMIXDIR=${INFORMIXDIR:-/opt/ibm/informix} INFORMIXSERVER=${INFORMIXSERVER:-informix}
export PATH="$INFORMIXDIR/bin:$PATH" DBACCNOIGN=1     # DBACCNOIGN: an error rolls the transaction back
. "$(dirname "$0")/orders_lib.sh"
minutes=${1:-15}; log=/tmp/orders_workload.log; out=/tmp/orders_workload.out
max=$(echo "SELECT MAX(id) FROM orders;" | dbaccess testdb - 2>/dev/null | grep -E '^ *[0-9]+ *$' | tr -d ' ')
run() { { echo "SET LOCK MODE TO WAIT 120;"; cat; } | dbaccess testdb - >>"$out" 2>&1; }
echo "$(date -u +%FT%TZ) start minutes=$minutes max_order=$max" >> "$log"
end=$((SECONDS + minutes * 60))
while [ $SECONDS -lt $end ]; do
  n=$((1 + RANDOM % max)); r=$((RANDOM % 100)); k=$((1 + RANDOM % 4))
  if [ $r -lt 30 ]; then kind=replace_one_tx      # delete + insert in one transaction
    { echo "BEGIN WORK; DELETE FROM order_items WHERE order_id = $n;"; items_sql $n $k; echo "COMMIT WORK;"; } | run; rc=$?
  elif [ $r -lt 42 ]; then kind=replace_two_tx    # delete commits, then the inserts commit separately
    echo "DELETE FROM order_items WHERE order_id = $n;" | run; rc=$?
    { echo "BEGIN WORK;"; items_sql $n $k; echo "COMMIT WORK;"; } | run; rc=$((rc + $?))
  elif [ $r -lt 50 ]; then kind=replace_split_inserts   # delete, then inserts spread over two transactions
    { echo "BEGIN WORK; DELETE FROM order_items WHERE order_id = $n;"; items_sql $n 1; echo "COMMIT WORK;"; } | run; rc=$?
    { echo "BEGIN WORK;"; items_sql $n $k; echo "COMMIT WORK;"; } | run; rc=$((rc + $?))
  elif [ $r -lt 72 ]; then kind=insert_only       # add items, nothing deleted
    { echo "BEGIN WORK;"; items_sql $n $k; echo "COMMIT WORK;"; } | run; rc=$?
  elif [ $r -lt 84 ]; then kind=update_all        # every item of the order; qty stays within 1..3, so copies stay copies
    echo "UPDATE order_items SET qty = 1 + MOD(qty, 3), updated_at = CURRENT YEAR TO FRACTION(5) WHERE order_id = $n;" | run; rc=$?
  elif [ $r -lt 90 ]; then kind=update_some       # the items of one sku only: some copies now differ from others
    echo "UPDATE order_items SET note = 'checked', updated_at = CURRENT YEAR TO FRACTION(5) WHERE order_id = $n AND sku = 'SKU-000$((1 + RANDOM % 6))';" | run; rc=$?
  elif [ $r -lt 92 ]; then kind=update_stamp      # only updated_at changes: the earlier row is overwritten by its stamp
    echo "UPDATE order_items SET updated_at = CURRENT YEAR TO FRACTION(5) WHERE order_id = $n;" | run; rc=$?
  else kind=new_order; max=$((max + 1)); n=$max
    { echo "BEGIN WORK; INSERT INTO orders (id, customer, created) VALUES ($n, 'Customer $n', CURRENT YEAR TO FRACTION(5));"; items_sql $n $k; echo "COMMIT WORK;"; } | run; rc=$?
  fi
  echo "$(date -u +%FT%TZ) $kind order=$n rc=$rc" >> "$log"
  sleep 1
done
echo "$(date -u +%FT%TZ) end" >> "$log"
