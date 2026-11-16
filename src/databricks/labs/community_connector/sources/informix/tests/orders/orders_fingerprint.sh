#!/bin/bash
# The source side of the comparison: order_items summed up in 25 buckets, one line each,
# as orders_compare_test_tool.py reads them. Run inside the Informix container once the
# workload has ended; save the output to a file and pass that file to the tool.
# updated_at counts by its day and its time of day to the 10 microseconds (HHMMSS and five
# fraction digits), sums small enough to stay exact where the tool reads them as floats.
export INFORMIXDIR=${INFORMIXDIR:-/opt/ibm/informix} INFORMIXSERVER=${INFORMIXSERVER:-informix}
export PATH="$INFORMIXDIR/bin:$PATH"
out=/tmp/orders_fingerprint.unl
dbaccess testdb - >/dev/null 2>&1 <<SQL
UNLOAD TO '$out' DELIMITER '|'
SELECT MOD(order_id, 25), COUNT(*), SUM(order_id), SUM(qty), SUM(price), SUM(order_id * qty * price), COUNT(note),
  SUM(DAY(updated_at)), SUM(CAST(TO_CHAR(updated_at, '%H%M%S%F5') AS INT8))
FROM order_items GROUP BY 1 ORDER BY 1;
SQL
cat "$out"
