#!/bin/bash
# The source side of the comparison: order_items summed up in 25 buckets, one line each,
# as orders_compare_test_tool.py reads them. Run inside the Informix container once the
# workload has ended; save the output to a file and pass that file to the tool.
export INFORMIXDIR=${INFORMIXDIR:-/opt/ibm/informix} INFORMIXSERVER=${INFORMIXSERVER:-informix}
export PATH="$INFORMIXDIR/bin:$PATH"
out=/tmp/orders_fingerprint.unl
dbaccess testdb - >/dev/null 2>&1 <<SQL
UNLOAD TO '$out' DELIMITER '|'
SELECT MOD(order_id, 25), COUNT(*), SUM(order_id), SUM(qty), SUM(price), SUM(order_id * qty * price), COUNT(note)
FROM order_items GROUP BY 1 ORDER BY 1;
SQL
cat "$out"
