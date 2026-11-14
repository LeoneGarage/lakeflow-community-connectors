#!/bin/bash
# Creates orders (keyed) and order_items (no key, indexed on order_id, FK to orders) in testdb and seeds them.
# Run inside the Informix container as a user that can run dbaccess, next to orders_lib.sh.
export INFORMIXDIR=${INFORMIXDIR:-/opt/ibm/informix} INFORMIXSERVER=${INFORMIXSERVER:-informix}
export PATH="$INFORMIXDIR/bin:$PATH" DBACCNOIGN=1
. "$(dirname "$0")/orders_lib.sh"
dbaccess testdb - <<'SQL' 2>&1 | grep -v "^$"
CREATE TABLE orders (
  id       SERIAL NOT NULL PRIMARY KEY,
  customer VARCHAR(40) NOT NULL,
  created  DATETIME YEAR TO FRACTION(5) NOT NULL
) LOCK MODE ROW;
CREATE TABLE order_items (
  order_id INT NOT NULL,
  sku      CHAR(8) NOT NULL,
  qty      INT NOT NULL,
  price    DECIMAL(10,2) NOT NULL,
  note     VARCHAR(20)
) LOCK MODE ROW;
CREATE INDEX ix_order_items_order_id ON order_items (order_id);
ALTER TABLE order_items ADD CONSTRAINT FOREIGN KEY (order_id) REFERENCES orders (id) CONSTRAINT fk_order_items_order;
SQL
{
  echo "BEGIN WORK;"
  for n in $(seq 1 300); do
    echo "INSERT INTO orders (id, customer, created) VALUES ($n, 'Customer $n', CURRENT YEAR TO FRACTION(5));"
    [ $((n % 10)) -eq 0 ] || items_sql "$n" $((1 + RANDOM % 4))    # every 10th order starts with no items
  done
  echo "COMMIT WORK;"
} > /tmp/orders_seed.sql
dbaccess testdb /tmp/orders_seed.sql >/tmp/orders_seed.out 2>&1; echo "seed rc=$?"
echo "SELECT COUNT(*) orders FROM orders; SELECT COUNT(*) items FROM order_items;
SELECT COUNT(*) dup_groups FROM (SELECT order_id, sku, qty, price, note, COUNT(*) c FROM order_items GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1);" | dbaccess testdb - 2>&1 | grep -v "^$"
