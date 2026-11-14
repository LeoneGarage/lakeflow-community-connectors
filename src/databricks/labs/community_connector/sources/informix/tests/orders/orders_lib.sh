# One random order item for order $1. Small value ranges, so identical rows also happen by chance.
item_sql() {
  local s=$((1 + RANDOM % 6)) q=$((1 + RANDOM % 2)) note=NULL
  case $((RANDOM % 10)) in 0|1) note="'gift'" ;; 2) note="'fragile'" ;; esac
  echo "INSERT INTO order_items (order_id, sku, qty, price, note) VALUES ($1, 'SKU-000$s', $q, $((s * 10)).50, $note);"
}
# $2 distinct draws for order $1; about a third of them are written two or three times (exact duplicates).
items_sql() {
  local i row copies
  for i in $(seq 1 "$2"); do
    row=$(item_sql "$1"); copies=1
    case $((RANDOM % 6)) in 0) copies=2 ;; 1) copies=3 ;; esac
    for _ in $(seq 1 $copies); do echo "$row"; done
  done
}
