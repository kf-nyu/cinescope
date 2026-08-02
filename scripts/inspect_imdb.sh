#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

COUNT_ROWS=0
if [[ "${1:-}" == "--count-rows" ]]; then
  COUNT_ROWS=1
fi

"$ROOT_DIR/scripts/validate_storage.sh"

DEST="${CINESCOPE_DATA_ROOT}/raw/imdb"
OUT_DIR="$ROOT_DIR/outputs/metrics"
mkdir -p "$OUT_DIR"
INVENTORY="$OUT_DIR/raw_data_inventory.txt"

FILES=(
  title.basics.tsv.gz
  title.ratings.tsv.gz
  title.principals.tsv.gz
  name.basics.tsv.gz
  title.crew.tsv.gz
)

{
  echo "CineScope raw IMDb inventory"
  echo "generated_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "storage_backend=${CINESCOPE_STORAGE_BACKEND}"
  echo "data_root=$DEST"
  echo
} > "$INVENTORY"

for file in "${FILES[@]}"; do
  path="$DEST/$file"
  echo "==== $file ====" | tee -a "$INVENTORY"
  if ! cinescope_is_file "$path"; then
    echo "MISSING: $path" | tee -a "$INVENTORY"
    echo | tee -a "$INVENTORY"
    continue
  fi

  if cinescope_is_hdfs; then
    cinescope_du_human "$path" | tee -a "$INVENTORY"
    echo "header_and_sample:" | tee -a "$INVENTORY"
    set +o pipefail
    hdfs dfs -cat "$path" | gzip -cd | head -n 3 | tee -a "$INVENTORY" || true
    set -o pipefail
    if (( COUNT_ROWS )); then
      rows="$(hdfs dfs -cat "$path" | gzip -cd | wc -l | tr -d ' ')"
      echo "row_count_excluding_header=$((rows - 1))" | tee -a "$INVENTORY"
    fi
  else
    if ! gzip -t "$path"; then
      echo "INVALID gzip: $path" | tee -a "$INVENTORY"
      echo | tee -a "$INVENTORY"
      continue
    fi
    size="$(ls -lh "$path" | awk '{print $5}')"
    bytes="$(stat -f%z "$path" 2>/dev/null || stat -c%s "$path")"
    echo "compressed_size=$size ($bytes bytes)" | tee -a "$INVENTORY"
    echo "header_and_sample:" | tee -a "$INVENTORY"
    set +o pipefail
    gzip -cd "$path" | head -n 3 | tee -a "$INVENTORY"
    set -o pipefail
    if (( COUNT_ROWS )); then
      rows="$(gzip -cd "$path" | wc -l | tr -d ' ')"
      echo "row_count_excluding_header=$((rows - 1))" | tee -a "$INVENTORY"
    fi
  fi
  echo | tee -a "$INVENTORY"
done

echo "Inventory saved to: $INVENTORY"
