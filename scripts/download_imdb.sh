#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

"$ROOT_DIR/scripts/validate_storage.sh"

DEST="${CINESCOPE_DATA_ROOT}/raw/imdb"
BASE_URL="https://datasets.imdbws.com"
FILES=(
  title.basics.tsv.gz
  title.ratings.tsv.gz
  title.principals.tsv.gz
  name.basics.tsv.gz
  title.crew.tsv.gz
)

STAGE="${CINESCOPE_DOWNLOAD_STAGING:-${TMPDIR:-/tmp}/cinescope-download/imdb}"
mkdir -p "$STAGE"
cinescope_mkdir "$DEST"

echo "Downloading IMDb datasets → staging: $STAGE"
echo "Final destination (${CINESCOPE_STORAGE_BACKEND}): $DEST"

for file in "${FILES[@]}"; do
  staged="$STAGE/$file"
  target="$DEST/$file"
  url="$BASE_URL/$file"

  if cinescope_is_file "$target"; then
    # For local, also verify gzip; for HDFS assume prior successful put.
    if ! cinescope_is_hdfs; then
      if gzip -t "$target" 2>/dev/null; then
        echo "  skip (valid archive): $file"
        continue
      fi
    else
      echo "  skip (exists on HDFS): $file"
      continue
    fi
  fi

  if [[ -f "$staged" ]] && gzip -t "$staged" 2>/dev/null; then
    echo "  reuse staged: $file"
  else
    echo "  fetching: $file"
    curl --fail --location --retry 3 --continue-at - \
      --output "$staged" \
      "$url"
    gzip -t "$staged"
  fi

  cinescope_put_file "$staged" "$target"
done

date -u +"%Y-%m-%dT%H:%M:%SZ" > "$STAGE/downloaded_at_utc.txt"
(
  cd "$STAGE"
  shasum -a 256 "${FILES[@]}" > SHA256SUMS
)
cinescope_put_file "$STAGE/downloaded_at_utc.txt" "$DEST/downloaded_at_utc.txt"
cinescope_put_file "$STAGE/SHA256SUMS" "$DEST/SHA256SUMS"

echo
echo "Download complete."
echo "Acquisition UTC: $(cat "$STAGE/downloaded_at_utc.txt")"
echo "Destination: $DEST"
