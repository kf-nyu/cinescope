#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

"$ROOT_DIR/scripts/validate_storage.sh"

DEST="${CINESCOPE_DATA_ROOT}/raw/oscars"
STAGE="${CINESCOPE_DOWNLOAD_STAGING:-${TMPDIR:-/tmp}/cinescope-download/oscars}"
mkdir -p "$STAGE"
cinescope_mkdir "$DEST"

URL="https://raw.githubusercontent.com/DLu/oscar_data/main/oscars.csv"
STAGED="$STAGE/oscars.csv"
TARGET="$DEST/oscars.csv"

echo "Downloading Oscar nominations → staging: $STAGE"
curl --fail --location --retry 3 --output "$STAGED" "$URL"
date -u +"%Y-%m-%dT%H:%M:%SZ" > "$STAGE/downloaded_at_utc.txt"

cat > "$STAGE/SOURCE.txt" <<'EOF'
dataset: Academy Award nominations with IMDb identifiers
file: oscars.csv
upstream: https://github.com/DLu/oscar_data
license: BSD-2-Clause
notes: Tab-separated despite .csv extension. FilmId is IMDb tconst (may be pipe-separated for multi-film nominations). Winner is empty (nominee) or True (winner).
proposal_alignment: Matches CineScope awards-recognition data source (Kaggle "The Oscar Award" is derived from the same Academy database lineage; this GitHub file adds FilmId for reliable joins).
EOF

(
  cd "$STAGE"
  shasum -a 256 oscars.csv SOURCE.txt > SHA256SUMS
)

cinescope_put_file "$STAGED" "$TARGET"
cinescope_put_file "$STAGE/downloaded_at_utc.txt" "$DEST/downloaded_at_utc.txt"
cinescope_put_file "$STAGE/SOURCE.txt" "$DEST/SOURCE.txt"
cinescope_put_file "$STAGE/SHA256SUMS" "$DEST/SHA256SUMS"

echo
echo "Download complete."
echo "Acquisition UTC: $(cat "$STAGE/downloaded_at_utc.txt")"
echo "Destination (${CINESCOPE_STORAGE_BACKEND}): $DEST"
