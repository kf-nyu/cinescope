#!/usr/bin/env bash
# Bootstrap CineScope on NYU Dataproc: derive NetID from login, write .env, create venv.
# Usage (on Dataproc login node):
#   git clone https://github.com/kf-nyu/cinescope.git cinescope
#   cd cinescope
#   bash scripts/setup_hpc.sh
#
# Options:
#   --force     overwrite an existing .env
#   --no-venv   only write .env (skip python venv / pip)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FORCE=0
NO_VENV=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    --no-venv) NO_VENV=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg (try --help)" >&2
      exit 1
      ;;
  esac
done

who="$(whoami 2>/dev/null || true)"
if [[ "$who" == *_nyu_edu ]]; then
  NETID="${who%_nyu_edu}"
elif [[ -n "$who" && "$who" != "root" ]]; then
  NETID="$who"
  echo "Warning: login user is not '*_nyu_edu' (got '$who'); using it as CINESCOPE_NETID." >&2
else
  echo "Error: could not determine NetID from whoami." >&2
  exit 1
fi

EXAMPLE="$ROOT_DIR/.env.hpc.example"
ENV_FILE="$ROOT_DIR/.env"
[[ -f "$EXAMPLE" ]] || { echo "Error: missing $EXAMPLE" >&2; exit 1; }

if [[ -f "$ENV_FILE" && "$FORCE" -ne 1 ]]; then
  echo "Error: $ENV_FILE already exists. Re-run with --force to overwrite, or edit it by hand." >&2
  exit 1
fi

cp "$EXAMPLE" "$ENV_FILE"
# Fill CINESCOPE_NETID=… from Dataproc login (short NetID only).
if grep -q '^CINESCOPE_NETID=' "$ENV_FILE"; then
  sed -i "s/^CINESCOPE_NETID=.*/CINESCOPE_NETID=${NETID}/" "$ENV_FILE"
else
  printf '\nCINESCOPE_NETID=%s\n' "$NETID" >>"$ENV_FILE"
fi

echo "Wrote .env with CINESCOPE_NETID=${NETID} (from whoami=${who})"
echo "  HDFS data root will be: hdfs:///user/${NETID}_nyu_edu/cinescope-data"
echo "  Never commit .env"

if [[ "$NO_VENV" -eq 0 ]]; then
  if [[ ! -x "$ROOT_DIR/.venv/bin/python" ]]; then
    python3 -m venv "$ROOT_DIR/.venv"
  fi
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.venv/bin/activate"
  pip install --upgrade pip
  pip install -r "$ROOT_DIR/requirements.txt"
  echo
  echo "Virtualenv ready. For this shell (and new shells):"
  echo "  source .venv/bin/activate"
  echo "  export PYTHONPATH=\"\$PWD/src\""
  echo "  export PYSPARK_PYTHON=\"\$PWD/.venv/bin/python\""
  echo "  export PYSPARK_DRIVER_PYTHON=\"\$PWD/.venv/bin/python\""
fi

echo
echo "Next:"
echo "  make init-storage && make validate-storage"
echo "  make download && make download-oscars"
echo "  make test"
echo "  make baseline && make cast-crew && make oscars"
echo "  make clean-hpc   # when finished"
