#!/usr/bin/env bash
# Try the app on your laptop with the sample dataset (no Azure needed).
# Uses a local DuckDB file; Genie is disabled in this mode.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
python3 -m pip install --quiet "streamlit>=1.49" pandas duckdb
cd "$ROOT/app"
[ "${1:-}" = "--reset" ] && rm -f local_mdm.duckdb && echo "Reset local data to the sample set."
MDM_BACKEND=local streamlit run app.py
