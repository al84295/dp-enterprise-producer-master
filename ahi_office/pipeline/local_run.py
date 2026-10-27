"""Run the AHI pipeline locally on DuckDB against the sample office files and print the ABC summary.

  python3 pipeline/local_run.py [--landing data/samples] [--as-of 2026-10-27] [--db ahi_local.duckdb]
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from ahi_pipeline import LocalEngine, Pipeline, PipelineFailure  # noqa: E402


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--landing", default=str(ROOT / "data" / "samples"))
    a.add_argument("--as-of", default="2026-10-27")
    a.add_argument("--db", default=":memory:")
    args = a.parse_args()
    eng = LocalEngine(db_path=args.db)
    p = Pipeline(eng, ROOT / "config" / "offices.yml", args.landing, as_of=args.as_of)
    try:
        p.run()
        print("Pipeline completed")
    except PipelineFailure as e:
        print("Pipeline stopped by a control:", e)
    print(eng.sql("SELECT hop, coalesce(profit_centre, '-') AS pc, entity, rows_in, rows_out, rows_rejected, status, notes "
                  "FROM ahi_dev.ops.abc_control_log ORDER BY started_at").to_string(index=False))
    return eng


if __name__ == "__main__":
    main()
