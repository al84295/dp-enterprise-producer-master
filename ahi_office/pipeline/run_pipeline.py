"""Databricks entry point (spark_python_task). Runs landing -> bronze -> silver -> gold with ABC and DQ checks.

Parameters: --catalog ahi_dev --landing /Volumes/ahi_dev/bronze/landing --mappings /Volumes/ahi_dev/ops/mappings
            --root <bundle files path> [--as-of YYYY-MM-DD]
"""
import argparse
import sys
from datetime import date
from pathlib import Path

a = argparse.ArgumentParser()
a.add_argument("--catalog", required=True)
a.add_argument("--landing", required=True)
a.add_argument("--mappings", required=True)
a.add_argument("--root", required=True)
a.add_argument("--as-of", default=date.today().isoformat())
args = a.parse_args()

root = Path(args.root)
sys.path.insert(0, str(root / "pipeline"))
from ahi_pipeline import Pipeline, PipelineFailure, SparkEngine  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
p = Pipeline(SparkEngine(spark, args.catalog), root / "config" / "offices.yml", args.landing, as_of=args.as_of,
             gold_sql_path=root / "sql" / "gold.sql", mappings_dir=args.mappings)
try:
    batch = p.run()
    print(f"Pipeline completed, batch {batch}")
except PipelineFailure as e:
    print(f"Pipeline stopped by a control: {e}")
    raise
