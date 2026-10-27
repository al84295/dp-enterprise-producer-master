"""Deploy the AHI office-files solution into an existing Azure Databricks workspace, load the sample files and run it.

  python3 scripts/deploy_ahi.py [--warehouse-name wh-enterprise-dev-mdm] [--catalog ahi_dev] [--no-samples]

Needs DATABRICKS_HOST and auth (Azure CLI works), and the Databricks CLI on PATH. Safe to re-run.
"""
import argparse
import io
import json
import os
import secrets
import subprocess
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import sql

ROOT = Path(__file__).resolve().parent.parent
w = WorkspaceClient()


def log(m):
    print(f"[ahi] {m}", flush=True)


def run_sql(wh, stmt):
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=wh, wait_timeout="50s")
    while r.status.state in (sql.StatementState.PENDING, sql.StatementState.RUNNING):
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != sql.StatementState.SUCCEEDED:
        raise SystemExit(f"SQL failed: {r.status.error.message if r.status.error else r.status.state}\n{stmt[:200]}")
    return r.result.data_array if r.result else None


def ensure_catalog(cat):
    try:
        w.catalogs.get(cat)
        log(f"Catalog {cat} exists")
        return
    except NotFound:
        pass
    locs = list(w.external_locations.list())
    loc = next((l for l in locs if l.name == "el-enterprise-dev"), locs[0] if locs else None)
    if not loc:
        raise SystemExit(f"Catalog {cat} does not exist and no external location was found. Create the catalog, then re-run.")
    w.catalogs.create(name=cat, storage_root=loc.url.rstrip("/") + f"/{cat}", comment="AHI domain (Arrowhead Intermediaries)")
    log(f"Created catalog {cat} on {loc.name}")


def ensure_genie(wh, cat):
    title = f"AHI gold reporting ({cat})"
    for s in (w.genie.list_spaces().spaces or []):
        if s.title == title:
            log(f"Genie space exists ({s.space_id})")
            return s.space_id
    hid = lambda: secrets.token_hex(16)  # noqa: E731
    tables = [(f"{cat}.gold.dim_policy", "Policy master: one row per policy term, current state"),
              (f"{cat}.gold.dim_producer", "Producer master: one row per mastered producer"),
              (f"{cat}.gold.dim_provider", "Provider master: carriers, MGAs and MGUs"),
              (f"{cat}.gold.fct_policies_in_force", "Policies in force by as-of date, profit centre, producer, provider and line of business"),
              (f"{cat}.ops.abc_control_log", "ABC control log: one row per load per hop"),
              (f"{cat}.ops.dq_results", "Data quality rule results per batch")]
    space = {"version": 2,
             "config": {"sample_questions": [{"id": hid(), "question": [q]} for q in [
                 "What is premium in force by profit centre?",
                 "Which producers have the most policies in force?",
                 "Which data quality rules failed in the last load?"]]},
             "data_sources": {"tables": sorted([{"identifier": t, "description": [d]} for t, d in tables], key=lambda x: x["identifier"])},
             "instructions": {"text_instructions": [{"id": hid(), "content": [
                 "AHI is Arrowhead Intermediaries. Profit centres are the offices that send data. ",
                 "Policies in force: effective on or before the as-of date, expiring after it, not cancelled. ",
                 "Use the latest as_of_date unless asked otherwise. PRD-UNKNOWN and PRV-UNKNOWN are placeholders for ",
                 "policies whose producer or provider did not match. All data is fictional sample data."]}]}}
    s = w.genie.create_space(warehouse_id=wh, serialized_space=json.dumps(space), title=title,
                             description="Ask questions about AHI gold data and load controls")
    log(f"Created Genie space ({s.space_id})")
    return s.space_id


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--warehouse-name", default="wh-enterprise-dev-mdm")
    a.add_argument("--catalog", default="ahi_dev")
    a.add_argument("--no-samples", action="store_true")
    args = a.parse_args()
    cat = args.catalog
    wh = next((x.id for x in w.warehouses.list() if x.name == args.warehouse_name), None)
    if not wh:
        raise SystemExit(f"Warehouse '{args.warehouse_name}' not found. Pass --warehouse-name.")

    ensure_catalog(cat)
    for s in ("bronze", "silver", "gold", "ops"):
        run_sql(wh, f"CREATE SCHEMA IF NOT EXISTS {cat}.{s}")
    run_sql(wh, f"CREATE VOLUME IF NOT EXISTS {cat}.bronze.landing COMMENT 'Office files as uploaded'")
    run_sql(wh, f"CREATE VOLUME IF NOT EXISTS {cat}.ops.mappings COMMENT 'Proposed and approved file mappings'")
    log("Schemas and volumes ready")

    env = dict(os.environ, DATABRICKS_BUNDLE_ENGINE=os.getenv("DATABRICKS_BUNDLE_ENGINE", "direct"))
    vars_ = ["--var", f"warehouse_id={wh}", "--var", f"catalog={cat}"]
    subprocess.run(["databricks", "bundle", "deploy", "-t", "dev", *vars_], cwd=ROOT, env=env, check=True)

    sp = w.apps.get("ahi-upload-dev").service_principal_client_id
    for g in [f"GRANT USE CATALOG ON CATALOG {cat} TO `{sp}`",
              f"GRANT USE SCHEMA ON SCHEMA {cat}.bronze TO `{sp}`",
              f"GRANT USE SCHEMA, SELECT ON SCHEMA {cat}.ops TO `{sp}`",
              f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {cat}.bronze.landing TO `{sp}`",
              f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {cat}.ops.mappings TO `{sp}`"]:
        run_sql(wh, g)
    log("Granted the upload app access to the landing and mappings volumes and read access to ops")

    if not args.no_samples:
        n = 0
        for f in sorted((ROOT / "data" / "samples").rglob("*")):
            if f.is_file():
                office, entity = f.parts[-3], f.parts[-2]
                w.files.upload(f"/Volumes/{cat}/bronze/landing/{office}/{entity}/sample/{f.name}", io.BytesIO(f.read_bytes()), overwrite=True)
                n += 1
        log(f"Uploaded {n} sample office files to the landing volume")
        log("Running the pipeline (first run takes a few minutes)...")
        subprocess.run(["databricks", "bundle", "run", "ahi_office_pipeline", "-t", "dev", *vars_], cwd=ROOT, env=env, check=True)

    ensure_genie(wh, cat)
    subprocess.run(["databricks", "bundle", "run", "ahi_upload", "-t", "dev", *vars_], cwd=ROOT, env=env, check=True)
    log(f"Done. Upload app: {w.apps.get('ahi-upload-dev').url}")
    log("Dashboard: Dashboards > 'AHI gold reporting (dev)'. Genie: Genie > 'AHI gold reporting (ahi_dev)'.")


if __name__ == "__main__":
    main()
