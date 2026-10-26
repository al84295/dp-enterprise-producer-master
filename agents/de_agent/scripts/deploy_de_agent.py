"""Deploy the data engineering agent into an existing Azure Databricks workspace.

  python3 scripts/deploy_de_agent.py --model <serving-endpoint-name> [--warehouse-name wh-enterprise-dev-mdm]
                                     [--engineers-group <group>]

Needs DATABRICKS_HOST (and Azure CLI or other Databricks auth) and the Databricks CLI on PATH.
Idempotent: safe to re-run.
"""
import argparse
import json
import os
import subprocess
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import apps, sql

ROOT = Path(__file__).resolve().parent.parent
w = WorkspaceClient()


def log(m):
    print(f"[de-agent] {m}", flush=True)


def run_sql(wh, stmt):
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=wh, wait_timeout="50s")
    while r.status.state in (sql.StatementState.PENDING, sql.StatementState.RUNNING):
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != sql.StatementState.SUCCEEDED:
        raise SystemExit(f"SQL failed: {r.status.error.message if r.status.error else r.status.state}\n{stmt[:200]}")


def statements(path):
    for part in path.read_text().split("\n-- ;"):
        body = "\n".join(l for l in part.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            yield body


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True, help="Chat model serving endpoint name (Serving page in the workspace)")
    p.add_argument("--warehouse-name", default="wh-enterprise-dev-mdm")
    p.add_argument("--engineers-group", help="Group to get CAN_USE on the app")
    p.add_argument("--read-schemas", default="enterprise_dev.bronze,enterprise_dev.silver",
                   help="Schemas the agent may read (SELECT only)")
    a = p.parse_args()

    try:
        w.serving_endpoints.get(a.model)
    except Exception:
        names = [e.name for e in w.serving_endpoints.list()]
        raise SystemExit(f"Serving endpoint '{a.model}' not found. Available: {', '.join(names) or 'none'}")
    wh = next((x.id for x in w.warehouses.list() if x.name == a.warehouse_name), None)
    if not wh:
        raise SystemExit(f"Warehouse '{a.warehouse_name}' not found. Deploy the Producer POC first or pass --warehouse-name.")
    log(f"Using warehouse {a.warehouse_name} ({wh}) and model endpoint {a.model}")

    for s in statements(ROOT / "sql" / "setup.sql"):
        run_sql(wh, s)
    log("Drafts volume and audit table ready")

    env = dict(os.environ, DATABRICKS_BUNDLE_ENGINE=os.getenv("DATABRICKS_BUNDLE_ENGINE", "direct"))
    vars_ = ["--var", f"warehouse_id={wh}", "--var", f"model_endpoint={a.model}"]
    subprocess.run(["databricks", "bundle", "deploy", "-t", "dev", *vars_], cwd=ROOT, env=env, check=True)

    app = w.apps.get("de-agent-dev")
    sp = app.service_principal_client_id
    grants = ["GRANT USE CATALOG ON CATALOG enterprise_dev TO `{sp}`",
              "GRANT USE SCHEMA ON SCHEMA enterprise_dev.sandbox TO `{sp}`",
              "GRANT READ VOLUME, WRITE VOLUME ON VOLUME enterprise_dev.sandbox.de_agent_drafts TO `{sp}`",
              "GRANT SELECT, MODIFY ON TABLE enterprise_dev.sandbox.de_agent_audit TO `{sp}`"]
    for sch in a.read_schemas.split(","):
        grants += [f"GRANT USE SCHEMA, SELECT ON SCHEMA {sch.strip()} TO `{{sp}}`"]
    for g in grants:
        run_sql(wh, g.format(sp=sp))
    log(f"Granted the app ({sp}) read-only access to {a.read_schemas} and write access to drafts and audit only")

    if a.engineers_group:
        w.apps.update_permissions("de-agent-dev", access_control_list=[apps.AppAccessControlRequest(
            group_name=a.engineers_group, permission_level=apps.AppPermissionLevel.CAN_USE)])
        log(f"Granted CAN_USE to {a.engineers_group}")

    subprocess.run(["databricks", "bundle", "run", "de_agent", "-t", "dev", *vars_], cwd=ROOT, env=env, check=True)
    log(f"Done. App URL: {w.apps.get('de-agent-dev').url}")


if __name__ == "__main__":
    main()
