"""Set up Databricks for the Producer MDM app: warehouse, catalog, tables, sample data, Genie space.

Authenticates with the Databricks SDK's default chain. After `az login`, setting
DATABRICKS_HOST to the workspace URL is enough (Azure CLI authentication).

  python3 setup_databricks.py platform --access-connector-id ... --storage-root abfss://...
  python3 setup_databricks.py grants --app-name producer-mdm-dev [--steward-group enterprise-data-stewards]

Every step is idempotent: re-running skips what already exists.
"""
import argparse
import csv
import json
import secrets
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists, ResourceConflict
from databricks.sdk.service import apps, catalog, sql

ROOT = Path(__file__).resolve().parent.parent
OUTPUTS = ROOT / ".deploy_outputs.json"
TABLES = ["producer_golden", "producer_source_records", "producer_change_log"]
INT_COLS, FLOAT_COLS = {"version"}, {"match_confidence"}
TS_COLS = {"updated_at", "reviewed_at", "loaded_at", "changed_at"}

class _LazyClient:
    """Create the WorkspaceClient on first use so helpers can be imported offline."""
    _c = None

    def __getattr__(self, name):
        if _LazyClient._c is None:
            _LazyClient._c = WorkspaceClient()
        return getattr(_LazyClient._c, name)


w = _LazyClient()


def log(msg):
    print(f"[setup] {msg}", flush=True)


def save(**kv):
    data = json.loads(OUTPUTS.read_text()) if OUTPUTS.exists() else {}
    data.update(kv)
    OUTPUTS.write_text(json.dumps(data, indent=2))
    return data


def load():
    return json.loads(OUTPUTS.read_text()) if OUTPUTS.exists() else {}


# ---------- SQL ----------
def run_sql(warehouse_id, statement):
    r = w.statement_execution.execute_statement(statement=statement, warehouse_id=warehouse_id, wait_timeout="50s")
    while r.status.state in (sql.StatementState.PENDING, sql.StatementState.RUNNING):
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != sql.StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed: {r.status.error.message if r.status.error else r.status.state}\n{statement[:300]}")
    return r.result.data_array if r.result else None


def literal(col, v):
    if v is None or v == "":
        return "NULL"
    if col in INT_COLS:
        return str(int(v))
    if col in FLOAT_COLS:
        return str(float(v))
    s = "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return f"TIMESTAMP {s}" if col in TS_COLS else s


def split_ddl(ddl):
    """Statements separated by '-- ;' lines, with comment-only lines removed."""
    out = []
    for part in ddl.split("\n-- ;"):
        body = "\n".join(l for l in part.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            out.append(body)
    return out


# ---------- steps ----------
def ensure_warehouse(name):
    for wh in w.warehouses.list():
        if wh.name == name:
            log(f"Warehouse exists: {name} ({wh.id})")
            return wh.id
    log(f"Creating serverless SQL warehouse {name} (takes a few minutes)...")
    wh = w.warehouses.create_and_wait(
        name=name, cluster_size="2X-Small", min_num_clusters=1, max_num_clusters=1, auto_stop_mins=10,
        enable_serverless_compute=True, warehouse_type=sql.CreateWarehouseRequestWarehouseType.PRO,
        tags=sql.EndpointTags(custom_tags=[sql.EndpointTagPair(key="product", value="producer-master")]))
    return wh.id


def ensure_catalog(name, env, access_connector_id, storage_root):
    try:
        w.catalogs.get(name)
        log(f"Catalog exists: {name}")
        return
    except NotFound:
        pass
    if not (access_connector_id and storage_root):
        raise SystemExit(f"Catalog {name} does not exist. Pass --access-connector-id and --storage-root "
                         f"(from the Bicep outputs) or use --catalog with an existing catalog.")
    cred, loc = f"sc-enterprise-{env}", f"el-enterprise-{env}"
    try:
        w.storage_credentials.create(name=cred, comment="Producer MDM managed storage",
                                     azure_managed_identity=catalog.AzureManagedIdentityRequest(
                                         access_connector_id=access_connector_id))
        log(f"Created storage credential {cred}")
    except (ResourceAlreadyExists, ResourceConflict):
        log(f"Storage credential exists: {cred}")
    for attempt in range(6):  # the role assignment can take a minute to propagate
        try:
            w.external_locations.create(name=loc, url=storage_root, credential_name=cred,
                                        comment="enterprise catalog managed storage")
            log(f"Created external location {loc}")
            break
        except (ResourceAlreadyExists, ResourceConflict):
            log(f"External location exists: {loc}")
            break
        except Exception as e:  # noqa: BLE001 - retry permission propagation errors
            if attempt == 5:
                raise
            log(f"Waiting for storage permissions to propagate ({e.__class__.__name__})...")
            time.sleep(30)
    w.catalogs.create(name=name, storage_root=storage_root.rstrip("/") + f"/{name}",
                      comment="Enterprise domain (master data)")
    log(f"Created catalog {name}")


def create_tables(warehouse_id, cat, schema):
    ddl = (ROOT / "sql" / "ddl.sql").read_text().replace("${catalog}", cat).replace("${schema}", schema)
    for body in split_ddl(ddl):
        run_sql(warehouse_id, body)
    log(f"Tables ready in {cat}.{schema}")


def load_sample_data(warehouse_id, cat, schema):
    n = run_sql(warehouse_id, f"SELECT count(*) FROM {cat}.{schema}.producer_golden")
    if n and int(n[0][0]) > 0:
        log("Sample data already loaded; skipping")
        return
    for t in TABLES:
        with open(ROOT / "data" / f"{t}.csv", newline="") as f:
            rows = list(csv.DictReader(f))
        cols = list(rows[0].keys())
        for i in range(0, len(rows), 100):
            values = ",\n".join("(" + ", ".join(literal(c, r[c]) for c in cols) + ")" for r in rows[i:i + 100])
            run_sql(warehouse_id, f"INSERT INTO {cat}.{schema}.{t} ({', '.join(cols)}) VALUES\n{values}")
        log(f"Loaded {len(rows)} rows into {t}")


def ensure_genie_space(warehouse_id, cat, schema, title):
    resp = w.genie.list_spaces()
    for s in resp.spaces or []:
        if s.title == title:
            log(f"Genie space exists: {title} ({s.space_id})")
            return s.space_id
    hexid = lambda: secrets.token_hex(16)  # noqa: E731
    space = {
        "version": 2,
        "config": {"sample_questions": [
            {"id": hexid(), "question": [q]} for q in [
                "Which active Specialty producers are missing an NPN?",
                "How many source records are waiting for steward review, by source system?",
                "Which producers changed in the last 7 days and who changed them?",
            ]]},
        "data_sources": {"tables": sorted([
            {"identifier": f"{cat}.{schema}.producer_golden",
             "description": ["Steward-managed golden record, one row per producer"]},
            {"identifier": f"{cat}.{schema}.producer_source_records",
             "description": ["Producer records from source systems with their match to a golden record"]},
            {"identifier": f"{cat}.{schema}.producer_change_log",
             "description": ["Audit trail of every steward action"]},
        ], key=lambda t: t["identifier"])},
        "instructions": {"text_instructions": [{"id": hexid(), "content": [
            "A producer is a licensed insurance agent or broker. NPN is the National Producer Number. ",
            "Golden records are in producer_golden. In producer_source_records, match_status 'Proposed' or ",
            "'Unmatched' means a data steward must review the record. Segment is Retail or Specialty. ",
            "All data in this space is fictional sample data.",
        ]}]},
    }
    created = w.genie.create_space(warehouse_id=warehouse_id, serialized_space=json.dumps(space), title=title,
                                   description="Ask questions about producer master data (BEACON MVP)")
    log(f"Created Genie space {title} ({created.space_id})")
    return created.space_id


def grants(app_name, cat, schema, steward_group):
    app = w.apps.get(app_name)
    sp = app.service_principal_client_id
    wh = load()["warehouse_id"]
    for g in [f"GRANT USE CATALOG ON CATALOG {cat} TO `{sp}`",
              f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {cat}.{schema} TO `{sp}`"]:
        run_sql(wh, g)
    log(f"Granted the app's service principal ({sp}) access to {cat}.{schema}")
    if steward_group:
        w.apps.update_permissions(app_name, access_control_list=[apps.AppAccessControlRequest(
            group_name=steward_group, permission_level=apps.AppPermissionLevel.CAN_USE)])
        log(f"Granted CAN_USE on the app to group {steward_group}")
    log(f"App URL: {app.url}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("platform")
    a.add_argument("--env", default="dev")
    a.add_argument("--catalog", default=None, help="default enterprise_<env>")
    a.add_argument("--schema", default="silver")
    a.add_argument("--access-connector-id")
    a.add_argument("--storage-root")
    a.add_argument("--warehouse-name", default=None)
    b = sub.add_parser("grants")
    b.add_argument("--app-name", required=True)
    b.add_argument("--steward-group")
    args = p.parse_args()

    if args.cmd == "platform":
        cat = args.catalog or f"enterprise_{args.env}"
        try:
            ms = w.metastores.current()
            log(f"Unity Catalog metastore: {ms.metastore_id}")
        except NotFound:
            raise SystemExit("This workspace has no Unity Catalog metastore assigned. Ask your Databricks "
                             "account admin to assign one (account console > Catalog), then re-run.")
        wh =ensure_warehouse(args.warehouse_name or f"wh-enterprise-{args.env}-mdm")
        ensure_catalog(cat, args.env, args.access_connector_id, args.storage_root)
        create_tables(wh, cat, args.schema)
        load_sample_data(wh, cat, args.schema)
        space = ensure_genie_space(wh, cat, args.schema, f"Producer master data ({args.env})")
        print(json.dumps(save(warehouse_id=wh, genie_space_id=space, catalog=cat, schema=args.schema), indent=2))
    else:
        o = load()
        grants(args.app_name, o["catalog"], o["schema"], args.steward_group)


if __name__ == "__main__":
    main()
