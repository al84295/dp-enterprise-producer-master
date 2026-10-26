"""Tools the data engineering agent can call, and the backends they run on.

DatabricksBackend: Unity Catalog via the Databricks SDK, SQL via a SQL warehouse, drafts in a UC volume.
LocalBackend:      DuckDB with sample bronze tables and a local drafts folder (demo and tests).
"""
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import sqlglot

from guards import check_name, check_read_only, with_limit

DRAFT_KINDS = {
    "dbt_model": ".sql",          # silver/gold model in dbt
    "dbt_schema": ".yml",         # dbt schema.yml with tests and docs
    "dq_expectations": ".sql",    # Lakeflow Declarative Pipelines expectations
    "odcs_contract": ".yaml",     # ODCS v3 data contract
    "bundle_resource": ".yml",    # Asset Bundle resource snippet
    "ingestion_config": ".yml",   # Lakeflow Connect / ingestion settings
    "runbook": ".md",             # runbook entry or design note
}


# ---------------------------------------------------------------- backends
class DatabricksBackend:
    def __init__(self, warehouse_id, drafts_volume):
        from databricks import sql as dbsql
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        self.cfg = Config()
        self.w = WorkspaceClient()
        self.warehouse_id = warehouse_id
        self.drafts_volume = drafts_volume.rstrip("/")
        self._dbsql = dbsql
        self._conn = None

    def _connection(self):
        if self._conn is None:
            self._conn = self._dbsql.connect(server_hostname=self.cfg.host,
                                             http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                                             credentials_provider=lambda: self.cfg.authenticate)
        return self._conn

    def query(self, sql, params=None):
        with self._connection().cursor() as cur:
            cur.execute(sql, params or {})
            if not cur.description:
                return []
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def list_tables(self, catalog, schema):
        return [{"name": t.full_name, "type": str(t.table_type.value if t.table_type else ""),
                 "comment": t.comment} for t in self.w.tables.list(catalog_name=catalog, schema_name=schema)]

    def describe(self, full_name):
        t = self.w.tables.get(full_name)
        cols = [{"name": c.name, "type": c.type_text, "comment": c.comment, "nullable": c.nullable}
                for c in (t.columns or [])]
        cat, sch, tbl = full_name.split(".")
        tags = self.query(
            f"SELECT column_name, tag_name, tag_value FROM {cat}.information_schema.column_tags "
            "WHERE schema_name = :s AND table_name = :t", {"s": sch, "t": tbl})
        return {"name": t.full_name, "comment": t.comment, "owner": t.owner,
                "type": str(t.table_type.value if t.table_type else ""), "columns": cols, "column_tags": tags}

    def save(self, rel_path, content):
        path = f"{self.drafts_volume}/{rel_path}"
        import io
        self.w.files.upload(path, io.BytesIO(content.encode()), overwrite=True)
        return path

    def audit(self, record, table):
        if not table:
            return
        self.query(f"INSERT INTO {table} (event_at, user_email, conversation_id, tool, arguments, outcome) "
                   "VALUES (:at, :u, :c, :t, :a, :o)",
                   {"at": record["at"], "u": record["user"], "c": record["conversation"], "t": record["tool"],
                    "a": record["args"][:4000], "o": record["outcome"][:4000]})


class LocalBackend:
    """DuckDB with three-part names (catalog.schema.table) loaded from the Producer sample CSVs."""

    def __init__(self, sample_dir=None, drafts_dir=None):
        import duckdb
        self.con = duckdb.connect()
        self.drafts_dir = Path(drafts_dir or os.getenv("DE_AGENT_DRAFTS_DIR", "drafts"))
        self.audit_log = []
        sample_dir = Path(sample_dir or os.getenv("DE_AGENT_SAMPLE_DIR", Path(__file__).resolve().parent / "sample"))
        self.con.execute("ATTACH ':memory:' AS enterprise_dev")
        for s in ("bronze", "silver", "gold"):
            self.con.execute(f"CREATE SCHEMA enterprise_dev.{s}")
        for f in sorted(sample_dir.glob("*.csv")):
            self.con.execute(f"CREATE TABLE enterprise_dev.bronze.{f.stem} AS "
                             f"SELECT * FROM read_csv('{f}', header = true, all_varchar = true)")

    def query(self, sql, params=None):
        if params:
            sql = re.sub(r"(?<![:\w]):(\w+)", lambda m: "$" + m.group(1), sql)
        cur = self.con.execute(sql, params or {})
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def list_tables(self, catalog, schema):
        rows = self.query("SELECT table_catalog, table_schema, table_name FROM information_schema.tables "
                          "WHERE table_catalog = :c AND table_schema = :s ORDER BY table_name", {"c": catalog, "s": schema})
        return [{"name": f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}", "type": "MANAGED", "comment": None}
                for r in rows]

    def describe(self, full_name):
        cat, sch, tbl = full_name.split(".")
        cols = self.query("SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                          "WHERE table_catalog = :c AND table_schema = :s AND table_name = :t ORDER BY ordinal_position",
                          {"c": cat, "s": sch, "t": tbl})
        if not cols:
            raise ValueError(f"Table {full_name} not found.")
        return {"name": full_name, "comment": None, "owner": "local", "type": "MANAGED",
                "columns": [{"name": c["column_name"], "type": c["data_type"], "comment": None,
                             "nullable": c["is_nullable"] == "YES"} for c in cols], "column_tags": []}

    def save(self, rel_path, content):
        p = self.drafts_dir / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return str(p)

    def audit(self, record, table=None):
        self.audit_log.append(record)


# ---------------------------------------------------------------- tool implementations
class Toolbox:
    def __init__(self, backend, user="unknown", conversation="local", audit_table=None):
        self.b = backend
        self.user = user
        self.conversation = conversation
        self.audit_table = audit_table
        self.drafts = []  # (kind, name, path, content)

    # each tool returns a JSON-serialisable result
    def list_tables(self, catalog, schema):
        return {"tables": self.b.list_tables(catalog, schema)}

    def describe_table(self, full_name):
        return self.b.describe(full_name)

    def profile_table(self, full_name, columns=None):
        info = self.b.describe(full_name)
        cols = [c["name"] for c in info["columns"]]
        if columns:
            cols = [c for c in cols if c in columns]
        cols = cols[:40]
        q = lambda c: f"`{c}`"  # noqa: E731
        parts = ["count(*) AS row_count"]
        for i, c in enumerate(cols):
            parts.append(f"sum(CASE WHEN {q(c)} IS NULL OR trim(CAST({q(c)} AS STRING)) = '' THEN 1 ELSE 0 END) AS n{i}")
            parts.append(f"approx_count_distinct({q(c)}) AS d{i}")
        sql = f"SELECT {', '.join(parts)} FROM {full_name}"
        if isinstance(self.b, LocalBackend):
            sql = sql.replace("`", '"').replace("AS STRING", "AS VARCHAR")
        row = self.b.query(sql)[0]
        n = int(row["row_count"] or 0)
        prof = []
        for i, c in enumerate(cols):
            nulls = int(row[f"n{i}"] or 0)
            prof.append({"column": c, "null_or_blank_pct": round(100 * nulls / n, 1) if n else None,
                         "approx_distinct": int(row[f"d{i}"] or 0)})
        sample = self.b.query(with_limit(f"SELECT * FROM {full_name}", 5) if not isinstance(self.b, LocalBackend)
                              else f"SELECT * FROM {full_name} LIMIT 5")
        return {"table": full_name, "row_count": n, "columns": prof, "sample_rows": sample}

    def run_sql(self, sql):
        ok, why = check_read_only(sql)
        if not ok:
            return {"error": why}
        limited = with_limit(sql)
        rows = self.b.query(limited if not isinstance(self.b, LocalBackend) else sqlglot.transpile(limited, read="databricks", write="duckdb")[0])
        return {"sql_run": limited, "row_count": len(rows), "rows": rows}

    def check_naming(self, full_name):
        issues = check_name(full_name)
        return {"name": full_name, "compliant": not issues, "issues": issues}

    def validate_sql(self, sql):
        try:
            stmts = sqlglot.parse(sql, read="databricks")
            return {"valid": True, "statements": len([s for s in stmts if s is not None])}
        except sqlglot.errors.ParseError as e:
            return {"valid": False, "error": str(e).splitlines()[0]}

    def save_draft(self, kind, name, content, target_table=None):
        if kind not in DRAFT_KINDS:
            return {"error": f"Unknown kind '{kind}'. Use one of: {', '.join(DRAFT_KINDS)}."}
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,80}", name or ""):
            return {"error": "Name must be lower snake_case, 2–80 characters."}
        findings = check_name(target_table) if target_table else []
        if kind in ("dbt_model", "dq_expectations"):
            v = self.validate_sql(re.sub(r"\{\{.*?\}\}", "placeholder_ref", content, flags=re.S))
            if not v["valid"]:
                findings.append(f"SQL did not parse: {v['error']}")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = self.b.save(f"{self.conversation}/{stamp}_{name}{DRAFT_KINDS[kind]}", content)
        self.drafts.append({"kind": kind, "name": name, "path": path, "content": content, "findings": findings})
        return {"saved_to": path, "status": "draft, needs human review", "findings": findings}

    # ---- dispatch with audit
    def call(self, name, args):
        fn = getattr(self, name, None)
        if fn is None or name.startswith("_") or name == "call":
            out = {"error": f"Unknown tool {name}."}
        else:
            try:
                out = fn(**args)
            except Exception as e:  # tool errors go back to the model, not to the user as a crash
                out = {"error": f"{type(e).__name__}: {str(e)[:500]}"}
        self.b.audit({"at": datetime.now(timezone.utc), "user": self.user, "conversation": self.conversation,
                      "tool": name, "args": json.dumps(args, default=str),
                      "outcome": "error" if "error" in out else "ok"}, self.audit_table)
        return out


def _fn(name, desc, props, required):
    return {"type": "function", "function": {"name": name, "description": desc,
            "parameters": {"type": "object", "properties": props, "required": required}}}


S = {"type": "string"}
TOOL_SCHEMAS = [
    _fn("list_tables", "List tables in a Unity Catalog schema.", {"catalog": S, "schema": S}, ["catalog", "schema"]),
    _fn("describe_table", "Columns, types, comments, owner and column tags of a table.", {"full_name": S}, ["full_name"]),
    _fn("profile_table", "Row count, null or blank percentage and approximate distinct count per column, plus 5 sample rows.",
        {"full_name": S, "columns": {"type": "array", "items": S}}, ["full_name"]),
    _fn("run_sql", "Run ONE read-only SQL statement (SELECT, DESCRIBE, SHOW). Results are capped at 200 rows. "
        "Writes and DDL are refused: draft them with save_draft instead.", {"sql": S}, ["sql"]),
    _fn("check_naming", "Check a catalog.schema.table name against the B&B naming standard.", {"full_name": S}, ["full_name"]),
    _fn("validate_sql", "Check that SQL parses as Databricks SQL. Does not run it.", {"sql": S}, ["sql"]),
    _fn("save_draft", "Save an artifact as a DRAFT for human review. Never deploys or runs it.",
        {"kind": {"type": "string", "enum": list(DRAFT_KINDS)}, "name": S, "content": S,
         "target_table": {"type": "string", "description": "Table the artifact creates, if any (checked against naming standards)."}},
        ["kind", "name", "content"]),
]
