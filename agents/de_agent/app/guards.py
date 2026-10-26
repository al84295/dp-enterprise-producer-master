"""Guardrails for the data engineering agent: read-only SQL and B&B naming standards."""
import re

import sqlglot
from sqlglot import exp

MAX_ROWS = 200

# Statement types the agent may run itself. Everything else is drafted for a person to run.
_READ_ONLY = (exp.Select, exp.Describe, exp.Show, exp.Union, exp.Intersect, exp.Except)
_WRITE_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter,
                exp.TruncateTable, exp.Grant, exp.Command)


def check_read_only(sql: str):
    """Return (ok, reason). Only a single read-only statement is allowed."""
    text = (sql or "").strip().rstrip(";")
    if not text:
        return False, "Empty statement."
    head = text.split(None, 1)[0].upper()
    if head in ("DESCRIBE", "DESC", "SHOW"):
        return (";" not in text), "Only one statement is allowed." if ";" in text else ""
    try:
        parsed = sqlglot.parse(text, read="databricks")
    except sqlglot.errors.ParseError as e:
        return False, f"Could not parse SQL: {str(e).splitlines()[0]}"
    if len(parsed) != 1 or parsed[0] is None:
        return False, "Only one statement is allowed."
    stmt = parsed[0]
    if not isinstance(stmt, _READ_ONLY):
        return False, f"{type(stmt).__name__} statements are not allowed; the agent drafts changes for review instead."
    if any(isinstance(n, _WRITE_NODES) for n in stmt.walk()):
        return False, "Statement contains a write or DDL operation."
    return True, ""


def with_limit(sql: str, limit: int = MAX_ROWS) -> str:
    """Add or cap a LIMIT on SELECT queries so results stay small."""
    text = sql.strip().rstrip(";")
    if text.split(None, 1)[0].upper() in ("DESCRIBE", "DESC", "SHOW"):
        return text
    stmt = sqlglot.parse_one(text, read="databricks")
    current = stmt.args.get("limit")
    if current is not None:
        try:
            n = int(current.expression.this)
            if n <= limit:
                return text
        except (AttributeError, ValueError, TypeError):
            pass
    return stmt.limit(limit).sql(dialect="databricks")


# ---------- naming standards (B&B modern data platform) ----------
_SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
ENVS = ("dev", "test", "prod")


def check_name(full_name: str):
    """Check catalog.schema.table against B&B standards. Returns a list of findings (empty = compliant)."""
    parts = full_name.strip("`").split(".")
    if len(parts) != 3:
        return [f"'{full_name}' should be a three-part name: catalog.schema.table."]
    cat, sch, tbl = parts
    issues = []
    for p in parts:
        if not _SNAKE.match(p):
            issues.append(f"'{p}' should be lower snake_case.")
    if not any(cat.endswith(f"_{e}") for e in ENVS):
        issues.append(f"Catalog '{cat}' should be <domain>_<env>, with env one of dev, test, prod.")
    allowed = ("bronze", "silver", "gold", "sandbox")
    if sch not in allowed:
        issues.append(f"Schema '{sch}' should be one of {', '.join(allowed)}.")
    if sch == "sandbox" and not cat.endswith("_dev"):
        issues.append("The sandbox schema is only allowed in dev catalogs.")
    if sch == "bronze" and "_" not in tbl:
        issues.append(f"Bronze table '{tbl}' should be <source>_<entity>.")
    if sch == "gold" and not tbl.startswith(("dim_", "fct_", "mv_")):
        issues.append(f"Gold table '{tbl}' should start with dim_, fct_ or mv_.")
    if sch == "silver" and tbl.startswith(("dim_", "fct_", "mv_")):
        issues.append(f"Silver table '{tbl}' should be named for the entity, without a gold prefix.")
    return issues
