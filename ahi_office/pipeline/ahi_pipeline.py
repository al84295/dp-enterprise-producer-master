"""AHI office files pipeline: landing -> bronze -> silver -> gold, with ABC and data quality checks at every hop.

Engine-agnostic: LocalEngine (DuckDB) for tests and demos, SparkEngine on Databricks.
Writes to <catalog>.ops: abc_control_log, dq_results, quarantine_<entity>.

  Hop 1  landing -> bronze   file checks, duplicate fingerprint, rows in = rows written, premium control total
  Hop 2  bronze  -> silver   cleansing, mastering, DQ rules (drop / warn), rows in = out + rejected + duplicates
  Hop 3  silver  -> gold     masters + policies-in-force KPI, tie-outs to silver, key integrity, one current row
"""
import hashlib
import json
import os
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from mapper import MappingRegistry, propose, signature

ENTITIES = ["producer", "provider", "policy"]       # order matters: policy checks refer to producer/provider
FORMAT_EXT = {"csv": (".csv",), "xlsx": (".xlsx", ".xls"), "txt": (".txt", ".psv")}
ABC_COLS = ["batch_id", "pipeline_name", "profit_centre", "entity", "hop", "source_name", "source_file_hash", "started_at",
            "ended_at", "run_by", "rows_in", "rows_out", "rows_rejected", "control_totals", "status", "notes"]
DQ_COLS = ["batch_id", "hop", "profit_centre", "entity", "rule", "action", "rows_checked", "rows_failed", "status", "checked_at"]


class PipelineFailure(Exception):
    pass


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def snake(name):
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def to_num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", "").str.strip(), errors="coerce")


# ---------------------------------------------------------------- engines
class LocalEngine:
    def __init__(self, catalog="ahi_dev", db_path=":memory:"):
        import duckdb
        self.con = duckdb.connect(db_path)
        self.catalog = catalog
        if catalog not in [r[0] for r in self.con.execute("SELECT database_name FROM duckdb_databases()").fetchall()]:
            self.con.execute(f"ATTACH ':memory:' AS {catalog}")
        for s in ("bronze", "silver", "gold", "ops"):
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")
        self.user = os.getenv("USER", "local")

    def exists(self, table):
        c, s, t = table.split(".")
        return bool(self.con.execute("SELECT 1 FROM information_schema.tables WHERE table_catalog=? AND table_schema=? AND table_name=?",
                                     [c, s, t]).fetchall())

    def read(self, table):
        return self.con.execute(f"SELECT * FROM {table}").df()

    def sql(self, q):
        cur = self.con.execute(q)
        return cur.df() if cur.description else None

    def write(self, df, table, mode):
        self.con.register("_df", df)
        if mode == "overwrite" or not self.exists(table):
            self.con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _df")
        else:
            existing = [r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall()]
            for c in df.columns:
                if c not in existing:
                    self.con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" VARCHAR')
            cols = ", ".join(f'"{c}"' for c in df.columns)
            self.con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _df")
        self.con.unregister("_df")


class SparkEngine:
    def __init__(self, spark, catalog):
        self.spark = spark
        self.catalog = catalog
        self.user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

    def exists(self, table):
        return self.spark.catalog.tableExists(table)

    def read(self, table):
        return self.spark.table(table).toPandas()

    def sql(self, q):
        df = self.spark.sql(q)
        return df.toPandas() if df.columns else None

    def write(self, df, table, mode):
        if df.empty:
            return
        df = df.copy()
        for c in df.columns:                      # all-null columns would be inferred as VOID, which Delta rejects
            if df[c].isna().all():
                df[c] = df[c].astype("string")
        sdf = self.spark.createDataFrame(df)
        (sdf.write.mode(mode).option("mergeSchema", "true").option("overwriteSchema", "true" if mode == "overwrite" else "false")
         .saveAsTable(table))


# ---------------------------------------------------------------- pipeline
class Pipeline:
    def __init__(self, engine, config_path, landing_root, as_of=None, gold_sql_path=None, mappings_dir=None):
        self.e = engine
        self.cat = engine.catalog
        self.cfg = yaml.safe_load(open(config_path))
        self.landing = Path(landing_root)
        self.as_of = as_of or date.today().isoformat()
        self.gold_sql = Path(gold_sql_path or Path(__file__).resolve().parent.parent / "sql" / "gold.sql")
        self.batch = now().strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6]
        repo_maps = Path(config_path).parent / "mappings"
        self.registry = MappingRegistry(self.cfg, mappings_dir or repo_maps, also_read=[repo_maps] if mappings_dir else [])
        self.proposals = []
        self.abc, self.dq = [], []

    # ---- logging
    def log_abc(self, **kw):
        row = {c: None for c in ABC_COLS}
        row.update(batch_id=self.batch, pipeline_name="ahi_office_files", run_by=self.e.user, ended_at=now(), **kw)
        row["control_totals"] = json.dumps(row["control_totals"] or {}, default=str)
        self.abc.append(row)
        return row

    def log_dq(self, hop, pc, entity, rule, action, checked, failed):
        status = "passed" if failed == 0 else {"warn": "warned", "drop": "dropped", "fail": "failed", "skip": "skipped"}[action]
        self.dq.append(dict(batch_id=self.batch, hop=hop, profit_centre=pc, entity=entity, rule=rule, action=action,
                            rows_checked=int(checked), rows_failed=int(failed), status=status, checked_at=now()))

    def flush_logs(self):
        if self.abc:
            df = pd.DataFrame(self.abc, columns=ABC_COLS)
            for c in ("rows_in", "rows_out", "rows_rejected"):
                df[c] = df[c].astype("Int64")
            self.e.write(df, f"{self.cat}.ops.abc_control_log", "append")
        if self.dq:
            self.e.write(pd.DataFrame(self.dq, columns=DQ_COLS), f"{self.cat}.ops.dq_results", "append")
        if self.proposals:
            self.e.write(pd.DataFrame(self.proposals), f"{self.cat}.ops.mapping_proposals", "append")
        self.abc, self.dq, self.proposals = [], [], []

    def run(self):
        try:
            self.ingest()
            self.to_silver()
            self.to_gold()
        finally:
            self.flush_logs()
        return self.batch

    # ================================================================ hop 1: landing -> bronze
    def loaded_hashes(self):
        t = f"{self.cat}.ops.abc_control_log"
        if not self.e.exists(t):
            return set()
        df = self.e.sql(f"SELECT source_file_hash FROM {t} WHERE hop = 'source_to_bronze' AND status IN ('passed', 'warned')")
        return set(df["source_file_hash"]) if df is not None else set()

    def read_file(self, path, office):
        oc = self.cfg["offices"][office]
        opts = dict(oc.get("read", {}))
        if oc["format"] == "xlsx":
            return pd.read_excel(path, dtype=str, keep_default_na=False, **opts)
        df = pd.read_csv(path, dtype=str, keep_default_na=False, **opts)
        if df.shape[1] == 1:   # delimiter changed: let pandas detect it
            df = pd.read_csv(path, dtype=str, keep_default_na=False, sep=None, engine="python")
        return df

    def ingest(self):
        done = self.loaded_hashes()
        for office, oc in self.cfg["offices"].items():
            for entity in ENTITIES:
                folder = self.landing / office / entity
                if not folder.exists():
                    continue
                for path in sorted(p for p in folder.rglob("*") if p.is_file()):
                    self.ingest_file(path, office, oc, entity, done)

    def ingest_file(self, path, office, oc, entity, done):
        hop, started = "source_to_bronze", now()
        h = file_hash(path)
        base = dict(profit_centre=office, entity=entity, hop=hop, source_name=str(path), source_file_hash=h, started_at=started)
        if path.suffix.lower() not in FORMAT_EXT[oc["format"]]:
            self.log_dq(hop, office, entity, "file format matches office config", "fail", 1, 1)
            self.log_abc(**base, rows_in=0, rows_out=0, rows_rejected=0, status="failed",
                         notes=f"Expected {oc['format']} file, got {path.suffix}")
            return
        if h in done:
            self.log_dq(hop, office, entity, "duplicate file fingerprint", "skip", 1, 1)
            self.log_abc(**base, rows_in=0, rows_out=0, rows_rejected=0, status="skipped", notes="File already loaded")
            return
        try:
            df = self.read_file(path, office)
        except Exception as ex:  # unreadable file
            self.log_dq(hop, office, entity, "file readable", "fail", 1, 1)
            self.log_abc(**base, rows_in=0, rows_out=0, rows_rejected=0, status="failed", notes=f"Unreadable: {ex}"[:500])
            return
        rows_in = len(df)
        self.log_dq(hop, office, entity, "file not empty", "fail", 1, int(rows_in == 0))
        if rows_in == 0:
            self.log_abc(**base, rows_in=0, rows_out=0, rows_rejected=0, status="failed", notes="Empty file")
            return
        sig = signature(df.columns)
        mapping = self.registry.approved(office, entity, sig)
        self.log_dq(hop, office, entity, "file structure has an approved mapping", "warn", 1, int(mapping is None))
        note = None
        if mapping is None:
            doc = propose(df, entity, self.cfg["reference"])
            path_p = self.registry.save_proposal(office, entity, doc)
            self.proposals.append(dict(batch_id=self.batch, profit_centre=office, entity=entity, signature=sig,
                                       source_file=path.name, proposal_file=path_p.name,
                                       mapping=json.dumps(doc["columns"]), missing_required=", ".join(doc["missing_required"]),
                                       low_confidence=", ".join(doc["low_confidence"]), auto_approvable=bool(doc["auto_approvable"]),
                                       proposed_at=now()))
            mapping = {"columns": {snake(k): v for k, v in doc["columns"].items()}}
            note = f"New file structure {sig}: mapping proposed in {path_p.name}; rows held in bronze until approved"
        premium_col = next((k for k, v in mapping["columns"].items() if v == "written_premium"), None)
        totals = {"record_count": rows_in, "schema_signature": sig}
        if premium_col:
            snake_cols = {snake(c): c for c in df.columns}
            if premium_col in snake_cols:
                totals["written_premium"] = round(float(to_num(df[snake_cols[premium_col]]).sum()), 2)
        extra = []
        df.columns = [snake(c) for c in df.columns]
        df["_batch_id"], df["_source_file"], df["_source_file_hash"] = self.batch, path.name, h
        df["_profit_centre"], df["_ingested_at"], df["_schema_signature"] = office, now(), sig
        table = f"{self.cat}.bronze.{office}_{entity}"
        self.e.write(df, table, "append")
        written = int(self.e.sql(f"SELECT count(*) AS n FROM {table} WHERE _source_file_hash = '{h}'")["n"][0])
        balanced = written == rows_in
        self.log_dq(hop, office, entity, "rows written = rows in file", "fail", rows_in, abs(written - rows_in))
        self.log_abc(**base, rows_in=rows_in, rows_out=written, rows_rejected=0, control_totals=totals,
                     status="failed" if not balanced else ("warned" if note else "passed"), notes=note)
        done.add(h)

    # ================================================================ hop 2: bronze -> silver
    def conformed(self, entity):
        """Union of all offices' bronze rows for an entity, mapped to the standard columns.

        Rows are mapped by their schema signature. Rows whose structure has no approved mapping are held.
        Returns (conformed rows, held row count, held detail).
        """
        std = self.cfg["standard"][entity]
        frames, held, detail = [], 0, []
        for office in self.cfg["offices"]:
            t = f"{self.cat}.bronze.{office}_{entity}"
            if not self.e.exists(t):
                continue
            bronze = self.e.read(t)
            for sig, grp in bronze.groupby("_schema_signature", dropna=False):
                m = self.registry.approved(office, entity, sig)
                if m is None:
                    held += len(grp)
                    detail.append(f"{office}:{sig} ({len(grp)} rows)")
                    continue
                keep = [c for c in m["columns"] if c in grp.columns]          # only this structure's columns
                df = grp[keep + ["_batch_id", "_source_file", "_ingested_at"]].rename(columns=m["columns"])
                for c in std:
                    if c not in df.columns:
                        df[c] = None
                df = df[std + ["_batch_id", "_source_file", "_ingested_at"]].copy()
                df.insert(0, "office", office)
                df["_date_format"] = m.get("date_format") or self.cfg["offices"][office].get("date_format")
                df["_mapping"] = m["source"]
                frames.append(df)
        rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return rows, held, detail

    def clean(self, entity, df):
        ref = self.cfg["reference"]
        for c in self.cfg["standard"][entity]:
            df[c] = df[c].astype(str).str.strip().replace({"": None, "None": None, "nan": None})
        if entity == "producer":
            df["email"] = df["email"].str.lower()
            df["state"] = df["state"].str.upper()
        if entity == "provider":
            df["provider_type"] = df["provider_type"].str.strip()
            df["state"] = df["state"].str.upper()
        if entity == "policy":
            for c in ("term_effective_date", "expiration_date"):
                df[c] = [pd.to_datetime(v, format=f, errors="coerce") if v else pd.NaT
                         for v, f in zip(df[c], df["_date_format"])]
                df[c] = pd.to_datetime(df[c]).dt.date
            df["written_premium"] = to_num(df["written_premium"])
            df["status"] = df["status"].map(lambda v: ref["policy_status"].get(v) if v else None)
            df["risk_state"] = df["risk_state"].str.upper()
        return df.drop(columns=["_date_format", "_mapping"])

    def rules(self, entity, df, silver):
        """List of (rule, action, failing mask). drop -> quarantine; warn -> kept, flagged."""
        ref = self.cfg["reference"]
        states, types, lobs = set(ref["states"]), set(ref["provider_types"]), set(ref["lines_of_business"])
        blank = lambda s: s.isna()  # noqa: E731
        if entity == "producer":
            key = df["office"] + "|" + df["source_key"].fillna("")
            return [("source key present", "drop", blank(df["source_key"])),
                    ("source key unique per office", "drop", key.duplicated(keep="first") & df["source_key"].notna()),
                    ("producer name present", "drop", blank(df["producer_name"])),
                    ("NPN present", "warn", blank(df["npn"])),
                    ("NPN format (digits, up to 10)", "warn", df["npn"].notna() & ~df["npn"].fillna("").str.fullmatch(r"\d{1,10}")),
                    ("email format", "warn", df["email"].notna() & ~df["email"].fillna("").str.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+")),
                    ("licence state valid", "warn", df["state"].notna() & ~df["state"].isin(states))]
        if entity == "provider":
            return [("source key present", "drop", blank(df["source_key"])),
                    ("provider name present", "drop", blank(df["provider_name"])),
                    ("provider type is Carrier, MGA or MGU", "drop", ~df["provider_type"].isin(types)),
                    ("NAIC code format (5 digits) for carriers", "warn",
                     (df["provider_type"] == "Carrier") & ~df["naic_code"].fillna("").str.fullmatch(r"\d{5}"))]
        # policy
        prod_keys = set(silver["producer"]["office"] + "|" + silver["producer"]["source_key"]) if "producer" in silver else set()
        prov_keys = set(silver["provider"]["office"] + "|" + silver["provider"]["source_key"]) if "provider" in silver else set()
        eff, exp = pd.to_datetime(df["term_effective_date"]), pd.to_datetime(df["expiration_date"])
        key = df["office"] + "|" + df["policy_number"].fillna("") + "|" + df["term_effective_date"].astype(str)
        return [("policy number present", "drop", blank(df["policy_number"])),
                ("valid effective and expiration dates", "drop", eff.isna() | exp.isna()),
                ("expiration after effective", "drop", eff.notna() & exp.notna() & (exp <= eff)),
                ("premium numeric and not negative", "drop", df["written_premium"].isna() | (df["written_premium"] < 0)),
                ("valid policy status", "drop", df["status"].isna()),
                ("valid line of business", "drop", ~df["line_of_business"].isin(lobs)),
                ("valid risk state", "drop", ~df["risk_state"].isin(states)),
                ("policy key unique (office, number, effective date)", "drop", key.duplicated(keep="first")),
                ("producer matches a silver producer", "warn", ~(df["office"] + "|" + df["producer_key"].fillna("")).isin(prod_keys)),
                ("provider matches a silver provider", "warn", ~(df["office"] + "|" + df["provider_key"].fillna("")).isin(prov_keys))]

    def master(self, entity, df):
        if entity == "producer":
            ok = df["npn"].fillna("").str.fullmatch(r"\d{1,10}")
            df["producer_id"] = ("PRD-" + df["npn"].fillna("")).where(ok, "PRD-" + df["office"].str.upper() + "-" + df["source_key"])
            df["match_method"] = ["npn" if o else "unmatched (steward review)" for o in ok]
        if entity == "provider":
            ok = df["naic_code"].fillna("").str.fullmatch(r"\d{5}")
            slug = df["provider_name"].str.lower().str.replace(r"[^a-z0-9]+", "-", regex=True).str.strip("-")
            df["provider_id"] = ("PRV-" + df["naic_code"].fillna("")).where(ok, "PRV-" + slug)
            df["match_method"] = ["naic" if o else "name (steward review)" for o in ok]
        return df

    def to_silver(self):
        hop, silver = "bronze_to_silver", {}
        th = self.cfg.get("thresholds", {})
        for entity in ENTITIES:
            max_pct = th.get(f"max_quarantine_pct_{entity}", th.get("max_quarantine_pct", 10))
            started = now()
            raw, held, held_detail = self.conformed(entity)
            self.log_dq(hop, None, entity, "rows held for mapping approval", "warn", len(raw) + held, held)
            if raw.empty:
                if held:
                    self.log_abc(profit_centre=None, entity=entity, hop=hop, source_name=f"{self.cat}.bronze.*_{entity}",
                                 started_at=started, rows_in=held, rows_out=0, rows_rejected=0, status="warned",
                                 control_totals={"rows_held": held}, notes="All rows held: " + ", ".join(held_detail))
                continue
            rows_in = len(raw)
            premium_in = round(float(to_num(raw["written_premium"]).sum()), 2) if entity == "policy" else None
            df = self.clean(entity, raw.copy())
            std = self.cfg["standard"][entity]
            dup = df.duplicated(subset=["office"] + std, keep="first")
            self.log_dq(hop, None, entity, "exact duplicate rows removed", "warn", rows_in, int(dup.sum()))
            df = df[~dup].reset_index(drop=True)
            raw_kept = raw[~dup.values].reset_index(drop=True)
            reasons = pd.Series([[] for _ in range(len(df))])
            warns = pd.Series([[] for _ in range(len(df))])
            for rule, action, mask in self.rules(entity, df, silver):
                mask = mask.fillna(True).astype(bool).reset_index(drop=True)
                self.log_dq(hop, None, entity, rule, action, len(df), int(mask.sum()))
                target = reasons if action == "drop" else warns
                for i in mask[mask].index:
                    target[i].append(rule)
            bad = reasons.map(bool)
            good = self.master(entity, df[~bad].copy())
            good["dq_warnings"] = warns[~bad].map(lambda w: "; ".join(w) or None).values
            q = raw_kept[bad.values].copy()
            q["reject_reason"] = reasons[bad].map("; ".join).values
            q["quarantined_at"] = now()
            rejected, out = len(q), len(good)
            pct = 100 * rejected / rows_in if rows_in else 0
            self.log_dq(hop, None, entity, f"quarantined share under {max_pct}%", "fail", rows_in, int(pct > max_pct) * rejected)
            balanced = rows_in == out + rejected + int(dup.sum())
            self.log_dq(hop, None, entity, "rows in = rows out + rejected + duplicates", "fail", rows_in, 0 if balanced else 1)
            totals = {"rows_duplicate": int(dup.sum()), "quarantine_pct": round(pct, 2), "rows_held": held}
            if entity == "policy":
                p_out = round(float(good["written_premium"].sum()), 2)
                p_rej = round(float(to_num(q["written_premium"]).fillna(0).sum()), 2)
                totals.update(written_premium_in=premium_in, written_premium_out=p_out, written_premium_rejected=p_rej)
                prem_ok = abs(premium_in - (p_out + p_rej + float(to_num(raw[dup.values]["written_premium"]).fillna(0).sum()))) < 0.01
                self.log_dq(hop, None, entity, "premium in = premium out + rejected + duplicates", "fail", rows_in, 0 if prem_ok else 1)
                balanced = balanced and prem_ok
            status = "failed" if (not balanced or pct > max_pct) else ("warned" if good["dq_warnings"].notna().any() or rejected else "passed")
            self.log_abc(profit_centre=None, entity=entity, hop=hop, source_name=f"{self.cat}.bronze.*_{entity}", started_at=started,
                         rows_in=rows_in + held, rows_out=out, rows_rejected=rejected, control_totals=totals,
                         status="warned" if (status == "passed" and held) else status,
                         notes=f"{rejected} rows quarantined ({pct:.1f}%)" + (f"; {held} rows held for mapping: {', '.join(held_detail)}" if held else ""))
            if status == "failed":
                raise PipelineFailure(f"Silver {entity} failed its controls; silver and gold were not updated.")
            self.e.write(q.astype(str).where(q.notna(), None), f"{self.cat}.ops.quarantine_{entity}", "overwrite")
            self.e.write(good, f"{self.cat}.silver.{entity}", "overwrite")
            silver[entity] = good

    # ================================================================ hop 3: silver -> gold
    def to_gold(self):
        hop, started, c = "silver_to_gold", now(), self.cat
        for stmt in [s.strip() for s in self.gold_sql.read_text().format(catalog=c, as_of=self.as_of).split(";") if s.strip()]:
            if not all(l.strip().startswith("--") for l in stmt.splitlines()):
                self.e.sql(stmt)
        one = lambda q: self.e.sql(q).iloc[0, 0]  # noqa: E731
        checks = [
            ("one current row per producer_id", f"SELECT count(*) - count(DISTINCT producer_id) FROM {c}.gold.dim_producer", 0),
            ("one current row per provider_id", f"SELECT count(*) - count(DISTINCT provider_id) FROM {c}.gold.dim_provider", 0),
            ("one current row per policy term", f"SELECT count(*) - count(DISTINCT policy_key) FROM {c}.gold.dim_policy", 0),
            ("dim_policy count ties to silver policy terms",
             f"SELECT (SELECT count(*) FROM {c}.gold.dim_policy) - (SELECT count(DISTINCT concat(office, '|', policy_number, '|', CAST(term_effective_date AS STRING))) FROM {c}.silver.policy)", 0),
            ("no orphan producer keys in dim_policy",
             f"SELECT count(*) FROM {c}.gold.dim_policy p LEFT JOIN {c}.gold.dim_producer d ON p.producer_id = d.producer_id WHERE d.producer_id IS NULL", 0),
            ("no orphan provider keys in dim_policy",
             f"SELECT count(*) FROM {c}.gold.dim_policy p LEFT JOIN {c}.gold.dim_provider d ON p.provider_id = d.provider_id WHERE d.provider_id IS NULL", 0),
            ("policies in force tie to silver",
             f"SELECT (SELECT coalesce(sum(policy_count), 0) FROM {c}.gold.fct_policies_in_force WHERE as_of_date = DATE '{self.as_of}') - "
             f"(SELECT count(*) FROM {c}.silver.policy WHERE term_effective_date <= DATE '{self.as_of}' AND expiration_date > DATE '{self.as_of}' AND status <> 'Cancelled')", 0),
            ("premium in force ties to silver",
             f"SELECT round((SELECT coalesce(sum(written_premium), 0) FROM {c}.gold.fct_policies_in_force WHERE as_of_date = DATE '{self.as_of}') - "
             f"(SELECT coalesce(sum(written_premium), 0) FROM {c}.silver.policy WHERE term_effective_date <= DATE '{self.as_of}' AND expiration_date > DATE '{self.as_of}' AND status <> 'Cancelled'), 2)", 0),
        ]
        failed = []
        for rule, q, expect in checks:
            v = float(one(q) or 0)
            bad = abs(v - expect) > 0.005
            self.log_dq(hop, None, "gold", rule, "fail", 1, int(bad))
            if bad:
                failed.append(f"{rule} (difference {v})")
        unknown = int(one(f"SELECT count(*) FROM {c}.gold.dim_policy WHERE producer_id = 'PRD-UNKNOWN' OR provider_id = 'PRV-UNKNOWN'"))
        total = int(one(f"SELECT count(*) FROM {c}.gold.dim_policy"))
        self.log_dq(hop, None, "gold", "policies linked to an unknown producer or provider", "warn", total, unknown)
        pif = self.e.sql(f"SELECT coalesce(sum(policy_count), 0) AS n, coalesce(sum(written_premium), 0) AS p FROM {c}.gold.fct_policies_in_force WHERE as_of_date = DATE '{self.as_of}'")
        self.log_abc(profit_centre=None, entity="gold", hop=hop, source_name=f"{c}.silver.*", started_at=started, rows_in=total,
                     rows_out=total, rows_rejected=0,
                     control_totals={"as_of_date": self.as_of, "policies_in_force": int(pif["n"][0]),
                                     "premium_in_force": round(float(pif["p"][0]), 2), "policies_unknown_party": unknown},
                     status="failed" if failed else ("warned" if unknown else "passed"),
                     notes="; ".join(failed) or (f"{unknown} policies on unknown producer or provider" if unknown else None))
        if failed:
            raise PipelineFailure("Gold controls failed: " + "; ".join(failed))
