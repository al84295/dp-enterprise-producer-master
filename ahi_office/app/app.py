"""AHI office file upload — Databricks App (Streamlit).

Office staff upload their producer, provider and policy files from their PCs. Files land in the landing volume,
which triggers the pipeline. The app also shows each file's ABC status, and lets data stewards approve
proposed mappings for new file structures.
"""
import io
import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import yaml
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

CATALOG = os.getenv("AHI_CATALOG", "ahi_dev")
OFFICES = [o.strip() for o in os.getenv("AHI_OFFICES", "pc01,pc02,pc03").split(",")]
ENTITIES = ["producer", "provider", "policy"]
LANDING = f"/Volumes/{CATALOG}/bronze/landing"
MAPPINGS = f"/Volumes/{CATALOG}/ops/mappings"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
JOB_ID = os.getenv("AHI_JOB_ID")
ALLOWED = (".csv", ".txt", ".psv", ".xlsx", ".xls")
MAX_MB = 50

st.set_page_config(page_title="AHI office file upload", layout="wide")
w = WorkspaceClient()
cfg = Config()


def user():
    try:
        return st.context.headers.get("X-Forwarded-Email") or "unknown"
    except Exception:
        return "unknown"


@st.cache_resource
def conn():
    return dbsql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                         credentials_provider=lambda: cfg.authenticate)


def q(stmt):
    with conn().cursor() as c:
        c.execute(stmt)
        cols = [d[0] for d in c.description] if c.description else []
        return pd.DataFrame(c.fetchall(), columns=cols)


st.title("AHI office file upload")
st.caption(f"Signed in as {user()} · files go to {LANDING}")
tab_up, tab_status, tab_map = st.tabs(["Upload files", "Load status", "Mapping approvals"])

with tab_up:
    c1, c2 = st.columns(2)
    office = c1.selectbox("Office (profit centre)", OFFICES)
    entity = c2.selectbox("File type", ENTITIES)
    files = st.file_uploader("Choose files", type=[e.strip(".") for e in ALLOWED], accept_multiple_files=True)
    if files and st.button("Upload", type="primary"):
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        for f in files:
            name = re.sub(r"[^A-Za-z0-9._-]+", "_", f.name)
            if not name.lower().endswith(ALLOWED):
                st.error(f"{f.name}: file type not allowed")
                continue
            if f.size > MAX_MB * 1024 * 1024:
                st.error(f"{f.name}: larger than {MAX_MB} MB")
                continue
            path = f"{LANDING}/{office}/{entity}/{day}/{name}"
            w.files.upload(path, io.BytesIO(f.getvalue()), overwrite=False)
            st.success(f"Uploaded {f.name} to {path}")
        st.info("The pipeline starts automatically when new files arrive. Check the Load status tab in a few minutes.")
    if JOB_ID and st.button("Run the pipeline now"):
        run = w.jobs.run_now(int(JOB_ID))
        st.success(f"Pipeline run started (run id {run.run_id}).")

with tab_status:
    if not WAREHOUSE_ID:
        st.warning("No SQL warehouse configured.")
    else:
        try:
            log = q(f"SELECT started_at, profit_centre, entity, hop, regexp_extract(source_name, '[^/]+$', 0) AS file, rows_in, rows_out, "
                    f"rows_rejected, status, notes FROM {CATALOG}.ops.abc_control_log ORDER BY started_at DESC LIMIT 200")
            st.dataframe(log, width="stretch", hide_index=True)
            dq = q(f"SELECT checked_at, hop, profit_centre, entity, rule, action, rows_checked, rows_failed, status "
                   f"FROM {CATALOG}.ops.dq_results WHERE rows_failed > 0 ORDER BY checked_at DESC LIMIT 200")
            st.subheader("Data quality findings")
            st.dataframe(dq, width="stretch", hide_index=True)
        except Exception as e:
            st.info(f"No load history yet ({type(e).__name__}).")

with tab_map:
    st.caption("When an office changes its file structure, the pipeline proposes a mapping and holds those rows "
               "in bronze. Review the mapping, then approve it to release the rows on the next run.")
    try:
        props = [f for f in w.files.list_directory_contents(MAPPINGS) if f.name.endswith(".proposed.yml")]
    except Exception:
        props = []
    if not props:
        st.success("No mappings waiting for approval.")
    for f in props:
        doc = yaml.safe_load(w.files.download(f.path).contents.read())
        with st.container(border=True):
            st.markdown(f"**{doc.get('office')} · {doc['entity']} · structure {doc['signature']}**")
            if doc.get("missing_required"):
                st.error("Missing required columns: " + ", ".join(doc["missing_required"]))
            if doc.get("low_confidence"):
                st.warning("Low-confidence matches: " + ", ".join(doc["low_confidence"]))
            edited = st.data_editor(pd.DataFrame([{"source column": k, "standard column": v,
                                                    "confidence": doc["confidence"].get(v)} for k, v in doc["columns"].items()]),
                                    key="ed" + f.name, hide_index=True, disabled=["source column", "confidence"])
            fmt = st.text_input("Date format", doc.get("date_format") or "", key="df" + f.name)
            if st.button("Approve mapping", key="ap" + f.name, disabled=bool(doc.get("missing_required"))):
                doc.update(status="approved", columns=dict(zip(edited["source column"], edited["standard column"])),
                           date_format=fmt or None, approved_by=user(), approved_at=datetime.now(timezone.utc).isoformat())
                w.files.upload(f.path.replace(".proposed.yml", ".approved.yml"), io.BytesIO(yaml.safe_dump(doc, sort_keys=False).encode()),
                               overwrite=True)
                w.files.delete(f.path)
                st.success("Approved. Held rows will flow to silver on the next pipeline run.")
                st.rerun()
