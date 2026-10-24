"""BEACON Master Data Steward (MVP): Producer.

Streamlit app for Databricks Apps. Data stewards browse, edit and match
producer master data stored in Unity Catalog, and ask questions through Genie.
"""
import os
import re
import uuid
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

LOCAL = os.getenv("MDM_BACKEND", "databricks").lower() == "local"
if LOCAL:
    import local_db
else:
    from databricks import sql
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config

CATALOG = os.getenv("MDM_CATALOG", "enterprise_dev")
SCHEMA = os.getenv("MDM_SCHEMA", "silver")
PREFIX = "" if LOCAL else f"{CATALOG}.{SCHEMA}."
GOLDEN = f"{PREFIX}producer_golden"
SOURCE = f"{PREFIX}producer_source_records"
LOG = f"{PREFIX}producer_change_log"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

EDITABLE = ["producer_name", "npn", "email", "phone", "agency_name", "segment",
            "office", "licensed_states", "status", "steward_notes"]
SEGMENTS = ["Retail", "Specialty"]
STATUSES = ["Active", "Inactive"]

st.set_page_config(page_title="BEACON Master Data Steward", layout="wide")


# ---------- connections ----------
@st.cache_resource
def get_config():
    return Config()


@st.cache_resource
def get_connection():
    cfg = get_config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def query(stmt, params=None):
    if LOCAL:
        return local_db.query(stmt, params)
    with get_connection().cursor() as cur:
        cur.execute(stmt, params or {})
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def execute(stmt, params=None):
    if LOCAL:
        return local_db.execute(stmt, params)
    with get_connection().cursor() as cur:
        cur.execute(stmt, params or {})


def current_user():
    """Steward identity forwarded by Databricks Apps; falls back for local runs."""
    try:
        h = st.context.headers
        return (h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username")
                or os.getenv("MDM_LOCAL_USER", "local-steward@example.com"))
    except Exception:
        return os.getenv("USER", "local-user")


def now():
    return datetime.now(timezone.utc)


def log_change(producer_id, action, field=None, old=None, new=None, reason=None):
    execute(
        f"INSERT INTO {LOG} VALUES (:cid, :pid, :action, :field, :old, :new, :reason, :by, :at)",
        {"cid": str(uuid.uuid4()), "pid": producer_id, "action": action, "field": field,
         "old": None if old is None else str(old), "new": None if new is None else str(new),
         "reason": reason, "by": current_user(), "at": now()},
    )


def validate(rec):
    errors = []
    if not (field(rec, "producer_name")).strip():
        errors.append("Producer name is required.")
    npn = (field(rec, "npn")).strip()
    if npn and not re.fullmatch(r"\d{1,10}", npn):
        errors.append("NPN must be up to 10 digits.")
    email = (field(rec, "email")).strip()
    if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        errors.append("Email address is not valid.")
    states = (field(rec, "licensed_states")).strip()
    if states and not re.fullmatch(r"[A-Z]{2}(,[A-Z]{2})*", states.replace(" ", "")):
        errors.append("Licensed states must be two-letter codes separated by commas, e.g. FL,GA.")
    return errors


def next_producer_id():
    df = query(f"SELECT max(producer_id) AS m FROM {GOLDEN}")
    last = df["m"].iloc[0] if not df.empty and df["m"].iloc[0] else "PRD-000000"
    return f"PRD-{int(last.split('-')[1]) + 1:06d}"


def clean(v):
    """Normalize empty strings and database nulls (None or NaN) to None."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str):
        return v.strip() or None
    return v


def field(rec, k):
    """Value for a form input: empty string for nulls."""
    v = clean(rec.get(k))
    return "" if v is None else str(v)


# ---------- sidebar ----------
st.sidebar.title("BEACON master data")
entity = st.sidebar.radio("Entity", ["Producer", "Client (coming soon)", "Carrier (coming soon)"], index=0)
st.sidebar.caption(f"Signed in as {current_user()}")
st.sidebar.caption("Data: local sample (DuckDB)" if LOCAL else f"Data: {CATALOG}.{SCHEMA}")
if entity != "Producer":
    st.info("Producer is the first entity in this MVP. Client and Carrier follow the same pattern.")
    st.stop()
if not LOCAL and not WAREHOUSE_ID:
    st.error("No SQL warehouse is configured. Add a SQL warehouse resource to the app (see README).")
    st.stop()

st.title("Producer master data")
tab_browse, tab_edit, tab_match, tab_hist, tab_genie = st.tabs(
    ["Browse", "Create or edit", "Match review", "Change history", "Ask Genie"])

# ---------- Browse ----------
with tab_browse:
    golden = query(f"SELECT * FROM {GOLDEN} ORDER BY producer_name")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Producers", len(golden))
    c2.metric("Missing NPN", int(golden["npn"].isna().sum()) if not golden.empty else 0)
    c3.metric("Missing email", int(golden["email"].isna().sum()) if not golden.empty else 0)
    pending = query(f"SELECT count(*) AS n FROM {SOURCE} WHERE match_status IN ('Proposed','Unmatched')")
    c4.metric("Records to review", int(pending["n"].iloc[0]))

    f1, f2, f3 = st.columns([2, 1, 1])
    term = f1.text_input("Search name, NPN, agency or email", key="search")
    seg = f2.multiselect("Segment", SEGMENTS, key="seg_filter")
    stat = f3.multiselect("Status", STATUSES, key="status_filter")
    view = golden.copy()
    if term:
        t = term.lower()
        view = view[view[["producer_name", "npn", "agency_name", "email"]].fillna("").apply(
            lambda r: any(t in str(x).lower() for x in r), axis=1)]
    if seg:
        view = view[view["segment"].isin(seg)]
    if stat:
        view = view[view["status"].isin(stat)]
    st.dataframe(view.drop(columns=["version"]), width="stretch", hide_index=True)

# ---------- Create or edit ----------
with tab_edit:
    golden = query(f"SELECT * FROM {GOLDEN} ORDER BY producer_name")
    options = ["➕ New producer"] + [f"{r.producer_id} · {r.producer_name}" for r in golden.itertuples()]
    choice = st.selectbox("Producer", options, key="edit_choice")
    is_new = choice.startswith("➕")
    rec = {} if is_new else golden[golden["producer_id"] == choice.split(" · ")[0]].iloc[0].to_dict()

    with st.form("edit_form"):
        a, b = st.columns(2)
        vals = {
            "producer_name": a.text_input("Producer name *", field(rec, "producer_name")),
            "npn": b.text_input("NPN", field(rec, "npn")),
            "email": a.text_input("Email", field(rec, "email")),
            "phone": b.text_input("Phone", field(rec, "phone")),
            "agency_name": a.text_input("Agency", field(rec, "agency_name")),
            "office": b.text_input("Office", field(rec, "office")),
            "segment": a.selectbox("Segment", SEGMENTS, index=SEGMENTS.index(rec["segment"]) if rec.get("segment") in SEGMENTS else 0),
            "status": b.selectbox("Status", STATUSES, index=STATUSES.index(rec["status"]) if rec.get("status") in STATUSES else 0),
            "licensed_states": a.text_input("Licensed states (e.g. FL,GA)", field(rec, "licensed_states")),
            "steward_notes": b.text_area("Steward notes", field(rec, "steward_notes")),
        }
        reason = st.text_input("Reason for change *")
        submitted = st.form_submit_button("Save")

    if submitted:
        vals = {k: clean(v) for k, v in vals.items()}
        errs = validate(vals) + ([] if (reason or "").strip() else ["A reason is required."])
        if errs:
            for e in errs:
                st.error(e)
        elif is_new:
            pid = next_producer_id()
            execute(
                f"INSERT INTO {GOLDEN} VALUES (:id, :producer_name, :npn, :email, :phone, :agency_name, :segment, "
                f":office, :licensed_states, :status, :steward_notes, 1, :by, :at)",
                {"id": pid, **vals, "by": current_user(), "at": now()},
            )
            log_change(pid, "create", reason=reason)
            st.success(f"Created {pid}.")
        else:
            pid = rec["producer_id"]
            latest = query(f"SELECT version FROM {GOLDEN} WHERE producer_id = :id", {"id": pid})
            if int(latest["version"].iloc[0]) != int(rec["version"]):
                st.warning("Someone else changed this producer while you were editing. Reload and try again.")
            else:
                changed = {k: v for k, v in vals.items() if clean(rec.get(k)) != v}
                if not changed:
                    st.info("No changes to save.")
                else:
                    sets = ", ".join(f"{k} = :{k}" for k in changed)
                    execute(
                        f"UPDATE {GOLDEN} SET {sets}, version = version + 1, updated_by = :by, updated_at = :at "
                        f"WHERE producer_id = :id AND version = :v",
                        {**changed, "by": current_user(), "at": now(), "id": pid, "v": int(rec["version"])},
                    )
                    for k, v in changed.items():
                        log_change(pid, "update", k, clean(rec.get(k)), v, reason)
                    st.success(f"Saved {len(changed)} change(s) to {pid}.")

# ---------- Match review ----------
with tab_match:
    st.caption("Approve or reject proposed matches between source records and golden records, or create a new golden record.")
    review = query(
        f"SELECT s.*, g.producer_name AS golden_name, g.npn AS golden_npn, g.email AS golden_email "
        f"FROM {SOURCE} s LEFT JOIN {GOLDEN} g ON s.matched_producer_id = g.producer_id "
        f"WHERE s.match_status IN ('Proposed','Unmatched') ORDER BY s.match_confidence DESC NULLS LAST")
    if review.empty:
        st.success("Nothing to review.")
    for r in review.itertuples():
        with st.container(border=True):
            left, right, act = st.columns([2, 2, 1.3])
            left.markdown(f"**Source: {r.source_system} · {r.source_key}**")
            left.write(f"{r.producer_name} · NPN {r.npn or '—'} · {r.email or '—'} · {r.agency_name or '—'}")
            if r.matched_producer_id:
                right.markdown(f"**Proposed match: {r.matched_producer_id}** ({r.match_confidence:.0%} confidence)")
                right.write(f"{r.golden_name} · NPN {r.golden_npn or '—'} · {r.golden_email or '—'}")
            else:
                right.markdown("**No match found**")
            key = f"{r.source_system}|{r.source_key}"
            params = {"sys": r.source_system, "key": r.source_key, "by": current_user(), "at": now()}
            if r.matched_producer_id and act.button("Approve match", key="ap" + key):
                execute(f"UPDATE {SOURCE} SET match_status='Matched', reviewed_by=:by, reviewed_at=:at "
                        f"WHERE source_system=:sys AND source_key=:key", params)
                log_change(r.matched_producer_id, "match_approve", "source", None, key)
                st.rerun()
            if r.matched_producer_id and act.button("Reject", key="rj" + key):
                execute(f"UPDATE {SOURCE} SET match_status='Rejected', matched_producer_id=NULL, reviewed_by=:by, "
                        f"reviewed_at=:at WHERE source_system=:sys AND source_key=:key", params)
                log_change(r.matched_producer_id, "match_reject", "source", key, None)
                st.rerun()
            if act.button("Create new producer", key="nw" + key):
                pid = next_producer_id()
                execute(
                    f"INSERT INTO {GOLDEN} (producer_id, producer_name, npn, email, agency_name, status, version, "
                    f"updated_by, updated_at) VALUES (:id, :n, :npn, :email, :agency, 'Active', 1, :by, :at)",
                    {"id": pid, "n": r.producer_name, "npn": r.npn, "email": r.email, "agency": r.agency_name,
                     "by": current_user(), "at": now()})
                execute(f"UPDATE {SOURCE} SET match_status='Matched', matched_producer_id=:id, match_confidence=1.0, "
                        f"reviewed_by=:by, reviewed_at=:at WHERE source_system=:sys AND source_key=:key",
                        {**params, "id": pid})
                log_change(pid, "new_from_source", "source", None, key)
                st.rerun()

# ---------- Change history ----------
with tab_hist:
    pid_filter = st.text_input("Filter by producer ID (optional)", key="hist_pid")
    hist = query(
        f"SELECT changed_at, producer_id, action, field, old_value, new_value, reason, changed_by FROM {LOG} "
        f"WHERE (:pid = '' OR producer_id = :pid) ORDER BY changed_at DESC LIMIT 500",
        {"pid": pid_filter.strip()})
    st.dataframe(hist, width="stretch", hide_index=True)

# ---------- Ask Genie ----------
with tab_genie:
    if LOCAL:
        st.info("Genie runs on Databricks. Deploy to Azure Databricks to ask questions in plain English.")
    elif not GENIE_SPACE_ID:
        st.info("Add a Genie space resource to the app to enable questions (see README).")
    else:
        w = WorkspaceClient()
        st.caption("Ask about producer data in plain English, e.g. \"Which active producers are missing an NPN?\"")
        if "genie_conv" not in st.session_state:
            st.session_state.genie_conv, st.session_state.genie_log = None, []
        for role, content in st.session_state.genie_log:
            with st.chat_message(role):
                if isinstance(content, pd.DataFrame):
                    st.dataframe(content, width="stretch", hide_index=True)
                else:
                    st.markdown(content)
        prompt = st.chat_input("Ask Genie about producers")
        if prompt:
            st.session_state.genie_log.append(("user", prompt))
            with st.spinner("Genie is thinking..."):
                if st.session_state.genie_conv:
                    msg = w.genie.create_message_and_wait(GENIE_SPACE_ID, st.session_state.genie_conv, prompt)
                else:
                    msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, prompt)
                    st.session_state.genie_conv = msg.conversation_id
                for att in msg.attachments or []:
                    if getattr(att, "text", None) and att.text.content:
                        st.session_state.genie_log.append(("assistant", att.text.content))
                    if getattr(att, "query", None):
                        if att.query.description:
                            st.session_state.genie_log.append(("assistant", att.query.description))
                        st.session_state.genie_log.append(("assistant", f"```sql\n{att.query.query}\n```"))
                        res = w.genie.get_message_attachment_query_result(
                            GENIE_SPACE_ID, msg.conversation_id, getattr(msg, "message_id", None) or msg.id, att.attachment_id)
                        sr = res.statement_response
                        if sr and sr.result and sr.result.data_array is not None:
                            cols = [c.name for c in sr.manifest.schema.columns]
                            st.session_state.genie_log.append(("assistant", pd.DataFrame(sr.result.data_array, columns=cols)))
            st.rerun()
        if st.session_state.genie_log and st.button("New conversation"):
            st.session_state.genie_conv, st.session_state.genie_log = None, []
            st.rerun()
