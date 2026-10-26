"""BEACON data engineering agent — Databricks App (Streamlit).

Chat with an agent that inspects Unity Catalog tables and drafts dbt models, quality rules, contracts
and bundle snippets to the B&B platform standards. It runs read-only SQL only; everything it writes is a
draft for a person to review.
"""
import os
import uuid

import streamlit as st

import agent
from prompts import EXAMPLES
from tools import Toolbox

MODEL = os.getenv("DE_AGENT_MODEL", "fake")              # serving endpoint name, or "fake" for the demo
LOCAL = os.getenv("DE_AGENT_BACKEND", "local" if MODEL == "fake" else "databricks") == "local"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
DRAFTS_VOLUME = os.getenv("DE_AGENT_DRAFTS_VOLUME", "/Volumes/platform_dev/sandbox/de_agent_drafts")
AUDIT_TABLE = os.getenv("DE_AGENT_AUDIT_TABLE")         # e.g. platform_dev.sandbox.de_agent_audit

st.set_page_config(page_title="BEACON data engineering agent", layout="wide")


def current_user():
    try:
        h = st.context.headers
        return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username") or "local-engineer@example.com"
    except Exception:
        return "local-engineer@example.com"


@st.cache_resource
def backend():
    if LOCAL:
        from tools import LocalBackend
        return LocalBackend()
    from tools import DatabricksBackend
    return DatabricksBackend(WAREHOUSE_ID, DRAFTS_VOLUME)


def client():
    if MODEL == "fake":
        from fake_model import FakeClient
        return FakeClient()
    return agent.databricks_client()


if "conv" not in st.session_state:
    st.session_state.conv = uuid.uuid4().hex[:12]
    st.session_state.history = []      # user/assistant turns sent to the model
    st.session_state.log = []          # what the page shows, including tool steps
    st.session_state.drafts = []

with st.sidebar:
    st.title("Data engineering agent")
    st.caption(f"Signed in as {current_user()}")
    st.caption(f"Model: {'scripted demo model' if MODEL == 'fake' else MODEL}")
    st.caption("Data: local sample tables" if LOCAL else f"Drafts: {DRAFTS_VOLUME}")
    st.markdown("**Guardrails**\n- Read-only SQL, 200-row cap\n- Changes are saved as drafts only\n"
                "- Every tool call is audited\n- Runs with the app's own Unity Catalog grants")
    if st.button("New conversation"):
        for k in ("conv", "history", "log", "drafts"):
            st.session_state.pop(k, None)
        st.rerun()

st.title("BEACON data engineering agent")
if not LOCAL and not WAREHOUSE_ID:
    st.error("No SQL warehouse is configured. Add a SQL warehouse resource to the app (see README).")
    st.stop()

if not st.session_state.log:
    st.caption("Try one of these:")
    cols = st.columns(2)
    for i, ex in enumerate(EXAMPLES):
        if cols[i % 2].button(ex, key=f"ex{i}", width="stretch"):
            st.session_state.pending = ex
            st.rerun()

for item in st.session_state.log:
    if item["kind"] == "msg":
        with st.chat_message(item["role"]):
            st.markdown(item["text"])
    else:
        with st.expander(f"Tool: {item['name']}", expanded=False):
            st.json(item["payload"], expanded=False)

prompt = st.chat_input("Ask the agent to profile, map, model or document a table") or st.session_state.pop("pending", None)
if prompt:
    st.session_state.log.append({"kind": "msg", "role": "user", "text": prompt})
    st.session_state.history.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    tb = Toolbox(backend(), user=current_user(), conversation=st.session_state.conv, audit_table=AUDIT_TABLE)

    def on_event(kind, payload):
        if kind == "tool_call":
            st.session_state.log.append({"kind": "tool", "name": payload["name"], "payload": payload["args"]})
            st.toast(f"Running {payload['name']}")

    with st.chat_message("assistant"):
        with st.spinner("Working..."):
            try:
                answer = agent.run(st.session_state.history, tb, client(), MODEL, on_event=on_event)
            except Exception as e:
                answer = f"Something went wrong calling the model: {type(e).__name__}: {e}"
        st.markdown(answer)
    st.session_state.history.append({"role": "assistant", "content": answer})
    st.session_state.log.append({"kind": "msg", "role": "assistant", "text": answer})
    st.session_state.drafts.extend(tb.drafts)
    st.rerun()

if st.session_state.drafts:
    st.divider()
    st.subheader("Drafts for review")
    for i, d in enumerate(st.session_state.drafts):
        with st.container(border=True):
            st.markdown(f"**{d['name']}** · {d['kind']} · `{d['path']}`")
            if d["findings"]:
                for f in d["findings"]:
                    st.warning(f)
            st.code(d["content"], language="sql" if d["kind"] in ("dbt_model", "dq_expectations") else "yaml")
            st.download_button("Download", d["content"], file_name=os.path.basename(d["path"]), key=f"dl{i}")
