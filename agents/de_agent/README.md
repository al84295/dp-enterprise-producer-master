# BEACON data engineering agent

A chat agent, deployed as a Databricks App, that helps data engineers onboard data to the platform standards. It inspects Unity Catalog tables and drafts the artifacts for each phase of the Data Onboarding Playbook. People review and deploy what it drafts.

| It can | It cannot |
|---|---|
| List, describe and profile tables (row counts, nulls, distinct values, samples) | Create, change, grant or delete anything |
| Run one read-only SQL statement at a time, capped at 200 rows | Run more than one statement, or any write or DDL |
| Check names against the B&B naming standard | Publish to production |
| Draft dbt models and schema.yml files, Lakeflow expectations, ODCS contracts, bundle snippets, ingestion configs and runbook entries | Deploy its own drafts |

## How it works

```
Engineer ──chat──▶ Databricks App (Streamlit) ──▶ Agent loop ──▶ Model serving endpoint (any chat model with tool calling)
                                                   │
                                                   ├─ list_tables, describe_table      → Unity Catalog (SDK)
                                                   ├─ profile_table, run_sql (guarded) → SQL warehouse
                                                   ├─ check_naming, validate_sql       → B&B standards, sqlglot
                                                   ├─ save_draft                       → UC volume enterprise_dev.sandbox.de_agent_drafts
                                                   └─ every call                       → audit table enterprise_dev.sandbox.de_agent_audit
```

- **Model-agnostic:** the loop uses the OpenAI-compatible API of Databricks Model Serving. Change the model by changing the endpoint name; no code changes.
- **Standards in the prompt:** `app/prompts.py` holds the B&B standards: naming, medallion roles, dbt tests, expectations, PII tagging and ODCS.

## Guardrails (defence in depth)

1. **SQL guard:** `guards.py` parses every statement and allows only a single `SELECT`, `DESCRIBE` or `SHOW`. It adds a row limit.
2. **Permissions:** the app's service principal has `SELECT` only on the schemas it may read. It can write only to the drafts volume and the audit table, so a write fails even if the guard were bypassed.
3. **Drafts only:** artifacts go to a volume for review and are deployed through the normal pull request and CI/CD path.
4. **Audit:** every tool call is logged with the signed-in user, arguments and outcome.
5. **Bounded:** at most 10 tool steps per question, and tool output is truncated before it goes back to the model.
6. **Untrusted data:** table contents are treated as data. The agent cannot act on instructions found in data, because it has no tool that changes anything.

## Try it locally (no Databricks needed)

```bash
cd app && pip install -r requirements.txt && streamlit run app.py
```

This uses a scripted demo model and DuckDB sample tables: `enterprise_dev.bronze.ams_producer` and `crm_producer`, built from the fictional Producer data. The tests run with `pytest` from this folder.

## Deploy to the POC workspace (Azure Cloud Shell)

You need the Producer POC deployed (workspace, warehouse and `enterprise_dev` catalog) and a chat model serving endpoint. To find one, go to **Serving** in the workspace: pay-per-token foundation model endpoints are listed there if your region has them. Then run:

```bash
git -C dp-enterprise-producer-master pull
bash dp*/agents/de_agent/scripts/cloudshell.sh <endpoint-name>
```

The script creates the drafts volume and audit table, deploys the app with Asset Bundles, applies the grants and starts the app. To open it, go to **Compute → Apps → de-agent-dev**.

## Path to production

- **Pull requests:** let the agent open pull requests in `dp-<domain>-<product>` repos, through a Git tool with a scoped token, instead of saving to a volume.
- **On-behalf-of-user authorization:** queries would then run with the engineer's own Unity Catalog permissions.
- **Tracing and evaluation:** use MLflow tracing and a small evaluation set ("draft a silver model for X") scored by reviewers.
- **More agents:** add the other playbook agents (ingestion, quality, context, publishing) as more tools or sub-agents, each with its own service principal.
