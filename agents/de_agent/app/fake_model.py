"""A scripted stand-in for a chat model, used for local demos and tests (DE_AGENT_MODEL=fake).

It mimics the OpenAI Chat Completions interface and tool calling, so the real agent loop, tools and
guardrails run end to end. Its final answers are built from the actual tool results.
"""
import json
import re
from types import SimpleNamespace as NS


def _call(i, name, args):
    return NS(id=f"call_{i}", type="function", function=NS(name=name, arguments=json.dumps(args)))


def _resp(content=None, calls=None):
    return NS(choices=[NS(message=NS(content=content, tool_calls=calls or None))])


def _tables(text):
    return re.findall(r"[a-z_]+_(?:dev|test|prod)\.[a-z_]+\.[a-z_]+", text)


class _Completions:
    def create(self, model, messages, tools=None, **_):
        user = next(m["content"] for m in reversed(messages) if m["role"] == "user").lower()
        tool_msgs = [m for m in messages if m["role"] == "tool"]
        results = [json.loads(m["content"]) for m in tool_msgs]
        names = [tc["function"]["name"] for m in messages if m["role"] == "assistant" for tc in m.get("tool_calls", [])]
        tables = _tables(user) or ["enterprise_dev.bronze.ams_producer"]

        if "drop" in user or "delete" in user:
            if not names:
                return _resp(calls=[_call(0, "run_sql", {"sql": f"DROP TABLE {tables[0]}"})])
            return _resp(f"I can't do that: {results[-1].get('error', '')} I can draft the change for a person to review.")

        if "dbt" in user or "silver model" in user:
            if not names:
                return _resp(calls=[_call(0, "describe_table", {"full_name": "enterprise_dev.bronze.ams_producer"}),
                                    _call(1, "describe_table", {"full_name": "enterprise_dev.bronze.crm_producer"})])
            if "save_draft" not in names:
                model_sql = (
                    "-- silver.producer: conformed producers from AMS and CRM (draft)\n"
                    "with ams as (\n  select AgentCode as source_key, 'AMS' as source_system, trim(AgentName) as producer_name,\n"
                    "         nullif(trim(NPN_No), '') as npn, lower(nullif(trim(EmailAddr), '')) as email,\n"
                    "         trim(AgencyName) as agency_name, cast(LoadDate as timestamp) as loaded_at\n"
                    "  from {{ source('bronze', 'ams_producer') }}\n),\n"
                    "crm as (\n  select contact_id, 'CRM', trim(full_name), nullif(trim(national_producer_number), ''),\n"
                    "         lower(nullif(trim(email), '')), trim(account_name), cast(created_ts as timestamp)\n"
                    "  from {{ source('bronze', 'crm_producer') }}\n)\n"
                    "select * from ams\nunion all\nselect * from crm\n")
                schema_yml = (
                    "version: 2\nmodels:\n  - name: producer\n    description: Producers from AMS and CRM, conformed (draft).\n"
                    "    columns:\n      - name: source_key\n        tests: [not_null]\n      - name: source_system\n"
                    "        tests:\n          - accepted_values: {values: ['AMS', 'CRM']}\n      - name: npn\n"
                    "        description: National Producer Number (pii)\n      - name: email\n        description: Email (pii)\n")
                return _resp(calls=[
                    _call(2, "check_naming", {"full_name": "enterprise_dev.silver.producer"}),
                    _call(3, "save_draft", {"kind": "dbt_model", "name": "producer", "content": model_sql,
                                            "target_table": "enterprise_dev.silver.producer"}),
                    _call(4, "save_draft", {"kind": "dbt_schema", "name": "producer_schema", "content": schema_yml})])
            saved = [r["saved_to"] for r in results if "saved_to" in r]
            return _resp("Drafted a silver `producer` dbt model that unions AMS and CRM, trims names, lower-cases "
                         "emails and turns blanks into nulls, plus a schema.yml with tests.\n\n"
                         + "\n".join(f"- Saved: `{p}`" for p in saved)
                         + "\n\nOpen questions: which source wins when AMS and CRM disagree, and should "
                           "matching to the Producer master happen in this model or the next one? "
                           "Review by: a data engineer, then the Producer data steward.")

        if not names:
            return _resp(calls=[_call(0, "profile_table", {"full_name": tables[0]})])
        p = results[-1]
        if "error" in p:
            return _resp(f"I couldn't profile it: {p['error']}")
        gaps = [c for c in p["columns"] if (c["null_or_blank_pct"] or 0) > 0]
        pii = [c["column"] for c in p["columns"] if re.search(r"name|email|npn|phone", c["column"], re.I) and not re.search(r"agency|account", c["column"], re.I)]
        lines = [f"`{p['table']}` has {p['row_count']} rows and {len(p['columns'])} columns."]
        lines += [f"- `{c['column']}`: {c['null_or_blank_pct']}% null or blank" for c in gaps]
        lines.append(f"- Likely PII to tag and mask: {', '.join('`'+c+'`' for c in pii)}")
        lines.append("\nColumn names don't follow snake_case; the silver model should rename them.")
        return _resp("\n".join(lines))


class FakeClient:
    chat = NS(completions=_Completions())
