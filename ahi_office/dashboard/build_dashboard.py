"""Generate the AI/BI dashboard definition (ahi_gold.lvdash.json) for the AHI gold layer and ABC controls."""
import json

C = "ahi_dev"
ds = [
    {"name": "pif", "displayName": "Policies in force", "queryLines": [
        f"SELECT f.as_of_date, f.profit_centre, f.line_of_business, f.policy_count, f.written_premium, ",
        f"       p.producer_name, v.provider_name, v.provider_type ",
        f"FROM {C}.gold.fct_policies_in_force f ",
        f"JOIN {C}.gold.dim_producer p ON p.producer_id = f.producer_id ",
        f"JOIN {C}.gold.dim_provider v ON v.provider_id = f.provider_id ",
        f"WHERE f.as_of_date = (SELECT max(as_of_date) FROM {C}.gold.fct_policies_in_force)"]},
    {"name": "masters", "displayName": "Master counts", "queryLines": [
        f"SELECT (SELECT count(*) FROM {C}.gold.dim_producer WHERE producer_id <> 'PRD-UNKNOWN') AS producers, ",
        f"       (SELECT count(*) FROM {C}.gold.dim_provider WHERE provider_id <> 'PRV-UNKNOWN') AS providers, ",
        f"       (SELECT count(*) FROM {C}.gold.dim_policy) AS policy_terms"]},
    {"name": "abc", "displayName": "ABC control log", "queryLines": [
        f"SELECT started_at, hop, profit_centre, entity, regexp_extract(source_name, '[^/]+$', 0) AS source, ",
        f"       rows_in, rows_out, rows_rejected, status, notes FROM {C}.ops.abc_control_log"]},
    {"name": "dq", "displayName": "Data quality findings", "queryLines": [
        f"SELECT checked_at, hop, profit_centre, entity, rule, action, rows_checked, rows_failed, status ",
        f"FROM {C}.ops.dq_results WHERE rows_failed > 0"]},
]


def field(name, expr):
    return {"name": name, "expression": expr}


def counter(wid, dataset, name, expr, title, x, y):
    return {"widget": {"name": wid, "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": [field(name, expr)], "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": name, "displayName": title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": 1, "height": 3}}


def bar(wid, dataset, dim, measure_name, measure_expr, title, x, y, w=3):
    return {"widget": {"name": wid, "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": [
        field(dim, f"`{dim}`"), field(measure_name, measure_expr)], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": {
            "x": {"fieldName": dim, "scale": {"type": "categorical"}, "displayName": dim.replace("_", " ").capitalize()},
            "y": {"fieldName": measure_name, "scale": {"type": "quantitative"}, "displayName": title}},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": 6}}


def table(wid, dataset, cols, title, x, y, w=6, h=8):
    return {"widget": {"name": wid, "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": [field(c, f"`{c}`") for c in cols], "disaggregated": True}}],
                       "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": [{"fieldName": c, "displayName": c.replace("_", " ").capitalize()} for c in cols]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


pages = [
    {"name": "gold", "displayName": "Gold reporting", "layout": [
        counter("c_pif", "pif", "policies", "SUM(`policy_count`)", "Policies in force", 0, 0),
        counter("c_prem", "pif", "premium", "SUM(`written_premium`)", "Premium in force", 1, 0),
        counter("c_prod", "masters", "producers", "SUM(`producers`)", "Producers (mastered)", 2, 0),
        counter("c_prov", "masters", "providers", "SUM(`providers`)", "Providers (mastered)", 3, 0),
        counter("c_terms", "masters", "policy_terms", "SUM(`policy_terms`)", "Policy terms", 4, 0),
        bar("b_office", "pif", "profit_centre", "premium", "SUM(`written_premium`)", "Premium in force by profit centre", 0, 3),
        bar("b_lob", "pif", "line_of_business", "policies", "SUM(`policy_count`)", "Policies in force by line of business", 3, 3),
        bar("b_type", "pif", "provider_type", "premium", "SUM(`written_premium`)", "Premium in force by provider type", 0, 9),
        bar("b_prov", "pif", "provider_name", "premium", "SUM(`written_premium`)", "Premium in force by provider", 3, 9),
        table("t_pif", "pif", ["profit_centre", "producer_name", "provider_name", "line_of_business", "policy_count", "written_premium"],
              "Policies in force detail", 0, 15),
    ]},
    {"name": "controls", "displayName": "Data quality and ABC", "layout": [
        table("t_abc", "abc", ["started_at", "hop", "profit_centre", "entity", "source", "rows_in", "rows_out", "rows_rejected", "status", "notes"],
              "ABC control log (every load, every hop)", 0, 0, 6, 9),
        table("t_dq", "dq", ["checked_at", "hop", "profit_centre", "entity", "rule", "action", "rows_checked", "rows_failed", "status"],
              "Data quality rules with findings", 0, 9, 6, 9),
    ]},
]
json.dump({"datasets": ds, "pages": pages}, open("ahi_gold.lvdash.json", "w"), indent=1)
print("ok")
