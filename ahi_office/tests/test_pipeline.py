import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline"))
from ahi_pipeline import LocalEngine, Pipeline, PipelineFailure  # noqa: E402
from mapper import propose  # noqa: E402

AS_OF = "2026-10-27"


@pytest.fixture()
def env(tmp_path):
    landing = tmp_path / "landing"
    shutil.copytree(ROOT / "data" / "samples", landing)
    cfgdir = tmp_path / "config"
    cfgdir.mkdir()
    shutil.copy(ROOT / "config" / "offices.yml", cfgdir / "offices.yml")
    shutil.copy(ROOT / "sql" / "gold.sql", tmp_path / "gold.sql")
    eng = LocalEngine(db_path=str(tmp_path / "ahi.duckdb"))
    mk = lambda: Pipeline(eng, cfgdir / "offices.yml", landing, as_of=AS_OF, gold_sql_path=tmp_path / "gold.sql")  # noqa: E731
    return eng, mk, landing, cfgdir


def abc(eng):
    return eng.sql("SELECT * FROM ahi_dev.ops.abc_control_log ORDER BY started_at")


def test_full_run_balances(env):
    eng, mk, *_ = env
    mk().run()
    log = abc(eng)
    assert (log[log.hop == "source_to_bronze"].status == "passed").all()
    for _, r in log[log.hop == "bronze_to_silver"].iterrows():      # rows in = out + rejected + duplicates (+ held)
        import json
        t = json.loads(r.control_totals)
        assert r.rows_in == r.rows_out + r.rows_rejected + t["rows_duplicate"] + t["rows_held"]
    pol = log[(log.hop == "bronze_to_silver") & (log.entity == "policy")].iloc[0]
    assert pol.rows_rejected == 15 - 3     # 5 bad rows per office, minus 1 orphan warn each (kept)
    dq = eng.sql("SELECT * FROM ahi_dev.ops.dq_results")
    gold_fails = dq[(dq.hop == "silver_to_gold") & (dq.action == "fail")]
    assert (gold_fails.rows_failed == 0).all()                        # all gold tie-outs pass
    q = eng.sql("SELECT reject_reason FROM ahi_dev.ops.quarantine_policy")
    assert q.reject_reason.str.contains("expiration after effective").sum() == 3


def test_reload_is_skipped(env):
    eng, mk, *_ = env
    mk().run()
    mk().run()
    log = abc(eng)
    assert (log[log.hop == "source_to_bronze"].status == "skipped").sum() == 9
    assert eng.sql("SELECT count(*) AS n FROM ahi_dev.bronze.pc01_policy").n[0] == 120   # no double load


def test_new_structure_is_held_then_mapped(env):
    eng, mk, landing, cfgdir = env
    mk().run()
    # pc01 changes its policy file: renamed, reordered columns, an extra column, semicolon delimiter
    src = pd.read_csv(landing / "pc01/policy/pc01_policy_20261026.csv", dtype=str)
    new = pd.DataFrame({
        "PolicyNum": [f"PC01-NEW{i}" for i in range(10)],
        "Producer": src["Agent Code"][:10], "Market": src["Carrier Code"][:10],
        "Effective": src["Eff Date"][:10], "Expiry": src["Exp Date"][:10],
        "Policy Status": src["Status"][:10], "Line": src["LOB"][:10],
        "Gross Premium": src["Written Premium"][:10], "Location State": src["Risk State"][:10],
        "Branch": ["North"] * 10})
    new.to_csv(landing / "pc01/policy/pc01_policy_20261102.csv", sep=";", index=False)
    mk().run()
    log = abc(eng)
    last = log[log.source_name.str.endswith("20261102.csv")].iloc[-1]
    assert last.status == "warned" and "mapping proposed" in last.notes
    sil = log[(log.hop == "bronze_to_silver") & (log.entity == "policy")].iloc[-1]
    assert '"rows_held": 10' in sil.control_totals
    proposals = list((cfgdir / "mappings").glob("pc01_policy_*.proposed.yml"))
    assert len(proposals) == 1
    doc = yaml.safe_load(proposals[0].read_text())
    assert doc["columns"]["PolicyNum"] == "policy_number"
    assert doc["columns"]["Producer"] == "producer_key"
    assert doc["columns"]["Market"] == "provider_key"
    assert doc["columns"]["Gross Premium"] == "written_premium"
    assert doc["date_format"] == "%m/%d/%Y"
    assert "Branch" in doc["unmapped_source_columns"]
    # a person reviews and approves (rename file, via pull request)
    proposals[0].rename(str(proposals[0]).replace(".proposed.yml", ".approved.yml"))
    mk().run()
    n = eng.sql("SELECT count(*) AS n FROM ahi_dev.silver.policy WHERE policy_number LIKE 'PC01-NEW%'").n[0]
    assert n == 9                                       # 1 of the 10 rows has expiration before effective
    q = eng.sql("SELECT count(*) AS n FROM ahi_dev.ops.quarantine_policy WHERE policy_number LIKE 'PC01-NEW%'").n[0]
    assert q == 1


def test_bad_batch_stops_before_silver(env, tmp_path):
    eng, mk, landing, cfgdir = env
    cfg = yaml.safe_load(open(cfgdir / "offices.yml"))
    cfg["thresholds"]["max_quarantine_pct"] = 1        # stricter than the sample data allows
    yaml.safe_dump(cfg, open(cfgdir / "offices.yml", "w"))
    with pytest.raises(PipelineFailure):
        mk().run()
    log = abc(eng)
    assert log[(log.hop == "bronze_to_silver") & (log.entity == "policy")].status.iloc[-1] == "failed"
    assert not eng.exists("ahi_dev.gold.fct_policies_in_force")


def test_propose_producer_mapping():
    cfg = yaml.safe_load(open(ROOT / "config" / "offices.yml"))
    df = pd.DataFrame({"Rep ID": ["A1", "A2"], "Rep Name": ["Jo Ellis", "Sam Webb"], "NPN #": ["17000001", "17000732"],
                       "E-mail": ["jo@example.com", "sam@example.com"], "Firm": ["X", "Y"], "St": ["FL", "TX"]})
    doc = propose(df, "producer", cfg["reference"])
    assert doc["columns"]["NPN #"] == "npn" and doc["columns"]["E-mail"] == "email" and doc["columns"]["St"] == "state"
