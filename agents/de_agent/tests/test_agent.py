import sys, pathlib
APP = pathlib.Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP))
import agent  # noqa: E402
from fake_model import FakeClient  # noqa: E402
from tools import LocalBackend, Toolbox  # noqa: E402


def make(tmp_path):
    return Toolbox(LocalBackend(sample_dir=APP / "sample", drafts_dir=tmp_path), user="tester@example.com", conversation="t1")


def test_profile(tmp_path):
    tb = make(tmp_path)
    events = []
    out = agent.run([{"role": "user", "content": "Profile enterprise_dev.bronze.ams_producer"}], tb, FakeClient(), "fake",
                    on_event=lambda k, p: events.append(k))
    assert "157 rows" in out
    assert "NPN_No" in out and "PII" in out
    assert events == ["tool_call", "tool_result"]
    assert tb.b.audit_log[0]["tool"] == "profile_table"


def test_dbt_drafts(tmp_path):
    tb = make(tmp_path)
    out = agent.run([{"role": "user", "content": "Draft a dbt silver model for producer"}], tb, FakeClient(), "fake")
    assert len(tb.drafts) == 2
    assert all(pathlib.Path(d["path"]).exists() for d in tb.drafts)
    assert tb.drafts[0]["findings"] == []          # SQL parses and name is compliant
    assert "Saved" in out


def test_write_refused(tmp_path):
    tb = make(tmp_path)
    out = agent.run([{"role": "user", "content": "drop enterprise_dev.bronze.ams_producer"}], tb, FakeClient(), "fake")
    assert "can't" in out
    assert tb.b.query("SELECT count(*) AS n FROM enterprise_dev.bronze.ams_producer")[0]["n"] == 157


def test_run_sql_and_bad_names(tmp_path):
    tb = make(tmp_path)
    r = tb.call("run_sql", {"sql": "select AgencyName, count(*) as n from enterprise_dev.bronze.ams_producer group by 1 order by 2 desc"})
    assert r["row_count"] > 0 and "LIMIT 200" in r["sql_run"]
    assert "error" in tb.call("save_draft", {"kind": "dbt_model", "name": "Bad Name", "content": "select 1"})
    r = tb.call("save_draft", {"kind": "dbt_model", "name": "bad_model", "content": "selec from", "target_table": "main.default.X"})
    assert r["findings"]                             # naming and SQL findings reported, still saved as a draft
    assert "error" in tb.call("nope", {})
