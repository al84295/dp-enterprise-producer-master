import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "app"))
from guards import check_read_only, with_limit, check_name  # noqa: E402


def test_select_allowed():
    assert check_read_only("SELECT * FROM enterprise_dev.silver.producer")[0]
    assert check_read_only("select count(*) from a.b.c where x = 1;")[0]
    assert check_read_only("WITH t AS (SELECT 1 AS a) SELECT a FROM t")[0]
    assert check_read_only("DESCRIBE TABLE enterprise_dev.silver.producer")[0]
    assert check_read_only("SHOW TABLES IN enterprise_dev.silver")[0]


def test_writes_blocked():
    for sql in ["DROP TABLE a.b.c", "DELETE FROM a.b.c", "INSERT INTO a.b.c VALUES (1)",
                "UPDATE a.b.c SET x = 1", "CREATE TABLE a.b.c (x INT)", "GRANT SELECT ON TABLE a.b.c TO `x`",
                "MERGE INTO a.b.c t USING d s ON t.id = s.id WHEN MATCHED THEN DELETE",
                "SELECT 1; DROP TABLE a.b.c", "ALTER TABLE a.b.c ADD COLUMN y INT", ""]:
        ok, why = check_read_only(sql)
        assert not ok, sql


def test_limit():
    assert "LIMIT 200" in with_limit("SELECT * FROM a.b.c")
    assert with_limit("SELECT * FROM a.b.c LIMIT 10").endswith("LIMIT 10")
    assert "LIMIT 200" in with_limit("SELECT * FROM a.b.c LIMIT 100000")
    assert with_limit("DESCRIBE a.b.c") == "DESCRIBE a.b.c"


def test_names():
    assert check_name("enterprise_dev.silver.producer") == []
    assert check_name("enterprise_prod.gold.dim_producer") == []
    assert check_name("specialty_dev.bronze.ams_producer") == []
    assert check_name("main.default.Producers")
    assert check_name("enterprise_dev.gold.producer")          # gold needs a prefix
    assert check_name("enterprise_prod.sandbox.x_y")           # sandbox only in dev
    assert check_name("enterprise_dev.silver.producer_golden") == []
    assert check_name("producer")                               # not three parts
