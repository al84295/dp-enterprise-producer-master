# AHI office files to gold reporting

This builds the AHI Architecture and Design Specification on Azure Databricks.
- **Upload:** offices (profit centres) upload their producer, provider and policy files from their PCs.
- **Pipeline:** the files go to bronze as delivered, are conformed in silver, and are published as gold masters and a policies-in-force KPI.
- **Checks:** ABC (audit, balance, control) and data quality checks run at every hop.
- **Reporting:** a Databricks dashboard and a Genie space sit on gold.

```
Office PC ──upload──▶ Upload app ──▶ /Volumes/ahi_dev/bronze/landing/<office>/<entity>/   (file arrival triggers the job)
                                          │
  Hop 1  landing → bronze   bronze.<office>_<entity>   as delivered + _batch_id, _source_file, _source_file_hash, _schema_signature
  Hop 2  bronze  → silver   silver.producer / provider / policy    mapped, cleansed, mastered, DQ rules
  Hop 3  silver  → gold     gold.dim_producer / dim_provider / dim_policy / fct_policies_in_force
                                          │
  ops.abc_control_log · ops.dq_results · ops.quarantine_<entity> · ops.mapping_proposals
                                          │
                      AI/BI dashboard "AHI gold reporting" + Genie space
```

## ABC and data quality checks by layer

| Hop | Audit | Balance | Control: data quality rules (action) |
|---|---|---|---|
| Landing → bronze | One log row per file: batch, file, fingerprint, signature, user, times | Rows written = rows in file. Premium control total recorded | File type matches the office (fail) · file readable (fail) · not empty (fail) · duplicate fingerprint (skip) · structure has an approved mapping (warn, rows held) |
| Bronze → silver | One log row per entity per run | Rows in = rows out + rejected + duplicates + held. Premium in = out + rejected + duplicates | Keys present and unique (drop) · names present (drop) · valid dates, expiration after effective (drop) · premium numeric, not negative (drop) · valid status, line of business, state, provider type (drop) · NPN, email and NAIC format (warn) · producer and provider match (warn) · quarantined share under threshold (fail, stops the run) |
| Silver → gold | One log row per run, with policies and premium in force | Gold policy terms = silver policy terms. Policies and premium in force tie to silver | One current row per producer, provider and policy (fail) · no orphan keys (fail) · policies on unknown producer or provider (warn) |

- **Quarantine:** dropped rows go to `ops.quarantine_<entity>` with the reason.
- **Fail actions:** a failed control stops the run before silver or gold is changed.
- **Thresholds:** set in `config/offices.yml`.

## Mapping process (different file structures)

Each office sends the same three file types, but the column names, order, delimiter and date format differ. The pipeline handles this without code changes:

1. **Fingerprint:** every bronze row carries a schema signature, a fingerprint of its file's column names.
2. **Known structure:** rows are mapped to the standard model using the office's mapping in `config/offices.yml`, or an approved mapping in `config/mappings/` (repo) or `/Volumes/ahi_dev/ops/mappings/`.
3. **New structure:**
   - **Proposal:** the file still lands in bronze. `pipeline/mapper.py` proposes a mapping from the column names (a synonym list and fuzzy matching) and the values (dates, numbers, emails, state codes, NPN and NAIC patterns). It also detects the date format. The proposal is written to the mappings volume and logged in `ops.mapping_proposals`.
   - **Held rows:** the file's rows are held, neither lost nor rejected, and counted in the balance check.
4. **Approval:** a data steward reviews the proposal in the upload app's **Mapping approvals** tab, edits it if needed, and approves it. On the next run, the held rows go through the quality rules into silver.

To try the mapper on any file: `python3 pipeline/mapper.py --file <file> --office pc04 --entity policy`.

## Try it locally (no Databricks)

```bash
pip install pandas pyyaml openpyxl duckdb pytest
python3 data/generate_samples.py     # 3 offices x 3 file types (CSV, Excel, pipe-delimited), fictional, with planted errors
python3 pipeline/local_run.py        # runs all three hops on DuckDB and prints the ABC log
python3 -m pytest tests              # balance, re-load skip, new-structure mapping, control failure, mapper
```

## Deploy to the POC workspace (Azure Cloud Shell)

You need the Producer POC deployed first (workspace, warehouse and storage). Then run:

```bash
git -C dp-enterprise-producer-master pull
bash dp*/ahi_office/scripts/cloudshell.sh
```

The script does the following:
- creates `ahi_dev` with the bronze, silver, gold and ops schemas, plus the landing and mappings volumes
- deploys the job (triggered by file arrival), the upload app and the dashboard with Asset Bundles
- applies the grants
- uploads the sample files and runs the pipeline once
- creates the Genie space

Then open:
- **Compute → Apps → ahi-upload-dev:** upload files, see load status, approve mappings
- **Dashboards → AHI gold reporting (dev):** the gold reporting page, plus a data quality and ABC page
- **Genie → AHI gold reporting (ahi_dev):** for example "What is premium in force by profit centre?"

## Not yet built (next steps)

- **Metric view:** `gold.mv_policies_in_force`.
- **Policy transactions and submissions:** these are in the spec but not in the three office file types.
- **SCD type 2 history:** for producer, provider and policy.
- **Steward review of matches:** reuse the Producer steward app pattern.
- **Scale:** the pipeline uses pandas inside the Spark job, which suits office-sized files. Move hops 2 and 3 to Lakeflow Declarative Pipelines for large volumes.
