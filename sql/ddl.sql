-- Producer master data MVP: schema and tables in Unity Catalog.
-- ${catalog} and ${schema} are filled in by scripts/setup_databricks.py
-- (defaults enterprise_dev and silver, per the B&B naming standard).
-- Statements are separated by a line containing only "-- ;".

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
  COMMENT 'Enterprise master data, steward-managed (BEACON MVP)'
-- ;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.producer_golden (
  producer_id      STRING    NOT NULL COMMENT 'Canonical producer ID (BEACON)',
  producer_name    STRING    NOT NULL,
  npn              STRING    COMMENT 'National Producer Number',
  email            STRING,
  phone            STRING,
  agency_name      STRING,
  segment          STRING    COMMENT 'Retail or Specialty',
  office           STRING,
  licensed_states  STRING    COMMENT 'Comma-separated state codes',
  status           STRING    COMMENT 'Active or Inactive',
  steward_notes    STRING,
  version          INT       NOT NULL COMMENT 'Incremented on every change (optimistic locking)',
  updated_by       STRING,
  updated_at       TIMESTAMP,
  CONSTRAINT producer_golden_pk PRIMARY KEY (producer_id)
) COMMENT 'Producer golden records'
-- ;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.producer_source_records (
  source_system       STRING NOT NULL COMMENT 'AMS-East, AMS-West, CRM or ProgramAdmin',
  source_key          STRING NOT NULL,
  producer_name       STRING,
  npn                 STRING,
  email               STRING,
  agency_name         STRING,
  matched_producer_id STRING COMMENT 'Proposed or confirmed golden record',
  match_confidence    DOUBLE COMMENT '0 to 1',
  match_status        STRING COMMENT 'Proposed, Matched, Rejected or Unmatched',
  reviewed_by         STRING,
  reviewed_at         TIMESTAMP,
  loaded_at           TIMESTAMP
) COMMENT 'Producer records as they arrive from source systems'
-- ;
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.producer_change_log (
  change_id    STRING NOT NULL,
  producer_id  STRING,
  action       STRING COMMENT 'create, update, match_approve, match_reject, new_from_source',
  field        STRING,
  old_value    STRING,
  new_value    STRING,
  reason       STRING,
  changed_by   STRING,
  changed_at   TIMESTAMP
) COMMENT 'Steward change history'
-- ;
ALTER TABLE ${catalog}.${schema}.producer_golden ALTER COLUMN npn SET TAGS ('pii' = 'true')
-- ;
ALTER TABLE ${catalog}.${schema}.producer_golden ALTER COLUMN email SET TAGS ('pii' = 'true')
-- ;
ALTER TABLE ${catalog}.${schema}.producer_golden ALTER COLUMN phone SET TAGS ('pii' = 'true')
