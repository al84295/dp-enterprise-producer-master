-- AHI gold layer. Placeholders {catalog} and {as_of} are filled in by the pipeline.
-- Written in SQL that runs on both Databricks SQL and DuckDB (for local tests).

-- Producer master: one current row per mastered producer, plus an "unknown" member for unmatched policies
CREATE OR REPLACE TABLE {catalog}.gold.dim_producer AS
SELECT producer_id, producer_name, npn, email, agency_name, state AS licence_state, match_method, source_count, offices_seen,
       CAST(_ingested_at AS TIMESTAMP) AS last_updated_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY producer_id ORDER BY _ingested_at DESC, office) AS rn,
         count(*) OVER (PARTITION BY producer_id) AS source_count,
         count(DISTINCT office) OVER (PARTITION BY producer_id) AS offices_seen
  FROM {catalog}.silver.producer
) x WHERE rn = 1
UNION ALL
SELECT 'PRD-UNKNOWN', 'Unknown producer', NULL, NULL, NULL, NULL, 'placeholder', 0, 0, NULL;

-- Provider master: carriers, MGAs and MGUs
CREATE OR REPLACE TABLE {catalog}.gold.dim_provider AS
SELECT provider_id, provider_name, provider_type, naic_code, state AS domicile_state, match_method, source_count, offices_seen,
       CAST(_ingested_at AS TIMESTAMP) AS last_updated_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY provider_id ORDER BY _ingested_at DESC, office) AS rn,
         count(*) OVER (PARTITION BY provider_id) AS source_count,
         count(DISTINCT office) OVER (PARTITION BY provider_id) AS offices_seen
  FROM {catalog}.silver.provider
) x WHERE rn = 1
UNION ALL
SELECT 'PRV-UNKNOWN', 'Unknown provider', NULL, NULL, NULL, 'placeholder', 0, 0, NULL;

-- Policy master: one row per policy term, linked to mastered producer and provider
CREATE OR REPLACE TABLE {catalog}.gold.dim_policy AS
SELECT concat(p.office, '|', p.policy_number, '|', CAST(p.term_effective_date AS STRING)) AS policy_key,
       p.office AS profit_centre, p.policy_number,
       CAST(p.term_effective_date AS DATE) AS term_effective_date, CAST(p.expiration_date AS DATE) AS expiration_date,
       p.status, p.line_of_business, CAST(p.written_premium AS DOUBLE) AS written_premium, p.risk_state,
       coalesce(sp.producer_id, 'PRD-UNKNOWN') AS producer_id,
       coalesce(sv.provider_id, 'PRV-UNKNOWN') AS provider_id,
       CAST(p._ingested_at AS TIMESTAMP) AS last_updated_at
FROM {catalog}.silver.policy p
LEFT JOIN {catalog}.silver.producer sp ON sp.office = p.office AND sp.source_key = p.producer_key
LEFT JOIN {catalog}.silver.provider sv ON sv.office = p.office AND sv.source_key = p.provider_key;

-- Policies-in-force KPI for the as-of date: effective on or before, expiring after, not cancelled
-- [Confirm] business definition and as-of frequency
CREATE OR REPLACE TABLE {catalog}.gold.fct_policies_in_force AS
SELECT DATE '{as_of}' AS as_of_date, profit_centre, producer_id, provider_id, line_of_business,
       count(*) AS policy_count, round(sum(written_premium), 2) AS written_premium
FROM {catalog}.gold.dim_policy
WHERE term_effective_date <= DATE '{as_of}' AND expiration_date > DATE '{as_of}' AND status <> 'Cancelled'
GROUP BY profit_centre, producer_id, provider_id, line_of_business;
