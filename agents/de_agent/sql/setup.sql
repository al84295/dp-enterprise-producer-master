-- BEACON data engineering agent: drafts volume and audit table (dev).
-- Statements are separated by a line containing only "-- ;". Run by scripts/deploy_de_agent.py,
-- or paste into the SQL editor. Grants for the app's service principal are applied by the script.

CREATE SCHEMA IF NOT EXISTS enterprise_dev.sandbox
  COMMENT 'Dev-only sandbox: agent drafts and audit'
-- ;
CREATE VOLUME IF NOT EXISTS enterprise_dev.sandbox.de_agent_drafts
  COMMENT 'Artifacts drafted by the data engineering agent, for human review'
-- ;
CREATE TABLE IF NOT EXISTS enterprise_dev.sandbox.de_agent_audit (
  event_at         TIMESTAMP,
  user_email       STRING  COMMENT 'Signed-in engineer the agent acted for',
  conversation_id  STRING,
  tool             STRING,
  arguments        STRING  COMMENT 'JSON arguments, truncated to 4,000 characters',
  outcome          STRING  COMMENT 'ok or error'
) COMMENT 'Every tool call made by the data engineering agent'
