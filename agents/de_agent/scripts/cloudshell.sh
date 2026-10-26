#!/usr/bin/env bash
# Deploy the data engineering agent from Azure Cloud Shell into the POC workspace.
#   bash dp*/agents/de_agent/scripts/cloudshell.sh <model-endpoint-name> [resource-group] [workspace-name]
# Run the Producer POC deploy first: it creates the workspace, warehouse and enterprise_dev catalog.
set -euo pipefail
MODEL="${1:?Pass the chat model serving endpoint name, from Serving in the Databricks workspace}"
RG="${2:-rg-mdm-poc}"
WS="${3:-bb-enterprise-dbw-dev}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p ~/bin
if ! ~/bin/databricks --version >/dev/null 2>&1; then
  curl -sL -o /tmp/dbx.zip https://github.com/databricks/cli/releases/download/v0.295.0/databricks_cli_0.295.0_linux_amd64.zip
  unzip -oq /tmp/dbx.zip databricks -d ~/bin
fi
export PATH=~/bin:$PATH
export DATABRICKS_HOST="https://$(az resource show -g "$RG" -n "$WS" --resource-type Microsoft.Databricks/workspaces --query properties.workspaceUrl -o tsv)"
export DATABRICKS_AUTH_TYPE=azure-cli
echo "Workspace: $DATABRICKS_HOST"
python3 -m pip install --quiet --upgrade "databricks-sdk>=0.60"
cd "$HERE"
python3 scripts/deploy_de_agent.py --model "$MODEL" 2>&1 | tee ~/de-agent-deploy.log
