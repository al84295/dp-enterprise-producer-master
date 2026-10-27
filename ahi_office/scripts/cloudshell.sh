#!/usr/bin/env bash
# Deploy the AHI office-files solution from Azure Cloud Shell into the POC workspace.
#   bash dp*/ahi_office/scripts/cloudshell.sh [resource-group] [workspace-name]
# Run the Producer POC deploy first: it creates the workspace, warehouse and storage.
set -euo pipefail
RG="${1:-rg-mdm-poc}"
WS="${2:-bb-enterprise-dbw-dev}"
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
python3 scripts/deploy_ahi.py 2>&1 | tee ~/ahi-deploy.log
