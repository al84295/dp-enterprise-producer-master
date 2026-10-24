#!/usr/bin/env bash
# Deploy the BEACON Producer MDM app (MVP) to Azure Databricks with the sample dataset.
#
# Usage:
#   scripts/deploy.sh -g <resource-group> [-l eastus2] [-e dev] [-s <steward-group>] [-c <existing-catalog>] [-t]
#   -t creates the workspace on the 14-day Premium trial tier (free DBUs).
#
# Needs: Azure CLI (logged in with `az login`), Databricks CLI v0.295 or later, Python 3.10+.
# Your Azure account needs Contributor and User Access Administrator (or Owner) on the
# resource group, because the template assigns a storage role to the access connector.
set -euo pipefail

RG="" LOCATION="eastus2" ENV="dev" STEWARD_GROUP="" CATALOG="" SKU="premium"
while getopts "g:l:e:s:c:t" opt; do
  case $opt in
    g) RG=$OPTARG ;; l) LOCATION=$OPTARG ;; e) ENV=$OPTARG ;;
    s) STEWARD_GROUP=$OPTARG ;; c) CATALOG=$OPTARG ;; t) SKU="trial" ;;
    *) echo "Usage: $0 -g <resource-group> [-l location] [-e env] [-s steward-group] [-c catalog]"; exit 1 ;;
  esac
done
[ -z "$RG" ] && { echo "Resource group is required: -g <name>"; exit 1; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
step() { printf '\n==> %s\n' "$*"; }

step "Checking tools"
command -v az >/dev/null || { echo "Install the Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli"; exit 1; }
command -v databricks >/dev/null || { echo "Install the Databricks CLI: https://docs.databricks.com/dev-tools/cli/install.html"; exit 1; }
az account show --query "{subscription:name, user:user.name}" -o tsv
databricks --version

step "1/6 Azure resources (resource group, Databricks workspace, storage, access connector)"
az group create -n "$RG" -l "$LOCATION" -o none
OUT=$(az deployment group create -g "$RG" -n producer-mdm-$ENV \
        --template-file infra/main.bicep --parameters env="$ENV" location="$LOCATION" skuName="$SKU" \
        --query properties.outputs -o json)
WORKSPACE_URL=$(echo "$OUT" | python3 -c "import json,sys;print(json.load(sys.stdin)['workspaceUrl']['value'])")
CONNECTOR_ID=$(echo "$OUT" | python3 -c "import json,sys;print(json.load(sys.stdin)['accessConnectorId']['value'])")
STORAGE_ROOT=$(echo "$OUT" | python3 -c "import json,sys;print(json.load(sys.stdin)['storageRootUrl']['value'])")
echo "Workspace: $WORKSPACE_URL"

export DATABRICKS_HOST="$WORKSPACE_URL"
export DATABRICKS_AUTH_TYPE="azure-cli"

step "2/6 Python dependencies for setup"
python3 -m pip install --quiet --upgrade "databricks-sdk>=0.60"

step "3/6 Warehouse, catalog, tables, sample data and Genie space"
ARGS=(platform --env "$ENV" --access-connector-id "$CONNECTOR_ID" --storage-root "$STORAGE_ROOT")
[ -n "$CATALOG" ] && ARGS+=(--catalog "$CATALOG")
python3 scripts/setup_databricks.py "${ARGS[@]}"
WAREHOUSE_ID=$(python3 -c "import json;print(json.load(open('.deploy_outputs.json'))['warehouse_id'])")
GENIE_ID=$(python3 -c "import json;print(json.load(open('.deploy_outputs.json'))['genie_space_id'])")
APP_CATALOG=$(python3 -c "import json;print(json.load(open('.deploy_outputs.json'))['catalog'])")
APP_SCHEMA=$(python3 -c "import json;print(json.load(open('.deploy_outputs.json'))['schema'])")

# keep app.yaml in step with the catalog and schema actually used
python3 - "$APP_CATALOG" "$APP_SCHEMA" <<'PY'
import re, sys
p = "app/app.yaml"; s = open(p).read()
s = re.sub(r'(name: MDM_CATALOG\n\s+value: )"[^"]*"', rf'\1"{sys.argv[1]}"', s)
s = re.sub(r'(name: MDM_SCHEMA\n\s+value: )"[^"]*"', rf'\1"{sys.argv[2]}"', s)
open(p, "w").write(s)
PY

VARS=(--var "env=$ENV" --var "warehouse_id=$WAREHOUSE_ID" --var "genie_space_id=$GENIE_ID")

step "4/6 Deploy the app with Databricks Asset Bundles"
databricks bundle deploy -t dev "${VARS[@]}"

step "5/6 Grant the app's service principal access to the data"
GARGS=(grants --app-name "producer-mdm-$ENV")
[ -n "$STEWARD_GROUP" ] && GARGS+=(--steward-group "$STEWARD_GROUP")
python3 scripts/setup_databricks.py "${GARGS[@]}"

step "6/6 Start the app"
databricks bundle run producer_mdm -t dev "${VARS[@]}"

echo
echo "Done. Open the app from $WORKSPACE_URL > Compute > Apps > producer-mdm-$ENV"
echo "To remove everything: databricks bundle destroy -t dev ${VARS[*]} && az group delete -n $RG"
