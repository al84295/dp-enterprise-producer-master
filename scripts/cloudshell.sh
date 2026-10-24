#!/usr/bin/env bash
# One-line deploy from Azure Cloud Shell (Bash):
#   curl -sL https://raw.githubusercontent.com/al84295/dp-enterprise-producer-master/main/scripts/cloudshell.sh | bash
# Optional deploy.sh arguments go after "bash -s --", e.g. ... | bash -s -- -g rg-mdm-poc -l westus2 -t
# Runs in the background and logs to ~/mdm-deploy.log, so a dropped phone connection doesn't stop it.
set -euo pipefail
ARGS=("$@")
[ ${#ARGS[@]} -eq 0 ] && ARGS=(-g rg-mdm-poc -l eastus2 -t)

cd ~
if [ -d dp-enterprise-producer-master/.git ]; then
  git -C dp-enterprise-producer-master pull -q
else
  git clone -q https://github.com/al84295/dp-enterprise-producer-master.git
fi

mkdir -p ~/bin
if ! ~/bin/databricks --version >/dev/null 2>&1; then
  curl -sL -o /tmp/dbx.zip https://github.com/databricks/cli/releases/download/v0.295.0/databricks_cli_0.295.0_linux_amd64.zip
  unzip -oq /tmp/dbx.zip databricks -d ~/bin
fi
export PATH=~/bin:$PATH
echo "Databricks CLI: $(databricks --version)"

echo "Registering the Azure Databricks provider (can take a minute)..."
az provider register --namespace Microsoft.Databricks --wait

echo "Starting deployment: scripts/deploy.sh ${ARGS[*]}"
cd ~/dp-enterprise-producer-master
nohup env PATH="$PATH" scripts/deploy.sh "${ARGS[@]}" > ~/mdm-deploy.log 2>&1 &
echo "Running in the background. Progress:  tail -f ~/mdm-deploy.log"
sleep 2
tail -f ~/mdm-deploy.log
