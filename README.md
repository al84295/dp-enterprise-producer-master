# BEACON Producer MDM (MVP) on Azure Databricks

A Streamlit Databricks App that data stewards use to manage producer master data. It runs on Azure Databricks, uses Unity Catalog and Genie, and ships with a fictional sample dataset.

## What gets built

| Layer | Resource | Name (dev) |
|---|---|---|
| Azure | Resource group | your choice (`-g`) |
| Azure | Azure Databricks workspace, Premium | `bb-enterprise-dbw-dev` |
| Azure | ADLS Gen2 storage and container (catalog managed storage) | `bbenterpriseucdev<hash>` / `enterprise-dev` |
| Azure | Access Connector for Azure Databricks, with Storage Blob Data Contributor on the storage account | `bb-enterprise-dbac-dev` |
| Unity Catalog | Storage credential and external location | `sc-enterprise-dev`, `el-enterprise-dev` |
| Unity Catalog | Catalog and schema | `enterprise_dev.silver` |
| Unity Catalog | Tables (PII columns tagged) | `producer_golden`, `producer_source_records`, `producer_change_log` |
| Databricks | Serverless SQL warehouse, 2X-Small, stops after 10 minutes idle | `wh-enterprise-dev-mdm` |
| Databricks | Genie space over the three tables | `Producer master data (dev)` |
| Databricks | App, deployed with Asset Bundles | `producer-mdm-dev` |

## Sample dataset

The files are in `data/`. All of it is fictional: emails are on example.com, phone numbers are in the 555-01xx range, and agencies and NPNs are made up. To regenerate the same data, run `python3 data/generate_sample_data.py`.

**Golden records: 150 producers**
- 66 Retail and 84 Specialty, spread across 12 agencies.
- Some quality gaps for stewards to fix: 12 have no NPN, 11 have no email and 11 have no phone.

**Source records: 258, from AMS-East, AMS-West, CRM and ProgramAdmin**

| Status | Count | What it means |
|---|---|---|
| Matched | 201 | Already confirmed |
| Proposed | 39 | Awaiting steward review |
| Unmatched | 12 | New producers with no golden record yet |
| Rejected | 6 | Look-alike names that were turned down |

Source names vary from the golden record, for example "J. Ellis", "ELLIS, JORDAN" or a middle initial.

**Change log: 150 entries**, one creation entry per golden record.

## Option A: try it locally first (5 minutes, no Azure)

```bash
scripts/run_local.sh            # opens http://localhost:8501
scripts/run_local.sh --reset    # start again from the sample data
```

The app runs on a local DuckDB copy of the sample data. Every tab works except Ask Genie, which needs Databricks. The screenshots in `docs/screenshots/` were taken in this mode.

## Option B: deploy to Azure

**What you need**

- Azure CLI, signed in with `az login` to the right subscription.
- Owner rights on the target resource group, or Contributor plus User Access Administrator. The template assigns a storage role, which needs the second permission.
- Databricks CLI v0.295 or later. The bundle's `genie_space` app resource is not recognized by older versions.
- Python 3.10 or later.
- Unity Catalog in the region. New Azure Databricks workspaces are normally enabled for it automatically. If yours isn't, the script stops and tells you to ask an account admin to assign a metastore.

**Run**

```bash
scripts/deploy.sh -g rg-bb-enterprise-mdm-dev -l eastus2 -e dev -s enterprise-data-stewards
```

The script takes about 15 to 25 minutes, most of it creating the workspace and the warehouse. It runs these steps:

1. **Azure resources:** deploys `infra/main.bicep` (workspace, storage, access connector, role assignment).
2. **Authentication:** signs in to the new workspace with your Azure CLI identity. On first sign-in, an Azure Contributor on the workspace becomes a workspace admin.
3. **Databricks setup:** `scripts/setup_databricks.py platform` creates the warehouse, storage credential, external location, catalog, schema and tables. It then loads the sample data and creates the Genie space.
4. **App deployment:** `databricks bundle deploy` creates the app with its SQL warehouse (Can use) and Genie space (Can run) resources.
5. **Access:** `setup_databricks.py grants` gives the app's service principal `USE CATALOG`, `USE SCHEMA`, `SELECT` and `MODIFY`. It also gives the steward group Can use on the app.
6. **Start:** `databricks bundle run producer_mdm` starts the app.

Every step can be run again safely: re-running skips what already exists.

**Flags**

| Flag | Meaning | Default |
|---|---|---|
| `-g` | Resource group (created if missing) | required |
| `-l` | Azure region; must support Databricks Apps and serverless SQL | `eastus2` |
| `-e` | Environment: `dev`, `test` or `prod` | `dev` |
| `-s` | Steward group to get access to the app | none |
| `-c` | Use an existing catalog instead of creating `enterprise_<env>` | create |

If you can't create storage credentials (it needs `CREATE STORAGE CREDENTIAL` on the metastore), pass `-c <a catalog you can write to>`. The script then skips the storage credential, external location and catalog steps.

**Remove everything**

```bash
databricks bundle destroy -t dev --var env=dev --var warehouse_id=<id> --var genie_space_id=<id>
az group delete -n rg-bb-enterprise-mdm-dev
```

Deleting the resource group doesn't remove the Unity Catalog objects, because the metastore lives at account level. Drop them separately:
- the `enterprise_dev` catalog, which you drop with `CASCADE`
- the external location
- the storage credential

## Using it

| Tab | What stewards do |
|---|---|
| Browse | Search and filter producers, and see quality counts |
| Create or edit | Change a record with validation and a required reason. Version locking stops two stewards overwriting each other |
| Match review | Approve, reject, or create a new producer from a source record |
| Change history | See who changed what, when, and why |
| Ask Genie | Ask questions in plain English, for example "Which active Specialty producers are missing an NPN?" |

## Costs

The serverless SQL warehouse stops after 10 minutes idle. A Databricks App is billed while it runs, so stop it when you aren't using it: `databricks apps stop producer-mdm-dev`. Monthly cost is to be confirmed with your Databricks pricing (SAD section 11).

## MVP limits

This deployment is for dev and demos only:
- **Network:** it uses public endpoints. Production needs VNet injection and private endpoints, following the B&B landing zone.
- **Write identity:** writes run as the app's service principal. Next step: on-behalf-of-user authorization.
- **Matching and survivorship:** both are manual.

## Files

```
infra/main.bicep              Azure resources
databricks.yml                Asset Bundle (the app and its resources)
app/                          Streamlit app (app.py, app.yaml, requirements.txt, local_db.py)
data/                         Sample dataset and its generator
sql/ddl.sql                   Unity Catalog schema and tables
scripts/deploy.sh             One-command deploy
scripts/setup_databricks.py   Warehouse, catalog, data, Genie, grants
scripts/run_local.sh          Local demo
docs/screenshots/             App screenshots on the sample data
```
