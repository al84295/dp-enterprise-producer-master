// BEACON Producer MDM (MVP): Azure resources for a dev environment.
//   - Azure Databricks workspace (Premium, required for Unity Catalog and Databricks Apps)
//   - ADLS Gen2 storage account + container for the enterprise_dev catalog's managed storage
//   - Access Connector for Azure Databricks (managed identity) with Storage Blob Data Contributor
// Deploy into an existing resource group: see scripts/deploy.sh.
// Network: public endpoints for the MVP. Production should use VNet injection and
// private endpoints per the B&B landing zone (open question in the SAD, section 12).

@description('Short environment name used in resource names.')
@allowed(['dev', 'test', 'prod'])
param env string = 'dev'

@description('Azure region. Must support Databricks Apps and serverless SQL.')
param location string = resourceGroup().location

@description('Workspace pricing tier. trial = Premium with 14 days of free DBUs (new workspaces only).')
@allowed(['premium', 'trial'])
param skuName string = 'premium'

@description('Prefix for resource names.')
param prefix string = 'bb-enterprise'

@description('Tags applied to every resource (budget tag per SAD section 11).')
param tags object = {
  domain: 'enterprise'
  product: 'producer-master'
  env: env
  costCenter: 'To be confirmed'
}

var suffix = uniqueString(resourceGroup().id)
var workspaceName = '${prefix}-dbw-${env}'
var storageName = toLower(take(replace('${prefix}uc${env}${suffix}', '-', ''), 24))
var connectorName = '${prefix}-dbac-${env}'
var managedRgName = 'databricks-rg-${workspaceName}-${suffix}'
var storageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: {
    name: skuName
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedRgName)
  }
}

resource connector 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: connectorName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'enterprise-${env}'
  properties: {
    publicAccess: 'None'
  }
}

resource connectorStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, connector.id, storageBlobDataContributor)
  scope: storage
  properties: {
    principalId: connector.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributor)
  }
}

output workspaceUrl string = 'https://${workspace.properties.workspaceUrl}'
output workspaceResourceId string = workspace.id
output accessConnectorId string = connector.id
output storageRootUrl string = 'abfss://${container.name}@${storage.name}.dfs.${environment().suffixes.storage}/'
