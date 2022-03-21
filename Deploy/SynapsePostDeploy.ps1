param(
  [string] $NetworkIsolationMode,
  [string] $SubscriptionID,
  [string] $ResourceGroupName,
  [string] $ResourceGroupLocation,
  [string] $SynapseWorkspaceName,
  [string] $SynapseWorkspaceID,
  [string] $KeyVaultName,
  [string] $KeyVaultID,
  [string] $WorkspaceDataLakeAccountName,
  [string] $WorkspaceDataLakeAccountID,
  [string] $RawDataLakeAccountName,
  [string] $RawDataLakeAccountID,
  [string] $CuratedDataLakeAccountName,
  [string] $CuratedDataLakeAccountID,
  [string] $UAMIIdentityID,
  [Parameter(Mandatory=$false)]
  [bool] $CtrlDeployAI,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $AzMLSynapseLinkedServiceIdentityID,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $AzMLWorkspaceName,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $TextAnalyticsAccountID,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $TextAnalyticsAccountName,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $TextAnalyticsEndpoint,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $AnomalyDetectorAccountID,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $AnomalyDetectorAccountName,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $AnomalyDetectorEndpoint,
  [Parameter(Mandatory=$false)]
  [bool] $CtrlDeployCosmosDB,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $CosmosDBAccountID,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $CosmosDBAccountName,
  [AllowEmptyString()]
  [Parameter(Mandatory=$false)]
  [string] $CosmosDBDatabaseName
)


#------------------------------------------------------------------------------------------------------------
# FUNCTION DEFINITIONS
#------------------------------------------------------------------------------------------------------------
function Set-SynapseControlPlaneOperation{
  param (
    [string] $SynapseWorkspaceID,
    [string] $HttpRequestBody
  )
  
  $uri = "https://management.azure.com$SynapseWorkspaceID`?api-version=2021-06-01"
  $token = (Get-AzAccessToken -Resource "https://management.azure.com").Token
  $headers = @{ Authorization = "Bearer $token" }

  $retrycount = 1
  $completed = $false
  $secondsDelay = 60

  while (-not $completed) {
    try {
      Invoke-RestMethod -Method Patch -ContentType "application/json" -Uri $uri -Headers $headers -Body $HttpRequestBody -ErrorAction Stop
      Write-Host "Control plane operation completed successfully."
      $completed = $true
    }
    catch {
      if ($retrycount -ge $retries) {
          Write-Host "Control plane operation failed the maximum number of $retryCount times."
          Write-Warning $Error[0]
          throw
      } else {
          Write-Host "Control plane operation failed $retryCount time(s). Retrying in $secondsDelay seconds."
          Write-Warning $Error[0]
          Start-Sleep $secondsDelay
          $retrycount++
      }
    }
  }
}

function Save-SynapseLinkedService{
  param (
    [string] $SynapseWorkspaceName,
    [string] $LinkedServiceName,
    [string] $LinkedServiceRequestBody
  )

  [string] $uri = "https://$SynapseWorkspaceName.dev.azuresynapse.net/linkedservices/$LinkedServiceName"
  $uri += "?api-version=2019-06-01-preview"

  Write-Host "Creating Linked Service [$LinkedServiceName]..."
  $retrycount = 1
  $completed = $false
  $secondsDelay = 60

  while (-not $completed) {
    try {
      Invoke-RestMethod -Method Put -ContentType "application/json" -Uri $uri -Headers $headers -Body $LinkedServiceRequestBody -ErrorAction Stop
      Write-Host "Linked service [$LinkedServiceName] created successfully."
      $completed = $true
    }
    catch {
      if ($retrycount -ge $retries) {
          Write-Host "Linked service [$LinkedServiceName] creation failed the maximum number of $retryCount times."
          Write-Warning $Error[0]
          throw
      } else {
          Write-Host "Linked service [$LinkedServiceName] creation failed $retryCount time(s). Retrying in $secondsDelay seconds."
          Write-Warning $Error[0]
          Start-Sleep $secondsDelay
          $retrycount++
      }
    }
  }
}


function Save-SynapseSampleCode{
  param (
    [string] $SampleCodeCollection
  )

  #Install Synapse PowerShell Module
  Install-Module az.synapse -Force

  #$sampleCodeIndex = Invoke-WebRequest 
  $sampleCodeIndex = Get-Content -Path ..\Sample\index.json

}

#------------------------------------------------------------------------------------------------------------
# MAIN SCRIPT BODY
#------------------------------------------------------------------------------------------------------------

$retries = 10
$secondsDelay = 60

#------------------------------------------------------------------------------------------------------------
# CONTROL PLANE OPERATION: ASSIGN SYNAPSE WORKSPACE ADMINISTRATOR TO USER-ASSIGNED MANAGED IDENTITY
# UAMI needs Synapse Admin rights before it can make calls to the Data Plane APIs to create Synapse objects
#------------------------------------------------------------------------------------------------------------

$token = (Get-AzAccessToken -Resource "https://dev.azuresynapse.net").Token
$headers = @{ Authorization = "Bearer $token" }

$uri = "https://$SynapseWorkspaceName.dev.azuresynapse.net/rbac/roleAssignments?api-version=2020-02-01-preview"

#Assign Synapse Workspace Administrator Role to UAMI
$body = "{
  roleId: ""6e4bf58a-b8e1-4cc3-bbf9-d73143322b78"",
  principalId: ""$UAMIIdentityID""
}"

Write-Host "Assign Synapse Administrator Role to UAMI..."

Invoke-RestMethod -Method Post -ContentType "application/json" -Uri $uri -Headers $headers -Body $body

#------------------------------------------------------------------------------------------------------------
# CONTROL PLANE OPERATION: ASSIGN SYNAPSE APACHE SPARK ADMINISTRATOR TO AZURE ML LINKED SERVICE MSI
# If AI Services are deployed, then Azure ML MSI needs Synapse Spark Admin rights to use Spark clusters as compute
#------------------------------------------------------------------------------------------------------------

if (-not ([string]::IsNullOrEmpty($AzMLSynapseLinkedServiceIdentityID))) {
  #Assign Synapse Apache Spark Administrator Role to Azure ML Linked Service Managed Identity
  # https://docs.microsoft.com/en-us/azure/machine-learning/how-to-link-synapse-ml-workspaces#link-workspaces-with-the-python-sdk

  $body = "{
    roleId: ""c3a6d2f1-a26f-4810-9b0f-591308d5cbf1"",
    principalId: ""$AzMLSynapseLinkedServiceIdentityID""
  }"

  Write-Host "Assign Synapse Apache Spark Administrator Role to Azure ML Linked Service Managed Identity..."
  Invoke-RestMethod -Method Post -ContentType "application/json" -Uri $uri -Headers $headers -Body $body

  # From: https://docs.microsoft.com/en-us/azure/synapse-analytics/security/how-to-manage-synapse-rbac-role-assignments
  # Changes made to Synapse RBAC role assignments may take 2-5 minutes to take effect.
  # Retry logic required before calling further APIs
}

#------------------------------------------------------------------------------------------------------------
# DATA PLANE OPERATION: CREATE AZURE KEY VAULT LINKED SERVICE
#------------------------------------------------------------------------------------------------------------

#Create AKV Linked Service. Linked Service name same as Key Vault's.

$body = "{
  name: ""$KeyVaultName"",
  properties: {
      annotations: [],
      type: ""AzureKeyVault"",
      typeProperties: {
          baseUrl: ""https://$KeyVaultName.vault.azure.net/""
      }
  }
}"

Save-SynapseLinkedService $SynapseWorkspaceName $KeyVaultName $body

#------------------------------------------------------------------------------------------------------------
# DATA PLANE OPERATION: CREATE WORKSPACE, RAW AND CURATED DATA LAKES LINKED SERVICES
#------------------------------------------------------------------------------------------------------------

$dataLakeAccountNames = $WorkspaceDataLakeAccountName, $RawDataLakeAccountName, $CuratedDataLakeAccountName
$dataLakeDFSEndpoints = "https://$WorkspaceDataLakeAccountName.dfs.core.windows.net", "https://$RawDataLakeAccountName.dfs.core.windows.net", "https://$CuratedDataLakeAccountName.dfs.core.windows.net"

for ($i = 0; $i -lt $dataLakeAccountNames.Length ; $i++ ) {

  $body = "{
    name: ""$($dataLakeAccountNames[$i])"",
    properties: {
      annotations: [],
      type: ""AzureBlobFS"",
      typeProperties: {
        url: ""$($dataLakeDFSEndpoints[$i])""
      },
      connectVia: {
        referenceName: ""AutoResolveIntegrationRuntime"",
        type: ""IntegrationRuntimeReference""
      }
    }
  }"

  Save-SynapseLinkedService $SynapseWorkspaceName $dataLakeAccountNames[$i] $body
}

#------------------------------------------------------------------------------------------------------------
# DATA PLANE OPERATION: CREATE AZURE ML LINKED SERVICE
#------------------------------------------------------------------------------------------------------------
#-AzMLWorkspaceName paramater will be passed blank if AI workload is not deployed.

if ($CtrlDeployAI) {
  $body = "{
    name: ""$AzMLWorkspaceName"",
    properties: {
      annotations: [],
      type: ""AzureMLService"",
      typeProperties: {
          subscriptionId: ""$SubscriptionID"",
          resourceGroupName: ""$ResourceGroupName"",
          mlWorkspaceName: ""$AzMLWorkspaceName"",
          authentication: ""MSI""
      },
      connectVia: {
          referenceName: ""AutoResolveIntegrationRuntime"",
          type: ""IntegrationRuntimeReference""
      }
    }
  }"

  Save-SynapseLinkedService $SynapseWorkspaceName $AzMLWorkspaceName $body
}

#------------------------------------------------------------------------------------------------------------
# DATA PLANE OPERATION: CREATE COSMOSDB LINKED SERVICE
#------------------------------------------------------------------------------------------------------------
#-CosmosDBAccountName paramater will be passed blank if CosmosDB workload is not deployed.

if ($CtrlDeployCosmosDB) {
  $body = "{
  name: ""$CosmosDBAccountName"",
  properties: {
    annotations: [],
    type: ""CosmosDb"",
    typeProperties: {
      connectionString: ""AccountEndpoint=https://$CosmosDBAccountName.documents.azure.com:443/;Database=OperationalDB"",
      accountKey: {
        type: ""AzureKeyVaultSecret"",
        store: {
          referenceName: ""$KeyVaultName"",
          type: ""LinkedServiceReference""
        },
        secretName: ""$CosmosDBAccountName-Key""
      }
    },
    connectVia: {
      referenceName: ""AutoResolveIntegrationRuntime"",
      type: ""IntegrationRuntimeReference""
    }
  }
}"

  Save-SynapseLinkedService $SynapseWorkspaceName $CosmosDBAccountName $body
}

#------------------------------------------------------------------------------------------------------------
# DATA PLANE OPERATION: CREATE COGNITIVE SERVICES (TEXT ANALYTICS AND ANOMALY DETECTOR) LINKED SERVICES
#------------------------------------------------------------------------------------------------------------
if ($CtrlDeployAI) {
  $cognitiveServiceNames = $TextAnalyticsAccountName, $AnomalyDetectorAccountName
  $cognitiveServiceEndpoints = $TextAnalyticsEndpoint, $AnomalyDetectorEndpoint
  $cognitiveServiceTypes = "TextAnalytics", "AnomalyDetector"

  for ($i = 0; $i -lt $cognitiveServiceNames.Length ; $i++ ) {
    $body = "{
      name: ""$($cognitiveServiceNames[$i])"",
      properties: {
          annotations: [],
          type: ""CognitiveService"",
          typeProperties: {
              subscriptionId: ""$SubscriptionID"",
              resourceGroup: ""$ResourceGroupName"",
              csName: ""$($cognitiveServiceNames[$i])"",
              csKind: ""$($cognitiveServiceTypes[$i])"",
              csLocation: ""$ResourceGroupLocation"",
              endPoint: ""$($cognitiveServiceEndpoints[$i])"",
              csKey: {
                  type: ""AzureKeyVaultSecret"",
                  store: {
                      referenceName: ""$KeyVaultName"",
                      type: ""LinkedServiceReference""
                  },
                  secretName: ""$($cognitiveServiceNames[$i])-Key""
              }
          },
          connectVia: {
              referenceName: ""AutoResolveIntegrationRuntime"",
              type: ""IntegrationRuntimeReference""
          }
      }
    }"
  
    Save-SynapseLinkedService $SynapseWorkspaceName $cognitiveServiceNames[$i] $body
  }
}

#------------------------------------------------------------------------------------------------------------
# DATA PLANE OPERATOR: CREATE AND APPROVE MANAGED PRIVATE ENDPOINTS
# For vNet-integrated deployments, create the private endpoints to the resources required by Synapse managed vNet
#------------------------------------------------------------------------------------------------------------

#Create Managed Private Endpoints
if ($NetworkIsolationMode -eq "vNet") {
  [string[]] $managedPrivateEndpointNames = $KeyVaultName, $WorkspaceDataLakeAccountName, $RawDataLakeAccountName, $CuratedDataLakeAccountName
  [string[]] $managedPrivateEndpointIDs = $KeyVaultID, $WorkspaceDataLakeAccountID, $RawDataLakeAccountID, $CuratedDataLakeAccountID
  [string[]] $managedPrivateEndpointGroups = 'vault', 'dfs', 'dfs', 'dfs'

  #If AI workload is deployed then add cognitive services to the list of managed endpoints.
  if($CtrlDeployAI) {
    [string[]] $cognitiveServicePrivateEndpointNames = $TextAnalyticsAccountName, $AnomalyDetectorAccountName
    [string[]] $cognitiveServicePrivateEndpointIDs = $TextAnalyticsAccountID, $AnomalyDetectorAccountID
    [string[]] $cognitiveServicePrivateEndpointGroups =  'account', 'account'

    $managedPrivateEndpointNames += $cognitiveServicePrivateEndpointNames
    $managedPrivateEndpointIDs += $cognitiveServicePrivateEndpointIDs
    $managedPrivateEndpointGroups += $cognitiveServicePrivateEndpointGroups
  }

  #If CosmosDB operational workload is deployed then add CosmosDB SQL and Analytical subsystems to the list of managed endpoints.
  if ($CtrlDeployCosmosDB) {
      [string[]] $cosmosDBPrivateEndpointNames = $CosmosDBAccountName, $CosmosDBAccountName
      [string[]] $cosmosDBPrivateEndpointIDs = $CosmosDBAccountID, $CosmosDBAccountID
      [string[]] $cosmosDBPrivateEndpointGroups = 'Analytical', 'Sql'
    
      $managedPrivateEndpointNames += $cosmosDBPrivateEndpointNames
      $managedPrivateEndpointIDs += $cosmosDBPrivateEndpointIDs
      $managedPrivateEndpointGroups += $cosmosDBPrivateEndpointGroups
  }

  for($i = 0; $i -le ($managedPrivateEndpointNames.Length - 1); $i += 1)
  {
    $managedPrivateEndpointName = [System.String]::Concat($managedPrivateEndpointNames[$i],"-",$managedPrivateEndpointGroups[$i])
    $managedPrivateEndpointID = $managedPrivateEndpointIDs[$i]
    $managedPrivateEndpointGroup = $managedPrivateEndpointGroups[$i] 

    $uri = "https://$SynapseWorkspaceName.dev.azuresynapse.net"
    $uri += "/managedVirtualNetworks/default/managedPrivateEndpoints/$managedPrivateEndpointName"
    $uri += "?api-version=2019-06-01-preview"

    $body = "{
        name: ""$managedPrivateEndpointName-$managedPrivateEndpointGroup"",
        type: ""Microsoft.Synapse/workspaces/managedVirtualNetworks/managedPrivateEndpoints"",
        properties: {
          privateLinkResourceId: ""$managedPrivateEndpointID"",
          groupId: ""$managedPrivateEndpointGroup"",
          name: ""$managedPrivateEndpointName""
        }
    }"

    Write-Host "Create Managed Private Endpoint for $managedPrivateEndpointName..."
    $retrycount = 1
    $completed = $false
    
    while (-not $completed) {
      try {
        Invoke-RestMethod -Method Put -ContentType "application/json" -Uri $uri -Headers $headers -Body $body -ErrorAction Stop
        Write-Host "Managed private endpoint for $managedPrivateEndpointName created successfully."
        $completed = $true
      }
      catch {
        if ($retrycount -ge $retries) {
          Write-Host "Managed private endpoint for $managedPrivateEndpointName creation failed the maximum number of $retryCount times."
          throw
        } else {
          Write-Host "Managed private endpoint creation for $managedPrivateEndpointName failed $retryCount time(s). Retrying in $secondsDelay seconds."
          Start-Sleep $secondsDelay
          $retrycount++
        }
      }
    }
  }

  #30 second delay interval for private link provisioning state = Succeeded
  $secondsDelay = 30

  #Approve Private Endpoints
  for($i = 0; $i -le ($managedPrivateEndpointNames.Length - 1); $i += 1)
  {
    $retrycount = 1
    $completed = $false
    
    while (-not $completed) {
      try {
        $managedPrivateEndpointName = [System.String]::Concat($managedPrivateEndpointNames[$i],"-",$managedPrivateEndpointGroups[$i])
        $managedPrivateEndpointID = $managedPrivateEndpointIDs[$i]
        # Approve KeyVault Private Endpoint
        $privateEndpoints = Get-AzPrivateEndpointConnection -PrivateLinkResourceId $managedPrivateEndpointID -ErrorAction Stop | where-object{$_.PrivateEndpoint.Id -match ($SynapseWorkspaceName + "." + $managedPrivateEndpointName)} | select-object Id, ProvisioningState, PrivateLinkServiceConnectionState
        
        foreach ($privateEndpoint in $privateEndpoints) {
          if ($privateEndpoint.ProvisioningState -eq "Succeeded") {
            if ($privateEndpoint.PrivateLinkServiceConnectionState.Status -eq "Pending") {
              Write-Host "Approving private endpoint for $managedPrivateEndpointName."
              Approve-AzPrivateEndpointConnection -ResourceId $privateEndpoint.Id -Description "Auto-Approved" -ErrorAction Stop    
              $completed = $true
            }
            elseif ($privateEndpoint.PrivateLinkServiceConnectionState.Status -eq "Approved") {
              $completed = $true
            }
          }
        }
        
        if(-not $completed) {
          throw "Private endpoint connection not yet provisioned."
        }
      }
      catch {
        if ($retrycount -ge $retries) {
          Write-Host "Private endpoint approval for $managedPrivateEndpointName has failed the maximum number of $retryCount times."
          throw
        } else {
          Write-Host "Private endpoint approval for $managedPrivateEndpointName has failed $retryCount time(s). Retrying in $secondsDelay seconds."
          Write-Warning $PSItem.ToString()
          Start-Sleep $secondsDelay
          $retrycount++
        }
      }
    }
  }
}

#------------------------------------------------------------------------------------------------------------
# CONTROL PLANE OPERATOR: DISABLE PUBLIC NETWORK ACCESS
# For vNet-integrated deployments, disable public network access. Access to Synapse only through private endpoints.
#------------------------------------------------------------------------------------------------------------

if ($NetworkIsolationMode -eq "vNet") {
  $body = "{properties:{publicNetworkAccess:""Disabled""}}"
  Set-SynapseControlPlaneOperation -SynapseWorkspaceID $SynapseWorkspaceID -HttpRequestBody $body
}