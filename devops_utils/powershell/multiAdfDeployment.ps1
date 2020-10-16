[CmdletBinding(DefaultParameterSetName = 'None')]
param
(
    [String] [Parameter(Mandatory = $true)] $resourceGroup,
    [String] [Parameter(Mandatory = $true)] $DeploymentJsonFile,
    [String] [Parameter(Mandatory = $true)] $tenant,
    [String] [Parameter(Mandatory = $true)] $currntDirectory
)
$jsondata = Get-Content -Raw -Path $DeploymentJsonFile | ConvertFrom-Json
$jsondata.$tenant.adf.PSobject.Properties  | ForEach-Object {
    $pipelinepath = $currntDirectory +"/"+ $($_.Value).pipeline
    $deploymentString = " New-AzResourceGroupDeployment -ResourceGroupName " + $resourceGroup +" -TemplateFile " + $pipelinepath
    $($_.Value).parameters.PSobject.Properties |  ForEach-Object {
        # if ( $($_.Name) -NotIn 'env' ,'tenant') {
        $deploymentString = $deploymentString + " -$($_.Name) $($_.Value)"
        # }
    }
    Invoke-Expression("echo $deploymentString" )
}