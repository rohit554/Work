[CmdletBinding(DefaultParameterSetName = 'None')]
param
(
    [String] [Parameter(Mandatory = $true)] $resourceGroup,
    [String] [Parameter(Mandatory = $true)] $DeploymentJsonFile,
    [String] [Parameter(Mandatory = $true)] $tenant,
    [String] [Parameter(Mandatory = $true)] $rgprefix,
    [String] [Parameter(Mandatory = $true)] $env,
    [String] [Parameter(Mandatory = $true)] $currntDirectory
)
# Start-Sleep -Seconds 15
$jsondata = Get-Content -Raw -Path $DeploymentJsonFile | ConvertFrom-Json
$jsondata.$tenant.appfunctions.PSobject.Properties  | ForEach-Object {
    $name_sfx = $($_.Value).parameters.name_sfx
    $srcpath = $currntDirectory +"/"+ $($_.Value).src_code + "/*"
    $stagingDir = $currntDirectory+"/" + "staging/"
    $archiveFile = $stagingDir + $name_sfx + ".zip"
    Invoke-Expression("mkdir -p "+$stagingDir)
    Compress-Archive -Path $srcpath -DestinationPath $archiveFile -Force
    $appName = $rgprefix + $name_sfx
    $deploymentString = " Publish-AzWebapp -ResourceGroupName " + $resourceGroup +" -Name " + $appName + " -ArchivePath " + $archiveFile + " -Force"
    Invoke-Expression($deploymentString)
    if ($env -In "dev", "uat" ) {

        Start-Sleep -Seconds 20
        $stopFuncString = "Stop-AzWebApp -ResourceGroupName " + $resourceGroup + " -Name " + $appName
        Invoke-Expression($stopFuncString)
    }
}