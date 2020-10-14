[CmdletBinding(DefaultParameterSetName = 'None')]
param
(
    [String] [Parameter(Mandatory = $true)] $keyvaultName,
    [String] [Parameter(Mandatory = $true)] $storageaccesskeySecretName,
    [String] [Parameter(Mandatory = $true)] $storageaccesskeyVarName,
    [String] [Parameter(Mandatory = $true)] $databricksinstanceurlSecretName,
    [String] [Parameter(Mandatory = $true)] $databricksinstanceurlVarName,
    [String] [Parameter(Mandatory = $true)] $dabaricksaccesstokenSecretName,
    [String] [Parameter(Mandatory = $true)] $dabaricksaccesstokenVarName
#   [String] $AzureFirewallName = "AzureWebAppFirewall"
)
# Remove-AzSqlServerFirewallRule -ServerName $ServerName -FirewallRuleName $AzureFirewallName -ResourceGroupName $ResourceGroup
$storageaccesskeySecretValue = (Get-AzKeyVaultSecret -VaultName $keyvaultName -Name $storageaccesskeySecretName ).SecretValue | ConvertFrom-SecureString -AsPlainText
$databricksinstanceurlSecretValue = (Get-AzKeyVaultSecret -VaultName $keyvaultName -Name $databricksinstanceurlSecretName ).SecretValue | ConvertFrom-SecureString -AsPlainText
$dabaricksaccesstokenSecretValue = (Get-AzKeyVaultSecret -VaultName $keyvaultName -Name $dabaricksaccesstokenSecretName ).SecretValue | ConvertFrom-SecureString -AsPlainText
# $sEncodedString=[Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($sStringToEncode))

echo "##vso[task.setvariable variable=$storageaccesskeyVarName]$storageaccesskeySecretValue"
echo "##vso[task.setvariable variable=$databricksinstanceurlVarName]$databricksinstanceurlSecretValue"
echo "##vso[task.setvariable variable=$dabaricksaccesstokenVarName]$dabaricksaccesstokenSecretValue"