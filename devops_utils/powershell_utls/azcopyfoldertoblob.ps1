[CmdletBinding(DefaultParameterSetName = 'None')]
param
(
  [String] [Parameter(Mandatory = $true)] $uploaddirectory,
  [String] [Parameter(Mandatory = $true)] $storageaccountname,
  [String] [Parameter(Mandatory = $true)] $blobcontainername,
  [String] [Parameter(Mandatory = $true)] $blobcontainerpath,
  [String] [Parameter(Mandatory = $true)] $storageKey
  
)
#Download AzCopy
#wget https://aka.ms/downloadazcopy-v10-linux
 
#Expand Archive
#tar -xvf downloadazcopy-v10-linux
 
#(Optional) Remove existing AzCopy version
#sudo rm /usr/bin/azcopy
 
#Move AzCopy to the destination you want to store it
#sudo cp ./azcopy_linux_amd64_*/azcopy /usr/bin/
$simpleblobconnector = 'https://' +$storageaccountname + '.blob.core.windows.net/' + $blobcontainername
$finalblobconnector = 'https://' +$storageaccountname + '.blob.core.windows.net/' + $blobcontainername + '/' +$blobcontainerpath
#azcopy --version
#azcopy --help
#azcopy -h
#azcopy login
azcopy --source $uploaddirectory --destination $finalblobconnector  --dest-key $storageKey --recursive --include-pattern="*.py;*.json;*txt;*.in" --exclude-path=".venv;build;dganalytics.egg-info;dist;.vscode" --exclude-pattern="*.pyc"