from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContentSettings,
    ContainerClient,
    generate_container_sas,
    ContainerSasPermissions,
)
from dganalytics.utils.utils import get_secret
import json
from pyspark.sql.types import StructType
import os
from urllib.parse import urlparse

def move_files_in_blob(container_name, file_paths, new_folder, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")

    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )
        container_client = blob_service_client.get_container_client(container_name)

        for path in file_paths:
            parsed_url = urlparse(path)
            original_blob_name = parsed_url.path.lstrip('/')  # e.g. folder1/file1.json

            # Construct new blob name (destination path)
            filename = os.path.basename(original_blob_name)
            new_blob_name = f"{new_folder.strip('/')}/{filename}"

            # Get source and destination blob clients
            source_blob = container_client.get_blob_client(original_blob_name)
            destination_blob = container_client.get_blob_client(new_blob_name)

            # Start copy operation
            source_url = source_blob.url
            destination_blob.start_copy_from_url(source_url)

            # Optionally, wait until the copy completes (can skip if handling asynchronously)
            properties = destination_blob.get_blob_properties()
            while properties.copy.status == 'pending':
                properties = destination_blob.get_blob_properties()

            # Delete the original blob after successful copy
            source_blob.delete_blob()

    except Exception as e:
        logger.exception("Error moving files in blob storage")
        raise
		
def delete_files_from_blob(container_name, file_paths, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")

    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)

        for path in file_paths:
            # Extract the blob name from the full wasbs:// URL
            # Example: wasbs://mycontainer@myaccount.blob.core.windows.net/folder/file.json
            parsed_url = urlparse(path)
            blob_name = parsed_url.path.lstrip('/')  # remove leading slash

            # Get the blob client and delete
            blob_client = container_client.get_blob_client(blob=blob_name)
            blob_client.delete_blob()

    except Exception as e:
        logger.exception("Error deleting files from blob storage")
        raise

# Delete json from storage account : datagamzuseruploadfiles
def delete_json_file_from_blob(containerName, fileName, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(containerName)
        blob_client = container_client.get_blob_client(blob=fileName)

        # Delete the blob
        blob_client.delete_blob()

        logger.info(f"Blob '{fileName}' deleted successfully.")
    except Exception as e:
        logger.exception(f"Error occurred: {e}")
        raise Exception


# Read json from storage account : datagamzuseruploadfiles
def read_json_file(containerName, fileName, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(containerName)
        blob_client = container_client.get_blob_client(blob=fileName)
        blob_data = blob_client.download_blob().readall()

        # Parse the JSON content
        json_data = json.loads(blob_data)
        return json_data

    except Exception as e:
        logger.exception(f"Error occurred: {e}")
        raise Exception


def get_schema(api_name):
    schema_path = os.path.join(
        "/dbfs/mnt/datagamz/code/dganalytics/dganalytics/helios/source_api_schemas",
        f"{api_name}.json",
    )
    with open(schema_path, "r") as f:
        schema = f.read()
    schema = StructType.fromJson(json.loads(schema))
    return schema


def get_files_list(containerName, tenant, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(containerName)

        # List all blobs in the container and filter for JSON files
        file_list = [
            blob.name
            for blob in container_client.list_blobs()
            if blob.name.endswith(".json") and blob.name.startswith(f"{tenant}/")
        ]
        return file_list

    except Exception as e:
        logger.exception(f"Error occurred while listing files in container: {e}")
        raise Exception


def get_folder_path(spark, container_name, tenant, api_name, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")

    folder_path = f"{tenant}/{api_name}"  # Folder inside the container

    try:
        # **Set Spark Configuration for Azure Blob Storage Access**
        spark.conf.set(
            f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key
        )
        # **Construct the WASBS Path**
        wasbs_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{folder_path}/*.json"
        if api_name in ['classification', 'classification_score']:
            wasbs_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{folder_path}/*"
        return wasbs_path
    except Exception as e:
        logger.exception(
            f"Error occurred while getting {api_name} folder path in container: {e}"
        )
        raise Exception

def get_root_folder_path(spark, container_name, logger):
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")

    try:
        # **Set Spark Configuration for Azure Blob Storage Access**
        spark.conf.set(
            f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key
        )
        # **the WASBS Path**
        return f"wasbs://{container_name}@{account_name}.blob.core.windows.net/"
    except Exception as e:
        logger.exception(
            f"Error occurred while getting {api_name} folder path in container: {e}"
        )
        raise Exception

def delete_all_folder_files(containerName, folder_path, logger):
    try:
        account_name = get_secret("storageaccountnameforprocessmap")
        account_key = get_secret("storageaccountkeyforprocessmap")

        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )
        container_client = blob_service_client.get_container_client(containerName)
        # blob_client = container_client.get_blob_client(blob=f"{folder_path}/*.json")

        blobs = container_client.list_blobs(name_starts_with=folder_path)
        for blob in blobs:
            container_client.delete_blob(blob.name)
        logger.info(f"All files under {folder_path} are deleted")
    except Exception as e:
        logger.exception(
            f"Error occurred while deleting files in {containerName}/{folder_path}: {e}"
        )
        raise Exception

def get_file_paths(spark, containerName, tenant, apiName, logger):
    root = get_root_folder_path(spark, containerName, logger)
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(containerName)

        # Server-side filtering using prefix and increased page size
        prefix = f"{tenant}/{apiName}/"
        blobs = container_client.list_blobs(
            name_starts_with=prefix
        )

        # List all blobs in the container and filter for JSON files
        file_list = [
            root + blob.name
            for blob in blobs
            if blob.name.endswith(".json")
        ]
        return file_list
    except Exception as e:
        logger.exception(f"An error occurred in get_file_paths: {e}")
        raise Exception