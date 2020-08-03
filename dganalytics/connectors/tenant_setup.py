import argparse
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from azure.core.exceptions import ResourceExistsError
from dganalytics.utils.utils import env, get_spark_session, get_path_vars, get_secret, get_dbutils


def setup_tenant_localdev(tenant: str):
    tenant_path = get_path_vars(tenant)[0]

    from pathlib import Path
    Path(tenant_path).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data")).mkdir(parents=True, exist_ok=True)

    Path(os.path.join(tenant_path, "data", "databases",
                      "dg_{}".format(tenant))).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data", "raw")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data", "pb_datasets")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data", "adhoc")).mkdir(
        parents=True, exist_ok=True)


def setup_tenant_databricks(tenant: str):
    from azure.storage.filedatalake import DataLakeServiceClient
    print("setting container")
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", get_secret("storageadlsgen2name")),
        credential=get_secret("storageaccesskey"))
    try:
        fs_client = service_client.create_file_system(tenant)
        fs_client.create_directory("code")
        fs_client.create_directory("data")
        fs_client.create_directory("logs")

        fs_client.create_directory("data/databases")
        fs_client.create_directory("data/databases/dg_{}".format(tenant))
        fs_client.create_directory("data/raw")
        fs_client.create_directory("data/pb_datasets")
        fs_client.create_directory("data/adhoc")
    except ResourceExistsError as e:
        print("setting container completed")
    except Exception as e:
        raise Exception(
            "Error waiting container in datalake for {}".format(tenant))


def mount_tenant_container(tenant: str, dbutils) -> None:
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": get_secret("storagegen2mountappclientid"),
               "fs.azure.account.oauth2.client.secret": get_secret("storagegen2mountappsecret"),
               "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(
                   get_secret("storagegen2mountapptenantid"))}
    if not any(mount.mountPoint == '/mnt/datagamz/{}'.format(tenant) for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(source="abfss://{}@{}.dfs.core.windows.net/".format(
            tenant, get_secret("storageadlsgen2name")), mount_point="/mnt/datagamz/{}".format(tenant),
            extra_configs=configs)


def create_tenant_database(tenant: str, spark: SparkSession):
    db_path = get_path_vars(tenant)[1]
    db_name = "dg_{}".format(tenant)
    spark.sql(
        "create database if not exists {}  LOCATION '{}'".format(db_name, db_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant

    spark = get_spark_session(app_name="Tenant_Setup", tenant=tenant)

    if env == 'local':
        setup_tenant_localdev(tenant)
        create_tenant_database(tenant, spark)
    elif env in ['dev', 'uat', 'prd']:
        dbutils = get_dbutils()

        setup_tenant_databricks(tenant)
        mount_tenant_container(tenant, dbutils)
        create_tenant_database(tenant, spark)

    else:
        raise Exception(
            "datagamz_env environment not configured correctly- local/dev/uat/prd")
