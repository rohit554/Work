import argparse
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from azure.core.exceptions import ResourceExistsError
from dganalytics.utils.utils import env, get_spark_session, get_path_vars, get_secret, get_dbutils, get_logger

global logger

def setup_tenant_localdev(tenant: str):
    tenant_path = get_path_vars(tenant)[0]

    from pathlib import Path
    Path(tenant_path).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data")).mkdir(parents=True, exist_ok=True)

    Path(os.path.join(tenant_path, "data", "databases",
                      "lcx_{}".format(tenant))).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data", "raw")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data", "pbdatasets")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(tenant_path, "data", "adhoc")).mkdir(
        parents=True, exist_ok=True)


def setup_tenant_databricks(tenant: str):
    from azure.storage.filedatalake import DataLakeServiceClient
    logger.info("Creating ADLS container for tenant")
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", get_secret("storageadlsgen2name")),
        credential=get_secret("storageaccesskey"))
    try:
        fs_client = service_client.create_file_system(tenant)
        # fs_client.create_directory("code")
        fs_client.create_directory("data")
        fs_client.create_directory("logs")

        fs_client.create_directory("data/databases")
        fs_client.create_directory("data/databases/lcx_{}".format(tenant))
        fs_client.create_directory("data/raw")
        fs_client.create_directory("data/pbdatasets")
        fs_client.create_directory("data/adhoc")
    except ResourceExistsError as e:
        logger.warn("Container already exists")
    except Exception as e:
        logger.exception("Error waiting container in datalake for {}".format(tenant), str(e))


def mount_tenant_container(tenant: str, dbutils) -> None:
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": get_secret("storagegen2mountappclientid"),
               "fs.azure.account.oauth2.client.secret": get_secret("storagegen2mountappsecret"),
               "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(
                   get_secret("storagegen2mountapptenantid"))}
    if not any(mount.mountPoint == '/mnt/livedcx/{}'.format(tenant) for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(source="abfss://{}@{}.dfs.core.windows.net/".format(
            tenant, get_secret("storageadlsgen2name")), mount_point="/mnt/livedcx/{}".format(tenant),
            extra_configs=configs)


def create_tenant_database(tenant: str, spark: SparkSession):
    db_path = get_path_vars(tenant)[1]
    db_name = "lcx_{}".format(tenant)
    spark.sql(
        "create database if not exists {}  LOCATION '{}'".format(db_name, db_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant

    spark = get_spark_session(app_name="Tenant_Setup",
                              tenant=tenant, default_db='default')
    logger = get_logger("livedcx", "Tenant_Setup")

    logger.info("Setting up tenant %s", tenant)
    if env == 'local':
        logger.debug("setting up tenant in local development")
        setup_tenant_localdev(tenant)
        create_tenant_database(tenant, spark)
    elif env in ['dev', 'uat', 'prd']:
        logger.debug("setting up tenant in databricks env %s", env)
        dbutils = get_dbutils()
        setup_tenant_databricks(tenant)
        mount_tenant_container(tenant, dbutils)
        create_tenant_database(tenant, spark)

    else:
        logger.exception("livedcx_env environment not configured correctly- local/dev/uat/prd")
    
    logger.info("Tenant setup completed")
