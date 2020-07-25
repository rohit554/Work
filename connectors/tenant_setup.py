import argparse
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(local: bool):
    conf = SparkConf()
    if local:
        import findspark
        findspark.init()
        conf = conf.set("spark.sql.warehouse.dir", "file:///C:/Users/naga_/datagamz/hivescratch").set("spark.sql.catalogImplementation","hive")

    spark = SparkSession.builder.config(conf=conf).appName(
        "Tenant Setup").getOrCreate().newSession()
    return spark


def get_localdev_path(tenant: str) -> str:
    from os.path import expanduser
    home = expanduser("~")
    path_prefix = os.path.join("file:///",
                               home, "datagamz", "analytics", "{}".format(tenant))

    return path_prefix


def setup_tenant_localdev(tenant: str):
    path_prefix = get_localdev_path(tenant)

    from pathlib import Path
    Path(path_prefix).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(path_prefix, "data")).mkdir(parents=True, exist_ok=True)

    Path(os.path.join(path_prefix, "data", "databases",
                      "dg_{}".format(tenant))).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(path_prefix, "data", "raw")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(path_prefix, "data", "pb_datasets")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(path_prefix, "data", "adhoc")).mkdir(
        parents=True, exist_ok=True)


def setup_tenant_databricks(tenant: str, dbutils):
    from azure.storage.filedatalake import DataLakeServiceClient
    print("setting container")
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", dbutils.secrets.get(scope="dgsecretscope", key="storageadlsgen2name")),
        credential=dbutils.secrets.get(scope="dgsecretscope", key="storageaccesskey"))

    fs_client = service_client.create_file_system(tenant)
    fs_client.create_directory("code")
    fs_client.create_directory("data")
    fs_client.create_directory("logs")

    fs_client.create_directory("data/databases")
    fs_client.create_directory("data/databases/dg_{}".format(tenant))
    fs_client.create_directory("data/raw")
    fs_client.create_directory("data/pb_datasets")
    fs_client.create_directory("data/adhoc")
    print("setting container completed")

def mount_tenant_container(tenant: str, env: str, dbutils) -> None:
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="dgsecretscope",key="storagegen2mountappclientid"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dgsecretscope",key="storagegen2mountappsecret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get(scope="dgsecretscope",key="storagegen2mountapptenantid"))}
    if not any(mount.mountPoint == '/mnt/datagamz/{}'.format(tenant) for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(source="abfss://{}@{}.dfs.core.windows.net/".format(
            tenant, dbutils.secrets.get(scope="dgsecretscope", key="storageadlsgen2name")), mount_point="/mnt/datagamz/{}".format(tenant), extra_configs=configs)


def create_tenant_database(tenant: str, spark: SparkSession, path: str, env: str):
    db_name = "dg_{}".format(tenant)
    spark.sql("create database if not exists {}  LOCATION '".format(db_name) + ("file:///" if env == 'local' else "") + os.path.join(path, 'data', 'databases', db_name) + "'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)

    args = parser.parse_args()
    tenant = args.tenant

    path_prefix = ""
    try:
        env = os.environ['datagamz_env']
    except:
        raise Exception("Please configure datagamz_env - local/dev/uat/prd")

    if env == 'local':
        spark = get_spark_session(local=True)
        setup_tenant_localdev(tenant)
        path_prefix = get_localdev_path(tenant)
        create_tenant_database(tenant, spark, path_prefix, env)
    elif env in ['dev', 'uat', 'prd']:
        spark = get_spark_session(local=False)
        '''
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
        '''
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        
        setup_tenant_databricks(tenant, dbutils)
        mount_tenant_container(tenant, env, dbutils)
        create_tenant_database(
            tenant, spark, "/mnt/datagamz/{}".format(tenant), env)
    else:
        raise Exception(
            "datagamz_env environment not configured correctly- local/dev/uat/prd")
