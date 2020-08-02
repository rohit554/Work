import argparse
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from azure.core.exceptions import ResourceExistsError

def get_spark_session(env: str):
    if env == "local":
        import findspark
        findspark.init(os.environ['SPARK_HOME'])
        pass
    spark = SparkSession.builder.appName("Tenant Setup") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate().newSession()
    return spark


def get_dbutils(spark):
    import IPython
    dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


def get_paths(tenant: str, env: str) -> str:
    local_path = ""
    db_path = ""
    if env == "local":
        from os.path import expanduser
        home = expanduser("~")
        local_path = os.path.join(
            home, "datagamz", "analytics", "{}".format(tenant))
        db_path = "file:///" + \
            local_path.replace("\\", "/") + "/data/databases/"
    else:
        db_path = "/mnt/datagamz/{}".format(tenant) + "data/databases/"
    return local_path, db_path


def setup_tenant_localdev(tenant: str, env: str):
    local_path, db_path = get_paths(tenant, env)

    from pathlib import Path
    Path(local_path).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(local_path, "data")).mkdir(parents=True, exist_ok=True)

    Path(os.path.join(local_path, "data", "databases",
                      "dg_{}".format(tenant))).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(local_path, "data", "raw")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(local_path, "data", "pb_datasets")).mkdir(
        parents=True, exist_ok=True)
    Path(os.path.join(local_path, "data", "adhoc")).mkdir(
        parents=True, exist_ok=True)


def setup_tenant_databricks(tenant: str, dbutils):
    from azure.storage.filedatalake import DataLakeServiceClient
    print("setting container")
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", dbutils.secrets.get(scope="dgsecretscope", key="storageadlsgen2name")),
        credential=dbutils.secrets.get(scope="dgsecretscope", key="storageaccesskey"))
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
        raise Exception("Error waiting container in datalake for {}".format(tenant))

def mount_tenant_container(tenant: str, env: str, dbutils) -> None:
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="dgsecretscope", key="storagegen2mountappclientid"),
               "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dgsecretscope", key="storagegen2mountappsecret"),
               "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get(scope="dgsecretscope", key="storagegen2mountapptenantid"))}
    if not any(mount.mountPoint == '/mnt/datagamz/{}'.format(tenant) for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(source="abfss://{}@{}.dfs.core.windows.net/".format(
            tenant, dbutils.secrets.get(scope="dgsecretscope", key="storageadlsgen2name")), mount_point="/mnt/datagamz/{}".format(tenant), extra_configs=configs)


def create_tenant_database(tenant: str, spark: SparkSession, path: str):
    db_name = "dg_{}".format(tenant)
    spark.sql(
        "create database if not exists {}  LOCATION '{}'".format(db_name, path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)

    args = parser.parse_args()
    tenant = args.tenant

    try:
        env = os.environ['datagamz_env']
        if env not in ["local", "dev", "uat", "prd"]:
            raise Exception(
                "Please configure datagamz_env - local/dev/uat/prd")
    except Exception as e:
        raise Exception("Please configure datagamz_env - local/dev/uat/prd")

    spark = get_spark_session(env)
    local_path, db_path = get_paths(tenant, env)

    if env == 'local':
        setup_tenant_localdev(tenant, env)
        create_tenant_database(tenant, spark, db_path)
    elif env in ['dev', 'uat', 'prd']:
        '''
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
        '''
        dbutils = get_dbutils(spark)

        setup_tenant_databricks(tenant, dbutils)
        mount_tenant_container(tenant, env, dbutils)
        create_tenant_database(tenant, spark, db_path)

    else:
        raise Exception(
            "datagamz_env environment not configured correctly- local/dev/uat/prd")
