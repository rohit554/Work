from pyspark.sql import SparkSession
from azure.storage.filedatalake import DataLakeServiceClient


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils

        from pyspark.dbutils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


def get_key_vars(tenant: str, app_name: str):
    appName = "{}_{}".format(app_name, tenant)
    spark = SparkSession.builder.appName("test").getOrCreate()

    dbutils = get_dbutils(spark)

    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get(
        scope="dgsecretscope", key="storagegen2mountappclientid"))
    spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(
        scope="dgsecretscope", key="storagegen2mountappsecret"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{}/oauth2/token".format(
        dbutils.secrets.get(scope="dgsecretscope", key="storagegen2mountapptenantid")))

    return spark, dbutils


def get_storage_client(dbutils):
    print("Getting Storage Client")
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", dbutils.secrets.get(scope="dgsecretscope", key="storageadlsgen2name")),
        credential=dbutils.secrets.get(scope="dgsecretscope", key="storageaccesskey"))

    return service_client
