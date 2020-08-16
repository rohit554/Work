from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import json
from datetime import datetime
import logging

def get_env():
    try:
        environment = os.environ['datagamz_env']
        if environment not in ["local", "dev", "uat", "prd"]:
            raise Exception(
                "Please configure datagamz_env - local/dev/uat/prd")
    except Exception as e:
        raise Exception(
            "Please configure datagamz_env - local/dev/uat/prd")
    return environment


def get_path_vars(tenant: str) -> str:
    global env
    tenant_path = ""
    db_path = ""
    log_path = ""
    if env == "local":
        from os.path import expanduser
        home = expanduser("~")
        tenant_path = os.path.join(
            home, "datagamz", "analytics", "{}".format(tenant))
        db_path = "file:///" + \
            tenant_path.replace("\\", "/") + "/data/databases"
        log_path = "file:///" + \
            tenant_path.replace("\\", "/") + "/logs"
    else:
        tenant_path = "/dbfs/mnt/datagamz/{}".format(tenant)
        db_path = "/dbfs/mnt/datagamz/{}/data/databases".format(tenant)
        log_path = "/dbfs/mnt/datagamz/{}/logs".format(tenant)
    return tenant_path, db_path, log_path


def get_logger(tenant: str, app_name: str):
    log = logging.getLevelName("py4j")
    return log


def get_dbutils():
    global dbutils
    import IPython
    if dbutils is None:
        dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


def get_secret(secret_key: str):
    global env
    global dbutils
    global secrets
    if env == "local":
        if secrets is None:
            with open(os.path.join(os.path.expanduser("~"), "datagamz", "analytics", "secrets.json")) as f:
                secrets = json.loads(f.read())
        return secrets[secret_key]
    else:
        dbutils = get_dbutils()
        return dbutils.secrets.get(scope='dgsecretscope', key='{}'.format(secret_key))


def get_spark_session(app_name: str, tenant: str, default_db: str):
    global env
    time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    app_name = f"{tenant}-{app_name}-{time}"
    conf = SparkConf().setAll([("spark.sql.sources.partitionOverwriteMode", "dynamic"),
                               ("spark.rpc.message.maxSize", 1024),
                               ("spark.databricks.session.share", False),
                               ("spark.sql.adaptive.enabled", True),
                               ("spark.sql.adaptive.coalescePartitions.enabled", True),
                               ("spark.sql.adaptive.advisoryPartitionSizeInBytes", 262144000),
                               ("spark.sql.cbo.enabled", True),
                               ("spark.sql.execution.arrow.pyspark.enabled", True),
                               ("spark.sql.execution.arrow.fallback.enabled", True),
                               ("spark.sql.execution.arrow.maxRecordsPerBatch", 20000),
                               ("spark.sql.files.maxRecordsPerFile", 20000),
                               ("spark.sql.optimizer.dynamicPartitionPruning.enabled", True),
                               ("spark.sql.parquet.filterPushdown", True),
                               ("spark.sql.shuffle.partitions", 5)
                               ])

    if env == "local":
        import findspark
        findspark.init(os.environ['SPARK_HOME'])

        conf = conf.setAll([("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0"),
                            ("spark.sql.extensions",
                             "io.delta.sql.DeltaSparkSessionExtension"),
                            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")])

    spark = SparkSession.builder.appName(app_name).config(
        conf=conf).getOrCreate().newSession()

    spark.sql(f"use {default_db}")
    return spark


dbutils = None
secrets = None
env = get_env()
