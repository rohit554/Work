import shutil
from numpy.lib.utils import lookfor
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import json
from datetime import datetime
import logging
import tempfile
import pymongo
import requests
import pandas as pd
from contextlib import contextmanager

@contextmanager
def get_mongo_conxn(mongodb_conxnx_uri):
    try:
        with pymongo.MongoClient( mongodb_conxnx_uri ) as mongodb_cluster_client:
            yield mongodb_cluster_client
    except Exception as e:
        raise Exception(
            f"Excepton {e} occured. Please mongodb_conxnx_uri for env"
        )

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


def get_path_vars(tenant: str):
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
        log_path = os.path.join(tenant_path, 'logs')
    else:
        tenant_path = "/dbfs/mnt/datagamz/{}".format(tenant)
        db_path = "dbfs:/mnt/datagamz/{}/data/databases".format(tenant)
        log_path = "/dbfs/mnt/datagamz/{}/logs".format(tenant)
    return tenant_path, db_path, log_path


def get_logger(tenant: str, app_name: str):
    # tenant_path, db_path, log_path = get_path_vars(tenant)
    # log_file = os.path.join(log_path, datetime.utcnow().strftime(
    #    '%Y%m%d'), tenant + '_' + app_name + '.log')
    # temp_file = tempfile.NamedTemporaryFile(delete=True)
    # temp_log_file_name = temp_file.name
    '''
    if env == "local":
        temp_log_file_name = log_file + "_temp"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    os.makedirs(os.path.dirname(temp_log_file_name), exist_ok=True)
    '''
    logger = logging.getLogger(f'dganalytics-{tenant}-{app_name}')

    existing_handlers = [
        handler.__class__.__name__ for handler in logger.handlers]
    if len(logger.handlers) == 1 and set(['StreamHandler']) == set(existing_handlers):
        return logger
    elif len(logger.handlers) == 0:
        pass
    else:
        logger.error("unable to get logging. multiple loggers exist")
    logger.setLevel(logging.DEBUG)
    '''
    existing_handlers = [
        handler.__class__.__name__ for handler in logger.handlers]
    if len(logger.handlers) == 2 and set(['FileHandler', 'StreamHandler']) == set(existing_handlers):
        return logger
    elif len(logger.handlers) == 0:
        pass
    else:
        logger.error("unable to get logging. multiple loggers exist")
    '''
    '''
    fh = logging.FileHandler(temp_log_file_name)
    fh.setLevel(logging.DEBUG)
    fh.__setattr__("orig_log_file", log_file)
    '''

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    # fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # logger.addHandler(fh)
    logger.addHandler(ch)

    py4j_logger = logging.getLogger("py4j").setLevel(logging.WARN)

    return logger


def flush_utils(spark: SparkSession, logger: logging.Logger) -> None:
    # spark.stop()
    '''
    file_handler = [
        handler for handler in logger.handlers if handler.__class__.__name__ == 'FileHandler'][0]

    shutil.copyfile(file_handler.baseFilename, file_handler.orig_log_file)
    file_handler.close()
    '''
    pass


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

def get_gamification_token():
    body = {
        "email": get_secret("gamificationuser"),
        "password": get_secret("gamificationpassword")
    }
    gamification_url = get_secret("gamificationurl")

    auth_resp = requests.post(
        f"{gamification_url}/api/auth/getAccessToken/", data=body)
    if auth_resp.status_code != 200 or 'access_token' not in auth_resp.json().keys():
        raise Exception("unable to get gamification access token")

    return auth_resp.json()['access_token'], auth_resp.json()['userId']

def push_gamification_data(df: pd.DataFrame, org_id: str, connection_name: str):
    # df = df.sample(n=100)
    token, user_Id = get_gamification_token()
    headers = {
        "email": get_secret("gamificationuser"),
        "id_token": token,
        "orgid": org_id
    }
    prefix = "# Mandatory fields are Date & UserID (Format must be YYYY-MM-DD)"    
    a = tempfile.NamedTemporaryFile()
    print(str(df.shape))
    a.file.write(bytes(prefix + "\n", 'utf-8'))
    a.file.write(bytes(df.to_csv(index=False, header=True, mode='a'), 'utf-8'))
    body = {
        "connectionName": f"{connection_name}",
        "user_id": f"{user_Id}"
    }
    files = [
        ('profile', open(a.name, 'rb'))
    ]
    print(f"{get_secret('gamificationurl')}/api/connection/uploaDataFile")

    # print(str(body))

    resp = requests.post(
        f"{get_secret('gamificationurl')}/api/connection/uploaDataFile", headers=headers, files=files, data=body)
    if resp.status_code != 200:
        raise Exception("publishing failed")
    else:
        print("File data submitted to API")
    a.close()


def get_spark_session(app_name: str, tenant: str, default_db: str = None):
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
                               ("spark.sql.execution.arrow.pyspark.fallback.enabled", True),
                               ("spark.sql.execution.arrow.maxRecordsPerBatch", 20000),
                               ("spark.sql.files.maxRecordsPerFile", 20000),
                               ("spark.sql.optimizer.dynamicPartitionPruning.enabled", True),
                               ("spark.sql.parquet.filterPushdown", True),
                               ("spark.sql.shuffle.partitions", 5),
                               ("spark.databricks.delta.snapshotPartitions", 3)
                               ])

    import findspark
    findspark.init(os.environ['SPARK_HOME'])
    if env == "local":
        import findspark
        findspark.init(os.environ['SPARK_HOME'])

        conf = conf.setAll([("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0"),
                            ("spark.sql.extensions",
                             "io.delta.sql.DeltaSparkSessionExtension"),
                            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")])

    spark = SparkSession.builder.appName(app_name).config(
        conf=conf).getOrCreate().newSession()
    if default_db is not None:
        spark.sql(f"use {default_db}")
    return spark


dbutils = None
secrets = None
env = get_env()
