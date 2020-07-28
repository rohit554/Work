import argparse
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(local: bool):
    conf = SparkConf()
    if local:
        import findspark
        findspark.init()
        #conf = conf.set("spark.sql.warehouse.dir", "file:///C:/Users/naga_/datagamz/hivescratch").set("spark.sql.catalogImplementation","hive")

    spark = SparkSession.builder.config().appName("Tenant Setup").getOrCreate().newSession()
    return spark


def get_localdev_path(tenant: str) -> str:
    from os.path import expanduser
    home = expanduser("~")
    path_prefix = os.path.join("file:///",
                               home, "datagamz", "analytics", "{}".format(tenant))

    return path_prefix

def create_database(tenant: str, spark: SparkSession, path: str):
    db_name = "dg_{}".format(tenant)
    p1 = path + "/data/databases/{}".format(db_name)
    print("Creating Client Database for Genesys PureCloud")
    spark.sql(
        "create database if not exists gpc_{}  LOCATION '{}'".format(tenant, p1))

    return True

def create_ingestion_stats_table(dbutils) -> bool:
    print("Creating Ingestion stats table for genesys")
    spark.sql("""
                create table if not exists gpc_{tenant}.ingestion_stats
                (
                    api_name bigint,
                    end_point bigint,
                    interval_start timestamp,
                    interval_end timestamp,
                    records_fetched bigint,
                    raw_op_file_name string,
                    adf_run_id string,
                    batch_api boolean,
                    batch_api_job_id string,
                    last_successful_cursor string,
                    instant_api boolean,
                    last_successful_pagenum int,
                    load_date_time timestamp
                )
                    using delta
            LOCATION '/mnt/datagamz/{tenant}/data/databases/gpc_{tenant}/ingestion_stats'""".format(tenant=tenant))
    return True

def create_folder_struct(dbutils):
    dbutils.fs.mkdirs("/mnt/datagamz/{tenant}/data/databases/gpc_{tenant}".format(tenant=tenant))
    dbutils.fs.mkdirs("/mnt/datagamz/{tenant}/data/raw/gpc".format(tenant=tenant))


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
        # setup_tenant_localdev(tenant)
        path_prefix = get_localdev_path(tenant)
        path = "file:///" + path_prefix.replace("\\", "/")
        create_database(tenant, spark, path)
        create_ingestion_stats_table(tenant, spark, path)
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
        
        #setup_tenant_databricks(tenant, dbutils)
        #mount_tenant_container(tenant, env, dbutils)
        #create_tenant_database(tenant, spark, "/mnt/datagamz/{}".format(tenant), env)

        create_folder_struct(dbutils)
        create_database(tenant, spark, path_prefix)
        create_ingestion_stats_table(tenant, spark, path_prefix)
    else:
        raise Exception(
            "datagamz_env environment not configured correctly- local/dev/uat/prd")
