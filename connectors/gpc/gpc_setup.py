
from connectors import common_utils as utils
from pyspark.sql import SparkSession
import argparse
import requests as rq

def create_database(spark: SparkSession, tenant: str) -> bool:
    print("Creating Client Database for Genesys PureCloud")
    spark.sql(
        "create database if not exists gpc_{tenant}  LOCATION '/mnt/datagamz/{tenant}/data/databases/gpc_{tenant}'".format(tenant=tenant))

    return True


def create_ingestion_stats_table(spark: SparkSession, tenant: str) -> bool:
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


def create_folder_struct(tenant: str, dbutils):
    dbutils.fs.mkdirs("/mnt/datagamz/{tenant}/data/databases/gpc_{tenant}".format(tenant=tenant))
    dbutils.fs.mkdirs("/mnt/datagamz/{tenant}/data/raw/gpc".format(tenant=tenant))
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', required=True)
    parser.add_argument('--tenant', required=True)

    args = parser.parse_args()
    tenant = args.tenant
    env = args.environment

    spark, dbutils = utils.get_key_vars(tenant, "GenesysPureCloudSetup")
    create_folder_struct(tenant, dbutils)
    create_database(spark, tenant)
    create_ingestion_stats_table(spark, tenant)
