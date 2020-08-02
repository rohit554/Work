import argparse
import os
from pyspark.sql import SparkSession
from .gpc_utils import get_spark_session, get_env, get_path_vars, get_schema
from .gpc_api_config import gpc_end_points

def create_database(spark: SparkSession, path: str, db_name: str):
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True


def create_ingestion_stats_table(spark: SparkSession, db_name: str, db_path: str):
    print("Creating Ingestion stats table for genesys")
    spark.sql(f"""
                create table if not exists {db_name}.ingestion_stats
                (
                    api_name string,
                    end_point string,
                    page_count int,
                    records_fetched bigint,
                    raw_data_file_loc string,
                    adf_run_id string,
                    extract_date date,
                    load_date_time timestamp
                )
                    using delta
            LOCATION '{db_path}/{db_name}/ingestion_stats'""")
    return True

def create_table(api_name: str, spark: SparkSession, db_name: str):
    schema = get_schema(api_name, tenant_path)
    table_name = f"r_{api_name}"
    print(table_name)
    if gpc_end_points[api_name]['raw_table_update']['partition'] is not None:
        partition = "partitioned by (" + ",".join(gpc_end_points[api_name]['raw_table_update']['partition']) + ")"
    else:
        partition = ""
    spark.createDataFrame(spark.sparkContext.emptyRDD(),
                          schema=schema).registerTempTable(table_name)
    create_qry = f"""create table if not exists {db_name}.{table_name} using delta {partition} as
                    select *, cast('1900-01-01' as date) extract_date from {table_name} limit 0"""
    spark.sql(create_qry)

    return True


def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str):
    create_table('users', spark, db_name)
    create_table('routing_queues', spark, db_name)
    create_table('groups', spark, db_name)
    create_table('users_details', spark, db_name)
    create_table('conversation_details', spark, db_name)
    create_table('wrapupcodes', spark, db_name)

    return True


def create_folder_struct(dbutils):
    dbutils.fs.mkdirs(
        "/mnt/datagamz/{tenant}/data/databases/gpc_{tenant}".format(tenant=tenant))
    dbutils.fs.mkdirs(
        "/mnt/datagamz/{tenant}/data/raw/gpc".format(tenant=tenant))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)

    args = parser.parse_args()
    tenant = args.tenant

    env = get_env()
    tenant_path, db_path, log_path, db_name = get_path_vars(tenant, env)

    spark = get_spark_session(app_name="gpc_setup", env=env, tenant=tenant)
    create_database(spark, db_path, db_name)
    create_ingestion_stats_table(spark, db_name, db_path)
    raw_tables(spark, db_name, db_path, tenant_path)
