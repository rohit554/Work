import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars
from dganalytics.connectors.confluence.confluence_utils import get_schema, get_dbname, confluence_utils_logger


def create_database(spark: SparkSession, path: str, db_name: str):
    logger.info("Creating database for confluence")
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True


def create_ingestion_stats_table(spark: SparkSession, db_name: str, db_path: str):
    logger.info("Creating Ingestion stats table for confluence")
    spark.sql(
        f"""
                create table if not exists {db_name}.ingestion_stats
                (
                    apiName string,
                    endPoint string,
                    pageCount int,
                    recordsFetched bigint,
                    rawDataFile_loc string,
                    adfRunId string,
                    extractDate date,
                    loadDateTime timestamp
                )
                    using delta
            LOCATION '{db_path}/{db_name}/ingestion_stats'"""
    )
    return True


# def create_dim_tables(spark: SparkSession, db_name: str):
#     logger.info("Setting confluence dim/fact tables")

#     spark.sql(
#         f"""CREATE TABLE IF NOT EXISTS {db_name}.fact_confluence_pages
#         (
#             id BIGINT,
#             type STRING,
#             createdByName STRING,
#             createdDate TIMESTAMP,
#             createdByAccountId STRING,
#             versionNumber INT,
#             lastUpdatedDate TIMESTAMP,
#             lastUpdatedByName STRING,
#             bodyValue STRING,
#             sourceRecordIdentifier LONG,
#             sourcePartition STRING
#         )
#         USING DELTA
#         PARTITIONED BY (createdDate)
#         LOCATION '{db_path}/{db_name}/fact_confluence_pages'
#         """
#     )

#     spark.sql(
#         f"""CREATE TABLE IF NOT EXISTS {db_name}.dim_confluence_index
#         (
#             id BIGINT,
#             status STRING,
#             title STRING,
#             excerpt STRING,
#             lastModified TIMESTAMP,
#             sourceRecordIdentifier LONG,
#             sourcePartition STRING
#         )
#         USING DELTA
#         PARTITIONED BY (lastModified)
#         LOCATION '{db_path}/{db_name}/dim_confluence_index'
#         """
#     )
#     return True


def create_raw_table(api_name: str, spark: SparkSession, db_name: str):
    schema = get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    logger.info(f"creating confluence raw table - {table_name}")
    spark.createDataFrame(spark.sparkContext.emptyRDD(),
                          schema=schema).regcreateOrReplaceTempViewsterTempTable(table_name)
    create_qry = f"""create table if not exists {db_name}.{table_name}
                        using delta partitioned by(extractDate, extractIntervalStartTime, extractIntervalEndTime) LOCATION
                                '{db_path}/{db_name}/{table_name}'
                        as
                    select *, cast('1900-01-01' as date) extractDate,
                    cast('1900-01-01 00:00:00' as timestamp) extractIntervalStartTime,
                    cast('1900-01-01 00:00:00' as timestamp) extractIntervalEndTime,
                    cast('1900-01-01 00:00:00' as timestamp) recordInsertTime,
                    monotonically_increasing_id() as recordIdentifier
                     from {table_name} limit 0"""
    spark.sql(create_qry)

    return True


def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str):
    logger.info("Setting confluence raw tables")
    create_raw_table("page_index", spark, db_name)
    create_raw_table("page_body", spark, db_name)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = confluence_utils_logger(tenant, "confluence_setup")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="confluence_Setup",
                              tenant=tenant, default_db='default')
    create_database(spark, db_path, db_name)
    create_ingestion_stats_table(spark, db_name, db_path)
    raw_tables(spark, db_name, db_path, tenant_path)
    #create_dim_tables(spark, db_name)
