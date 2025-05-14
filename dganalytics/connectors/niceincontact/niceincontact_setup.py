import argparse
from dganalytics.connectors.niceincontact.niceincontact_utils import NiceInContactClient
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points
from pyspark.sql import SparkSession


def create_database(spark: SparkSession, path: str, db_name: str, logger):
    """
    Create a Spark SQL database if it does not already exist.

    Args:
        spark (SparkSession): Spark session object.
        path (str): Base path for the database location.
        db_name (str): Name of the database to create.
        logger: Logger object for logging progress.

    Returns:
        bool: True if the database creation command is issued.
    """
    logger.info("Creating database for genesys")
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")
    return True

def create_ingestion_stats_table(spark: SparkSession, db_name: str, db_path: str, logger):
    """
    Create the ingestion stats table in the specified database.

    Args:
        spark (SparkSession): Spark session object.
        db_name (str): Database name where the table should be created.
        db_path (str): Path for storing table data.
        logger: Logger object for logging progress.

    Returns:
        bool: True if the table creation command is issued.
    """
    logger.info("Creating Ingestion stats table for genesys")
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

def create_raw_table(api_name: str, spark: SparkSession, db_name: str, db_path: str, client: NiceInContactClient):
    """
    Create a raw delta table for the given API using its JSON schema.

    Args:
        api_name (str): The name of the API to create a raw table for.
        spark (SparkSession): Spark session object.
        db_name (str): Name of the database where the table will be created.
        db_path (str): Base path for the table's storage.
        client (NiceInContactClient): Configured NICE inContact client object.

    Returns:
        bool: True if the table is created successfully.
    """
    schema = client.get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    client.logger.info(f"creating genesys raw table - {table_name}")
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

def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str, client : NiceInContactClient):
    """
    Create raw tables for all NICE inContact APIs defined in the API config.

    Args:
        spark (SparkSession): Spark session object.
        db_name (str): Target database name.
        db_path (str): Base path for storing raw tables.
        tenant_path (str): Path specific to the tenant.
        client (NiceInContactClient): Configured client instance.

    Returns:
        bool: True if all raw tables are created.
    """
    client.logger.info("Setting genesys raw tables")
    apis = list(niceincontact_end_points.keys())
    for api in apis:
        client.logger.info(f"Creating raw table for API: {api}")
        create_raw_table(api, spark, db_name, db_path, client)

    client.logger.info("Raw tables creation completed.")
    return True

if __name__ == "__main__":
    app_name = "NICEINCONTACT_Setup"
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    client = NiceInContactClient(tenant, app_name.lower())
    client.logger.info("Setup started...")

    db_name = client.db_name
    tenant_path, db_path, log_path = client.tenant_path, client.db_path, client.log_path

    spark = get_spark_session(app_name=app_name,
                              tenant=tenant, default_db='default')
    
    create_database(spark, db_path, db_name, client.logger)
    create_ingestion_stats_table(spark, db_name, db_path, client.logger)
    raw_tables(spark, db_name, db_path, tenant_path, client)