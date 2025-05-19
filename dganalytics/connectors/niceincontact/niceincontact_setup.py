import argparse
from dganalytics.connectors.gpc.gpc_utils import get_dbname
from dganalytics.connectors.niceincontact.niceincontact_utils import niceincontact_utils_logger, get_schema
from dganalytics.utils.utils import get_path_vars, get_spark_session
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
    logger.info("Creating database for NICE inContact")
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
    logger.info("Creating Ingestion stats table for NICE inContact")
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

def create_raw_table(api_name: str, spark: SparkSession, db_name: str, db_path: str, logger):
    """
    Create a raw delta table for the given API using its JSON schema.

    Args:
        api_name (str): The name of the API to create a raw table for.
        spark (SparkSession): Spark session object.
        db_name (str): Name of the database where the table will be created.
        db_path (str): Base path for the table's storage.

    Returns:
        bool: True if the table is created successfully.
    """
    schema = get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    logger.info(f"Creating niceincontact raw table - {table_name}")
    spark.createDataFrame(spark.sparkContext.emptyRDD(),
                          schema=schema).createOrReplaceTempView(table_name)
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

def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str, logger):
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
    logger.info("Setting Nice InContact raw tables")
    apis = ["agents"]
    for api in apis:
        logger.info(f"Creating raw table for API: {api}")
        create_raw_table(api, spark, db_name, db_path, logger)

    logger.info("Raw tables creation completed.")
    return True

def create_dim_agents_table(spark, db_name: str, db_path: str):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_agents (
            agentId LONG,
            userName STRING,
            firstName STRING,
            middleName STRING,
            lastName STRING,
            userId STRING,
            emailAddress STRING,
            isActive BOOLEAN,
            teamId LONG,
            teamName STRING,
            reportToId LONG,
            reportToName STRING,
            isSupervisor BOOLEAN,
            lastLogin TIMESTAMP,
            location STRING,
            profileId LONG,
            profileName STRING,
            timeZone STRING,
            country STRING,
            state STRING,
            city STRING,
            hireDate TIMESTAMP,
            terminationDate TIMESTAMP,
            employmentType INT,
            employmentTypeName STRING,
            atHome BOOLEAN,
            hourlyCost DOUBLE,
            maxConcurrentChats INT,
            maxEmailAutoParkingLimit INT,
            userType STRING,
            agentVoiceThreshold INT,
            agentChatThreshold INT,
            agentEmailThreshold INT,
            agentWorkItemThreshold INT,
            agentDeliveryMode STRING,
            agentTotalContactCount INT,
            agentContactAutoFocus BOOLEAN,
            agentRequestContact BOOLEAN,
            address1 STRING,
            address2 STRING,
            zipCode STRING,
            timeZoneOffset STRING,
            systemUser STRING,
            crmUserName STRING,
            lastUpdated TIMESTAMP,
            createDate TIMESTAMP,
            sourceRecordIdentifier LONG,
            sourcePartition STRING
        )
        USING DELTA
        PARTITIONED BY (isActive, teamId)
        LOCATION '{db_path}/{db_name}/dim_agents'
        """
    )


def create_dim_tables(spark: SparkSession, db_name: str, db_path: str, logger):
    logger.info("Setting Nice InContact dim/fact tables")
    create_dim_agents_table(spark, db_name, db_path)
    logger.info("Agent dim table creation completed")



if __name__ == "__main__":
    app_name = "NICEINCONTACT_Setup"
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = niceincontact_utils_logger(tenant, app_name.lower())
    logger.info("Setup started...")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    spark = get_spark_session(app_name=app_name,
                              tenant=tenant, default_db='default')
    
    create_database(spark, db_path, db_name, logger)
    create_ingestion_stats_table(spark, db_name, db_path, logger)
    raw_tables(spark, db_name, db_path, tenant_path, logger)
    create_dim_tables(spark, db_name, db_path, logger)