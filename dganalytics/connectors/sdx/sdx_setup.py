import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars
from dganalytics.connectors.sdx.sdx_utils import get_schema, get_dbname, sdx_utils_logger


def create_database(spark: SparkSession, path: str, db_name: str):
    logger.info("Creating database for surveydynamix")
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True

def create_dim_tables(spark: SparkSession, db_name: str):
    logger.info("Setting surveydynamix dim/fact tables")
    spark.sql(f"""
                create table {db_name}.dim_hellofresh_interactions
                (
                    surveyId bigint,
                    agentId string,
                    callType string,
                    surveyCompletionDate timestamp,
                    createdAt timestamp,
                    email string,
                    externalRef string,
                    conversationId string,
                    conversationKey string,
                    queueKey string,
                    userKey string,
                    mediaType string,
                    wrapUpCodeKey string,
                    restricted int,
                    surveySentDate timestamp,
                    statusDescription string,
                    status string,
                    surveyTypeId bigint,
                    surveyType string,
                    updatedAt timestamp,
                    respondentLanguage string,
                    country string,
                    agentEmail string,
                    agentGroup string,
                    contactChannel string,
                    wrapUpName string,
                    comments string,
                    OcsatAchieved int,
                    OcsatMax int,
                    improvement_categories string,
                    csatAchieved int,
                    csatMax int,
                    fcr int,
                    fcrMaxResponse int,
                    npsScore int,
                    npsMaxResponse int,
                    ces int,
                    cesMaxResponse int,
                    openText string,
                    selServerCsat int,
                    selServerCsatMaxResponse int,
                    usCsat int,
                    usCsatMaxResponse int,
                    originatingDirection string,
                    surveySentDatePart date
                )
            using delta
            PARTITIONED BY (surveySentDatePart)
            LOCATION '{db_path}/{db_name}/dim_hellofresh_interactions'
            """)
    return True


def create_raw_table(api_name: str, spark: SparkSession, db_name: str):
    schema = get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    logger.info(f"creating surveydynamix raw table - {table_name}")
    spark.createDataFrame(spark.sparkContext.emptyRDD(),
                          schema=schema).registerTempTable(table_name)
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
    logger.info("Setting surveydynamix raw tables")
    create_raw_table("interactions", spark, db_name)
    create_raw_table("questions", spark, db_name)
    create_raw_table("surveys", spark, db_name)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = sdx_utils_logger(tenant, "sdx_setup")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="SDX_Setup",
                              tenant=tenant, default_db='default')
    create_database(spark, db_path, db_name)
    # create_ingestion_stats_table(spark, db_name, db_path)
    raw_tables(spark, db_name, db_path, tenant_path)
    create_dim_tables(spark, db_name)
