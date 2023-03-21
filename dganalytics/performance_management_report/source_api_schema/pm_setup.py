import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars
from dganalytics.connectors.gpc_v2.gpc_utils import get_schema, get_dbname, gpc_utils_logger


def create_database(spark: SparkSession, path: str, db_name: str):
    logger.info("Creating database for performacnce_management")
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True

def create_Coaching_Performacne_table(spark: SparkSession, db_name: str, db_path: str):
    logger.info("Creating Coaching Performacne table for performance management")
    spark.sql(
        f"""
                create table if not exists {db_name}.Coaching Performacne
                (
                    org_id string,
                    team_leader_id string,
                    kpi_id string,
                    agent_id string,
                    campaign_id string,
                    date_start string,
                    date_end string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/Coaching Performacne'"""
    )
    return True

def create_raw_table(api_name: str, spark: SparkSession, db_name: str):
    schema = get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    logger.info(f"creating performmance management raw table - {table_name}")
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



def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str):
    logger.info("Setting performance management raw tables")
    create_raw_table("Coaching Performacne", spark, db_name)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = gpc_utils_logger(tenant, "pm_setup")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="PM_Setup",
                              tenant=tenant, default_db='default')
    create_database(spark, db_path, db_name)
    create_Coaching_Performacne_table(spark, db_name, db_path)
    raw_tables(spark, db_name, db_path, tenant_path)