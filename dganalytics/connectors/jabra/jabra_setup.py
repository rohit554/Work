import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_logger, get_spark_session, get_path_vars


def create_database(spark: SparkSession, path: str, db_name: str):
    logger.info("creating database for jabra")
    spark.sql(f"create database if not exists {db_name} LOCATION '{path}/{db_name}'")

    return True


def create_tables(spark: SparkSession, db_name: str):
    logger.info("creating tables")

    spark.sql(f"""
                create table {db_name}.fact_conversations
                (
                    userId string,
                    eventInsertTimestamp timestamp,
                    conversationDate date,
                    conversationStartTimestamp timestamp,
                    conversationEndTimestamp timestamp,
                    conversationCrossTalkRate int,
                    conversationAverageDbLevel int,
                    surroundingAverageDbLevel int,
                    muteButtonPressCount int
                )
            using delta
            PARTITIONED BY (conversationDate)
            LOCATION '{db_path}/{db_name}/fact_conversations'
            """)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = get_logger(tenant, "jabra_setup")
    db_name = f"jabra_{tenant}"
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="jabra_setup", tenant=tenant, default_db='default')

    create_database(spark, db_path, db_name)
    create_tables(spark, db_name)
