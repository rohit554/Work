import argparse
import os
from datetime import datetime
from dganalytics.utils.utils import get_path_vars, get_spark_session, get_logger, export_powerbi_parquet
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit, to_date
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DateType

schema = StructType([StructField('userId', StringType(), False),
                     StructField('eventInsertTimestamp', TimestampType(), True),
                     StructField('conversationDate', DateType(), True),
                     StructField('conversationStartTimestamp', TimestampType(), False),
                     StructField('conversationEndTimestamp', TimestampType(), False),
                     StructField('conversationCrossTalkRate', IntegerType(), True),
                     StructField('conversationAverageDbLevel', IntegerType(), True),
                     StructField('surroundingAverageDbLevel', IntegerType(), True),
                     StructField('muteButtonPressCount', IntegerType(), True)])


def load_jabra_events(spark: SparkSession, tenant: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    current_timestamp = datetime.utcnow()
    event_json_files_dir = f"{tenant_path}/data/raw/jabra/"
    event_json_files = []
    events_temp_table = f"{tenant}_jabra_events_new"
    logger.info(f"checking for new jabra events in {event_json_files_dir}")

    for file in os.scandir(event_json_files_dir):
        if not file.path.endswith(".json") or not file.is_file():
            continue

        if datetime.fromtimestamp(file.stat().st_mtime) < current_timestamp:
            event_json_files.append(f"/mnt/datagamz/{tenant}/data/raw/jabra/{file.name}")

    if len(event_json_files) == 0:
        logger.info(f"no new jabra events found in {event_json_files_dir}")
        return

    logger.info(f"{len(event_json_files)} new jabra events found in {event_json_files_dir}, processing...")
    df = spark.read.option("mode", "FAILFAST").schema(schema).json(path=event_json_files).drop_duplicates()
    df = df.withColumn("eventInsertTimestamp", to_timestamp(lit(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))))
    df = df.withColumn("conversationDate", to_date("conversationStartTimestamp"))
    df.createOrReplaceTempView(events_temp_table)
    logger.info("loading new jabra events into fact_conversations")
    spark.sql(f"insert into fact_conversations select * from {events_temp_table}")

    for event_json_file in event_json_files:
        logger.info(f"deleting {event_json_file}...")
        os.remove(f"/dbfs{event_json_file}")

    logger.info("exporting all jabra events to power bi...")

    export_powerbi_parquet(tenant, spark.sql("select * from fact_conversations"), "gFactConversations")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    db_name = f"jabra_{tenant}"
    app_name = f"jabra_event_{tenant}"
    spark = get_spark_session(app_name, tenant, default_db=db_name)

    global logger
    logger = get_logger(tenant, app_name)

    try:
        logger.info(f"loading jabra events for {tenant}...")
        load_jabra_events(spark, tenant)
        logger.info(f"finished loading jabra events for {tenant}!")
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
