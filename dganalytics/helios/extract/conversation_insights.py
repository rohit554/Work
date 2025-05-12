from dganalytics.helios.helios_utils import helios_utils_logger
import json
from datetime import datetime, timedelta
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    col,
    to_json,
    to_date,
    to_timestamp,
    lit,
    monotonically_increasing_id,
    size,
)
import os
import traceback
import argparse
from dganalytics.utils.azure_utils import (
    delete_json_file_from_blob,
    read_json_file,
    get_schema,
    get_files_list,
)


def update_raw_table(spark, containerName, fileName, tenant, api_name, logger):
    try:
        json_content = read_json_file(containerName, fileName)
        schema = get_schema(api_name)

        df = spark.createDataFrame(
            spark.sparkContext.parallelize([json_content]), schema=schema
        )
        if df.select(size(col(api_name)) > 0).collect()[0][0]:
            extract_df = spark.sql(
                f"""
                    select
                    conversationId conversation_id,
                    extractDate,
                    extractIntervalStartTime,
                    extractIntervalEndTime,,
                    row_number() OVER(
                        PARTITION BY extractDate
                        ORDER BY
                        recordInsertTime DESC
                    ) as rn
                    from
                    gpc_{tenant}.raw_speechandtextanalytics_transcript
                    where
                    conversationId = '{fileName.rsplit("-", 1)[0]}' qualify rn = 1
                """
            )

            final_df = df.join(extract_df, on="conversation_id", how="inner").drop("rn")

            tableName = (
                f"raw_conversation_{api_name}"
                if api_name in ["insights", "quality", "compliance", "confirmity"]
                else f"raw_{api_name}"
            )

            # Append the data to the raw table
            final_df.write.format("delta").mode("append").saveAsTable(
                f"gpc_testinsight.{tableName}"
            )

            logger.info(
                f"Data from file '{fileName}' appended to table :gpc_{tenant}.{tableName}"
            )

            update_audit(spark, fileName.rsplit("-", 1)[0], tenant, api_name)

        # Delete the processed file from the container
        delete_json_file_from_blob(containerName, fileName)

    except Exception as e:
        logger.exception(f"An error occurred while processing file '{fileName}': {e}")


def process_files(spark, run_id, containerName, tenant, api_name, logger):
    try:
        # List all JSON files in the container process_and_append_file
        file_list = get_files_list(containerName, f"{tenant}/{api_name}")

        if not file_list:
            logger.info("No JSON files found in the container.")
            return

        # Process each JSON file in the container
        for fileName in file_list:
            update_raw_table(
                spark,
                f"{containerName}/{tenant}/{api_name}",
                fileName.replace(f"{tenant}/{api_name}/", ""),
                tenant,
                api_name,
                logger,
            )

    except Exception as e:
        logger.exception(f"An error occurred: {e}")


def update_audit(spark, conversationId: str, tenant: str, api_name: str):
    api_column_mapping = {
        "insights": "isInsightsReceived",
        "quality": "isQualityReceived",
        "compliance": "isComplianceReceived",
        "conformity": "isConformityReceived",
        "conversation_map": "isConversationMapReceived",
        "value_stream": "isValueStreamReceived",
        "apprehension": "isApprehensionReceived",
        "summary": "isSummaryReceived",
    }

    spark.sql(
        f"""
          update
            dgdm_testinsights.transcript_insights_audit
            set
            {api_column_mapping.get(api_name)} = true,
            responseFetchedTimestamp = current_timestamp()
            where
            conversationId = '{conversationId}'
          """
    )