import datetime
from pyspark.sql.functions import (
    col,
    to_timestamp,
    lit,
    monotonically_increasing_id,
    size,
)
from dganalytics.utils.azure_utils import (
    get_schema,
    get_folder_path,
    delete_all_folder_files,
	get_file_paths,
	delete_files_from_blob,
	move_files_in_blob
)
from pyspark.sql.utils import AnalysisException

def process_files(spark, run_id, containerName, tenant, api_name, logger):
    try:
        paths = get_file_paths(spark, containerName, tenant, api_name, logger)
        
        schema = get_schema(api_name)

        df = spark.read.option("multiline", "true").schema(schema).json(paths)
        df = df.repartition(50)
        df.cache()
        logger.info(f"Importing files from blob for {api_name}")
        print(df.count())  # Materialize the cache

        df.createOrReplaceTempView(api_name)
        extract_df = spark.sql(
            f"""
                           select
                                conversation_id,
                                extractDate,
                                extractIntervalStartTime,
                                extractIntervalEndTime
                                from
                                (
                                    select
                                    conversationId conversation_id,
                                    extractDate,
                                    extractIntervalStartTime,
                                    extractIntervalEndTime,
                                    row_number() OVER (
                                        PARTITION BY conversationId
                                        ORDER BY
                                        recordInsertTime DESC
                                    ) rn
                                    from
                                    gpc_{tenant}.raw_conversation_details
                                    where
                                    conversationId in (
                                        select
                                        distinct conversationId
                                        from
                                        {api_name}
                                    )
                                )
                                where
                                rn = 1
                    """
        )
        final_df = df.join(extract_df, on="conversation_id", how="inner")

        final_df = final_df.withColumn(
            "recordInsertTime",
            to_timestamp(lit(datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S"))),
        )

        final_df = final_df.withColumn(
            "recordIdentifier", monotonically_increasing_id()
        )

        tableName = (
            f"gpc_{tenant}.raw_conversation_{api_name}"
            if api_name in ["insights", "quality", "compliance", "conformance"]
            else f"gpc_{tenant}.raw_{api_name}"
        )

        final_df.write.format("delta").mode(
            "append"
        ).saveAsTable(tableName)
        
        
        if final_df.count() > 0:
            move_files_in_blob(containerName, paths, 'archive/{tenant}/api_name', logger)
        
        # Unpersist to release memory
        df.unpersist()

    except AnalysisException as ae:
        logger.info(f"No files found for: {api_name}")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise Exception 


def update_audit(spark, conversation_ids_str, tenant: str, api_name: str):
    api_column_mapping = {
        "insights": "isInsightsReceived",
        "quality": "isQualityReceived",
        "compliance": "isComplianceReceived",
        "conformance": "isConformityReceived",
        "conversation_map": "isConversationMapReceived",
        "value_stream": "isValueStreamReceived",
        "apprehension": "isApprehensionReceived",
        "objection": "isObjectionReceived",
        "summary": "isSummaryReceived"
    }

    spark.sql(
        f"""
          update
            dgdm_{tenant}.transcript_insights_audit
            set
            {api_column_mapping.get(api_name)} = true,
            responseFetchedTimestamp = current_timestamp()
            where
            conversationId in ({conversation_ids_str})
          """
    )