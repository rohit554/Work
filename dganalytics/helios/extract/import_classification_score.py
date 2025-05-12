from dganalytics.utils.azure_utils import (
    delete_json_file_from_blob,
    read_json_file,
    get_schema,
    get_files_list,
    get_folder_path,
    delete_all_folder_files,
)
from pyspark.sql.functions import explode, col
from pyspark.sql.utils import AnalysisException
def process_classification_score_files(
    spark, run_id, containerName, tenant, api_name, logger
):
    try:
        wasbs_path = get_folder_path(spark, containerName, tenant, api_name, logger)
        
        # Load schema based on the API type
        
        schema = get_schema("classification_score")
        df = spark.read.option("multiline", "true").schema(schema).json(wasbs_path)


        logger.info("Processing nested classification_score data")
        df.display()
        # Explode the classification array
        df = df.withColumn("classification", explode(col("classification")))
        df = df.select(
            col("tenant_id"),
            col("type").alias("call_type"),
            col("type"),
            col("raw_phrases").alias("raw_phrases"),
            col("classification.category").alias("category"),
            col("classification.confidence").alias("confidence"),
            col("classification.reasoning").alias("reasoning")
        ).distinct()

        df.createOrReplaceTempView("classification_score")
        df.display()
        spark.sql("select * from dgdm_hellofresh.classification_score limit 5").display()
        # Delete existing records in classification_score
        spark.sql(
            f""" DELETE FROM
                    dgdm_{tenant}.classification_score cs
                WHERE EXISTS (
                    SELECT 1 FROM classification_score c
                    WHERE cs.tenant_id = c.tenant_id
                    AND cs.type = c.type
                    AND cs.raw_phrases = c.raw_phrases
                
                )
            """
        )

        # Insert into classification_score
        spark.sql(
            f""" INSERT INTO
                    dgdm_{tenant}.classification_score
                SELECT
                    DISTINCT tenant_id,
                    'outbound' call_type,
                    type,
                    raw_phrases,
                    category,
                    confidence,
                    reasoning
                FROM
                    classification_score
            """
        )

        # Delete processed files from Azure Blob
        delete_all_folder_files(containerName, f"{tenant}/{api_name}/", logger)
    except AnalysisException as ae:
        logger.info(f"No files found at path: {wasbs_path}")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise Exception