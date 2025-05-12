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
def process_classification_files(
    spark, run_id, containerName, tenant, api_name, logger
):
    try:
        wasbs_path = get_folder_path(spark, containerName, tenant, api_name, logger)
        
        # Load schema based on the API type
        
        schema = get_schema("classification")
        df = spark.read.option("multiline", "true").schema(schema).json(wasbs_path)

        logger.info("Processing flat classification data")

        df = df.select(
            "tenant_id", 
            col("type"), 
            col("raw_phrases").alias("phrase"), 
            "label"
        ).distinct()

        # Create a temporary view for Spark SQL
        df.createOrReplaceTempView("classification")
        # df.display()
        # spark.sql("""select * from dgdm_hellofresh.label_classification limit 5""").display()

        # Delete existing records in label_classification
        spark.sql(
            f""" DELETE FROM
                    dgdm_{tenant}.label_classification lc
                WHERE EXISTS (
                    SELECT 1 FROM classification c
                    WHERE lc.tenant_id = c.tenant_id
                    AND lc.type = c.type
                    AND lc.phrase = c.phrase
                )
            """
        )

        # Insert into label_classification
        spark.sql(
            f""" INSERT INTO
                    dgdm_{tenant}.label_classification (tenant_id, call_type, type, phrase, label)
                SELECT
                    DISTINCT tenant_id,'outbound' call_type,
                    type,
                    phrase,
                    label
                FROM
                    classification
            """
        )
        # Delete processed files from Azure Blob
        delete_all_folder_files(containerName, f"{tenant}/{api_name}/", logger)


        
    except AnalysisException as ae:
        logger.info(f"No files found at path: {wasbs_path}")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise Exception