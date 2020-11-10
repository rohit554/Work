from pyspark.sql import SparkSession


def dim_evaluation_forms(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluation_forms = spark.sql("""
                    insert overwrite dim_evaluation_forms
                            select distinct id as evaluationFormId, name as evaluationFormName,
                            published,recordIdentifier as sourceRecordIdentifier,
concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
 from raw_evaluation_forms
                    """)