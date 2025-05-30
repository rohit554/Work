"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def dim_dispositions(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw dispositions data into the dimension table `dim_dispositions`.
    This function reads from the raw dispositions data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE dim_dispositions
        SELECT
            dispositionId,
            dispositionName,
            notes,
            lastUpdated,
            classificationId,
            systemOutcome,
            isActive,
            isPreviewDisposition,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_dispositions
    """)
