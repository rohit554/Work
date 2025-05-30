"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def dim_dispositions_skills(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw dispositions skills data into the dimension table `dim_dispositions_skills`.
    This function reads from the raw dispositions skills data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE dim_dispositions_skills
        SELECT
            dispositionId,
            dispositionName,
            isActive,
            skills,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_dispositions_skills
    """)