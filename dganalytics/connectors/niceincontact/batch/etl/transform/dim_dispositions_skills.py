"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def dim_dispositions_skills(spark: SparkSession):
    """
    Transforms the raw dispositions skills data into the dimension table `dim_dispositions_skills`.
    This function reads from the raw dispositions skills data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.dim_dispositions_skills
        SELECT
            dispositionId,
            dispositionName,
            isActive,
            skills,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_dispositions_skills
    """)