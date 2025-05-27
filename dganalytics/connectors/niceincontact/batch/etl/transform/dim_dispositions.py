"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def dim_dispositions(spark: SparkSession):
    """
    Transforms the raw dispositions data into the dimension table `dim_dispositions`.
    This function reads from the raw dispositions data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.dim_dispositions
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
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_dispositions
    """)
