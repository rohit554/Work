from pyspark.sql import SparkSession
"""
This module contains the function to transform raw skills summary data into a fact table for Nice inContact.
"""
def fact_skills_summary(spark: SparkSession):
    """
    Loads the fact_skills_summary fact table from raw skills summary data.
    This function reads from the raw skills summary data and writes to the fact table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE niceincontact_infobell.fact_skills_summary
        SELECT
            skillId,
            mediaTypeId,
            campaignId,
            extractIntervalStartTime,
            contactsOffered,
            contactsHandled,
            abandonCount,
            averageHandleTime,
            abandonRate,
            extractDate,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_skills_summary
    """)