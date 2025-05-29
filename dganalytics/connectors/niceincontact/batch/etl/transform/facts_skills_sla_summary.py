from pyspark.sql import SparkSession

def fact_skills_sla_summary(spark: SparkSession):
    """
    Loads the fact_skills_sla_summary fact table from raw skills SLA summary data.
    This function reads from the raw skills SLA summary data and writes to the fact table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE niceincontact_infobell.fact_skills_sla_summary
        SELECT
            skillId,
            contactsWithinSLA,
            contactsOutOfSLA,
            totalContacts,
            serviceLevel,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_skills_sla_summary
    """)