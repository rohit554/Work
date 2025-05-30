from pyspark.sql import SparkSession

def fact_skills_sla_summary(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Loads the fact_skills_sla_summary fact table from raw skills SLA summary data.
    This function reads from the raw skills SLA summary data and writes to the fact table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE fact_skills_sla_summary
        SELECT
            skillId,
            contactsWithinSLA,
            contactsOutOfSLA,
            totalContacts,
            serviceLevel,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_skills_sla_summary
    """)