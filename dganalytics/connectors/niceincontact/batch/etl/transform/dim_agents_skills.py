"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact."""
from pyspark.sql import SparkSession

def dim_agents_skills(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw agents skills data into the dimension table `dim_agents_skills`.
    This function reads from the raw agents skills data and writes to the dimension table with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
    INSERT OVERWRITE dim_agents_skills
    SELECT DISTINCT
        skillId,
        useDisposition,
        useSecondaryDispositions,
        requireDisposition,
        useACW,
        outboundStrategy,
        extractDate
    FROM raw_agents_skills
""")