"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_agents_skills(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """"
    Transforms the raw agents and skills data into the fact table `fact_agents_skills`.
    This function reads from the raw agents and skills data and writes to the fact table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql(
        """
        INSERT OVERWRITE fact_agents_skills
        SELECT
            agentId,
            skillId,
            campaignId,
            agentProficiencyValue,
            agentProficiencyName,
            isSkillActive,
            isDialer,
            isNaturalCalling,
            isNaturalCallingRunning,
            screenPopTriggerEvent,
            lastUpdateTime,
            lastPollTime,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_agents_skills
    """)
