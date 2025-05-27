"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_agents_skills(spark: SparkSession):
    """"
    Transforms the raw agents and skills data into the fact table `fact_agents_skills`.
    This function reads from the raw agents and skills data and writes to the fact table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.fact_agents_skills
        SELECT
            a.agentId,
            a.internalId,
            a.teamId,
            a.firstName,
            a.lastName,
            s.campaignId,
            s.emailFromAddress,
            s.skillId,
            s.skillName,
            s.isActive AS isSkillActive,
            a.isActive AS isAgentActive,
            s.outboundStrategy,
            s.requireDisposition,
            s.priorityBlending,
            s.mediaTypeId,
            s.mediaTypeName,
            current_date() AS extractDate,
            current_timestamp() AS extractIntervalStartTime,
            current_timestamp() AS extractIntervalEndTime,
            current_timestamp() AS recordInsertTime
        FROM spark_catalog.niceincontact_infobell.raw_agents a
        JOIN spark_catalog.niceincontact_infobell.raw_skills s
          ON a.teamId = s.campaignId
    """)