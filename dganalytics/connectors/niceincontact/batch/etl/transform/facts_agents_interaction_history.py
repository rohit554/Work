"""
This module contains the function to transform raw interaction history into the fact table `fact_agent_interaction` for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_agent_interaction_history(spark: SparkSession):
    """
    Transforms raw interaction history into the fact table.

    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE park_catalog.niceincontact_infobell.fact_agent_interaction_history
        SELECT
            contactId,
            masterContactId,
            contactStartDate,
            targetAgentId,
            fileName,
            pointOfContact,
            lastUpdateTime,
            mediaTypeId,
            mediaTypeName,
            mediaSubTypeId,
            mediaSubTypeName,
            agentId,
            firstName,
            lastName,
            teamId,
            teamName,
            campaignId,
            campaignName,
            skillId,
            skillName,
            isOutbound,
            fromAddr,
            toAddr,
            primaryDispositionId,
            secondaryDispositionId,
            transferIndicatorId,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime
        FROM park_catalog.niceincontact_infobell.raw_agents_interaction_history
    """)
