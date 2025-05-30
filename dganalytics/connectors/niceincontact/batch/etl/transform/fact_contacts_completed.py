"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact."""
from pyspark.sql import SparkSession

def fact_contacts_completed(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw contacts completed data into the fact table `fact_contacts_completed`.
    This function reads from the raw contacts completed data and writes to the fact table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql(
        """
        INSERT INTO fact_contacts_completed
        SELECT
            contactId,
            agentId,
            skillId,
            teamId,
            campaignId,
            primaryDispositionId,
            secondaryDispositionId,
            abandoned,
            abandonSeconds,
            acwSeconds,
            agentSeconds,
            callbackTime,
            conferenceSeconds,
            holdSeconds,
            holdCount,
            inQueueSeconds,
            postQueueSeconds,
            preQueueSeconds,
            releaseSeconds,
            totalDurationSeconds,
            serviceLevelFlag,
            isOutbound,
            isRefused,
            isShortAbandon,
            highProficiency,
            lowProficiency,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_contacts_completed
        """
    )