"""
This module contains the function to transform raw contact data into a fact table for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_contacts(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw contacts data into the fact table `fact_contacts`.
    This function reads from the raw contacts data and writes to the fact table
    with necessary transformations.
    
    :param spark: SparkSession object
    :return: None
    """
    #update or insert based on data 
    spark.sql(
        """
        INSERT OVERWRITE TABLE fact_contacts
        SELECT
            contactId,
            agentId,
            skillId,
            teamId,
            campaignId,
            primaryDispositionId,
            secondaryDispositionId,
            abandonSeconds,
            acwSeconds,
            agentSeconds,
            callbackTime,
            conferenceSeconds,
            holdSeconds,
            inQueueSeconds,
            postQueueSeconds,
            preQueueSeconds,
            releaseSeconds,
            totalDurationSeconds,
            serviceLevelFlag,
            isOutbound,
            isRefused,
            isShortAbandon,
            abandoned,
            holdCount,
            highProficiency,
            lowProficiency,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_contacts
        """
    )