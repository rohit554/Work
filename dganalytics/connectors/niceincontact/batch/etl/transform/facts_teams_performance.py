"""
This module contains the function to transform raw team performance data
into the fact table for Nice inContact.
"""

from pyspark.sql import SparkSession

def fact_teams_performance(spark: SparkSession):
    """
    Transforms the raw team performance data into the fact table `fact_teams_performance`.
    This function reads from the raw team performance data and writes to the fact table
    with necessary transformations.
    
    :param spark: SparkSession object
    :return: None
    """
    spark.sql(
        """
        INSERT OVERWRITE TABLE niceincontact_infobell.fact_teams_performance
        SELECT
            teamId,
            agentOffered,
            inboundHandled,
            inboundTime,
            inboundTalkTime,
            inboundAvgTalkTime,
            outboundHandled,
            outboundTime,
            outboundTalkTime,
            outboundAvgTalkTime,
            totalHandled,
            totalAvgHandled,
            totalTalkTime,
            totalAvgTalkTime,
            totalAvgHandleTime,
            consultTime,
            availableTime,
            unavailableTime,
            avgAvailableTime,
            avgUnavailableTime,
            acwTime,
            refused,
            percentRefused,
            loginTime,
            workingRate,
            occupancy,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_teams_performance
        """
    )
