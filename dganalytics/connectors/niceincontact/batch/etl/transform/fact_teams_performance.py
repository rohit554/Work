"""
This module contains the function to transform raw team performance data
into the fact table for Nice inContact.
"""

from pyspark.sql import SparkSession

def fact_teams_performance(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw team performance data into the fact table `fact_teams_performance`.
    This function reads from the raw team performance data and writes to the fact table
    with necessary transformations.
    
    :param spark: SparkSession object
    :return: None
    """
    spark.sql(
        """
        INSERT OVERWRITE TABLE fact_teams_performance
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
            extractIntervalEndTime
        FROM raw_teams_performance
        """
    )
