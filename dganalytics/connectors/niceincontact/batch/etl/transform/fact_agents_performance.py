"""
This module contains the function to transform raw agent performance data into the fact table `fact_agent_performance` for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_agent_performance(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms raw agent performance into the fact table.

    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT fact_agent_performance
        SELECT
            agentId,
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
            totalTalkTime,
            totalAvgTalkTime,
            totalAvgHandleTime,
            consultTime,
            availableTime,
            unavailableTime,
            acwTime,
            refused,
            percentRefused,
            loginTime,
            workingRate,
            occupancy,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_agents_performance
    """)
