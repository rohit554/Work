"""
This module contains the function to transform raw agent performance data into the fact table `fact_agent_performance` for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_agent_performance(spark: SparkSession):
    """
    Transforms raw agent performance into the fact table.

    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.fact_agent_performance
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
            extractIntervalEndTime,
            recordInsertTime
        FROM spark_catalog.niceincontact_infobell.raw_agents_performance
    """)
