from pyspark.sql import SparkSession

def fact_contacts(spark: SparkSession):
    spark.sql(
        f"""
        INSERT OVERWRITE TABLE niceincontact_infobell.fact_contacts
        SELECT
            contactId,
            agentId,
            skillId,
            teamId,
            campaignId,
            primaryDispositionId,
            secondaryDispositionId,
            CAST(abandonSeconds AS DOUBLE),
            CAST(acwSeconds AS DOUBLE),
            CAST(agentSeconds AS DOUBLE),
            callbackTime,
            CAST(conferenceSeconds AS DOUBLE),
            CAST(holdSeconds AS DOUBLE),
            CAST(inQueueSeconds AS DOUBLE),
            CAST(postQueueSeconds AS DOUBLE),
            CAST(preQueueSeconds AS DOUBLE),
            CAST(releaseSeconds AS DOUBLE),
            CAST(totalDurationSeconds AS DOUBLE),
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
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_contacts
        """
    )