from pyspark.sql import SparkSession

def dim_contacts(spark: SparkSession):
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.dim_contacts
        SELECT
            contactId,
            masterContactId,
            contactStartDate,
            agentStartDate,
            digitalContactStateId,
            digitalContactStateName,
            contactStateCategory,
            endReason,
            fromAddress,
            toAddress,
            fileName,
            mediaTypeId,
            mediaTypeName,
            mediaSubTypeId,
            mediaSubTypeName,
            pointOfContactId,
            pointOfContactName,
            refuseReason,
            refuseTime,
            routingAttribute,
            routingTime,
            stateId,
            stateName,
            targetAgentId,
            transferIndicatorId,
            transferIndicatorName,
            isTakeover,
            isLogged,
            isWarehoused,
            isAnalyticsProcessed,
            analyticsProcessedDate,
            dateACWWarehoused,
            dateContactWarehoused,
            current_date() AS extractDate,
            current_timestamp() AS recordInsertTime
        FROM spark_catalog.niceincontact_infobell.raw_contacts
    """)