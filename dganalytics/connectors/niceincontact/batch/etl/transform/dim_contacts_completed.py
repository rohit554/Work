from pyspark.sql import SparkSession

def dim_contacts_completed(spark: SparkSession):
    spark.sql(
        f"""
        INSERT INTO spark_catalog.niceincontact_infobell.dim_contacts_completed
        SELECT
            contactId,
            masterContactId,
            contactStartDate,
            lastUpdateTime,
            endReason,
            fromAddress,
            toAddress,
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
            transferIndicatorId,
            transferIndicatorName,
            isTakeover,
            isLogged,
            isAnalyticsProcessed,
            analyticsProcessedDate,
            dateACWWarehoused,
            dateContactWarehoused,
            extractDate,
            recordInsertTime
        FROM spark_catalog.niceincontact_infobell.raw_contacts_completed
        """
    )