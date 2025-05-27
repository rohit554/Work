from pyspark.sql import SparkSession

def dim_dispositions(spark: SparkSession):
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.dim_dispositions
        SELECT
            dispositionId,
            dispositionName,
            notes,
            lastUpdated,
            classificationId,
            systemOutcome,
            isActive,
            isPreviewDisposition,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_dispositions
    """)
