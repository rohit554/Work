from pyspark.sql import SparkSession

def dim_contacts_custom_data(spark):
    spark.sql(
        f"""
        INSERT INTO spark_catalog.niceincontact_infobell.dim_contacts_custom_data
        SELECT
            contactId,
            name,
            value,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_contacts_custom_data
        """
    )