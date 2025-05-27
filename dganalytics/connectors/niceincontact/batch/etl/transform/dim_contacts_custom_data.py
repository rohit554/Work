"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def dim_contacts_custom_data(spark: SparkSession):
    """
    Transforms the raw contacts custom data into the dimension table `dim_contacts_custom_data`.
    This function reads from the raw contacts custom data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None"""
    spark.sql(
        """
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