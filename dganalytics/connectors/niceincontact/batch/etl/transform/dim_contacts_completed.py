
"""
This module contains the function to transform and load the dim_contacts_completed table
in the Nice InContact data warehouse.
"""
from pyspark.sql import SparkSession

def dim_contacts_completed(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw contacts completed data into the dimension table `dim_contacts_completed`.
    This function reads from the raw contacts completed data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None"""
    spark.sql(
        """
        INSERT INTO dim_contacts_completed
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
            extractDate
        FROM raw_contacts_completed
        """
    )