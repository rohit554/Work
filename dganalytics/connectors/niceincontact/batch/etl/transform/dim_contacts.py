"""
This module contains the function to transform raw agent data into a dimension table for Nice inContact.
"""
from pyspark.sql import SparkSession

def dim_contacts(spark: SparkSession):
    """
    Transforms the raw contacts data into the dimension table `dim_contacts`.
    This function reads from the raw contacts data and writes to the dimension table
    with necessary transformations.
    
    :param spark: SparkSession object
    :return: None
    """
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