"""
This module contains the function to transform raw contact data into a fact table for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_contacts(spark: SparkSession):
    """
    Transforms the raw contacts data into the fact table `fact_contacts`.
    This function reads from the raw contacts data and writes to the fact table
    with necessary transformations.
    
    :param spark: SparkSession object
    :return: None
    """
    #update or insert based on data 
    """
This module contains the function to transform raw media playback data into a fact table for Nice inContact.
"""
from pyspark.sql import SparkSession

def fact_media_playback_contact(spark: SparkSession):
    """
    Transforms the raw media playback contact data into the fact table `fact_media_playback_contact`.
    This function reads from the raw media playback data and writes to the fact table
    with necessary transformations (currently a direct overwrite).
    
    :param spark: SparkSession object
    :return: None
    """
    # update or insert based on data
    spark.sql(
        """
        INSERT OVERWRITE TABLE niceincontact_infobell.fact_media_playback_contact
        SELECT
            contactId,
            acdcontactId,
            brandEmbassyConfig,
            elevatedInteraction,
            interactions,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_media_playback_contact
        """
    )
