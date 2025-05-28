"""
This file contains the transformation logic for the `dim_agents` table in the Nice InContact data warehouse.
"""
from pyspark.sql import SparkSession

def dim_agents(spark: SparkSession):
    """
    Transforms the raw agents data into the dimension table `dim_agents`.
    This function reads from the raw agents data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.dim_agents
        SELECT
            agentId,
            AgentId,
            userName,
            UserName,
            firstName,
            FirstName,
            middleName,
            MiddleName,
            lastName,
            LastName,
            emailAddress,
            Email,
            isActive,
            IsActive,
            teamId,
            TeamId,
            teamName,
            TeamName,
            reportToId,
            ReportToId,
            isSupervisor,
            IsSupervisor,
            lastLogin,
            LastLogin,
            lastUpdated,
            LastModified,
            location,
            Location,
            custom1,
            Custom1,
            custom2,
            Custom2,
            custom3,
            Custom3,
            custom4,
            Custom4,
            custom5,
            Custom5,
            internalId,
            InternalId,
            userId,
            reportToName,
            profileId,
            profileName,
            timeZone,
            country,
            countryName,
            state,
            city,
            chatRefusalTimeout,
            phoneRefusalTimeout,
            workItemRefusalTimeout,
            defaultDialingPattern,
            defaultDialingPatternName,
            useTeamMaxConcurrentChats,
            maxConcurrentChats,
            notes,
            createDate,
            inactiveDate,
            hireDate,
            terminationDate,
            hourlyCost,
            extractDate,
            recordInsertTime
        FROM spark_catalog.niceincontact_infobell.raw_agents
    """)
