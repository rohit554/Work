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
            AgentId,
            UserName,
            FirstName,
            MiddleName,
            LastName,
            Email,
            IsActive,
            TeamId,
            TeamName,
            ReportToId,
            IsSupervisor,
            LastLogin,
            lastUpdated,
            LastModified,
            Location,
            Custom1,
            Custom2,
            Custom3,
            Custom4,
            Custom5,
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
            rehireStatus,
            employmentType,
            employmentTypeName,
            referral,
            atHome,
            hiringSource,
            ntLoginName,
            scheduleNotification,
            federatedId,
            useTeamEmailAutoParkingLimit,
            maxEmailAutoParkingLimit,
            sipUser,
            systemUser,
            systemDomain,
            crmUserName,
            useAgentTimeZone,
            timeDisplayFormat,
            sendEmailNotifications,
            apiKey,
            telephone1,
            telephone2,
            userType,
            isWhatIfAgent,
            timeZoneOffset,
            requestContact,
            recordingNumbers,
            BusinessUnitId,
            ReportToFirstName,
            ReportToMiddleName,
            ReportToLastName,
            extractDate,
            recordInsertTime
        FROM spark_catalog.niceincontact_infobell.raw_agents
    """)
