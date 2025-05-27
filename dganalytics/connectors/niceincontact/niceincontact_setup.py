import argparse
from dganalytics.connectors.niceincontact.niceincontact_utils import niceincontact_utils_logger, get_schema, get_dbname
from dganalytics.utils.utils import get_path_vars, get_spark_session
from pyspark.sql import SparkSession


def create_database(spark: SparkSession, path: str, db_name: str, logger):
    """
    Create a Spark SQL database if it does not already exist.

    Args:
        spark (SparkSession): Spark session object.
        path (str): Base path for the database location.
        db_name (str): Name of the database to create.
        logger: Logger object for logging progress.

    Returns:
        bool: True if the database creation command is issued.
    """
    logger.info("Creating database for NICE inContact")
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")
    return True

def create_ingestion_stats_table(spark: SparkSession, db_name: str, db_path: str, logger):
    """
    Create the ingestion stats table in the specified database.

    Args:
        spark (SparkSession): Spark session object.
        db_name (str): Database name where the table should be created.
        db_path (str): Path for storing table data.
        logger: Logger object for logging progress.

    Returns:
        bool: True if the table creation command is issued.
    """
    logger.info("Creating Ingestion stats table for NICE inContact")
    spark.sql(
        f"""
                create table if not exists {db_name}.ingestion_stats
                (
                    apiName string,
                    endPoint string,
                    pageCount int,
                    recordsFetched bigint,
                    rawDataFile_loc string,
                    adfRunId string,
                    extractDate date,
                    loadDateTime timestamp
                )
                    using delta
            LOCATION '{db_path}/{db_name}/ingestion_stats'"""
    )
    return True

def create_raw_table(api_name: str, spark: SparkSession, db_name: str, db_path: str, logger):
    """
    Create a raw delta table for the given API using its JSON schema.

    Args:
        api_name (str): The name of the API to create a raw table for.
        spark (SparkSession): Spark session object.
        db_name (str): Name of the database where the table will be created.
        db_path (str): Base path for the table's storage.

    Returns:
        bool: True if the table is created successfully.
    """
    schema = get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    logger.info(f"Creating niceincontact raw table - {table_name}")
    spark.createDataFrame(spark.sparkContext.emptyRDD(),
                          schema=schema).createOrReplaceTempView(table_name)
    create_qry = f"""create table if not exists {db_name}.{table_name}
                        using delta partitioned by(extractDate, extractIntervalStartTime, extractIntervalEndTime) LOCATION
                                '{db_path}/{db_name}/{table_name}'
                        as
                    select *, cast('1900-01-01' as date) extractDate,
                    cast('1900-01-01 00:00:00' as timestamp) extractIntervalStartTime,
                    cast('1900-01-01 00:00:00' as timestamp) extractIntervalEndTime,
                    cast('1900-01-01 00:00:00' as timestamp) recordInsertTime,
                    monotonically_increasing_id() as recordIdentifier
                     from {table_name} limit 0"""
    spark.sql(create_qry)

    return True

def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str, logger):
    """
    Create raw tables for all NICE inContact APIs defined in the API config.

    Args:
        spark (SparkSession): Spark session object.
        db_name (str): Target database name.
        db_path (str): Base path for storing raw tables.
        tenant_path (str): Path specific to the tenant.
        client (NiceInContactClient): Configured client instance.

    Returns:
        bool: True if all raw tables are created.
    """
    logger.info("Setting Nice InContact raw tables")
    apis = ["agents","teams","contacts"]
    for api in apis:
        logger.info(f"Creating raw table for API: {api}")
        create_raw_table(api, spark, db_name, db_path, logger)

    logger.info("Raw tables creation completed.")
    return True


# def create_dim_tables(spark: SparkSession, db_name: str, db_path: str, logger):
#     logger.info("Creating NiceInContact dimension, bridge, and fact tables")

#     spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
#     spark.sql(f"USE {db_name}")

#     # Dimension Tables
#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.dim_agents (
#             agentId BIGINT,
#             userName STRING,
#             firstName STRING,
#             lastName STRING,
#             middleName STRING,
#             emailAddress STRING,
#             isActive BOOLEAN,
#             teamId BIGINT,
#             profileId BIGINT,
#             profileName STRING,
#             countryName STRING,
#             city STRING,
#             hireDate STRING,
#             terminationDate STRING,
#             employmentTypeName STRING,
#             atHome BOOLEAN,
#             extractDate DATE,
#             recordInsertTime TIMESTAMP,
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/dim_agents'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.dim_skills (
#             skillId BIGINT,
#             skillName STRING,
#             mediaTypeId BIGINT,
#             mediaTypeName STRING,
#             isActive BOOLEAN,
#             campaignId BIGINT,
#             campaignName STRING,
#             isOutbound BOOLEAN,
#             outboundStrategy STRING
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/dim_skills'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.dim_dispositions (
#             dispositionId BIGINT,
#             dispositionName STRING,
#             systemOutcome STRING,
#             isActive BOOLEAN,
#             isPreviewDisposition BOOLEAN
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/dim_dispositions'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.dim_teams (
#             teamId INT,
#             teamName STRING,
#             isActive BOOLEAN,
#             description STRING,
#             agentCount INT,
#             analyticsEnabled BOOLEAN,
#             requestContact BOOLEAN,
#             contactAutoFocus BOOLEAN
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/dim_teams'
#         """
#     )

#     # Bridge Tables
#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.bridge_agents_skills (
#             agentId BIGINT,
#             skillId BIGINT,
#             agentProficiencyValue INT,
#             agentProficiencyName STRING,
#             isActive BOOLEAN,
#             isSkillActive BOOLEAN,
#             lastUpdateTime STRING
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/bridge_agents_skills'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.bridge_dispositions_skills (
#             dispositionId BIGINT,
#             dispositionName STRING,
#             isActive BOOLEAN,
#             skillId BIGINT,
#             mediaTypeId BIGINT,
#             mediaTypeName STRING
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/bridge_dispositions_skills'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.bridge_teams_agents (
#             teamId BIGINT,
#             teamName STRING,
#             agentId BIGINT,
#             userName STRING,
#             firstName STRING,
#             lastName STRING,
#             emailAddress STRING,
#             isActive BOOLEAN,
#             profileId BIGINT,
#             profileName STRING,
#             country STRING,
#             city STRING
#         )
#         USING DELTA
#         LOCATION '{db_path}/{db_name}/bridge_teams_agents'
#         """
#     )

#     # Fact Tables
#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.fact_contacts (
#             abandoned BOOLEAN,
#             abandonSeconds DOUBLE,
#             acwSeconds DOUBLE,
#             agentId BIGINT,
#             agentSeconds DOUBLE,
#             contactId BIGINT,
#             contactStartDate STRING,
#             dispositionNotes STRING,
#             mediaTypeId INT,
#             mediaTypeName STRING,
#             skillId BIGINT,
#             skillName STRING,
#             teamId BIGINT,
#             teamName STRING,
#             totalDurationSeconds DOUBLE,
#             extractDate DATE,
#             recordInsertTime TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (extractDate)
#         LOCATION '{db_path}/{db_name}/fact_contacts'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.fact_skills_summary (
#             businessUnitId BIGINT,
#             abandonRate FLOAT,
#             agentsLoggedIn BIGINT,
#             averageHandleTime STRING,
#             skillId BIGINT,
#             skillName STRING,
#             serviceLevel BIGINT,
#             extractDate DATE,
#             recordInsertTime TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (extractDate)
#         LOCATION '{db_path}/{db_name}/fact_skills_summary'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.fact_skills_sla_summary (
#             businessUnitId BIGINT,
#             skillId BIGINT,
#             skillName STRING,
#             contactsWithinSLA BIGINT,
#             contactsOutOfSLA BIGINT,
#             totalContacts BIGINT,
#             serviceLevel DOUBLE,
#             extractDate DATE,
#             recordInsertTime TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (extractDate)
#         LOCATION '{db_path}/{db_name}/fact_skills_sla_summary'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.fact_interaction_segments (
#             segmentId STRING,
#             acdContactId BIGINT,
#             agentContactId STRING,
#             resolved BOOLEAN,
#             publishedAt STRING,
#             startTime STRING,
#             endTime STRING,
#             mediaType STRING,
#             contactEndReason STRING,
#             dispositionCode STRING,
#             extractDate DATE,
#             recordInsertTime TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (extractDate)
#         LOCATION '{db_path}/{db_name}/fact_interaction_segments'
#         """
#     )

#     spark.sql(
#         f"""
#         CREATE TABLE IF NOT EXISTS {db_name}.fact_media_playback (
#             contactId STRING,
#             acdcontactId STRING,
#             elevatedInteraction BOOLEAN,
#             extractDate DATE,
#             recordInsertTime TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (extractDate)
#         LOCATION '{db_path}/{db_name}/fact_media_playback'
#         """
#     )

def create_dim_tables(spark: SparkSession, db_name: str, db_path: str, logger):
    logger.info("Creating full dim_agents table with all available fields")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")

    spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {db_name}.dim_agents (
        agentId BIGINT,
        userName STRING,
        userId STRING,
        firstName STRING,
        middleName STRING,
        lastName STRING,
        emailAddress STRING,
        isActive BOOLEAN,
        teamId BIGINT,
        teamName STRING,
        reportToId BIGINT,
        reportToName STRING,
        isSupervisor BOOLEAN,
        lastLogin STRING,
        lastUpdated STRING,
        location STRING,
        custom1 STRING,
        custom2 STRING,
        custom3 STRING,
        custom4 STRING,
        custom5 STRING,
        internalId STRING,
        profileId BIGINT,
        profileName STRING,
        timeZone STRING,
        country STRING,
        countryName STRING,
        state STRING,
        stateCode STRING,
        city STRING,
        chatRefusalTimeout BIGINT,
        phoneRefusalTimeout BIGINT,
        workItemRefusalTimeout BIGINT,
        emailRefusalTimeout BIGINT,
        voicemailRefusalTimeout BIGINT,
        smsRefusalTimeout BIGINT,
        digitalRefusalTimeout BIGINT,
        defaultDialingPattern BIGINT,
        defaultDialingPatternName STRING,
        useTeamMaxConcurrentChats BOOLEAN,
        maxConcurrentChats BIGINT,
        notes STRING,
        createDate STRING,
        inactiveDate STRING,
        hireDate STRING,
        terminationDate STRING,
        hourlyCost DOUBLE,
        rehireStatus BOOLEAN,
        employmentType BIGINT,
        employmentTypeName STRING,
        referral STRING,
        atHome BOOLEAN,
        hiringSource STRING,
        ntLoginName STRING,
        scheduleNotification BIGINT,
        federatedId STRING,
        useTeamEmailAutoParkingLimit BOOLEAN,
        maxEmailAutoParkingLimit BIGINT,
        sipUser STRING,
        systemUser STRING,
        systemDomain STRING,
        crmUserName STRING,
        useAgentTimeZone BOOLEAN,
        timeDisplayFormat STRING,
        sendEmailNotifications BOOLEAN,
        apiKey STRING,
        telephone1 STRING,
        telephone2 STRING,
        userType STRING,
        isWhatIfAgent BOOLEAN,
        timeZoneOffset STRING,
        requestContact BOOLEAN,
        chatThreshold BIGINT,
        useTeamChatThreshold BOOLEAN,
        emailThreshold BIGINT,
        useTeamEmailThreshold BOOLEAN,
        workItemThreshold BIGINT,
        useTeamWorkItemThreshold BOOLEAN,
        contactAutoFocus BOOLEAN,
        useTeamContactAutoFocus BOOLEAN,
        useTeamRequestContact BOOLEAN,
        voiceThreshold BIGINT,
        useTeamVoiceThreshold BOOLEAN,
        subject STRING,
        issuer STRING,
        isOpenIdProfileComplete BOOLEAN,
        teamUuid STRING,
        maxPreview BOOLEAN,
        deliveryMode STRING,
        totalContactCount BIGINT,
        useTeamDeliveryModeSettings BOOLEAN,
        isBillable BOOLEAN,
        agentVoiceThreshold BIGINT,
        agentChatThreshold BIGINT,
        agentEmailThreshold BIGINT,
        agentWorkItemThreshold BIGINT,
        agentDeliveryMode STRING,
        agentTotalContactCount BIGINT,
        agentContactAutoFocus BOOLEAN,
        agentRequestContact BOOLEAN,
        agentMaxVersion BIGINT,
        userNameDomain STRING,
        combinedUserNameDomain STRING,
        rowNumber BIGINT,
        SmsThreshold BIGINT,
        UseTeamsmsThreshold BOOLEAN,
        LoginAuthenticatorId STRING,
        DigitalThreshold BIGINT,
        UseTeamDigitalThreshold BOOLEAN,
        AgentPhoneTimeout BIGINT,
        AgentPhoneTimeoutSeconds BIGINT,
        address1 STRING,
        address2 STRING,
        zipCode STRING,
        noFixedAddress BOOLEAN,
        agentCxaClientVersion BIGINT,
        agentCxaReleasePrevious BOOLEAN,
        AgentIntegration BOOLEAN,
        channelLock BOOLEAN,
        digitalEngagementEnabled BOOLEAN,
        teams_channel_Id STRING,
        recordingNumbers STRING,
        integratedSoftphoneWebRtcUrls STRING,
        extractDate DATE,
        recordInsertTime TIMESTAMP
    )
    USING DELTA
    LOCATION '{db_path}/{db_name}/dim_agents'
    """
)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_teams (
            teamId INT,
            teamName STRING,
            isActive BOOLEAN,
            description STRING,
            notes STRING,
            lastUpdateTime STRING,
            inViewEnabled BOOLEAN,
            wfoEnabled BOOLEAN,
            wfm2Enabled BOOLEAN,
            qm2Enabled BOOLEAN,
            maxConcurrentChats INT,
            agentCount INT,
            maxEmailAutoParkingLimit INT,
            inViewGamificationEnabled BOOLEAN,
            inViewChatEnabled BOOLEAN,
            inViewWallboardEnabled BOOLEAN,
            inViewLMSEnabled BOOLEAN,
            analyticsEnabled BOOLEAN,
            requestContact BOOLEAN,
            contactAutoFocus BOOLEAN,
            chatThreshold INT,
            emailThreshold INT,
            workItemThreshold INT,
            smsThreshold INT,
            digitalThreshold INT,
            voiceThreshold INT,
            teamLeadId STRING,
            deliveryMode STRING,
            totalContactCount INT,
            niceAudioRecordingEnabled BOOLEAN,
            niceDesktopAnalyticsEnabled BOOLEAN,
            niceQmEnabled BOOLEAN,
            niceScreenRecordingEnabled BOOLEAN,
            niceSpeechAnalyticsEnabled BOOLEAN,
            niceWfmEnabled BOOLEAN,
            niceQualityOptimizationEnabled BOOLEAN,
            niceSurvey_CustomerEnabled BOOLEAN,
            nicePerformanceManagementEnabled BOOLEAN,
            niceAnalyticsEnabled BOOLEAN,
            niceLessonManagementEnabled BOOLEAN,
            niceCoachingEnabled BOOLEAN,
            niceStrategicPlannerEnabled BOOLEAN,
            niceShiftBiddingEnabled BOOLEAN,
            niceWfoAdvancedEnabled BOOLEAN,
            niceWfoEssentialsEnabled BOOLEAN,
            cxoneCustomerAuthenticationEnabled BOOLEAN,
            socialThreshold INT,
            teamUuid STRING,
            channelLock STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_teams'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_skills (
            skillId BIGINT,
            skillName STRING,
            mediaTypeId BIGINT,
            mediaTypeName STRING,
            workItemQueueType STRING,
            isActive BOOLEAN,
            campaignId BIGINT,
            campaignName STRING,
            notes STRING,
            acwTypeId BIGINT,
            stateIdACW BIGINT,
            stateNameACW STRING,
            maxSecondsACW BIGINT,
            acwPostTimeoutStateId BIGINT,
            acwPostTimeoutStateName STRING,
            requireDisposition BOOLEAN,
            allowSecondaryDisposition BOOLEAN,
            agentRestTime BIGINT,
            makeTranscriptAvailable BOOLEAN,
            transcriptFromAddress STRING,
            displayThankyou BOOLEAN,
            thankYouLink STRING,
            popThankYou BOOLEAN,
            popThankYouURL STRING,
            isOutbound BOOLEAN,
            outboundStrategy STRING,
            isRunning BOOLEAN,
            priorityBlending BOOLEAN,
            callerIdOverride STRING,
            scriptId BIGINT,
            scriptName STRING,
            emailFromAddress STRING,
            emailFromEditable BOOLEAN,
            emailBccAddress STRING,
            emailParking BOOLEAN,
            chatWarningThreshold BIGINT,
            agentTypingIndicator BOOLEAN,
            patronTypingPreview BOOLEAN,
            interruptible BOOLEAN,
            callSuppressionScriptId BIGINT,
            reskillHours BIGINT,
            reskillHoursName STRING,
            countReskillHours BOOLEAN,
            minWFIAgents BIGINT,
            minWFIAvailableAgents BIGINT,
            useScreenPops BOOLEAN,
            screenPopTriggerEvent STRING,
            useCustomScreenPops BOOLEAN,
            screenPopDetail STRING,
            minWorkingTime BIGINT,
            agentless BOOLEAN,
            agentlessPorts BIGINT,
            initialPriority BIGINT,
            acceleration BIGINT,
            maxPriority BIGINT,
            serviceLevelThreshold BIGINT,
            serviceLevelGoal BIGINT,
            enableShortAbandon BOOLEAN,
            shortAbandonThreshold BIGINT,
            countShortAbandons BOOLEAN,
            messageTemplateId BIGINT,
            smsTransportCodeId BIGINT,
            smsTransportCode STRING,
            dispositions ARRAY<STRUCT<dispositionId: BIGINT, dispositionName: STRING, priority: BIGINT, isPreviewDisposition: BOOLEAN>>,
            deliverMultipleNumbersSerially BOOLEAN,
            cradleToGrave BOOLEAN,
            priorityInterrupt BOOLEAN,
            outboundTelecomRouteId BIGINT,
            requireManualAccept BOOLEAN,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_skills'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_dispositions (
            dispositionId BIGINT,
            dispositionName STRING,
            notes STRING,
            lastUpdated TIMESTAMP,
            classificationId BIGINT,
            systemOutcome STRING,
            isActive BOOLEAN,
            isPreviewDisposition BOOLEAN,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_dispositions'
        """
    )



if __name__ == "__main__":
    app_name = "niceincontact_setup"
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = niceincontact_utils_logger(tenant, app_name.lower())
    logger.info("Setup started...")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    spark = get_spark_session(app_name=app_name,
                              tenant=tenant, default_db='default')
    
    create_database(spark, db_path, db_name, logger)
    create_ingestion_stats_table(spark, db_name, db_path, logger)
    raw_tables(spark, db_name, db_path, tenant_path, logger)
    create_dim_tables(spark, db_name, db_path, logger)