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
    apis = ["agents", "teams", "teams_agents", "skills", "agents_skills", "contacts",
            "contacts_completed", "contacts_custom_data", "dispositions", "dispositions_skills"
            "segments_analyzed", "media_playback_contact", "media_playback_chat_email_segment", 
            "media_playback_voice_segment", "segments_analyzed_transcript", "wfm_data_agents"]
    for api in apis:
        logger.info(f"Creating raw table for API: {api}")
        create_raw_table(api, spark, db_name, db_path, logger)

    logger.info("Raw tables creation completed.")
    return True

def create_dim_tables(spark: SparkSession, db_name: str, db_path: str, logger):
    """
    Create dimension tables for NICE inContact with all available fields.
    Args:
        spark (SparkSession): Spark session object.
        db_name (str): Name of the database where the tables will be created.
        db_path (str): Base path for storing table data.
        logger: Logger object for logging progress.
    Returns:
        None
    """
    logger.info("Creating full dim_agents table with all available fields")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.niceincontact_infobell.fact_agents_skills (
            agentId BIGINT,
            internalId STRING,
            teamId BIGINT,
            firstName STRING,
            lastName STRING,
            campaignId BIGINT,
            emailFromAddress STRING,
            skillId BIGINT,
            skillName STRING,
            isSkillActive BOOLEAN,
            isAgentActive BOOLEAN,
            outboundStrategy STRING,
            requireDisposition BOOLEAN,
            priorityBlending BOOLEAN,
            mediaTypeId LONG,
            mediaTypeName STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_agents_skills'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_contacts_completed (
            contactId BIGINT,
            masterContactId BIGINT,
            contactStartDate TIMESTAMP,
            lastUpdateTime TIMESTAMP,
            endReason STRING,
            fromAddress STRING,
            toAddress STRING,
            mediaTypeId BIGINT,
            mediaTypeName STRING,
            mediaSubTypeId BIGINT,
            mediaSubTypeName STRING,
            pointOfContactId BIGINT,
            pointOfContactName STRING,
            refuseReason STRING,
            refuseTime TIMESTAMP,
            routingAttribute STRING,
            routingTime TIMESTAMP,
            transferIndicatorId BIGINT,
            transferIndicatorName STRING,
            isTakeover BOOLEAN,
            isLogged BOOLEAN,
            isAnalyticsProcessed BOOLEAN,
            analyticsProcessedDate TIMESTAMP,
            dateACWWarehoused TIMESTAMP,
            dateContactWarehoused TIMESTAMP,
            extractDate DATE,
            recordInsertTime TIMESTAMP
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_contacts_completed'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_contacts_completed (
            contactId BIGINT,
            agentId BIGINT,
            skillId BIGINT,
            teamId BIGINT,
            campaignId BIGINT,
            primaryDispositionId BIGINT,
            secondaryDispositionId BIGINT,
            abandoned BOOLEAN,
            abandonSeconds DOUBLE,
            acwSeconds DOUBLE,
            agentSeconds DOUBLE,
            callbackTime BIGINT,
            conferenceSeconds DOUBLE,
            holdSeconds DOUBLE,
            holdCount BIGINT,
            inQueueSeconds DOUBLE,
            postQueueSeconds DOUBLE,
            preQueueSeconds DOUBLE,
            releaseSeconds DOUBLE,
            totalDurationSeconds DOUBLE,
            serviceLevelFlag BIGINT,           ---->#validate
            isOutbound BOOLEAN,
            isRefused BOOLEAN,
            isShortAbandon BOOLEAN,
            highProficiency BIGINT,-----#validate
            lowProficiency BIGINT,----------#validate
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/fact_contacts_completed'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_contacts_custom_data (
            contactId BIGINT,
            name STRING,
            value STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_contacts_custom_data'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_contacts (
            contactId BIGINT,
            masterContactId BIGINT,
            contactStartDate TIMESTAMP,
            agentStartDate TIMESTAMP,
            digitalContactStateId BIGINT,
            digitalContactStateName STRING,
            contactStateCategory STRING,
            endReason STRING,
            fromAddress STRING,
            toAddress STRING,
            fileName STRING,
            mediaTypeId BIGINT,
            mediaTypeName STRING,
            mediaSubTypeId BIGINT,
            mediaSubTypeName STRING,
            pointOfContactId BIGINT,
            pointOfContactName STRING,
            refuseReason STRING,
            refuseTime TIMESTAMP,
            routingAttribute STRING,
            routingTime TIMESTAMP,
            stateId BIGINT,
            stateName STRING,
            targetAgentId BIGINT,
            transferIndicatorId BIGINT,
            transferIndicatorName STRING,
            isTakeover BOOLEAN,
            isLogged BOOLEAN,
            isWarehoused BOOLEAN,
            isAnalyticsProcessed BOOLEAN,
            analyticsProcessedDate TIMESTAMP,
            dateACWWarehoused TIMESTAMP,
            dateContactWarehoused TIMESTAMP,
            extractDate DATE,
            recordInsertTime TIMESTAMP
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_contacts'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_contacts (
            contactId BIGINT,
            agentId BIGINT,
            skillId BIGINT,
            teamId BIGINT,
            campaignId BIGINT,
            primaryDispositionId INT,
            secondaryDispositionId INT,
            abandonSeconds DOUBLE,
            acwSeconds DOUBLE,
            agentSeconds DOUBLE,
            callbackTime INT,
            conferenceSeconds DOUBLE,
            holdSeconds DOUBLE,
            inQueueSeconds DOUBLE,
            postQueueSeconds DOUBLE,
            preQueueSeconds DOUBLE,
            releaseSeconds DOUBLE,
            totalDurationSeconds DOUBLE,
            serviceLevelFlag INT,
            isOutbound BOOLEAN,
            isRefused BOOLEAN,
            isShortAbandon BOOLEAN,
            abandoned BOOLEAN,
            holdCount INT,
            highProficiency INT,
            lowProficiency INT,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/fact_contacts'
        """
    )

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
            teamId BIGINT,
            teamName STRING,
            isActive BOOLEAN,
            description STRING,
            notes STRING,
            lastUpdateTime STRING,
            inViewEnabled BOOLEAN,
            wfoEnabled BOOLEAN,
            wfm2Enabled BOOLEAN,
            qm2Enabled BOOLEAN,
            maxConcurrentChats BIGINT,
            agentCount BIGINT,
            maxEmailAutoParkingLimit BIGINT,
            inViewGamificationEnabled BOOLEAN,
            inViewChatEnabled BOOLEAN,
            inViewWallboardEnabled BOOLEAN,
            inViewLMSEnabled BOOLEAN,
            analyticsEnabled BOOLEAN,
            requestContact BOOLEAN,
            contactAutoFocus BOOLEAN,
            chatThreshold BIGINT,
            emailThreshold BIGINT,
            workItemThreshold BIGINT,
            smsThreshold BIGINT,
            digitalThreshold BIGINT,
            voiceThreshold BIGINT,
            teamLeadId STRING,
            deliveryMode STRING,
            totalContactCount BIGINT,
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
            socialThreshold BIGINT,
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

    logger.info("Creating dim_skills table with all available fields")
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
    #Main entry point for the Nice inContact setup script.
    #This script initializes the necessary Spark SQL database and tables for Nice inContact data ingestion.
    #It requires a tenant name as an argument to configure the database and paths accordingly.
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