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
    apis = ["agents", "teams", "teams_agents", "teams_performace", "skills", "agents_skills", 
            "agents_performance", "agents_interaction_history", "contacts",
            "contacts_completed", "contacts_custom_data", "dispositions", "dispositions_skills",
            "skills_summary", "skills_sla_summary",
            "segments_analyzed", "media_playback_contact", "media_playback_chat_email_segment", 
            "media_playback_voice_segment", "segments_analyzed_transcript", "wfm_data_agents",
            "wfm_data_agents_schedule_adherence", "wfm_data_agents_scorecards", "wfm_data_agents_schedule_adherence",
            "wfm_data_skills_contacts"]
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
    logger.info("Creating dimensions and facts tables with all required fields")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")

    logger.info("Creating dim_agents table with all available fields")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_agents (
            agentId BIGINT,
            userName STRING,
            firstName STRING,
            middleName STRING,
            lastName STRING,
            emailAddress STRING,
            isActive BOOLEAN,
            teamId BIGINT,
            teamName STRING,
            reportToId BIGINT,
            isSupervisor BOOLEAN,
            lastLogin TIMESTAMP,
            lastUpdated TIMESTAMP,
            lastModified TIMESTAMP,
            location STRING,
            custom1 STRING,
            custom2 STRING,
            custom3 STRING,
            custom4 STRING,
            custom5 STRING,
            internalId STRING,
            userId STRING,
            reportToName STRING,
            profileId BIGINT,
            profileName STRING,
            timeZone STRING,
            country STRING,
            countryName STRING,
            state STRING,
            city STRING,
            chatRefusalTimeout BIGINT,
            phoneRefusalTimeout BIGINT,
            workItemRefusalTimeout BIGINT,
            defaultDialingPattern BIGINT,
            defaultDialingPatternName STRING,
            useTeamMaxConcurrentChats BOOLEAN,
            maxConcurrentChats BIGINT,
            notes STRING,
            createDate TIMESTAMP,
            inactiveDate TIMESTAMP,
            hireDate TIMESTAMP,
            terminationDate TIMESTAMP,
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
            systemUser BOOLEAN,
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
            requestContact STRING,
            recordingNumbers ARRAY<STRING>,
            businessUnitId BIGINT,
            reportToFirstName STRING,
            reportToMiddleName STRING,
            reportToLastName STRING
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_agents'
        """)

    logger.info("Creating dim_teams table with all available fields")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_agents (
            agentId BIGINT,
            userName STRING,
            firstName STRING,
            middleName STRING,
            lastName STRING,
            userId STRING,
            emailAddress STRING,
            isActive BOOLEAN,
            teamId BIGINT,
            teamName STRING,
            reportToId STRING,
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
            city STRING,
            chatRefusalTimeout BIGINT,
            phoneRefusalTimeout BIGINT,
            workItemRefusalTimeout BIGINT,
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
            employmentType STRING,
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
            recordingNumbers ARRAY<STRING>,
            subject STRING,
            issuer STRING,
            isOpenIdProfileComplete BOOLEAN,
            teamUuid STRING,
            maxPreview BOOLEAN,
            deliveryMode STRING,
            totalContactCount BIGINT,
            useTeamDeliveryModeSettings BOOLEAN,
            emailRefusalTimeout BIGINT,
            voicemailRefusalTimeout BIGINT,
            smsRefusalTimeout BIGINT,
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
            customerCard BOOLEAN,
            locked BOOLEAN,
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
            DigitalRefusalTimeout BIGINT,
            agentCxaClientVersion BIGINT,
            agentCxaReleasePrevious STRING,
            stateCode STRING,
            AgentIntegration BOOLEAN,
            channelLock BOOLEAN,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_agents'
        """)

    logger.info("Creating fact_teams_agents table with all available fields")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS spark_catalog.niceincontact_infobell.fact_teams_agents (
            teamId BIGINT,
            teamName STRING,
            isActive BOOLEAN,
            inViewEnabled BOOLEAN,
            wfoEnabled BOOLEAN,
            wfm2Enabled BOOLEAN,
            qm2Enabled BOOLEAN,
            description STRING,
            notes STRING,
            lastUpdateTime STRING,
            inViewGamificationEnabled BOOLEAN,
            inViewChatEnabled BOOLEAN,
            inViewLMSEnabled BOOLEAN,
            analyticsEnabled BOOLEAN,
            voiceThreshold BIGINT,
            chatThreshold BIGINT,
            emailThreshold BIGINT,
            socialThreshold BIGINT,
            workItemThreshold BIGINT,
            requestContact BOOLEAN,
            contactAutoFocus BOOLEAN,
            agentId BIGINT,
            userName STRING,
            firstName STRING,
            middleName STRING,
            lastName STRING,
            emailAddress STRING,
            isActive BOOLEAN,
            reportToId BIGINT,
            isSupervisor BOOLEAN,
            lastLogin STRING,
            lastModified STRING,
            locationId BIGINT,
            custom1 STRING,
            custom2 STRING,
            custom3 STRING,
            custom4 STRING,
            custom5 STRING,
            internalId STRING,
            securityProfileId BIGINT,
            timeZone STRING,
            country STRING,
            state STRING,
            city STRING,
            chatRefusalTimeout BIGINT,
            phoneRefusalTimeout BIGINT,
            voicemailRefusalTimeout BIGINT,
            workItemRefusalTimeout BIGINT,
            defaultDialingPattern STRING,
            maxConcurrentChats BIGINT,
            notes_agent STRING,
            hireDate STRING,
            terminationDate STRING,
            hourlyCost DOUBLE,
            rehireStatus BOOLEAN,
            employmentType STRING,
            referral STRING,
            atHome BOOLEAN,
            hiringSource STRING,
            ntLoginName STRING,
            scheduleNotification STRING,
            federatedIdValue STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_teams_agents'
    """)

    logger.info("Creating fact_teams_performance table with all available fields")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_teams_performance (
            teamId STRING,
            agentOffered STRING,
            inboundHandled STRING,
            inboundTime STRING,
            inboundTalkTime STRING,
            inboundAvgTalkTime STRING,
            outboundHandled STRING,
            outboundTime STRING,
            outboundTalkTime STRING,
            outboundAvgTalkTime STRING,
            totalHandled STRING,
            totalAvgHandled STRING,
            totalTalkTime STRING,
            totalAvgTalkTime STRING,
            totalAvgHandleTime STRING,
            consultTime STRING,
            availableTime STRING,
            unavailableTime STRING,
            avgAvailableTime STRING,
            avgUnavailableTime STRING,
            acwTime STRING,
            refused STRING,
            percentRefused STRING,
            loginTime STRING,
            workingRate STRING,
            occupancy STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_teams_performance'
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

    logger.info("Creating dim_dispositions table with all available fields")
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

    logger.info("Creating dim_dispositions_skills table with all available fields")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_dispositions_skills (
            dispositionId BIGINT,
            dispositionName STRING,
            isActive BOOLEAN,
            skills ARRAY<STRUCT<skillId: BIGINT, mediaTypeId: BIGINT, mediaTypeName: STRING>>,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_dispositions_skills'
    """)

    logger.info("Creating dim_agents_skills table with all available fields")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_agents_skills (
            skillId BIGINT,
            useDisposition BOOLEAN,
            useSecondaryDispositions BOOLEAN,
            requireDisposition BOOLEAN,
            useACW BOOLEAN,
            outboundStrategy STRING,
            extractDate DATE,
            recordInsertTime TIMESTAMP
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/dim_agents_skills'
    """)

    logger.info("Creating fact_agents_skills table with all available fields")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_agents_skills (
            agentId BIGINT,
            skillId BIGINT,
            campaignId BIGINT,
            agentProficiencyValue BIGINT,
            agentProficiencyName STRING,
            isSkillActive BOOLEAN,
            isDialer BOOLEAN,
            isNaturalCalling BOOLEAN,
            isNaturalCallingRunning BIGINT,
            screenPopTriggerEvent STRING,
            lastUpdateTime STRING,
            lastPollTime STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/fact_agents_skills'
    """)

    logger.info("Creating fact_agent_performance table with all available fields")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_agent_performance (
            agentId STRING,
            teamId STRING,
            agentOffered STRING,
            inboundHandled STRING,
            inboundTime STRING,
            inboundTalkTime STRING,
            inboundAvgTalkTime STRING,
            outboundHandled STRING,
            outboundTime STRING,
            outboundTalkTime STRING,
            outboundAvgTalkTime STRING,
            totalHandled STRING,
            totalTalkTime STRING,
            totalAvgTalkTime STRING,
            totalAvgHandleTime STRING,
            consultTime STRING,
            availableTime STRING,
            unavailableTime STRING,
            acwTime STRING,
            refused STRING,
            percentRefused STRING,
            loginTime STRING,
            workingRate STRING,
            occupancy STRING,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_agent_performance'
    """)


    logger.info("Creating fact_agent_interaction table with all available fields")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_agent_interaction_history (
            contactId BIGINT,
            masterContactId BIGINT,
            contactStartDate STRING,
            targetAgentId BIGINT,
            fileName STRING,
            pointOfContact STRING,
            lastUpdateTime STRING,
            mediaTypeId BIGINT,
            mediaTypeName STRING,
            mediaSubTypeId BIGINT,
            mediaSubTypeName STRING,
            agentId BIGINT,
            firstName STRING,
            lastName STRING,
            teamId BIGINT,
            teamName STRING,
            campaignId BIGINT,
            campaignName STRING,
            skillId BIGINT,
            skillName STRING,
            isOutbound STRING,
            fromAddr STRING,
            toAddr STRING,
            primaryDispositionId BIGINT,
            secondaryDispositionId BIGINT,
            transferIndicatorId BIGINT,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_agent_interaction_history'
    """)


    logger.info("Creating fact_skills_summary table with all available fields")

    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.fact_skills_summary (
            skillId BIGINT,
            mediaTypeId BIGINT,
            campaignId BIGINT,
            extractIntervalStartTime TIMESTAMP,
            contactsOffered BIGINT,
            contactsHandled BIGINT,
            abandonCount BIGINT,
            averageHandleTime STRING,
            abandonRate FLOAT,
            extractDate DATE,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_skills_summary'
    """)

    logger.info("Creating fact_skills_sla_summary table with all available fields")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_skills_sla_summary (
            skillId BIGINT,
            contactsWithinSLA BIGINT,
            contactsOutOfSLA BIGINT,
            totalContacts BIGINT,
            serviceLevel DOUBLE,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        PARTITIONED BY (extractDate, extractIntervalStartTime, extractIntervalEndTime)
        LOCATION '{db_path}/{db_name}/fact_skills_sla_summary'
    """)

    logger.info("Creating dim_contacts table with all available fields")
    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {db_name}.dim_contacts (
                contactId BIGINT,
                masterContactId BIGINT,
                contactStartDate STRING,
                agentStartDate STRING,
                digitalContactStateId STRING,
                digitalContactStateName STRING,
                contactStateCategory STRING,
                endReason STRING,
                fromAddress STRING,
                toAddress STRING,
                fileName STRING,
                mediaTypeId INT,
                mediaTypeName STRING,
                mediaSubTypeId INT,
                mediaSubTypeName STRING,
                pointOfContactId BIGINT,
                pointOfContactName STRING,
                refuseReason STRING,
                refuseTime STRING,
                routingAttribute INT,
                routingTime INT,
                stateId INT,
                stateName STRING,
                targetAgentId INT,
                transferIndicatorId INT,
                transferIndicatorName STRING,
                isTakeover BOOLEAN,
                isLogged BOOLEAN,
                isWarehoused BOOLEAN,
                isAnalyticsProcessed BOOLEAN,
                analyticsProcessedDate STRING,
                dateACWWarehoused STRING,
                dateContactWarehoused STRING,
                extractDate DATE,
                recordInsertTime TIMESTAMP
            )
            USING DELTA
            LOCATION '{db_path}/{db_name}/dim_contacts'
            """
        )

    logger.info("Creating fact_contacts table with all available fields")
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

    logger.info("Creating dim_contacts_completed table with all available fields")
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

    logger.info("Creating fact_contacts_completed table with all available fields")
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
            serviceLevelFlag BIGINT,
            isOutbound BOOLEAN,
            isRefused BOOLEAN,
            isShortAbandon BOOLEAN,
            highProficiency BIGINT,
            lowProficiency BIGINT,
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

    logger.info("Creating dim_contacts_custom_data table with all available fields")
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


    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_media_playback_contact (
            contactId STRING,
            acdcontactId STRING,
            brandEmbassyConfig STRUCT<
                threadId: STRING,
                caseId: STRING,
                sessionId: STRING
            >,
            elevatedInteraction BOOLEAN,
            interactions ARRAY<STRUCT<
                mediaType: STRING,
                channelType: STRING,
                startTime: STRING,
                endTime: STRING,
                data: STRUCT<
                    startTime: STRING,
                    endTime: STRING,
                    acwEndTime: STRING,
                    fileToPlayUrl: STRING,
                    videoImageUrl: STRING,
                    waveformDataList: ARRAY<STRUCT<
                        channel: INT,
                        normalizedPcmData: ARRAY<DOUBLE>
                    >>,
                    participantDataList: ARRAY<STRUCT<
                        participantType: STRING,
                        agentName: STRING,
                        participantId: STRING,
                        segmentId: STRING,
                        userId: STRING,
                        voiceStages: ARRAY<STRUCT<
                            stageType: STRING,
                            startTime: STRING,
                            endTime: STRING,
                            recordingID: STRING,
                            displays: ARRAY<STRUCT<
                                width: INT,
                                height: INT,
                                topLeftX: INT,
                                topLeftY: INT
                            >>,
                            mergedParentId: STRING
                        >>,
                        screenStages: ARRAY<STRUCT<
                            stageType: STRING,
                            startTime: STRING,
                            endTime: STRING,
                            recordingID: STRING,
                            displays: ARRAY<STRUCT<
                                width: INT,
                                height: INT,
                                topLeftX: INT,
                                topLeftY: INT
                            >>,
                            mergedParentId: STRING
                        >>,
                        channel: INT
                    >>,
                    segmentsDataList: ARRAY<STRUCT<
                        startTime: STRING,
                        endTime: STRING,
                        acwEndTime: STRING,
                        openReasonType: STRING,
                        closeReasonType: STRING,
                        directionType: STRING,
                        source: STRING,
                        firstSegment: BOOLEAN,
                        originalSourceFromSdr: STRING,
                        segmentId: STRING,
                        channelType: STRING,
                        customerRestricted: BOOLEAN,
                        isCustomerRestricted: BOOLEAN
                    >>,
                    dfoStages: ARRAY<STRUCT<
                        stageType: STRING,
                        startTime: STRING,
                        endTime: STRING,
                        displays: STRING,
                        mergedParentId: STRING
                    >>,
                    participants: ARRAY<STRUCT<
                        participantType: STRING,
                        participantName: STRING,
                        actions: ARRAY<STRUCT<
                            timeStamp: STRING,
                            action: STRING
                        >>,
                        screenStages: ARRAY<STRUCT<
                            stageType: STRING,
                            startTime: STRING,
                            endTime: STRING,
                            displays: ARRAY<STRUCT<
                                width: INT,
                                height: INT,
                                topLeftX: INT,
                                topLeftY: INT
                            >>,
                            mergedParentId: STRING
                        >>
                    >>,
                    content: STRUCT<
                        sentTime: STRING,
                        from: STRING,
                        to: ARRAY<STRING>,
                        cc: ARRAY<STRING>,
                        bcc: ARRAY<STRING>,
                        subject: STRING,
                        body: STRING
                    >,
                    transferPoints: ARRAY<STRING>,
                    messages: ARRAY<STRUCT<
                        participantType: STRING,
                        participantName: STRING,
                        text: STRING,
                        timeStamp: STRING
                    >>,
                    categoryMatchesList: ARRAY<STRUCT<
                        categoryHierarchy: ARRAY<STRING>,
                        secondsOffsets: ARRAY<DOUBLE>,
                        confidence: INT
                    >>,
                    sentiments: ARRAY<STRUCT<
                        overallSentiment: STRING,
                        segmentStartTime: STRING,
                        channel: INT
                    >>
                >,
                `@type`: STRING,
                callTaggingList: ARRAY<STRUCT<
                    updateTime: STRING,
                    value: STRING
                >>,
                dynamicBusinessData: ARRAY<STRING>
            >>,
            extractDate DATE,
            extractIntervalStartTime TIMESTAMP,
            extractIntervalEndTime TIMESTAMP,
            recordInsertTime TIMESTAMP,
            recordIdentifier BIGINT
        )
        USING DELTA
        LOCATION '{db_path}/{db_name}/fact_media_playback_contact'
    """)

    

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
    logger.info(f"Setup started for tenant: {tenant}")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    spark = get_spark_session(app_name=app_name,
                              tenant=tenant, default_db='default')
    
    create_database(spark, db_path, db_name, logger)
    create_ingestion_stats_table(spark, db_name, db_path, logger)
    raw_tables(spark, db_name, db_path, tenant_path, logger)
    create_dim_tables(spark, db_name, db_path, logger)