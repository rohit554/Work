import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars


def create_database(spark: SparkSession, path: str, db_name: str):
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

def create_model_tables(spark: SparkSession, path: str, db_name: str):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_users (
            userId        STRING,
            userName      STRING,
            userFullName  STRING,
            userEmail     STRING,
            userTitle     STRING,
            department    STRING,
            managerId     STRING,
            hireDate      DATE,
            state         STRING
        )
        USING DELTA
        LOCATION '{db_name}/dim_users'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_queues (
            queueId               STRING,
            queueName             STRING,
            callSLDuration        FLOAT,
            callSLPercentage      FLOAT,
            callbackSLDuration    FLOAT,
            callbackSLPercentage  FLOAT,
            chatSLDuration        FLOAT,
            chatSLPercentage      FLOAT,
            emailSLDuration       FLOAT,
            emailSLPercentage     FLOAT,
            messageSLDuration     FLOAT,
            messageSLPercentage   FLOAT
        )
        USING DELTA
        LOCATION '{db_name}/dim_queues'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_wrap_up_codes (
            wrapUpId    STRING,
            wrapUpCode  STRING
        )
        USING DELTA
        LOCATION '{db_name}/dim_wrapUpCodes'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_media_types (
            mediaTypeId INT,
            mediaType   STRING
        )
        USING DELTA
        LOCATION '{db_name}/dim_media_types'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_interaction_originating_direction (
            originatingDirectionId INT,
            originatingDirection   STRING
        )
        USING DELTA
        LOCATION '{db_name}/dim_interaction_originating_direction'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_divisions (
            divisionId    STRING,
            name          STRING,
            homeDivision  STRING
        )
        USING DELTA
        LOCATION '{db_name}/dim_divisions'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_date (
            dateId              INT,
            dateVal             DATE,
            year                INT,
            quarter             INT,
            quarterId           STRING,
            MMYYYY              INT,
            monthNum            INT,
            monthName           STRING,
            monthNameShort      STRING,
            dayOfMonth          INT,
            dayOfWeek           INT,
            dayOfYear           INT,
            weekOfYear          INT,
            weekDayName         STRING,
            weekDayNameShort    STRING
        )
        USING DELTA
        LOCATION '{db_name}/dim_date'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.config_conversation_cost (
            unit        STRING,
            costPerSec  FLOAT
        )
        USING DELTA
        LOCATION '{db_name}/config_conversation_cost'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.config_kpi_targets (
            id          INT,
            name        STRING,
            target      FLOAT,
            unit        STRING,
            startDay    INT,
            endDay      INT
        )
        USING DELTA
        LOCATION '{db_name}/config_kpi_targets'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_conversations (
            conversationId             STRING,
            conversationStartDateId    INT,
            conversationStart          TIMESTAMP,
            conversationEnd            TIMESTAMP,
            originatingDirectionId     INT,
            divisionIds                ARRAY<STRING>,
            initialParticipantPurpose  STRING,
            initialSessionMediaTypeId  INT,
            initialSessionMessageType  STRING,
            location                   STRING
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_conversations'

    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_conversation_participants (
            conversationId          STRING,
            participantId           STRING,
            participantName         STRING,
            purpose                 STRING,
            userId                  STRING,
            conversationStartDateId    INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_conversation_participants'

    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_conversation_sessions (
            conversationId          STRING,
            participantId           STRING,
            sessionId               STRING,
            addressFrom             STRING,
            addressOther            STRING,
            addressSelf             STRING,
            addressTo               STRING,
            ani                     STRING,
            callbackNumbers         STRING,
            callbackScheduledTime   TIMESTAMP,
            callbackUserName        STRING,
            direction               STRING,
            dnis                    STRING,
            mediaTypeId             INT,
            messageType             STRING,
            peerId                  STRING,
            conversationStartDateId    INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_conversation_sessions'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_conversation_session_segments (
            conversationId          STRING,
            participantId           STRING,
            sessionId               STRING,
            segmentStart            TIMESTAMP,
            segmentEnd              TIMESTAMP,
            queueId                 STRING,
            errorCode               STRING,
            disconnectType          STRING,
            segmentType             STRING,
            wrapUpCodeId            STRING,
            wrapUpNote              STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_conversation_session_segments'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_evaluations (
            conversationId          STRING,
            evaluationId            STRING,
            evaluationFormId        STRING,
            evaluationFormName      STRING,
            status                  STRING,
            agentHasRead            BOOLEAN,
            anyFailedKillQuestions  BOOLEAN,
            comments                STRING,
            evaluationFormPublished BOOLEAN,
            neverRelease            BOOLEAN,
            resourceType            STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_evaluations'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dim_surveys (
            conversationId          STRING,
            surveyId                STRING,
            surveyStatus            STRING,
            surveyFormContextId     STRING,
            surveyFormId            STRING,
            surveyFormName          STRING,
            agentId                 STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_surveys'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_conversation_metrics (
            conversationId          STRING,
            participantId           STRING,
            sessionId               STRING,
            mediaTypeId             INT,
            originatingDirectionId  INT,
            eventTime               TIMESTAMP,
            name                    STRING,
            value                   FLOAT,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/fact_conversation_metrics'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_conversation_evaluations (
            conversationId          STRING,
            evaluationId            STRING,
            evaluatorId             STRING,
            agentId                 STRING,
            assignedDate            TIMESTAMP,
            releaseDate             TIMESTAMP,
            changedDate             TIMESTAMP,
            totalCriticalScore      FLOAT,
            totalNonCriticalScore   FLOAT,
            totalScore              FLOAT,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/fact_conversation_evaluations'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_conversation_surveys (
            conversationId          STRING,
            surveyId                STRING,
            queueId                 STRING,
            userId                  STRING,
            surveyFormId            STRING,
            SurveyCompletionDate    TIMESTAMP,
            promoterScore           FLOAT,
            maxPromoterScore        FLOAT,
            satisfactionScore       FLOAT,
            maxSatisfactionScore    FLOAT,
            isResolved              BOOLEAN,
            isFirstTimeResolution   BOOLEAN,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/fact_conversation_surveys'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_conversation_transcript (
            conversationId          STRING,
            communicationId         STRING,
            mediaTypeId             INT,
            originatingDirectionId  INT,
            phraseText              STRING,
            phraseStartTime         TIMESTAMP,
            phraseEndTime           TIMESTAMP,
            participantPurpose      STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/fact_conversation_transcript'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_transcript_insights (
            conversationId          STRING,
            satisfaction            STRING,
            resolved                STRING,
            process_knowledge       STRING,
            system_knowledge        STRING,
            additional_service        STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/fact_transcript_insights'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.fact_transcript_contact_reasons (
            conversationId          STRING,
            contactReason           STRING,
            mainInquiry             STRING,
            rootCause               STRING,
            inquiry_type            STRING,
            additional_inquiry      STRING,
            main_inquiry_raw        STRING,
            root_cause_raw          STRING,
            contact_reason_raw      STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/fact_transcript_contact_reasons'
    """)

    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.fact_transcript_actions (
                conversationId  STRING,
                category        STRING,
                action          STRING,
                action_label    STRING,
                contact_reason  STRING,
                main_inquiry    STRING,
                root_cause      STRING,
                startTime       TIMESTAMP,
                endTime         TIMESTAMP,
                speaker         STRING,
                start_line      STRING,
                end_line        STRING,
                conversationStartDateId INT                
            )
            USING DELTA
            PARTITIONED BY (conversationStartDateId)
            LOCATION '{db_name}/fact_transcript_actions'
        """)

    spark.sql(f"""
        CREATE TABLE dgdm_{tenant}.dim_conversation_location
        (
            id STRING,
            name STRING
        )
    """)
    
    spark.sql(f"""
       CREATE TABLE IF NOT EXISTS dgdm_{tenant}.dim_conversation_ivr_events
        (
            conversationId STRING,
            eventName STRING,
            eventValue STRING,
            eventTime TIMESTAMP,
            participantName STRING,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)      
        LOCATION '{db_name}/dim_conversation_ivr_events' 
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dgdm_{tenant}.dim_conversation_ivr_menu_selections
        (
            conversationId STRING,
            index INT,
            menuId STRING,
            previousMenuId STRING,
            nextSelectedMenuId STRING,
            menuEntryTime TIMESTAMP,
            menuExitTime TIMESTAMP,
            menuExitReason STRING,
            conversationStart TIMESTAMP,
            conversationStartDateId INT
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/dim_conversation_ivr_menu_selections' 
    """)
    
    spark.sql(f"""
       CREATE TABLE IF NOT EXISTS dgdm_{tenant}.dim_conversation_session_flow
        (
            conversationId	STRING,
            participantId	STRING,
            sessionId	STRING,	
            flowId	STRING,
            endingLanguage	STRING,
            entryReason	STRING,
            entryType	STRING,
            exitReason	STRING,
            flowName	STRING,
            flowType	STRING,
            flowVersion	STRING,
            flowOutcome	STRING,
            flowOutcomeStartTimestamp	TIMESTAMP,
            flowOutcomeEndTimestamp	TIMESTAMP,
            flowOutcomeId	STRING,
            flowOutcomeValue	STRING,
            issuedCallback	STRING,
            startingLanguage	STRING,
            transferTargetAddress	STRING,
            transferTargetName	STRING,
            transferType	STRING,
            conversationStartDateId	INT
			
        )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)       
        LOCATION '{db_name}/dim_conversation_session_flow' 
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dgdm_{tenant}.mv_cost_calculation (
        conversationId STRING,
        conversationStartDateId INT,
        mediaTypeId INT,
        region STRING,
        originatingDirectionId	INT,
        contactReason STRING,
        mainInquiry STRING,
        rootCause STRING,
        inquiry_type STRING,
        resolved STRING,
        satisfaction STRING,
        queueIds ARRAY <STRING>,
        wrapUpCodeIds ARRAY <STRING>,
        tHandle BIGINT        
        ) USING DELTA 
        PARTITIONED BY (conversationStartDateId) 
        LOCATION '{db_name}/mv_cost_calculation'
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dgdm_{tenant}.mv_transcript_results (
        noOfInteractions INT,
        conversationStartDateId INT,
        dateVal DATE,
        mediaTypeId INT,
        region STRING,
        originatingDirectionId	INT,
        contactReason STRING,
        mainInquiry STRING,
        rootCause STRING,
        inquiry_type STRING,
        resolved STRING,
        satisfaction STRING,
        queueIds ARRAY <STRING>,
        wrapUpCodeIds ARRAY <STRING>
        ) USING DELTA 
        PARTITIONED BY (conversationStartDateId) 
        LOCATION '{db_name}/mv_transcript_results'       
    """)
    
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dgdm_{tenant}.mv_conversation_metrics (
            conversationId STRING,
            mediaTypeId INT,
            conversationStartDateId INT,
            userId STRING,
            nBlindTransferred INT,
            nConsultTransferred INT,
            nConsult INT,
            nTransferred INT,
            resolved INT,
            notResolved INT,
            tHandle BIGINT,
            tHandleResolved BIGINT,
            tHandleNotResolved BIGINT,
            tTalkComplete BIGINT,
            tHeldComplete BIGINT,
            tAcwComplete BIGINT,
            satisfaction INT
            )
            USING DELTA
            PARTITIONED BY (conversationStartDateId)
            LOCATION '{db_name}/mv_conversation_metrics'
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dgdm_{tenant}.mv_classification (
        contactReason STRING,
        mainInquiry STRING,
        mediaTypeId INT,
        tHandle BIGINT,
        additionalService INT,
        resolved INT,
        totalInteractions INT,
        businessValue INT,
        customerValue INT,
        category STRING
        ) 
        USING DELTA 
        PARTITIONED BY (contactReason, mainInquiry ) 
        LOCATION '{db_name}/mv_classification'       
    """)
    
    spark.sql(f"""
           CREATE TABLE IF NOT EXISTS dgdm_{tenant}.dim_user_teams
            (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            userId STRING,
            userFullName STRING,
            groupId STRING,
            groupName STRING,
            teamName STRING,
            role STRING,
            region STRING,
            site STRING,
            timezone STRING,
            provider STRING,
            startDate DATE,
            endDate DATE,
            isActive BOOLEAN
            )
            USING DELTA
            LOCATION  '{db_name}/dim_user_teams'   
        """)
    
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dgdm_{tenant}.transcript_insights_audit (
                        status   BOOLEAN,
                        conversationId STRING,
                        error_or_status_code STRING,
                        transcriptSize INT,
                        transcriptProcessingStartTime TIMESTAMP, 
                        transcriptProcessingEndTime TIMESTAMP, 
                        conversationStartDateId INT,
                        error STRING,
                        url STRING,
                        recordInsertTime TIMESTAMP
                        )
                        USING DELTA
                        PARTITIONED BY (conversationStartDateId)
                        LOCATION '{db_name}/transcript_insights_audit'
        """)
    
    spark.sql(f"""
           CREATE TABLE IF NOT EXISTS dgdm_{tenant}.helios_process_map (
                conversationId	STRING,
                category	STRING,
                action	STRING,
                action_label	STRING,
                eventStart	TIMESTAMP,
                eventEnd	TIMESTAMP,
                contact_reason	STRING,
                main_inquiry	STRING,
                root_cause	STRING,
                location STRING,
                originatingDirectionId	INT,
                mediatypeId	INT,
                AuthenticationStatus	STRING,
                resolved	STRING,
                hashold	BOOLEAN,
                hasconsult	BOOLEAN,
                hasconsulttransfer	BOOLEAN,
                hasblindtransfer	BOOLEAN,
                userNames	STRING,
                teamNames	STRING,
                speaker	STRING,
                finalQueueName	STRING,
                finalWrapupCode	STRING,
                conversationStartDateId INT
            )
            USING DELTA
            PARTITIONED BY (conversationStartDateId)
            LOCATION '{db_name}/helios_process_map'                 
        """)
    
    spark.sql(f"""
           CREATE TABLE IF NOT EXISTS dgdm_{tenant}.dim_ivr_menus
            (
                menuId STRING,
                menuName STRING
            )
            USING DELTA
            LOCATION '{db_name}/dim_ivr_menus'
        """)
    spark.sql(f"""
           CREATE TABLE IF NOT EXISTS dgdm_{tenant}.dim_resolution
            (
                id INT,
                name STRING
            )
            USING DELTA
            PARTITIONED BY (id)
            LOCATION '{db_name}/dim_resolution'
        """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant

    db_name = f"dgdm_{tenant}"
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="dgdm_Setup",
                              tenant=tenant, default_db='default')
    create_database(spark, db_path, db_name)
    create_model_tables(spark, db_path, db_name)
