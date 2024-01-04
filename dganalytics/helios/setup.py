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
            initialSessionMessageType  STRING
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
            inquiry_type               STRING,
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
                startTime       TIMESTAMP,
                endTime         TIMESTAMP
            )
            USING DELTA
            LOCATION '{db_name}/fact_transcript_actions'
        """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dgdm_simplyenergy.mv_transcript_results (
            noOfInteractions INT,
            conversationStartDateId INT,
            dateVal DATE,
            mediaTypeId INT,
            contactReason STRING,
            mainInquiry STRING,
            rootCause STRING,
            inquiry_type STRING,
            resolved STRING,
            satisfaction STRING,
            queueIds ARRAY<STRING>
            )
        USING DELTA
        PARTITIONED BY (conversationStartDateId)
        LOCATION '{db_name}/mv_transcript_results'
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
