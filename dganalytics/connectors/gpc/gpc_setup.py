import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import get_schema, get_dbname, gpc_utils_logger


def create_database(spark: SparkSession, path: str, db_name: str):
    logger.info("Creating database for genesys")
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True


def create_ingestion_stats_table(spark: SparkSession, db_name: str, db_path: str):
    logger.info("Creating Ingestion stats table for genesys")
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


def create_dim_tables(spark: SparkSession, db_name: str):
    logger.info("Setting genesys dim/fact tables")

    spark.sql(
        f"""create table if not exists {db_name}.fact_conversation_metrics
            (
                conversationId string,
                agentId string,
                mediaType string,
                messageType string,
                originatingDirection string, 
                queueId string,
                wrapUpCode string,
                wrapUpNote string,
                emitDateTime timestamp comment 'aggregated values into 15 min interval buckets',
                nAbandon int,
                nAcd int,
                nAcw int,
                nAnswered int,
                nBlindTransferred int,
                nConnected int,
                nConsult int,
                nConsultTransferred int,
                nError int,
                nHandle int,
                nHeldComplete int,
                nOffered int,
                nOutbound int,
                nOutboundAbandoned int,
                nOutboundAttempted int,
                nOutboundConnected int,
                nOverSla int,
                nShortAbandon int,
                nTalkComplete int,
                nTransferred int,
                tAbandon float,
                tAcd float,
                tAcw float,
                tAgentResponse float,
                tAnswered float,
                tContacting float,
                tDialing float,
                tHandle float,
                tHeldComplete float,
                tIvr float,
                tNotResponding float,
                tShortAbandon float,
                tTalkComplete float,
                tVoicemail float,
                tWait float,
                emitDate date,
                sourceRecordIdentifier long,
                soucePartition string
            )
            using delta
            PARTITIONED BY (emitDate)
            LOCATION '{db_path}/{db_name}/fact_conversation_metrics'
            """
    )

    spark.sql(
        f"""create table if not exists {db_name}.dim_conversations
            (
                conversationId string,
                conversationStart timestamp,
                conversationEnd timestamp,
                originatingDirection string,
                sessionStart timestamp comment 'first segment start date',
                sessionEnd timestamp comment 'last segment end date',
                queueId string,
                mediaType string,
                messageType string,
                agentId string,
                wrapUpCode string,
                wrapUpNote string,
                conversationStartDate date,
                sourceRecordIdentifier long,
                soucePartition string
            )
            using delta
            PARTITIONED BY (conversationStartDate)
            LOCATION '{db_path}/{db_name}/dim_conversations'
            """
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_users
                (
                    userName string,
                    userId string,
                    userFullName string,
                    userEmail string,
                    userTitle string,
                    department string,
                    managerId string,
                    managerFullName string,
                    state string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_users'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_wrapup_codes
                (
                    wrapupId string,
                    wrapupCode string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_wrapup_codes'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_user_groups
                (
                    userId string,
                    groupId string, 
                    groupName string, 
                    groupDescription string,
                    groupState string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_user_groups'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_routing_queues
                (
                    queueId string,
                    queueName string, 
                    wrapupPrompt string, 
                    callSLDuration float,
                    callSLPercentage float,
                    callbackSLDuration float,
                    callbackSLPercentage float,
                    chatSLDuration float,
                    chatSLPercentage float,
                    emailSLDuration float,
                    emailSLPercentage float,
                    messageSLDuration float,
                    messageSLPercentage float,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_routing_queues'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_routing_status
                (
                    userId string,
                    startTime timestamp, 
                    endTime timestamp, 
                    routingStatus string,
                    startDate date,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (startDate)
            LOCATION '{db_path}/{db_name}/fact_routing_status'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_primary_presence
                (
                    userId string,
                    startTime timestamp, 
                    endTime timestamp, 
                    systemPresence string,
                    startDate date,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (startDate)
            LOCATION '{db_path}/{db_name}/fact_primary_presence'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_evaluations
                (
                    evaluationId string,
                    evaluatorId string,
                    agentId string,
                    conversationId string,
                    evaluationFormId string,
                    status string,
                    assignedDate timestamp, 
                    releaseDate timestamp, 
                    changedDate timestamp, 
                    conversationDate timestamp, 
                    mediaType string, 
                    agentHasRead boolean, 
                    anyFailedKillQuestions boolean,
                    comments string,
                    evaluationFormName string,
                    evaluationFormPublished boolean,
                    neverRelease boolean,
                    resourceType string,
                    queueId string,
                    wrapUpCode string,
                    conversationDatePart date,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (conversationDatePart)
            LOCATION '{db_path}/{db_name}/dim_evaluations'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_evaluation_total_scores
                (
                    evaluationId string,
                    totalCriticalScore float,
                    totalNonCriticalScore float,
                    totalScore float,
                    conversationDatePart date,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (conversationDatePart)
            LOCATION '{db_path}/{db_name}/fact_evaluation_total_scores'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_evaluation_question_group_scores
                (
                    evaluationId string,
                    questionGroupId string,
                    markedNA boolean,
                    maxTotalCriticalScore float,
                    maxTotalCriticalScoreUnweighted float,
                    maxTotalNonCriticalScore float,
                    maxTotalNonCriticalScoreUnweighted float,
                    maxTotalScore float,
                    maxTotalScoreUnweighted float,
                    totalCriticalScore float,
                    totalCriticalScoreUnweighted float,
                    totalNonCriticalScore float,
                    totalNonCriticalScoreUnweighted float,
                    totalScore float,
                    totalScoreUnweighted float,
                    conversationDatePart date,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (conversationDatePart)
            LOCATION '{db_path}/{db_name}/fact_evaluation_question_group_scores'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_evaluation_question_scores
                (
                    evaluationId string,
                    questionGroupId string,
                    questionId string,
                    answerId string,
                    comments string,
                    failedKillQuestion boolean,
                    markedNA boolean,
                    score float,
                    conversationDatePart date,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (conversationDatePart)
            LOCATION '{db_path}/{db_name}/fact_evaluation_question_scores'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_wfm_day_metrics
                (
                    userId string,
                    managementUnitId string,
                    startDate timestamp,
                    startDatePart date,
                    endDate timestamp,
                    impact string,
                    actualLengthSecs int,
                    adherenceScheduleSecs int,
                    conformanceActualSecs int,
                    conformanceScheduleSecs int,
                    dayStartOffsetSecs int,
                    exceptionCount int,
                    exceptionDurationSecs int,
                    impactSeconds int,
                    scheduleLengthSecs int,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (startDatePart)
            LOCATION '{db_path}/{db_name}/fact_wfm_day_metrics'"""
    )
    spark.sql(
        f"""
                create table if not exists {db_name}.fact_wfm_actuals
                (
                    userId string,
                    managementUnitId string,
                    startDate timestamp,
                    startDatePart date,
                    endDate timestamp,
                    actualActivityCategory string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (startDatePart)
            LOCATION '{db_path}/{db_name}/fact_wfm_actuals'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.fact_wfm_exceptions
                (
                    userId string,
                    managementUnitId string,
                    startDate timestamp,
                    startDatePart date,
                    endDate timestamp,
                    actualActivityCategory string,
                    impact string,
                    routingStatus string,
                    scheduledActivityCategory string,
                    scheduledActivityCodeId string,
                    systemPresence string,
                    secondaryPresenceId string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
                    PARTITIONED BY (startDatePart)
            LOCATION '{db_path}/{db_name}/fact_wfm_exceptions'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_evaluation_forms
                (
                    evaluationFormId string,
                    evaluationFormName string,
                    published boolean,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_evaluation_forms'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_evaluation_form_question_groups
                (
                    evaluationFormId string,
                    questionGroupId string,
                    questionGroupName string,
                    defaultAnswersToHighest boolean,
                    defaultAnswersToNA boolean,
                    manualWeight boolean,
                    naEnabled boolean,
                    weight float,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_evaluation_form_question_groups'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_evaluation_form_questions
                (
                    evaluationFormId string,
                    questionGroupId string,
                    questionId string,
                    questionText string,
                    commentsRequired boolean,
                    helpText string,
                    isCritical boolean,
                    isKill boolean,
                    naEnabled boolean,
                    questionType string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_evaluation_form_questions'"""
    )

    spark.sql(
        f"""
                create table if not exists {db_name}.dim_evaluation_form_answer_options
                (
                    evaluationFormId string,
                    questionGroupId string,
                    questionId string,
                    answerOptionId string,
                    answerOptionText string,
                    answerOptionValue string,
                    sourceRecordIdentifier long,
                    soucePartition string
                )
                    using delta
            LOCATION '{db_path}/{db_name}/dim_evaluation_form_answer_options'"""
    )

    return True


def create_raw_table(api_name: str, spark: SparkSession, db_name: str):
    schema = get_schema(api_name)
    table_name = "raw_" + f"{api_name}"
    logger.info(f"creating genesys raw table - {table_name}")
    spark.createDataFrame(spark.sparkContext.emptyRDD(),
                          schema=schema).registerTempTable(table_name)
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

    spark.sql(
        f"""create table if not exists {db_name}.fact_wfm_forecast
            (
                FORECAST_ID string,
                PLANNING_GROUP_ID string,
                IntervalStart timestamp,
                WEEKDATE string,
                PLANNING_GROUP string,
                DATA_REF_DT timestamp,
                BU_NAME string,
                averageHandleTimeSeconds float,
                offered float,
                META_REF_DT timestamp
            )
            using delta
            PARTITIONED BY (WEEKDATE)
            LOCATION '{db_path}/{db_name}/fact_wfm_forecast'
            """
    )
    return True


def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str):
    logger.info("Setting genesys raw tables")
    create_raw_table("users", spark, db_name)
    create_raw_table("routing_queues", spark, db_name)
    create_raw_table("groups", spark, db_name)
    create_raw_table("users_details", spark, db_name)
    create_raw_table("conversation_details", spark, db_name)
    create_raw_table("wfm_adherence", spark, db_name)
    create_raw_table("wrapup_codes", spark, db_name)
    create_raw_table("evaluations", spark, db_name)
    create_raw_table("evaluation_forms", spark, db_name)
    create_raw_table("divisions", spark, db_name)
    create_raw_table("business_units", spark, db_name)
    create_raw_table("management_units", spark, db_name)
    create_raw_table("management_unit_users", spark, db_name)
    create_raw_table("activity_codes", spark, db_name)
    create_raw_table("presence_definitions", spark, db_name)
    create_raw_table("wfm_forecast_data", spark, db_name)
    create_raw_table("wfm_forecast_meta", spark, db_name)
    create_raw_table("wfm_planninggroups", spark, db_name)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = gpc_utils_logger(tenant, "gpc_setup")

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="GPC_Setup",
                              tenant=tenant, default_db='default')
    create_database(spark, db_path, db_name)
    create_ingestion_stats_table(spark, db_name, db_path)
    raw_tables(spark, db_name, db_path, tenant_path)
    create_dim_tables(spark, db_name)
