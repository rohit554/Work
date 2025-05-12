from pyspark.sql import SparkSession


def fact_conversation_surveys(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    surveys = spark.sql(f"""
        SELECT 
            conversationId,
            conversationStart,
            conversationEnd,
            originatingDirection,
            survey.userId AS agentId,
            survey.queueId AS queueId,
            survey.surveyId AS surveyId,
            survey.eventTime AS eventTime,
            survey.surveyStatus AS surveyStatus,
            survey.surveyCompletedDate AS surveyCompletedDate,
            survey.surveyFormContextId AS surveyFormContextId,
            survey.surveyFormId AS surveyFormId,
            survey.surveyFormName AS surveyFormName,
            survey.surveyPromoterScore AS surveyPromoterScore,
            survey.oSurveyTotalScore AS oSurveyTotalScore,
            TRY_CAST(survey.eventTime AS date) AS eventDate,
            recordIdentifier as sourceRecordIdentifier,
            concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as sourcePartition
        FROM (
            SELECT
                conversationId,
                conversationStart,
                conversationEnd,
                originatingDirection,
                explode(surveys) AS survey,
                recordIdentifier,
                extractDate,
                extractIntervalStartTime,
                extractIntervalEndTime
            FROM
                raw_conversation_details
            WHERE
                extractDate = '{extract_date}'
                AND extractIntervalStartTime = '{extract_start_time}'
                AND extractIntervalEndTime = '{extract_end_time}'
				AND surveys IS NOT NULL
        )
        WHERE 
            survey IS NOT NULL
    """)

    surveys.createOrReplaceTempView("surveys")
    spark.sql("delete from fact_conversation_surveys where conversationId in (select distinct conversationId from surveys)")
    spark.sql("insert into fact_conversation_surveys select * from surveys")
