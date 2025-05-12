from dganalytics.utils.utils import get_spark_session, get_path_vars, push_gamification_data
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger, get_path_vars
import os
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import concurrent.futures

def get_config_file_data(tenant_path, file_name):
    data = pd.read_json(os.path.join(tenant_path, 'data', 'config', file_name))
    data = pd.DataFrame(data['values'].tolist())
    header = data.iloc[0]
    data = data[1:]
    data.columns = header
    return data

def get_config_csv_file_data(tenant_path, file_name):
    path = os.path.join(tenant_path, "data", "config", file_name)
    data = pd.read_csv(path)
    return data

def push_outbound_sales(spark): 
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    outbound_wrapups = get_config_csv_file_data(tenant_path, 'Outbound_Wrapup_Codes.csv')
    outbound_wrapups = spark.createDataFrame(outbound_wrapups)
    outbound_wrapups.createOrReplaceTempView("outbound_wrap_codes")

    user_df = get_config_csv_file_data(tenant_path, 'User_Group_region_Sites.csv')
    user_df = spark.createDataFrame(user_df)
    user_df.createOrReplaceTempView("user_timezones")

    df=spark.sql(f"""        
        with CTE as
        (
            SELECT a.conversationId,
            from_utc_timestamp(a.conversationStart, trim(ut.timeZone)) as conversationStart,
            from_utc_timestamp(a.conversationEnd, trim(ut.timeZone)) as conversationEnd,
            a.userId,
            a.wrapUpCode
            from
            (
                SELECT
                conversationId,
                conversationStart,
                conversationEnd,
                userId,
                element_at(session.segments, size(session.segments)).wrapUpCode AS wrapUpCode
                FROM(
                    SELECT
                    conversationId,
                    conversationStart,
                    conversationEnd,
                    participant.userId,
                    element_at(participant.sessions, size(participant.sessions)) AS session
                    FROM
                    (
                        SELECT
                            conversationId,
                            conversationStart,
                            element_at(participants, size(participants)) AS participant,
                            conversationEnd
                            FROM
                            (
                                select
                                    conversationId,
                                    conversationStart,
                                    conversationEnd,
                                    participants,
                                    row_number() over (
                                    partition by conversationId
                                    order by
                                        recordInsertTime DESC
                                    ) rn
                                from
                                    gpc_hellofresh.raw_conversation_details
                                where
                                    extractDate >= DATE_SUB(CURRENT_TIMESTAMP(), 3)
                                    and originatingDirection = 'outbound'
                            )
                            where rn = 1
                    )
                )
            ) a
            join user_timezones ut
            on a.userId = ut.userId
        )   
        select UserID, 
        date_format(date, 'dd-MM-yyyy') Date,
        salesCount as Sales,
        case when decisionMakerContactCount is not null or decisionMakerContactCount > 0 then round((salesCount/decisionMakerContactCount)*100) else null end as `Percent Customer Connect`
        
        from
        (
            select date,
            userId,
            SUM(CASE WHEN decisionMakerContact THEN 1 ELSE 0 END) AS decisionMakerContactCount,
            SUM(CASE WHEN sales THEN 1 ELSE 0 END) AS salesCount
            from(

                select 
                conversationId,cast(conversationStart as date) date,cw.wrapUpCode,ow.wrapupCode,cw.userId,
                    decisionMakerContact,
                    sales
                from CTE cw
                join outbound_wrap_codes ow
                on cw.wrapUpCode = ow.wrapupId                
            )
            group by userId,date
        )
    """)
    print("Uploading outbound sales data")
    push_gamification_data(df.toPandas(), 'HELLOFRESHANZ', 'HF_OutboundSales_Connection')
    return True

def push_conversation(spark):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    timezones = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping.json")
    timezones = spark.createDataFrame(timezones)
    timezones.createOrReplaceTempView("queue_timezones")    
    df_conversation = spark.sql(f"""
                        SELECT
                        UserId,
                        date_format(from_utc_timestamp(intervalStart, trim(coalesce(ut.timeZone, 'UTC'))), 'dd-MM-yyyy') as Date,
                        SUM(CASE WHEN mediaType = 'voice' AND tAcw <> 0 THEN tAcw ELSE NULL END) AS tAcw_voice,
                        SUM(CASE WHEN mediaType = 'voice' THEN NULLIF(nAcw, 0) ELSE "" END) AS nAcw_voice,
                        SUM(CASE WHEN mediaType = 'voice' AND tHeldComplete <> 0 THEN tHeldComplete ELSE NULL END) AS tHeldComplete_voice,
                        SUM(CASE WHEN mediaType = 'voice' THEN NULLIF(nHandle, 0) ELSE "" END) AS nHandle_voice,
                        SUM(CASE WHEN mediaType = 'voice' AND tTalkComplete <> 0 THEN tTalkComplete ELSE NULL END) AS tTalkComplete_voice,
                        SUM(CASE WHEN mediaType = 'voice' THEN NULLIF(nTalkComplete, 0) ELSE "" END) AS nTalkComplete_voice,
                        SUM(CASE WHEN (messageType IN ('webmessaging', 'open') OR mediaType = 'chat') THEN NULLIF(tAcw, 0) ELSE "" END) as tACW_chat,
                        SUM(CASE WHEN (messageType IN ('webmessaging', 'open') OR mediaType = 'chat') THEN NULLIF(nAcw, 0) ELSE "" END) AS nAcw_chat,
                        SUM(CASE WHEN (messageType IN ('webmessaging', 'open') OR mediaType = 'chat') THEN NULLIF(tTalkComplete, 0) ELSE "" END) AS tTalkComplete_chat,
                        SUM(CASE WHEN (messageType IN ('webmessaging', 'open') OR mediaType = 'chat') THEN NULLIF(nTalkComplete, 0) ELSE "" END) AS nTalkComplete_chat,
                        AVG(tNotResponding / 1000) AS NRT
                    FROM
                        gpc_hellofresh.fact_conversation_aggregate_metrics a
                      JOIN gpc_hellofresh.dim_routing_queues b
                      ON a.queueId = b.queueId
                      JOIN queue_timezones ut
                      ON b.queueName = ut.queueName
                      WHERE date >= DATE_SUB(CURRENT_Date(), 3)
                     GROUP BY
                         UserId,
                         date_format(from_utc_timestamp(intervalStart, trim(coalesce(ut.timeZone, 'UTC'))), 'dd-MM-yyyy')
                     """)
    print("Uploading conversation data")
    push_gamification_data(df_conversation.toPandas(), 'HELLOFRESHANZ', 'HF_Conversation_Connection')
    return True

def push_survey(spark):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    timezones = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping.json")
    timezones = spark.createDataFrame(timezones)
    timezones.createOrReplaceTempView("queue_timezones") 
    df_survey = spark.sql(f"""
                          SELECT 
                          a.userKey UserId,
                          date_format(from_utc_timestamp(surveyCompletionDate, trim(coalesce(ut.timeZone, 'UTC'))), 'dd-MM-yyyy') as `Date`,
                          SUM(CASE 
                              WHEN a.csatAchieved != '-1' OR a.mediaType = 'chat' OR b.messageType IN ('webmessaging', 'open') 
                              THEN a.csatAchieved 
                              ELSE 0
                          END) AS `agent_csat_sum_chat`,
                          COUNT(CASE 
                              WHEN a.csatAchieved != '-1' OR a.mediaType = 'chat' OR b.messageType IN ('webmessaging', 'open') 
                              THEN 1
                          END) AS `agent_csat_count_chat`,
                          SUM(CASE 
                              WHEN a.csatAchieved != '-1' OR a.mediaType = 'voice' 
                              THEN a.csatAchieved 
                              ELSE 0
                          END) AS `agent_csat_sum_voice`,
                          COUNT(CASE 
                              WHEN a.csatAchieved != '-1' OR a.mediaType = 'voice' 
                              THEN 1
                          END) AS `agent_csat_count_voice`,
                          SUM(CASE WHEN a.createdAt IS NOT NULL OR b.mediaType = 'voice' THEN 1 ELSE 0 END) AS `count_scheduled_at_voice`,
                          SUM(CASE WHEN b.mediaType = 'voice' THEN NULLIF(b.nAcw, 0) ELSE 0 END) AS `sum_tHandleCount_voice`,
                          COUNT(CASE WHEN a.createdAt IS NOT NULL OR a.mediaType = 'chat' OR b.messageType IN ('webmessaging', 'open') THEN 1 END) AS `count_scheduled_at_chat`,
                          SUM(CASE WHEN b.mediaType = 'chat' OR b.messageType IN ('webmessaging', 'open') THEN NULLIF(b.nAcw, 0) ELSE 0 END) AS `sum_tHandleCount_chat`,
                          AVG(case when status = 'Completed' THEN a.fcr ELSE NULL END) nfcr
                      FROM sdx_hellofresh.dim_hellofresh_interactions a
                      JOIN gpc_hellofresh.fact_conversation_aggregate_metrics b
                          ON a.surveySentDatePart = b.date and a.userKey = b.userId
                      JOIN gpc_hellofresh.dim_routing_queues c
                          ON b.queueId = c.queueId
                      JOIN queue_timezones ut
                          ON c.queueName = ut.queueName
                      WHERE 
                          a.surveyCompletionDate >= DATEADD(DAY, -16, GETDATE())
                          
                      GROUP BY 
                          a.userKey,
                          date_format(from_utc_timestamp(surveyCompletionDate, trim(coalesce(ut.timeZone, 'UTC'))), 'dd-MM-yyyy')
                          """)
    print("Uploading Surveys data")
    push_gamification_data(df_survey.toPandas(), 'HELLOFRESHANZ', 'HF_Survey_Connection')
    return True

def push_quality(spark):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    timezones = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping.json")
    timezones = spark.createDataFrame(timezones)
    timezones.createOrReplaceTempView("queue_timezones")
    df_quality=spark.sql(f"""        
                        SELECT DISTINCT
                            c.agentId as UserId,
                            date_format(from_utc_timestamp(c.conversationDate, trim(coalesce(f.timeZone, 'UTC'))), 'dd-MM-yyyy') as `Date`,
                            SUM(CASE WHEN a.markedNA = false AND b.failedKillQuestion = false AND d.mediaType = 'voice' THEN a.totalScore ELSE 0 END) * 100 AS pRefined_Score_voice,
                            SUM(CASE WHEN (d.messageType IN ('webmessaging', 'open') OR d.mediaType = 'CALL') THEN NULLIF(a.maxTotalScore, 0) ELSE 0 END) as zMaxAnswerValue_voice,
                            SUM(CASE WHEN (c.mediaType = 'MESSAGE') THEN a.maxTotalScore ELSE 0 END) as zMaxAnswerValue_chat,
                            SUM(CASE WHEN  a.markedNA = false AND b.failedKillQuestion = false AND d.messageType IN ('webmessaging', 'open') OR c.mediaType = 'MESSAGE' THEN a.totalScore ELSE 0 END) * 100 AS pRefined_Score_chat
                        FROM 
                            gpc_hellofresh.fact_evaluation_question_group_scores a
                        JOIN gpc_hellofresh.fact_evaluation_question_scores b 
                        ON a.conversationDatePart = b.conversationDatePart and a.evaluationId = b.evaluationId AND a.questionGroupId = b.questionGroupId
                        JOIN gpc_hellofresh.dim_evaluations c 
                        ON a.conversationDatePart = c.conversationDatePart and a.evaluationId = c.evaluationId
                        JOIN (SELECT
                              DISTINCT 
                              queueId, 
                              userId, 
                              mediaType, 
                              messageType 
                        FROM gpc_hellofresh.fact_conversation_aggregate_metrics
                        where date >= DATE_SUB(current_date(), 3)) d 
                        ON c.agentid = d.userId AND c.queueId = d.queueId
                        JOIN gpc_hellofresh.dim_routing_queues e 
                        ON c.queueId = e.queueId
                        JOIN queue_timezones f 
                        ON e.queueName = f.queueName
                        WHERE c.conversationDate >= DATE_SUB(current_date(), 3)
                        GROUP BY
                          c.agentId,
                          date_format(from_utc_timestamp(c.conversationDate, trim(coalesce(f.timeZone, 'UTC'))), 'dd-MM-yyyy')
                        """)
    print("Uploading Quality data")
    push_gamification_data(df_quality.toPandas(), 'HELLOFRESHANZ', 'HF_Quality_Connection')
    return True

def push_adherence(spark):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    timezones = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping.json")
    timezones = spark.createDataFrame(timezones)
    timezones.createOrReplaceTempView("queue_timezones")
    df_adherence = spark.sql(f"""
                             SELECT
                              D.userId as `UserID`,
                              date_format(from_utc_timestamp(startDate, trim(coalesce(ut.timeZone, 'UTC'))), 'dd-MM-yyyy') as Date,
                              SUM(D.adherenceScheduleSecs) AS adherenceScheduleSecs,
                              SUM(D.exceptionDurationSecs) AS exceptionDurationSecs
                          FROM
                              gpc_hellofresh.fact_wfm_day_metrics D
                          JOIN
                              gpc_hellofresh.fact_conversation_aggregate_metrics a
                              ON d.startDatePart = a.date and a.userId = D.userId
                          JOIN
                              gpc_hellofresh.dim_routing_queues b
                              ON a.queueId = b.queueId
                          JOIN queue_timezones ut
                              ON b.queueName = ut.queueName
                          WHERE
                              D.startDate >= DATE_SUB(CURRENT_DATE(), 63)
                          GROUP BY
                              D.userId,
                              date_format(from_utc_timestamp(startDate, trim(coalesce(ut.timeZone, 'UTC'))), 'dd-MM-yyyy')
                              """)
    print("Uploading Adherence data")
    push_gamification_data(df_adherence.toPandas(), 'HELLOFRESHANZ', 'HF_Adherence_Connection')
    return True

def push_evaluation_form(spark):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    timezones = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping.json")
    timezones = spark.createDataFrame(timezones)
    timezones.createOrReplaceTempView("queue_timezones")

    df_evaluation = spark.sql("""
                                  SELECT DISTINCT
                                      DE.agentId AS UserID,
                                      DATE_FORMAT(FROM_UTC_TIMESTAMP(TS.conversationDatePart, TRIM(COALESCE(ut.timeZone, 'UTC'))), 'dd-MM-yyyy') AS Date,
                                      AVG(CASE WHEN TRIM(DE.evaluationFormName) = 'ANZ Outbound Compliance Scorecard.V2' THEN TS.totalScore ELSE NULL END) AS QA_Compliance,
                                      AVG(CASE WHEN TRIM(DE.evaluationFormName) = 'ANZ Outbound Performance Scorecard' THEN TS.totalScore ELSE NULL END) AS QA_Performance
                                  FROM 
                                      gpc_hellofresh.fact_evaluation_total_scores TS
                                  JOIN 
                                      gpc_hellofresh.dim_evaluations DE 
                                      ON DE.conversationDatePart = TS.conversationDatePart
                                      AND TS.evaluationId = DE.evaluationId 
                                      
                                  JOIN 
                                      gpc_hellofresh.dim_routing_queues e ON DE.queueId = e.queueId
                                  JOIN 
                                     queue_timezones ut ON e.queueName = ut.queueName
                                  WHERE 
                                      TS.totalScore IS NOT NULL
                                      AND TRIM(DE.evaluationFormName) IN ('ANZ Outbound Compliance Scorecard.V2', 'ANZ Outbound Performance Scorecard')
                                      AND TS.conversationDatePart >= DATE_SUB(CURRENT_DATE(), 33)
                                  GROUP BY 
                                      DE.agentId, 
                                      DATE_FORMAT(FROM_UTC_TIMESTAMP(TS.conversationDatePart, TRIM(COALESCE(ut.timeZone, 'UTC'))), 'dd-MM-yyyy')
                                """)
    print("Uploading Evaluation data")
    push_gamification_data(df_evaluation.toPandas(), 'HELLOFRESHANZ', 'HF_OutboundEvaluation_Connection')
    return True

def push_helios_data(spark):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    timezones = get_config_file_data(tenant_path, "Queue_TimeZone_Mapping.json")
    timezones = spark.createDataFrame(timezones)
    timezones.createOrReplaceTempView("queue_timezones")

    df_helios_data = spark.sql("""
								WITH CTE1 AS (
									SELECT DISTINCT
										c.agentId AS userId,
										DATE_FORMAT(
											FROM_UTC_TIMESTAMP(c.conversationDate, TRIM(COALESCE(f.timeZone, 'UTC'))), 
											'yyyy-MM-dd'
										) AS `date`,
										SUM(totalScore) * 100 / SUM(maxTotalScore) AS `Quality Score`
									FROM 
										gpc_hellofresh.fact_evaluation_question_group_scores a
									JOIN 
										gpc_hellofresh.fact_evaluation_question_scores b 
										ON a.conversationDatePart = b.conversationDatePart 
										AND a.evaluationId = b.evaluationId 
										AND a.questionGroupId = b.questionGroupId
									JOIN 
										gpc_hellofresh.dim_evaluations c 
										ON a.conversationDatePart = c.conversationDatePart 
										AND a.evaluationId = c.evaluationId
									JOIN (
										SELECT DISTINCT 
											queueId, 
											userId, 
											mediaType, 
											messageType 
										FROM 
											gpc_hellofresh.fact_conversation_aggregate_metrics
										WHERE 
											date >= DATE_SUB(CURRENT_DATE(), 10)
									) d 
										ON c.agentId = d.userId AND c.queueId = d.queueId
									JOIN 
										gpc_hellofresh.dim_routing_queues e 
										ON c.queueId = e.queueId
									JOIN 
										queue_timezones f 
										ON e.queueName = f.queueName
									WHERE 
										c.conversationDate >= DATE_SUB(CURRENT_DATE(), 10)
									GROUP BY
										c.agentId,
										DATE_FORMAT(
											FROM_UTC_TIMESTAMP(c.conversationDate, TRIM(COALESCE(f.timeZone, 'UTC'))), 
											'yyyy-MM-dd'
										)
								),

								CTE2 AS (
									SELECT
										userId,
										DATE_FORMAT(
											FROM_UTC_TIMESTAMP(conversationStart, TRIM(COALESCE(ut.timeZone, 'UTC'))), 
											'yyyy-MM-dd'
										) AS date,
										TRY_DIVIDE(
											SUM(CASE
												WHEN b.wrapUpCode IN (
													'Z-OUT-Success-1Week',
													'Z-OUT-Success-2Week',
													'Z-OUT-Success-3Week',
													'Z-OUT-Success-4Week',
													'Z-OUT-Success-5+Week-TL APPROVED'
												) THEN 1 ELSE 0 END) * 100,
											SUM(CASE 
												WHEN b.wrapUpCode IN (
													'Z-OUT-Success-1Week',
													'Z-OUT-Success-2Week',
													'Z-OUT-Success-3Week',
													'Z-OUT-Success-4Week',
													'Z-OUT-Success-5+Week-TL APPROVED',
													'Z-OUT-Failure'
												) THEN 1 ELSE 0 END)
										) AS `Reactivation Rate`
									FROM
										gpc_hellofresh.dim_last_handled_conversation a
									JOIN 
										gpc_hellofresh.dim_wrapup_codes b 
										ON a.wrapUpCodeId = b.wrapUpId
									JOIN 
										gpc_hellofresh.dim_routing_queues rq 
										ON a.queueId = rq.queueId
									JOIN 
										queue_timezones ut 
										ON rq.queueName = ut.queueName
									WHERE 
										a.conversationStart >= DATE_SUB(CURRENT_DATE(), 10)
									GROUP BY
										userId,
										DATE_FORMAT(
											FROM_UTC_TIMESTAMP(conversationStart, TRIM(COALESCE(ut.timeZone, 'UTC'))), 
											'yyyy-MM-dd'
										)
								),
								CTE3 AS (SELECT
								  explode(userIds) as userId,
								  DATE_FORMAT(metricDate, 'yyyy-MM-dd') date,
								  ROUND((SUM(noOfYesAnswer) * 100) / SUM(totalQuestions), 2) AS Conformance
  
								FROM
								  dgdm_hellofresh.mv_process_conformance
								  WHERE metricDate >= DATE_SUB(CURRENT_DATE(), 10)
								GROUP BY
								  userIds,
								  metricDate)

								SELECT 
                                    COALESCE(CTE1.userId, CTE2.userId, CTE3.userId) AS userId,
                                    COALESCE(CTE1.date, CTE2.date, CTE3.date) AS date,
                                    CTE1.`Quality Score`,
                                    CTE2.`Reactivation Rate`,
                                    CTE3.Conformance
                                FROM 
                                    CTE1
                                FULL OUTER JOIN CTE2 
                                    ON CTE1.userId = CTE2.userId AND CTE1.date = CTE2.date
                                FULL OUTER JOIN CTE3 
                                    ON COALESCE(CTE1.userId, CTE2.userId) = CTE3.userId
                                AND COALESCE(CTE1.date, CTE2.date) = CTE3.date
                                WHERE
                                    CTE1.`Quality Score` IS NOT NULL
                                    OR CTE2.`Reactivation Rate` IS NOT NULL
                                    OR CTE3.Conformance IS NOT NULL
								    ;
                                """)
    print("Uploading helios_data")
    push_gamification_data(df_helios_data.toPandas(), 'HELLOFRESHHELIOS', 'HellofreshHelios_Connection')
    return True


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    app_name = "hellofresh_push_gamification_data"
    spark = get_spark_session(app_name, tenant)
    logger = gpc_utils_logger(tenant, app_name)

    functions_to_run = [
        push_conversation,
        push_survey,
        push_quality,
        push_adherence,
        push_evaluation_form,
        push_outbound_sales,
		push_helios_data
    ]

    try:
        logger.info("hellofresh_push_gamification_data")

        # Use ThreadPoolExecutor to run functions in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(func, spark): func.__name__ for func in functions_to_run}

            for future in concurrent.futures.as_completed(futures):
                func_name = futures[future]
                try:
                    future.result()  # This will raise an exception if the function raised one
                    logger.info(f"{func_name} completed successfully.")
                except Exception as e:
                    logger.exception(f"Error in {func_name}: {e}", stack_info=True, exc_info=True)
                    raise

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise