from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger, get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd
from dganalytics.clients.hellofresh.export_gamification_data.keyword_map_table import keyword_map


def get_base_data(spark: SparkSession, extract_date: str):
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    spark.sql(keyword_map)
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_timezones = spark.createDataFrame(queue_timezones)
    queue_timezones.createOrReplaceTempView("queue_timezones")

    user_timezone = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.createOrReplaceTempView("user_timezone")

    backword_days = 16

    df = spark.sql(f"""
        SELECT * FROM (
                SELECT
                    u.userId, u.date, u.department, cm.nAnswered, cm.tAnswered/1000.0 tAnswered, cm.nAcw, cm.tAcw/1000.0 tAcw, cm.nHeldComplete, cm.tHeldComplete/1000.0 tHeldComplete,
                    cm.nHandle, cm.tHandle/1000.0 tHandle, cm.nTalkComplete, cm.tTalkComplete/1000.0 tTalkComplete,
                    cm.chat_tAcw/1000.0 chat_tAcw, cm.chat_tHeldComplete/1000.0 chat_tHeldComplete, cm.chat_tHandle/1000.0 chat_tHandle, cm.chat_tTalkComplete/1000.0 chat_tTalkComplete,
                    cm.email_tAcw/1000.0 email_tAcw, cm.email_tHeldComplete/1000.0 email_tHeldComplete, cm.email_tHandle/1000.0 email_tHandle, cm.email_tTalkComplete/1000.0 email_tTalkComplete,
                    cm.voice_tAcw/1000.0 voice_tAcw, cm.voice_tHeldComplete/1000.0 voice_tHeldComplete, cm.voice_tHandle/1000.0 voice_tHandle, cm.voice_tTalkComplete/1000.0 voice_tTalkComplete,
                    cm.social_tAcw/1000.0 social_tAcw, cm.social_tHeldComplete/1000.0 social_tHeldComplete, cm.social_tHandle/1000.0 social_tHandle, cm.social_tTalkComplete/1000.0 social_tTalkComplete,
                    cm.chat_nAcw, cm.chat_nHeldComplete, cm.chat_nHandle, cm.chat_nTalkComplete,
                    cm.email_nAcw, cm.email_nHeldComplete, cm.email_nHandle, cm.email_nTalkComplete,
                    cm.voice_nAcw, cm.voice_nHeldComplete, cm.voice_nHandle, cm.voice_nTalkComplete,
                    cm.social_nAcw, cm.social_nHeldComplete, cm.social_nHandle, cm.social_nTalkComplete,
                    us.duration, us.interacting_duration, us.idle_duration, us.not_responding_duration,
                    qa.totalScore, wfm.adherencePercentage, wfm.conformancePercentage, kw.kw_count, survey.csat,
                    wfm.adherenceScheduleSecs, wfm.exceptionDurationSecs, wfm.adherenceScheduleSecs,
                    wfm.conformanceActualSecs, wfm.conformanceScheduleSecs, qa.totalScoreSum, qa.totalScoreCount,
                    survey.csatSum, survey.csatCount,survey.nSurveySent,
                    survey.fcr, 
                    survey.nSurveysCompleted,
                    up.oqtTime userPresenceOqtTime, up.totalTime userPresenceTotalTime,
                    wc.retentionOfCustomer
                    from (select userId, date, department from gpc_hellofresh.dim_users,
                        (select explode(sequence((cast('{extract_date}' as date))-{backword_days} + 2, (cast('{extract_date}' as date))+1, interval 1 day )) as date) dates) u
                    left join
                    (select
                    cast(from_utc_timestamp(a.intervalStart, trim(c.timeZone)) as date) date, a.userId agentId,
                    sum(a.nAnswered) nAnswered, sum(a.tAnswered) tAnswered,
                    sum(a.nAcw) nAcw, sum(a.tAcw) tAcw,
                    sum(a.nHeldComplete) nHeldComplete, sum(a.tHeldComplete) tHeldComplete,
                    sum(a.nHandle) nHandle, sum(a.tHandle) tHandle,
                    sum(a.nTalkComplete) nTalkComplete, sum(a.tTalkComplete) tTalkComplete,

                    sum(case when mediaType = 'chat' then a.nAcw else null end) chat_nAcw, sum(case when mediaType = 'chat' then a.tAcw else null end) chat_tAcw,
                    sum(case when mediaType = 'chat' then a.nHeldComplete else null end) chat_nHeldComplete, sum(case when mediaType = 'chat' then a.tHeldComplete else null end) chat_tHeldComplete,
                    sum(case when mediaType = 'chat' then a.nHandle else null end) chat_nHandle, sum(case when mediaType = 'chat' then a.tHandle else null end) chat_tHandle,
                    sum(case when mediaType = 'chat' then a.nTalkComplete else null end) chat_nTalkComplete, sum(case when mediaType = 'chat' then a.tTalkComplete else null end) chat_tTalkComplete,

                    sum(case when mediaType = 'email' then a.nAcw else null end) email_nAcw, sum(case when mediaType = 'email' then a.tAcw else null end) email_tAcw,
                    sum(case when mediaType = 'email' then a.nHeldComplete else null end) email_nHeldComplete, sum(case when mediaType = 'email' then a.tHeldComplete else null end) email_tHeldComplete,
                    sum(case when mediaType = 'email' then a.nHandle else null end) email_nHandle, sum(case when mediaType = 'email' then a.tHandle else null end) email_tHandle,
                    sum(case when mediaType = 'email' then a.nTalkComplete else null end) email_nTalkComplete, sum(case when mediaType = 'email' then a.tTalkComplete else null end) email_tTalkComplete,

                    sum(case when mediaType = 'voice' then a.nAcw else null end) voice_nAcw, sum(case when mediaType = 'voice' then a.tAcw else null end) voice_tAcw,
                    sum(case when mediaType = 'voice' then a.nHeldComplete else null end) voice_nHeldComplete, sum(case when mediaType = 'voice' then a.tHeldComplete else null end) voice_tHeldComplete,
                    sum(case when mediaType = 'voice' then a.nHandle else null end) voice_nHandle, sum(case when mediaType = 'voice' then a.tHandle else null end) voice_tHandle,
                    sum(case when mediaType = 'voice' then a.nTalkComplete else null end) voice_nTalkComplete, sum(case when mediaType = 'voice' then a.tTalkComplete else null end) voice_tTalkComplete,

                    sum(case when mediaType = 'message' then a.nAcw else null end) social_nAcw, sum(case when mediaType = 'message' then a.tAcw else null end) social_tAcw,
                    sum(case when mediaType = 'message' then a.nHeldComplete else null end) social_nHeldComplete, sum(case when mediaType = 'message' then a.tHeldComplete else null end) social_tHeldComplete,
                    sum(case when mediaType = 'message' then a.nHandle else null end) social_nHandle, sum(case when mediaType = 'message' then a.tHandle else null end) social_tHandle,
                    sum(case when mediaType = 'message' then a.nTalkComplete else null end) social_nTalkComplete, sum(case when mediaType = 'message' then a.tTalkComplete else null end) social_tTalkComplete

                    from gpc_hellofresh.fact_conversation_aggregate_metrics a, gpc_hellofresh.dim_routing_queues b, queue_timezones c
                            where a.queueId = b.queueId
                                and b.queueName = c.queueName
                                and cast(intervalStart as date) >= ((cast('{extract_date}' as date)) - {backword_days})
                    group by
                            cast(from_utc_timestamp(a.intervalStart, trim(c.timeZone)) as date), a.userId) cm
                    on cm.agentId = u.userId
                    and cm.date = u.date
                    left join
                    (
                    select
                    cast(from_utc_timestamp(frs.startTime, trim(ut.timeZone)) as date) date, frs.userId, 
                    sum(to_unix_timestamp(endTime) - to_unix_timestamp(startTime)) duration,
                    sum(case when frs.routingStatus = 'INTERACTING' then to_unix_timestamp(endTime) - to_unix_timestamp(startTime) else null end) interacting_duration,
                    sum(case when frs.routingStatus = 'IDLE' then to_unix_timestamp(endTime) - to_unix_timestamp(startTime) else null end) idle_duration,
                    sum(case when frs.routingStatus = 'NOT_RESPONDING' then to_unix_timestamp(endTime) - to_unix_timestamp(startTime) else null end) not_responding_duration
                    from gpc_hellofresh.fact_routing_status frs, user_timezone ut
                        where frs.userId = ut.userId
                            and frs.startDate >= ((cast('{extract_date}' as date)) - {backword_days})
                        group by cast(from_utc_timestamp(frs.startTime, trim(ut.timeZone)) as date), frs.userId
                    ) us
                    on us.userId = u.userId
                    and us.date = u.date
                    left join
                    (
                        select
                    cast(from_utc_timestamp(fpp.startTime, trim(ut.timeZone)) as date) date, fpp.userId, 
                    sum(case when fpp.systemPresence in ('ON_QUEUE', 'TRAINING') then to_unix_timestamp(endTime) - to_unix_timestamp(startTime) else null end) oqtTime,
                    sum(case when fpp.systemPresence in ('AWAY','BUSY','MEETING','AVAILABLE','IDLE','ON_QUEUE','MEAL','BREAK','TRAINING') then to_unix_timestamp(endTime) - to_unix_timestamp(startTime) else null end) totalTime
                    from gpc_hellofresh.fact_primary_presence fpp, user_timezone ut
                        where fpp.userId = ut.userId
                            and fpp.startDate >= ((cast('{extract_date}' as date)) - {backword_days})
                        group by cast(from_utc_timestamp(fpp.startTime, trim(ut.timeZone)) as date), fpp.userId
                    ) up
                    on up.userId = u.userId
                    and up.date = u.date
                    left join
                    (
                    select 
                    cast(from_utc_timestamp(a.releaseDate, trim(e.timeZone)) as date) date,
                    a.agentId,
                    avg(c.totalScore) totalScore,
                    sum(c.totalScore) totalScoreSum,
                    count(c.totalScore) totalScoreCount
                    from gpc_hellofresh.dim_evaluations a, gpc_hellofresh.dim_evaluation_forms b,
                        gpc_hellofresh.fact_evaluation_total_scores c,
                    gpc_hellofresh.dim_routing_queues d, queue_timezones e
                    where a.evaluationFormId = b.evaluationFormId
                    and a.evaluationId = c.evaluationId
                    and a.conversationDatePart = c.conversationDatePart
                    and a.releaseDate >= ((cast('{extract_date}' as date)) - {backword_days})
                    and a.queueId = d.queueId
                                and d.queueName = e.queueName
                    group by cast(from_utc_timestamp(a.releaseDate, trim(e.timeZone)) as date), a.agentId
                    ) qa
                    on qa.agentId = u.userId
                    and qa.date = u.date
                    left join
                    (select
                    cast(from_utc_timestamp(fw.startDate, trim(ut.timeZone)) as date) date,  fw.userId,
                    ((sum(fw.adherenceScheduleSecs) - sum(exceptionDurationSecs))/(sum(fw.adherenceScheduleSecs))) * 100 as adherencePercentage,
                    sum(fw.adherenceScheduleSecs) adherenceScheduleSecs,
                    sum(fw.exceptionDurationSecs) exceptionDurationSecs,
                    (sum(fw.conformanceActualSecs)/sum(fw.conformanceScheduleSecs)) * 100 as conformancePercentage,
                    sum(fw.conformanceActualSecs) conformanceActualSecs,
                    sum(fw.conformanceScheduleSecs) conformanceScheduleSecs
                    from gpc_hellofresh.fact_wfm_day_metrics fw, user_timezone ut
                                where fw.userId = ut.userId
                                and fw.startDatePart >= ((cast('{extract_date}' as date)) - {backword_days})
                                group by cast(from_utc_timestamp(fw.startDate, trim(ut.timeZone)) as date),  fw.userId
                    ) wfm
                    on wfm.userId = u.userId
                    and wfm.date = u.date
                    left join 
                    (
                    select 
                    cast(from_utc_timestamp(conversationEnd, trim(qt.timeZone)) as date) date, agentId,
                    ((count(distinct (case when instr(trim(lower(dc.wrapUpNote)), trim(lower(km.keyword))) > 0 then dc.wrapUpNote else null end)))/count(distinct dc.wrapUpNote)*100) as kw_count
                    from gpc_hellofresh.dim_conversations dc, gpc_hellofresh.dim_wrapup_codes dwc, gpc_hellofresh.dim_routing_queues drq, keyword_map km, queue_timezones qt
                    where conversationStartDate >= ((cast('{extract_date}' as date)) - {backword_days})
                    and dc.wrapUpCode = dwc.wrapupId
                    and drq.queueId = dc.queueId
                    and qt.queueName = drq.queueName
                    and lower(trim(substr(drq.queueName, 0, 2))) = lower(trim(km.region))
                    and lower(trim(dwc.wrapupCode)) = lower(trim(km.wrapcode))
                    group by cast(from_utc_timestamp(conversationEnd, trim(qt.timeZone)) as date), agentId
                    ) kw
                    on kw.agentId = u.userId
                    and kw.date = u.date
                    left join
                    (
                    select
                    cast(from_utc_timestamp(s.surveySentDate, trim(c.timeZone)) as date) date, s.userKey userId, 
                    round(sum(coalesce(s.csatAchieved, 0))/sum(case when  s.csatAchieved is null or s.csatAchieved = -1 then 0 else 1 end) * 20, 0) as csat,
                    sum(coalesce(s.csatAchieved, 0)) csatSum,
                    COUNT(surveySentDate) nSurveySent,
                    sum(case when  s.csatAchieved is null or s.csatAchieved = -1 then 0 else 1 end) csatCount,
                    sum(case when s.status = 'Completed' THEN fcr ELSE NULL END) fcr,
                    sum(case when s.status = 'Completed' THEN 1 ELSE 0 END) nSurveysCompleted
                    from sdx_hellofresh.dim_hellofresh_interactions  s, gpc_hellofresh.dim_routing_queues b, queue_timezones c
                    where s.surveySentDatePart >=  ((cast('{extract_date}' as date)) - {backword_days})
                    and s.queueKey = b.queueId
                    and b.queueName = c.queueName
                    group by cast(from_utc_timestamp(s.surveySentDate, trim(c.timeZone)) as date), s.userKey
                    ) survey
                    on
                    survey.userId = u.userId
                    and survey.date = u.date

                    left join
                    (
                    select  date,
                            agentId,
                            SUM(CASE WHEN wrapUpCode = '36185a4e-371b-4c0d-95f6-8d3fe958b8d5' THEN 1 ELSE 0 END) retentionOfCustomer
                    from (select distinct dc.ConversationId,
                                          cast(from_utc_timestamp(conversationEnd, trim(timeZone)) as date) date,
                                          dc.agentId,
                                          dc.wrapUpCode
                    from gpc_hellofresh.dim_conversations dc, gpc_hellofresh.dim_routing_queues drq, queue_timezones qt
                    where conversationStartDate >= ((cast('{extract_date}' as date)) - {backword_days})
                          and drq.queueId = dc.queueId
                          and qt.queueName = drq.queueName)
                    group by date, agentId
                    )wc
                    on 
                    wc.agentId = u.userId
                    and wc.date = u.date
                    )
        WHERE cast(date as date) >=  (cast('{extract_date}' as date) - ({backword_days} + 2))

    """)
    df.cache()

    return df


def push_anz_data(spark):
    anz = spark.sql("""
        SELECT * FROM (
                SELECT 
                        userId `UserID`,
                        date_format(cast(date as date), 'dd-MM-yyyy') `Date`,
                        conformancePercentage Conformance,
                        totalScore `QA Score`,
                        adherencePercentage Adherence,
                        kw_count Keyword,
                        voice_tAcw/voice_nAcw `Voice ACW`,
                        chat_tAcw/chat_nAcw `Chat ACW`,
                        social_tAcw/social_nAcw `Social ACW`,                  
                        voice_tHeldComplete/voice_nHeldComplete `Voice Hold Time`,
                        not_responding_duration `Not Responding Time`,
                        csat CSAT,
                        fcr * 100 / nSurveysCompleted as `FCR Percent`,
                        voice_tHandle/voice_nHandle `AHT Voice`,
                        chat_tHandle/chat_nHandle `AHT Chat`,
                        social_tHandle/social_nHandle `AHT Social`,
                        retentionOfCustomer `Retention Of Customer`,
                        (tAcw/nAcw) `ACW`,
                        tHeldComplete/nHeldComplete `Hold Time`
                FROM hf_game_data
                WHERE department IN (   'EP AU Manila',
                                        'HF AU Sydney',
                                        'HF AU Manila',
                                        'HF NZ Sydney',
                                        'HF NZ Manila',
                                        'EP AU Sydney',
                                        'MultiBrand ANZ Sydney',
                                        'HF AU Cebu',
                                        'AU EP HelloConnect Man',
                                        'AU EP HelloFresh Syd',
                                        'AU HF HelloConnect Ceb',
                                        'AU HF HelloConnect Man',
                                        'AU HF HelloFresh Syd',
                                        'NZ HelloConnect Man',
                                        'NZ HF HelloFresh Syd',
                                        'ANZ EP+HF HelloFresh Syd',
                                        'EP AU HC MNL',
                                        'EP AU HF SYD',
                                        'HF AU HC CEB',
                                        'HF AU HC MNL',
                                        'HF AU HF SYD',
                                        'HF NZ HC MNL',
                                        'HF NZ HF SYD',
                                        'HF ANZ HF SYD')
                )
        WHERE NOT (Conformance IS NULL
                    AND `QA Score` IS NULL
                    AND Adherence IS NULL
                    AND Keyword IS NULL
                    AND `Voice ACW` IS NULL
                    AND `Chat ACW` IS NULL
                    AND `Social ACW` IS NULL
                    AND `Voice Hold Time` IS NULL
                    AND `Not Responding Time` IS NULL
                    AND CSAT IS NULL
                    AND `FCR Percent` IS NULL
                    AND `AHT Voice` IS NULL
                    AND `AHT Chat` IS NULL
                    AND `AHT Social` IS NULL
                    AND `Retention Of Customer` IS NULL
                    AND `ACW` IS NULL
                    AND `Hold Time` IS NULL)
    """)
    push_gamification_data(anz.toPandas(), 'HELLOFRESHANZ', 'ANZConnection')
    return True


def push_us_data(spark):
    us = spark.sql("""
        SELECT * FROM (
            SELECT  userId `UserID`,
                    date_format(cast(date as date), 'MM/dd/yyyy') `Date`,
                    tAcw/nAcw `ACW`,
                    not_responding_duration `Not Responding Time`,
                    csat `CSAT`,
                    ((userPresenceOqtTime * 1.0)/userPresenceTotalTime)*100 `OQT`,
                    voice_tAcw/voice_nAcw `Voice ACW`,
                    chat_tAcw/chat_nAcw `Chat ACW`,
                    email_tAcw/chat_nAcw `Email ACW`,
                    voice_tHeldComplete/voice_nHeldComplete `Voice Hold Time` 
            FROM hf_game_data
            WHERE department IN ('MultiBrand US Newark')
            )
            WHERE NOT (`ACW` IS NULL
                        AND `Voice ACW` IS NULL
                        AND `Chat ACW` IS NULL
                        AND `Email ACW` is null
                        AND `Voice Hold Time` IS NULL
                        AND `Not Responding Time` IS NULL
                        AND CSAT IS NULL
                        AND OQT IS NULL)
    """)
    push_gamification_data(us.toPandas(), 'HELLOFRESHUS', 'NewHFUSConnection')
    return True


def push_uk_data(spark):
    uk = spark.sql("""
        SELECT * FROM (
            SELECT  userId `UserID`,
                    date_format(cast(date as date), 'dd-MM-yyyy') `Date`,
                    totalScore `QA Score`,
                    csat CSAT,
                    (voice_tHeldComplete + voice_tTalkComplete + voice_tAcw)/float(voice_nHandle) `AHT - Voice`,
                    (chat_tTalkComplete + chat_tAcw)/float(chat_nHandle) `AHT - Chat`,
                    (social_tTalkComplete + social_tAcw)/float(social_nHandle) `AHT - Social`,
                    conformancePercentage Conformance,
                    adherencePercentage Adherence,
                    (nAnswered/((interacting_duration + idle_duration)/3600)) Productivity,
                    userPresenceOqtTime/3600 `Total On Queue Time`,
                    (tTalkComplete/nTalkComplete) `ATT - Chat`,
                    (chat_tAcw/chat_nAcw) `ACW - Chat`
            FROM hf_game_data
            WHERE department IN (   'HF UK Manila',
                                    'INT. Manila',
                                    'UK HF HelloConnect Man',
                                    'INT HF HelloConnect Man',
                                    'HF UK HC MNL')
            )
            WHERE NOT (`Productivity` IS NULL
                      AND `Adherence` IS NULL
                      AND `Conformance` IS NULL
                      AND `AHT - Social` IS NULL
                      AND `AHT - Chat` IS NULL
                      AND `AHT - Voice` IS NULL
                      AND `CSAT` IS NULL
                      AND `QA Score` IS NULL
                      AND `Total On Queue Time` IS NULL
                      AND `ATT - Chat` IS NULL
                      AND `ACW - Chat` IS NULL)
    """)
    push_gamification_data(uk.toPandas(), 'HELLOFRESHUK', 'ukconnection')
    return True


def push_ca_data(spark):
    ca = spark.sql("""
        SELECT * FROM (
            SELECT
                userId `UserID`,
                date_format(cast(date as date), 'dd-MM-yyyy') `Date`,
                totalScore `QA Score`,
                csat CSAT,
                (voice_tHeldComplete + voice_tTalkComplete + voice_tAcw)/float(voice_nHandle) `AHT - Voice`,
                (chat_tTalkComplete + chat_tAcw)/float(chat_nHandle) `AHT - Chat`,
                (email_tTalkComplete + email_tAcw)/float(email_nHandle) `AHT - Email`,
                conformancePercentage Conformance,
                adherencePercentage Adherence,
                (nAnswered/((interacting_duration + idle_duration)/3600)) Productivity,
                userPresenceOqtTime/3600 `Total On Queue Time`,
                (tTalkComplete/nTalkComplete) `ATT - Chat`,
                (tAcw/nAcw) `ACW`
            FROM hf_game_data
            WHERE department IN (   'HF CA Manila',
                                    'CP CA Manila',
                                    'CA CP HelloConnect Man',
                                    'CA HF HelloConnect Man',
                                    'CP CA HC MNL',
                                    'HF CA HC MNL')
            )
            WHERE NOT (`Productivity` IS NULL
                      AND `Adherence` IS NULL
                      AND `Conformance` IS NULL
                      AND `AHT - Email` IS NULL
                      AND `AHT - Chat` IS NULL
                      AND `AHT - Voice` IS NULL
                      AND `CSAT` IS NULL
                      AND `QA Score` IS NULL
                      AND `Total On Queue Time` IS NULL
                      AND `ACW` IS NULL)
    """)
    push_gamification_data(ca.toPandas(), 'HELLOFRESHCA', 'HFCA')
    return True


def push_benx_data(spark):
    benx = spark.sql("""
        SELECT * FROM (
            SELECT
              userId `UserId`,
              date_format(cast(date as date), 'dd-MM-yyyy') `Date`,
              totalScore `QA Score`,
              csat `CSAT Score`,
              voice_tHandle/voice_nHandle `AHT Voice`,
              chat_tHandle/chat_nHandle `AHT Chat`,
              social_tHandle/social_nHandle `AHT Social`,
              adherencePercentage `Adherence`,
              nSurveySent `No Surveys Sent`,
              CASE WHEN userPresenceOqtTime IS NOT NULL
              THEN COALESCE(not_responding_duration, 0) * 100 / (userPresenceOqtTime) ELSE NULL END  `Not Responding TIme`
                           
            FROM hf_game_data 
            WHERE department IN ( 'HF BNL HF AMS' )
            )
            WHERE NOT (`AHT Voice` IS NULL
                      AND `AHT Chat` IS NULL
                      AND `AHT Social` IS NULL
                      AND `CSAT Score` IS NULL
                      AND `QA Score` IS NULL
                      AND `Adherence` IS NULL
                      AND `No Surveys Sent` IS NULL
                      AND `Not Responding TIme` IS NULL 
                       
                      )
    """)
   
    push_gamification_data(benx.toPandas(), 'HELLOFRESHBENELUX', 'HFBenelux')
    return True 


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    tenant = 'hellofresh'
    db_name = get_dbname(tenant)
    app_name = "hellofresh_push_gamification_data"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("hellofresh_push_gamification_data")

        df = get_base_data(spark, extract_date)
        df = df.drop_duplicates()
        df.createOrReplaceTempView("hf_game_data")

        push_anz_data(spark)
        push_us_data(spark)
        push_uk_data(spark)
        push_ca_data(spark)
        push_benx_data(spark)

        spark.sql(f"""
                        MERGE INTO dg_hellofresh.kpi_raw_data
                        USING hf_game_data 
                            ON kpi_raw_data.userId = hf_game_data.UserID
                            AND kpi_raw_data.date = hf_game_data.Date
                        WHEN MATCHED THEN 
                            UPDATE SET *
                        WHEN NOT MATCHED THEN
                            INSERT *
                    """)

        pb_export = spark.sql(
            "SELECT * FROM dg_hellofresh.kpi_raw_data")
        export_powerbi_csv(tenant, pb_export, 'kpi_raw_data')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
