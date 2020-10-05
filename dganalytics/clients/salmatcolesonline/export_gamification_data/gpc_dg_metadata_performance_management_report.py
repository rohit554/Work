from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql import SparkSession


def get_coles_data(spark: SparkSession, extract_date: str, org_id: str):
    df = spark.sql(f"""
select * from (
select
	COALESCE(cqnr.UserID,
		wfm.UserID) as UserID, COALESCE(cqnr.Date, wfm.Date) as Date, DailyAdherencePercentage, SumDailyQAScoreVoice, CountDailyQAScoreVoice, SumDailyQAScoreMessage, CountDailyQAScoreMessage, 
		SumDailyQAScoreEmail, CountDailyQAScoreEmail, SumDailyQAScore, CountDailyQAScore, SumDailyHoldTimeVoice, CountDailyHoldTimeVoice, 
		SumDailyHoldTimeMessage, CountDailyHoldTimeMessage, SumDailyHoldTimeEmail, CountDailyHoldTimeEmail, 
		SumDailyHoldTime, CountDailyHoldTime, SumDailyAcwTimeVoice, CountDailyAcwTimeVoice, SumDailyAcwTimeMessage, CountDailyAcwTimeMessage, 
		SumDailyAcwTimeEmail, CountDailyAcwTimeEmail, SumDailyAcwTime, CountDailyAcwTime, SumDailyNotRespondingTime
from
	(
	select
		COALESCE(cnr.UserID,
		quality.UserID) as UserID, COALESCE(cnr.Date,
		quality.Date) as Date, 
		SumDailyQAScoreVoice, CountDailyQAScoreVoice, SumDailyQAScoreMessage, CountDailyQAScoreMessage, 
		SumDailyQAScoreEmail, CountDailyQAScoreEmail, SumDailyQAScore, CountDailyQAScore, SumDailyHoldTimeVoice, CountDailyHoldTimeVoice, 
		SumDailyHoldTimeMessage, CountDailyHoldTimeMessage, SumDailyHoldTimeEmail, CountDailyHoldTimeEmail, 
		SumDailyHoldTime, CountDailyHoldTime, SumDailyAcwTimeVoice, CountDailyAcwTimeVoice, SumDailyAcwTimeMessage, CountDailyAcwTimeMessage, 
		SumDailyAcwTimeEmail, CountDailyAcwTimeEmail, SumDailyAcwTime, CountDailyAcwTime, SumDailyNotRespondingTime
	from
		(
		select
			COALESCE(conv.UserID,
			nr.UserID) as UserID, COALESCE(conv.Date,
			nr.Date) as Date, 
				SumDailyHoldTimeVoice, CountDailyHoldTimeVoice, 
		SumDailyHoldTimeMessage, CountDailyHoldTimeMessage, SumDailyHoldTimeEmail, CountDailyHoldTimeEmail, 
		SumDailyHoldTime, CountDailyHoldTime, SumDailyAcwTimeVoice, CountDailyAcwTimeVoice, SumDailyAcwTimeMessage, CountDailyAcwTimeMessage, 
		SumDailyAcwTimeEmail, CountDailyAcwTimeEmail, SumDailyAcwTime, CountDailyAcwTime, SumDailyNotRespondingTime
		from
			(
			SELECT
				agentId as UserID, cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) as Date, 
				sum(case when lower(mediaType) = 'voice' then coalesce(tHeldComplete, 0) else 0 end) as SumDailyHoldTimeVoice,
				sum(case when lower(mediaType) = 'voice' then coalesce(nHeldComplete, 0) else 0 end) as CountDailyHoldTimeVoice, 
				sum(case when lower(mediaType) = 'message' then coalesce(tHeldComplete, 0) else 0 end) as SumDailyHoldTimeMessage,
				sum(case when lower(mediaType) = 'message' then coalesce(nHeldComplete, 0) else 0 end) as CountDailyHoldTimeMessage, 
				sum(case when lower(mediaType) = 'email' then coalesce(tHeldComplete, 0) else 0 end) as SumDailyHoldTimeEmail,
				sum(case when lower(mediaType) = 'email' then coalesce(nHeldComplete , 0) else 0 end) as CountDailyHoldTimeEmail, 
				sum(coalesce(tHeldComplete, 0)) as SumDailyHoldTime, 
				sum(coalesce(nHeldComplete, 0)) as CountDailyHoldTime, 
				sum(case when lower(mediaType) = 'voice' then coalesce(tAcw, 0) else 0 end) as SumDailyAcwTimeVoice, 
				sum(case when lower(mediaType) = 'voice' then coalesce(nAcw , 0) else 0 end) as CountDailyAcwTimeVoice, 
				sum(case when lower(mediaType) = 'message' then coalesce(tAcw, 0) else 0 end)  as SumDailyAcwTimeMessage, 
				sum(case when lower(mediaType) = 'message' then coalesce(nAcw , 0) else 0 end) as CountDailyAcwTimeMessage, 
				sum(case when lower(mediaType) = 'email' then coalesce(tAcw, 0) else 0 end) as SumDailyAcwTimeEmail, 
				sum(case when lower(mediaType) = 'email' then coalesce(nAcw , 0) else 0 end) as CountDailyAcwTimeEmail, 
				sum(coalesce(tAcw, 0)) as SumDailyAcwTime,
				sum(coalesce(nAcw , 0)) as CountDailyAcwTime
			FROM
				fact_conversation_metrics
			WHERE
				cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) <= (cast('{extract_date}' as date))
                and cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) >= (cast('{extract_date}' as date) -15)
			group by
				agentId , cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) ) conv
		FULL OUTER JOIN (
			select
				userId as UserID, cast(from_utc_timestamp(startTime , 'Australia/Sydney') as date) as Date, sum(unix_timestamp(endTime) - unix_timestamp(startTime)) as SumDailyNotRespondingTime
			from
				fact_routing_status
			where
				routingStatus = 'NOT_RESPONDING'
				and cast(from_utc_timestamp(startTime , 'Australia/Sydney') as date) <= (cast('{extract_date}' as date))
                and cast(from_utc_timestamp(startTime , 'Australia/Sydney') as date) >= (cast('{extract_date}' as date) - 15)
			group by
				userId, cast(from_utc_timestamp(startTime , 'Australia/Sydney') as date) ) nr on
			nr.UserID = conv.UserID
			and nr.Date = conv.Date ) cnr
	FULL OUTER JOIN (
		select
			b.agentId as UserID, cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) as Date, 
            sum(case when upper(b.mediaType) = 'CALL' and a.totalScore is not null then a.totalScore else 0.0 end) as SumDailyQAScoreVoice, 
            sum(case when upper(b.mediaType) = 'CALL' and a.totalScore is not null then 1 else 0 end) as CountDailyQAScoreVoice, 
            sum(case when upper(b.mediaType) = 'MESSAGE' and a.totalScore is not null then a.totalScore else 0.0 end) as SumDailyQAScoreMessage, 
            sum(case when upper(b.mediaType) = 'MESSAGE' and a.totalScore is not null then 1 else 0 end) as CountDailyQAScoreMessage, 
            sum(case when upper(b.mediaType) = 'EMAIL' and a.totalScore is not null then a.totalScore else 0.0 end) as SumDailyQAScoreEmail, 
            sum(case when upper(b.mediaType) = 'EMAIL' and a.totalScore is not null then 1 else 0 end) as CountDailyQAScoreEmail, 
            sum(case when a.totalScore is not null then a.totalScore else 0.0 end) as SumDailyQAScore,
            sum(case when a.totalScore is not null then 1 else 0 end) as CountDailyQAScore
		from
			fact_evaluation_total_scores a, dim_evaluations b
		where
			a.evaluationId = b.evaluationId
			and cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) <= (cast('{extract_date}' as date))
            and cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) >= (cast('{extract_date}' as date) - 15)
		group by
			b.agentId, cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) ) quality on
		quality.UserID = cnr.UserID
		and cnr.Date = quality.Date ) cqnr
	FULL OUTER JOIN
	(
		 select b.userId as UserID, a.Date, a.TimeAdheringToSchedule as DailyAdherencePercentage
from dg_salmatcolesonline.wfm_verint_export a, dim_users b
 where 
 a.`Date` <= (cast('{extract_date}' as date))
 and a.`Date` >= (cast('{extract_date}' as date) - 15)
 and
( concat(trim(element_at(split(trim(a.Employee), ","),2)) , ' ' , trim(element_at(split(trim(a.Employee), ","),1))) = b.userFullName 
or (concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),1) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = b.userFullName
or lower(concat(trim(element_at(split(trim(a.Employee), ","),2)) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(b.userFullName)
or lower(concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),1) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(b.userFullName)
or lower(concat(trim(element_at(split(trim(a.Employee), ","),2)) , '.', trim(element_at(split(trim(a.Employee), ","),1)))) = lower(ELEMENT_at(split(b.userName, "@"),1))
or lower(concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),1) , '.' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(ELEMENT_at(split(b.userName, "@"),1))
or lower(concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),2) , '.' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(ELEMENT_at(split(b.userName, "@"),1))
)
	) wfm		
	on wfm.UserID = cqnr.UserID
	and wfm.Date = cqnr.Date
)		
where
	UserID is not NULL and Date is not NULL
                """)
    return df


if __name__ == "__main__":
    tenant, run_id, extract_date, org_id = dg_metadata_export_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_dg_metadata_colesonline_export"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("gpc_dg_metadata_colesonline_export")

        df = get_coles_data(spark, extract_date, org_id)
        df = df.drop_duplicates()
        df.registerTempTable("coles_activity")

        spark.sql(f"""
						merge into dg_salmatcolesonline.kpi_raw_data
						using coles_activity 
							on kpi_raw_data.userId = coles_activity.UserID
							and kpi_raw_data.date = coles_activity.Date
						when matched then 
							update set *
						when not matched then
							insert *
					""")

        pb_export = spark.sql(
            "select * from dg_salmatcolesonline.kpi_raw_data")
        export_powerbi_csv(tenant, pb_export, 'kpi_raw_data')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
