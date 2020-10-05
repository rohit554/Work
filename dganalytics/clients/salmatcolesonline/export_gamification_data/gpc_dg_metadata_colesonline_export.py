from dganalytics.utils.utils import get_spark_session, push_gamification_data, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import dg_metadata_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql import SparkSession


def get_coles_data(spark: SparkSession, extract_date: str, org_id: str):
    df = spark.sql(f"""
select * from (
select
	COALESCE(cqnr.UserID,
		wfm.UserID) as UserID, COALESCE(cqnr.Date,
		wfm.Date) as Date, DailyAdherencePercentage, AvgDailyQAScoreVoice, AvgDailyQAScoreMessage, 
		AvgDailyQAScoreEmail, AvgDailyQAScore, AvgDailyHoldTimeVoice, 
		AvgDailyHoldTimeMessage, AvgDailyHoldTimeEmail, AvgDailyHoldTime, AvgDailyAcwTimeVoice, AvgDailyAcwTimeMessage, 
		AvgDailyAcwTimeEmail, AvgDailyAcwTime, SumDailyNotRespondingTime
from
	(
	select
		COALESCE(cnr.UserID,
		quality.UserID) as UserID, COALESCE(cnr.Date,
		quality.Date) as Date, AvgDailyQAScoreVoice, AvgDailyQAScoreMessage, AvgDailyQAScoreEmail, AvgDailyQAScore, AvgDailyHoldTimeVoice, AvgDailyHoldTimeMessage, AvgDailyHoldTimeEmail, AvgDailyHoldTime, AvgDailyAcwTimeVoice, AvgDailyAcwTimeMessage, AvgDailyAcwTimeEmail, AvgDailyAcwTime, SumDailyNotRespondingTime
	from
		(
		select
			COALESCE(conv.UserID,
			nr.UserID) as UserID, COALESCE(conv.Date,
			nr.Date) as Date, AvgDailyHoldTimeVoice, AvgDailyHoldTimeMessage, AvgDailyHoldTimeEmail, AvgDailyHoldTime, AvgDailyAcwTimeVoice, AvgDailyAcwTimeMessage, AvgDailyAcwTimeEmail, AvgDailyAcwTime, SumDailyNotRespondingTime
		from
			(
			SELECT
				agentId as UserID, cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) as Date, sum(case when lower(mediaType) = 'voice' then coalesce(tHeldComplete, 0) else 0 end)/ sum(case when mediaType = 'voice' then coalesce(nHeldComplete, 0) else 0 end) as AvgDailyHoldTimeVoice, sum(case when lower(mediaType) = 'message' then coalesce(tHeldComplete, 0) else 0 end)/ sum(case when mediaType = 'message' then coalesce(nHeldComplete, 0) else 0 end) as AvgDailyHoldTimeMessage, sum(case when lower(mediaType) = 'email' then coalesce(tHeldComplete, 0) else 0 end)/ sum(case when mediaType = 'email' then coalesce(nHeldComplete , 0) else 0 end) as AvgDailyHoldTimeEmail, sum(coalesce(tHeldComplete, 0))/ sum(coalesce(nHeldComplete, 0)) as AvgDailyHoldTime, sum(case when lower(mediaType) = 'voice' then coalesce(tAcw, 0) else 0 end)/ sum(case when mediaType = 'voice' then coalesce(nAcw , 0) else 0 end) as AvgDailyAcwTimeVoice, sum(case when lower(mediaType) = 'message' then coalesce(tAcw, 0) else 0 end)/ sum(case when mediaType = 'message' then coalesce(nAcw , 0) else 0 end) as AvgDailyAcwTimeMessage, sum(case when lower(mediaType) = 'email' then coalesce(tAcw, 0) else 0 end)/ sum(case when mediaType = 'email' then coalesce(nAcw , 0) else 0 end) as AvgDailyAcwTimeEmail, sum(coalesce(tAcw, 0))/ sum(coalesce(nAcw , 0)) as AvgDailyAcwTime
			FROM
				fact_conversation_metrics
			WHERE
				cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) <= (cast('{extract_date}' as date))
                and cast(from_utc_timestamp(emitDateTime, 'Australia/Sydney') as date) >= (cast('{extract_date}' as date) -7)
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
                and cast(from_utc_timestamp(startTime , 'Australia/Sydney') as date) >= (cast('{extract_date}' as date) - 7)
			group by
				userId, cast(from_utc_timestamp(startTime , 'Australia/Sydney') as date) ) nr on
			nr.UserID = conv.UserID
			and nr.Date = conv.Date ) cnr
	FULL OUTER JOIN (
		select
			b.agentId as UserID, cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) as Date, AVG(case when upper(b.mediaType) = 'CALL' then a.totalScore else NULL end) as AvgDailyQAScoreVoice, AVG(case when upper(b.mediaType) = 'MESSAGE' then a.totalScore else NULL end) as AvgDailyQAScoreMessage, AVG(case when upper(b.mediaType) = 'EMAIL' then a.totalScore else NULL end) as AvgDailyQAScoreEmail, AVG(a.totalScore) as AvgDailyQAScore
		from
			fact_evaluation_total_scores a, dim_evaluations b
		where
			a.evaluationId = b.evaluationId
			and cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) <= (cast('{extract_date}' as date))
            and cast(from_utc_timestamp(b.releaseDate , 'Australia/Sydney') as date) >= (cast('{extract_date}' as date) - 7)
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
 and a.`Date` >= (cast('{extract_date}' as date) - 7)
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
        push_gamification_data(
            df.toPandas(), 'SALMATCOLESONLINE', 'ColesProbe')

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
