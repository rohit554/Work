from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd


def export_users_routing_status_sliced(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.registerTempTable("user_timezone")

    routing = spark.sql(f"""
            select frs.userId userKey,
from_utc_timestamp(frs.startTime, trim(ut.timeZone)) startTime,
from_utc_timestamp(frs.endTime, trim(ut.timeZone)) endTime,
routingStatus routingStatus,
from_utc_timestamp(frs.startTime, trim(ut.timeZone)) timeSlot,
1800 timeDiff,
'users_routing_status' pTableFlag
from gpc_hellofresh.fact_routing_status frs, user_timezone ut
    where frs.userId = ut.userId
        and frs.startDate >= cast('2020-02-01' as date)
        and ut.region {" = 'US'" if region == 'US' else " <> 'US'"}
    """)

    routing.registerTempTable("routing")
    '''
    slots = spark.sql("""select date_time from (select explode(date_time) as date_time from 
                                (SELECT sequence(cast(concat(add_months(current_date(), -13), ' 00:00:00') as timestamp),
                                cast(concat(current_date() + 2, ' 00:00:00') as timestamp), interval 30 minutes) as date_time))
                """)
    slots.registerTempTable("slots")

    df = spark.sql("""

        select /*+ RANGE_JOIN(fact_routing_status, 100), BROADCAST(slots) */ 
            userKey,
            (case when routing.startTime > slots.date_time then routing.startTime else slots.date_time end) as startTime,
            (case when routing.endTime > (slots.date_time + interval 30 minutes) then (slots.date_time + interval 30 minutes) else routing.endTime end) as endTime,
            routingStatus,
            slots.date_time as timeSlot,
            (to_unix_timestamp(case when routing.endTime > (slots.date_time + interval 30 minutes) then (slots.date_time + interval 30 minutes) else routing.endTime end)
                - to_unix_timestamp((case when routing.startTime > slots.date_time then routing.startTime else slots.date_time end))
            ) as timeDiff,
            pTableFlag
             from
                routing, slots
                where slots.date_time >= from_unixtime((((floor((to_unix_timestamp(routing.startTime) - 978267600)/1800.0) * 30) * 60) + 978267600))
                and slots.date_time < from_unixtime((((CEILING((to_unix_timestamp(routing.endTime) - 978267600)/1800.0) * 30) * 60) + 978267600))
    """)
    '''
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    df = spark.sql("""
                select userKey as userKey,
                    date_format((case when startTime > from_unixtime(timeSlot) then startTime else from_unixtime(timeSlot) end), 'yyyy-MM-dd HH:mm:ss') as startTime,
                    date_format((case when endTime > (from_unixtime(timeSlot) + interval 30 minutes) then (from_unixtime(timeSlot) + interval 30 minutes) else endTime end), 'yyyy-MM-dd HH:mm:ss') as endTime,
                    routingStatus, date_format(from_unixtime(timeSlot), 'yyyy-MM-dd HH:mm:ss') timeSlot, 
                    (to_unix_timestamp(case when endTime > (from_unixtime(timeSlot) + interval 30 minutes) then (from_unixtime(timeSlot) + interval 30 minutes) else endTime end)
                            - to_unix_timestamp((case when startTime > from_unixtime(timeSlot) then startTime else from_unixtime(timeSlot) end))
                    ) as timeDiff,
                    pTableFlag
                        from (
                    select userKey, startTime, endTime, routingStatus, 
                        explode(sequence(
                            cast((((floor((to_unix_timestamp(startTime) - 978267600)/1800.0) * 30) * 60) + 978267600) as long),
                            cast((((floor((to_unix_timestamp(endTime) - 978267600)/1800.0) * 30) * 60) + 978267600) as long),
                            1800
                        ))
                        timeSlot, timeDiff, pTableFlag from routing
                            where 
                            cast((((floor((to_unix_timestamp(startTime) - 978267600)/1800.0) * 30) * 60) + 978267600) as long) 
                            < cast((((floor((to_unix_timestamp(endTime) - 978267600)/1800.0) * 30) * 60) + 978267600) as long)
                        )
                """)

    return df
