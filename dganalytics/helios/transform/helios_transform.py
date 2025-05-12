from dganalytics.helios.helios_utils import get_sql_query, get_insert_overwrite_sql_query, helios_utils_logger, get_update_sql_query
import time
import os
import pandas as pd
from dganalytics.utils.utils import get_path_vars
from datetime import datetime, timedelta

def helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    today = datetime.strptime(extract_start_time, "%Y-%m-%dT%H:%M:%S")

    # Check if it's Sunday and the time is exactly midnight
    if tenant == "hellofresh" and today.weekday() == 6 and today.hour == 0 and today.minute == 0 and today.second == 0 and transformation not in ["dim_conversations", "fact_conversation_metrics", "dim_conversation_participants", "dim_conversation_sessions", "dim_conversation_session_segments",
                        "dim_surveys", "dim_evaluations", "fact_conversation_evaluations", "fact_conversation_surveys"]:
        # Subtract 7 days
        extract_start_time = (today - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")
        # extract_date = datetime.strptime(extract_start_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
        
    logger = helios_utils_logger(tenant, "helios-"+transformation)
    if transformation == "mv_flagged_conversations":
        spark.sql(f"""with CTE as (
              select
                CS.conversationId,
                CS.conversationStartDateId,
                complianceScore,
                wrapUpCode
              from
                (
                  select
                    conversationId,
                    conversationStartDateId,
                    ((num * 100) / denom) complianceScore
                  from
                    (
                      select
                        conversationId,
                        COUNT(DISTINCT question) AS denom,
                        SUM(
                          CASE
                            WHEN answer = 'yes' THEN 1
                            ELSE 0
                          END
                        ) AS num,
                        conversationStartDateId
                      from
                        (
                          select
                            *
                          from
                            dgdm_{tenant}.fact_transcript_compliance
                          where
                            conversationStartDateId >= 20250101
                        )
                      group by
                        conversationId,
                        conversationStartDateId
                    )
                  where
                    ((num * 100) / denom) < 100
                ) CS
                  JOIN (
                    select
                      conversationId,
                      conversationStartDateId,
                      wrapUpCode
                    from
                      (
                        SELECT
                          s.conversationId,
                          s.conversationStartDateId,
                          s.wrapUpCodeId AS finalWrapupCode
                        FROM
                          (
                            SELECT
                              conversationId,
                              wrapUpCodeId,
                              conversationStartDateId,
                              ROW_NUMBER() OVER (
                                  PARTITION BY conversationStartDateId, conversationId, segmentType
                                  ORDER BY segmentEnd DESC
                                ) AS rn
                            FROM
                              dgdm_{tenant}.dim_conversation_session_segments
                            WHERE
                              segmentType = 'wrapup'
                              and wrapUpCodeId is not null
                          ) AS s
                        WHERE
                          s.rn = 1
                      ) C
                        join dgdm_{tenant}.dim_wrap_up_codes w
                          on C.finalWrapupCode = w.wrapUpId
                    where
                      w.wrapupCode IN (
                        'Z-OUT-Success-1Week',
                        'Z-OUT-Success-2Week',
                        'Z-OUT-Success-3Week',
                        'Z-OUT-Success-4Week',
                        'Z-OUT-Success-5+Week-TL APPROVED'
                      )
                  ) seg
                    on CS.conversationId = seg.conversationId
                    and CS.conversationStartDateId = seg.conversationStartDateId
            ) update
              dgdm_{tenant}.dim_conversations C
            set
              C.isManualAssessmentNeeded = TRUE
            where
              exists (
                select
                  1
                from
                  CTE
                where
                  C.conversationId = CTE.conversationId
                  and C.conversationStartDateId = CTE.conversationStartDateId
              )
        """)
        spark.sql(f"""update
              dgdm_{tenant}.dim_conversations C
            set
              hasManuallyEvaluated = TRUE
            where
              exists (
                select
                  1
                from
                  (
                    select
                      conversationId,
                      d.dateId conversationStartDateId
                    from
                      gpc_{tenant}.dim_evaluations e
                    join dgdm_{tenant}.dim_date d 
                        on e.conversationDatePart = d.dateVal
                    where
                      e.status = 'FINISHED' 
                      and e.conversationDatePart >= date_sub(CAST('{extract_date}' AS DATE), 15)
                  ) E
                where
                  c.conversationId = E.conversationId
                  and C.conversationStartDateId = E.conversationStartDateId
                  and C.isManualAssessmentNeeded = TRUE
              )
        """)
    if tenant == "hellofresh":
        tenant_path =get_path_vars(tenant)[0]
        outbound_wrapups = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'Outbound_Wrapup_Codes.csv'))
        outbound_wrapups = spark.createDataFrame(outbound_wrapups)
        outbound_wrapups.createOrReplaceTempView("outbound_wrap_codes")
        user_df = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'))
        user_df = spark.createDataFrame(user_df)
        user_df.createOrReplaceTempView("user_timezones")

    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "select"))
    if df is not None and df.count() > 0 :
        logger.info(f"Number of Selected rows for {transformation} : {df.count()}")
        
        df.createOrReplaceTempView(transformation)
        
        if transformation == "dim_conversation_ivr_menu_selections":
            menu_df=spark.sql(f""" select menuId,REPLACE(menuId, '_', ' ') AS menuName from(
                        select distinct menuId from dim_conversation_ivr_menu_selections dcims where not exists(select 1 from dgdm_simplyenergy.dim_ivr_menus dim where dim.menuId = dcims.menuId))
                """)
            
            if menu_df.isEmpty():
                logger.info("we didnt have any New Menus")
            else:
                logger.info(f"We have {menu_df.count()} New Menus")
                menu_df.createOrReplaceTempView('dim_ivr_menus')
                spark.sql(f"""insert into dgdm_{tenant}.dim_ivr_menus
                            select * from dim_ivr_menus""")
                
        if transformation not in ['fact_transcript_actions', 'fact_conversation_transcript_actions']:
            df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "delete"))
            logger.info(f"Number of Deleted rows from {transformation} : {df.count()}")# resolve issue of concurrent append exception
            time.sleep(10)
        if transformation == 'fact_conversation_transcript_actions':
            conversation_ids = (
                    df.select("conversationId")
                    .distinct()
                    .rdd.flatMap(lambda x: x)
                    .collect()
            )
            # Convert to SQL-compatible format
            conversation_ids_str = ",".join(f"'{c}'" for c in conversation_ids)
            spark.sql(f"""
                    delete from dgdm_{tenant}.{transformation} where conversationId in ({conversation_ids_str})
                    """)
        
        df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "insert"))
        logger.info(f"Number of Inserted rows into {transformation} : {df.count()}")

    
def helios_overwrite_transformation(spark, transformation, tenant):
    logger = helios_utils_logger(tenant, "helios-"+transformation)
    df = spark.sql(get_insert_overwrite_sql_query(spark, transformation, tenant))
    logger.info(f"Number of Insert/overwritten rows into {transformation} : {df.count()}")

def helios_update_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    logger = helios_utils_logger(tenant, "helios-update"+transformation)
    df = spark.sql(get_update_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "update"))
    logger.info(f"Number of Updated rows for location column in {transformation} : {df.count()}")