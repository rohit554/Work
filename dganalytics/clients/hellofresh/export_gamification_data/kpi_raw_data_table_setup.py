from dganalytics.utils.utils import get_spark_session, get_path_vars
tenant = 'hellofresh'
app_name = "gamification_hellofresh_export"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name, tenant, default_db='dg_hellofresh')

spark.sql(f"""
            create table if not exists dg_hellofresh.kpi_raw_data
                (
                    userId string,
                    date date,
                    department string,
                    nAnswered int,
                    tAnswered float,
                    nAcw int,
                    tAcw float,
                    nHeldComplete int,
                    tHeldComplete float,
                    nHandle int,
                    tHandle float,
                    nTalkComplete int,
                    tTalkComplete float,
                    chat_tAcw float,
                    chat_tHeldComplete float,
                    chat_tHandle float,
                    chat_tTalkComplete float,
                    email_tAcw float,
                    email_tHeldComplete float,
                    email_tHandle float,
                    email_tTalkComplete float,
                    voice_tAcw float,
                    voice_tHeldComplete float,
                    voice_tHandle float,
                    voice_tTalkComplete float,
                    social_tAcw float,
                    social_tHeldComplete float,
                    social_tHandle float,
                    social_tTalkComplete float,
                    chat_nAcw int,
                    chat_nHeldComplete int,
                    chat_nHandle int,
                    chat_nTalkComplete int,
                    email_nAcw int,
                    email_nHeldComplete int,
                    email_nHandle int,
                    email_nTalkComplete int,
                    voice_nAcw int,
                    voice_nHeldComplete int,
                    voice_nHandle int,
                    voice_nTalkComplete int,
                    social_nAcw int,
                    social_nHeldComplete int,
                    social_nHandle int,
                    social_nTalkComplete int,
                    duration int,
                    interacting_duration int,
                    idle_duration int,
                    not_responding_duration int,
                    totalScore float,
                    adherencePercentage float,
                    conformancePercentage float,
                    kw_count float,
                    csat float,
                    adherenceScheduleSecs int,
                    exceptionDurationSecs int,
                    conformanceActualSecs int,
                    conformanceScheduleSecs int,
                    totalScoreSum float,
                    totalScoreCount int,
                    csatSum float,
                    csatCount int,
                    userPresenceOqtTime int,
                    userPresenceTotalTime int
                )
                using delta
                partitioned by(date)
                LOCATION '{db_path}/dg_hellofresh/kpi_raw_data'
            """)