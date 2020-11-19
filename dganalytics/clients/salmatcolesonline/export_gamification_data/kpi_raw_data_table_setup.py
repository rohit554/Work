from dganalytics.utils.utils import get_spark_session, get_path_vars
tenant = 'salmatcolesonline'
app_name = "gpc_dg_metadata_colesonline_export"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name, tenant, default_db='dg_salmatcolesonline')

spark.sql(f"""
            create table if not exists dg_salmatcolesonline.kpi_raw_data
                (
                    userId string,
                    date date,
                    DailyAdherencePercentage float,
                    SumDailyQAScoreVoice float,
                    CountDailyQAScoreVoice int,
                    SumDailyQAScoreMessage float,
                    CountDailyQAScoreMessage int,
    	            SumDailyQAScoreEmail float,
                    CountDailyQAScoreEmail int,
                    SumDailyQAScore float,
                    CountDailyQAScore int,
                    SumDailyHoldTimeVoice float,
                    CountDailyHoldTimeVoice int,
    	            SumDailyHoldTimeMessage float,
                    CountDailyHoldTimeMessage int,
                    SumDailyHoldTimeEmail float,
                    CountDailyHoldTimeEmail int,
                    SumDailyHoldTime float,
                    CountDailyHoldTime int,
                    SumDailyAcwTimeVoice float,
                    CountDailyAcwTimeVoice int,
                    SumDailyAcwTimeMessage float,
                    CountDailyAcwTimeMessage int,
		            SumDailyAcwTimeEmail float,
                    CountDailyAcwTimeEmail int,
                    SumDailyAcwTime float,
                    CountDailyAcwTime int,
                    SumDailyNotRespondingTime float,
                    CountDailyHandleChat int,
                    CountDailyHandleEmail int,
                    CountDailyHandleVoice int,
                    CountDailyHandleMessage int,
                    CountDailyHandle int
                )
                using delta
                partitioned by(date)
                LOCATION '{db_path}/dg_salmatcolesonline/kpi_raw_data'
            """)