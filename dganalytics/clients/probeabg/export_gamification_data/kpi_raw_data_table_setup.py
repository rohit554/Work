from dganalytics.utils.utils import get_spark_session, get_path_vars
tenant = 'probeabg'
app_name = "gpc_dg_metadata_probeabg_export"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name, tenant, default_db='dg_probeabg')

spark.sql(f"""
            create table if not exists dg_probeabg.kpi_raw_data
                (
                    userId string,
                    date date,
                    DailyAdherencePercentage float,
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
                    SumDailyHandleTimeVoice float,
                    CountDailyHandleVoice int,
                    SumDailyHandleTimeMessage float,
                    CountDailyHandleMessage int,
		            SumDailyHandleTimeEmail float,
                    CountDailyHandleEmail int,
                    SumDailyHandleTime float,
                    CountDailyHandle int
                )
                using delta
                partitioned by(date)
                LOCATION '{db_path}/dg_probeabg/kpi_raw_data'
            """)
