
from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'probeabg'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "probeabg_custom_wfm_setup"
spark = get_spark_session(
    app_name=app_name, tenant=tenant, default_db=f'dg_{tenant}')

spark.sql(f"""
        create table if not exists 
            dg_{tenant}.wfm_verint_export
            (
                Date DATE,
                Organisation string,
                Employee string,
                Supervisor string,
                `TimeAdheringToScheduleHours` double,
                `TimeNotAdheringToScheduleHours` double,
                `TotalTimeScheduledHours` double,
                `TimeAdheringToSchedule` double,
                `TimeNotAdheringToSchedule` double,
                `AdherenceViolations` int,
                Employee_ID string,
                Username string,
                genesysUserId string
            )
            using delta
            partitioned by(Date)
            LOCATION '{db_path}/dg_probeabg/wfm_verint_export'
        """)
