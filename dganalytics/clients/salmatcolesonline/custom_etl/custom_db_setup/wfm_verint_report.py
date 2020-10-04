
from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'salmatcolesonline'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "salmatcolesonline_custom_wfm_setup"
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
                `Time Adhering toSchedule (Hours)` double,
                `Time Not Adhering to Schedule (Hours)` double,
                `Total Time Scheduled (Hours)` double,
                `Time Adhering to Schedule (%)` double,
                `Time Not Adhering to Schedule (%)` double,
                `Adherence Violations` int
            )
            using delta
            LOCATION '{db_path}/dg_salmatcolesonline/wfm_verint_export'
        """)