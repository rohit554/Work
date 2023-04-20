from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'airbnbprod'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "airbnb_kpi_data"
spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=f'dg_{tenant}')

spark.sql("USE dg_airbnbprod")

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS airbnb_day_wise
            (   UserID string,
                Date string,
                Escalation_rate string,
                NPS string,
                Reopen_Rate string,
                Resolution_Rate string,
                SPD_logged string,
                Schedule_Adherence string,
                Ticket_Occupancy string,
                Total_Crewbie_Love_Score string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_airbnbprod/airbnb_day_wise'
        """)
