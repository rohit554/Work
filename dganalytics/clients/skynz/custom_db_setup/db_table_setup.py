from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'skynz'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "skynz_user_data"
spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=f'dg_{tenant}')

spark.sql("USE dg_skynz")

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS user_data
            (   Probe_ID string,
                Sky_ID string,
                Employee_Name string,
                Sky_Name string,
                Sky_email string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_skynz/user_data'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS inbound_quality_data
            (   EID string,
                Timestamp String,
                Overall_Score string,
                Score_Create_Connection string,
                Score_Wrap string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_skynz/inbound_quality_data'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS outbound_quality_data
            (   EID string,
                Timestamp String,
                Overall_Score string,
                Score_Confirm_Need string,
                Score_Add_Value string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_skynz/outbound_quality_data'
        """)