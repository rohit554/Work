from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'probesky'
tenant_path, db_path, log_path = get_path_vars(tenant)
db_name = f"dg_{tenant}"
app_name = f"{tenant}_users"
spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS users
            (   Probe_ID string,
                Sky_ID string,
                Employee_Name string,
                Sky_Name string,
                Sky_email string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/{db_name}/users'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS inbound_evaluations
            (   EID string,
                Timestamp String,
                Overall_Score string,
                Score_Create_Connection string,
                Score_Wrap string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/{db_name}/inbound_evaluations'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS outbound_evaluations
            (   EID string,
                Timestamp String,
                Overall_Score string,
                Score_Confirm_Need string,
                Score_Add_Value string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/{db_name}/outbound_evaluations'
        """)