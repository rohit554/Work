from dganalytics.utils.utils import get_spark_session, get_path_vars

app_name = "startekflipkartcx_table_setup"
tenant = "startekflipkartcx"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)

spark.sql(f"""
                create database if not exists dg_{tenant} LOCATION '{db_path}/dg_startekflipkartcx'
            """)

spark.sql(f"""
        create table if not exists 
            dg_startekflipkartcx.startekflipkartcx_attendance
            (userId string, 
             reportDate string,
             isPresent string,
             orgId string,
             recordInsertDate TIMESTAMP
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_{tenant}/startekflipkartcx_attendance'
            """)