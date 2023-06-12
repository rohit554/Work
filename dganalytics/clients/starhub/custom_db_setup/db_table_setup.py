from dganalytics.utils.utils import get_spark_session, get_path_vars


app_name = "starthub_table_setup"
tenant = "starthub"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)


spark.sql(f"""
                create database if not exists dg_{tenant} LOCATION '{db_path}/dg_starthub'
            """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dg_{tenant}.starthub_user
            (   password STRING,
                first_name STRING,
                middle_name STRING,
                last_name STRING,
                gender STRING,
                user_id STRING,
                user_start_date STRING,
                email STRING,
                dateofbirth STRING,
                team STRING,
                role STRING,
                license_id STRING,
                Full_Name STRING,
                Communication_Email STRING,
                LOB STRING,
                last_working_date STRING,
                orgId STRING
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_{tenant}/starthub_user'
        """)