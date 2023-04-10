from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'airbnbprod'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "airbnb_users_data"
spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=f'dg_{tenant}')

spark.sql("USE dg_airbnbprod")

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS airbnb_user
            (   password STRING,
                first_name STRING,
                middle_name STRING,
                last_name STRING,
                name STRING,
                manager STRING,
                gender STRING,
                user_id STRING,
                Emp_code STRING,
                LDAP_ID STRING,
                CCMS_ID STRING,
                user_start_date STRING,
                email STRING,
                dateofbirth STRING,
                team STRING,
                role STRING,
                Communication_Email STRING,
                LOB STRING,
                orgId STRING
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_airbnbprod/airbnb_user'
        """)
