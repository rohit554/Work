
from dganalytics.utils.utils import get_spark_session, get_path_vars


app_name = "datagamz_performance_management_setup"
tenant = "datagamz"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)


spark.sql(f"""
        create table if not exists 
            dg_performance_management.hellofresh_kpi_raw_data
            (
                userId string,
                reportDate timestamp,
                userName string,
                csat double,
                keyWord double,
                notRespondingTime double,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/hellofresh_kpi_raw_data'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.hellofresh_users
            (
                userId string,
                email string,
                firstName string,
                lastName string,
                mongoUserId string,
                name string,
                quartile string,
                roleId string,
                teamLeadName string,
                teamName string,
                hellofreshOrgId string,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/hellofresh_users'
        """)
