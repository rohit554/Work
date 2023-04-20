from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'airbnbprod'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "airbnb_kpi_data"
spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=f'dg_{tenant}')

spark.sql("USE dg_airbnbprod")

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS Ambassador_wise_conformance
            (   Month string,
                IST_Date string,
                ECN string,
                Name string,
                Team_Leader string,
                Team_Manager string,
                LOB string,
                Span string,
                Shift string,
                Attendance_Status string,
                Login_Target string,
                Overtime string,
                Total_Target string,
                Break_Target string,
                No_of_Present_days string,
                Frist_Login_time_IST string,
                Last_Loged_out_time_IST string,
                Outbound string,
                Email string,
                Unavailable string,
                Casework string,
                NotReady string,
                Ready string,
                Busy string,
                AfterCallWork string,
                Unknown string,
                NO_REASON string,
                ManualSetACWPeriod string,
                Phone_Issue string,
                Messaging string,
                Coaching string,
                Meeting string,
                no_answer string,
                SystemIssues string,
                Project string,
                Training string,
                Lunch string,
                Break string,
                Secondment string,
                Off_Call_Activity string,
                Downtime string,
                Net_Login_Hrs string,
                Total_Login_Hrs string,
                Break_Hrs string,
                Actual_Overtime string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_airbnbprod/Ambassador_wise_conformance'
        """)
