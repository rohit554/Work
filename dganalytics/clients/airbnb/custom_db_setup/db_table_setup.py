from dganalytics.utils.utils import get_spark_session, get_path_vars


app_name = "airbnb_table_setup"
tenant = "airbnbprod"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)


spark.sql(f"""
                create database if not exists dg_{tenant} LOCATION '{db_path}/dg_airbnbprod'
            """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dg_{tenant}.airbnb_user_data
            (   password STRING,
                first_name STRING,
                middle_name STRING,
                last_name STRING,
                name STRING,
                manager STRING,
                gender STRING,
                user_id STRING,
                Emp_code STRING,
                airbnb_id STRING,
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
            LOCATION '{db_path}/dg_{tenant}/airbnb_user_data'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dg_{tenant}.airbnb_day_wise
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
            LOCATION '{db_path}/dg_{tenant}/airbnb_day_wise'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dg_{tenant}.day_wise_behaviour_score
            (   Recorded_Date string,
                airbnb_agent_id string,
                Metrics string,
                Unique_Media_Files_All_Interactions string,
                Avg_Sentiment_Score_Media_Set_Filtered string,
                Calls_Be_Empathetic string,
                Behavioral_Score string,
                Avg_Indexed_Set_Expectations_All_Interactions string,
                Avg_Indexed_Actively_All_Interactions string,
                Avg_Indexed_Innappropriate_Action_All_Interactions string,
                Avg_Indexed_Effective_Questioning_All_Interactions string,
                Avg_Indexed_Demonstrate_Ownership_All_Interactions string,
                Avg_Indexed_Be_Empathetic_All_Interactions string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_{tenant}/day_wise_behaviour_score'
        """)

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dg_{tenant}.Ambassador_wise_conformance
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
            LOCATION '{db_path}/dg_{tenant}/Ambassador_wise_conformance'
        """)

spark.sql(f"""
        create table if not exists 
            dg_{tenant}.airbnb_attendance
            (empId string, 
             reportDate string,
             isPresent string,
             orgId string,
             recordInsertDate TIMESTAMP
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_{tenant}/airbnb_attendance'
            """)