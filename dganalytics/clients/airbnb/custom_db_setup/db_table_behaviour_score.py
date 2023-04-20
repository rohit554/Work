from dganalytics.utils.utils import get_spark_session, get_path_vars

tenant = 'airbnbprod'
tenant_path, db_path, log_path = get_path_vars(tenant)
app_name = "airbnb_kpi_data"
spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=f'dg_{tenant}')

spark.sql("USE dg_airbnbprod")

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS day_wise_behaviour_score
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
            LOCATION '{db_path}/dg_airbnbprod/day_wise_behaviour_score'
        """)
