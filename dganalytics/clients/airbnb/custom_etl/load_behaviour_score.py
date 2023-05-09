from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import argparse
import pandas as pd
import datetime
import os
import numpy as np
import re

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('kpi_data', tenant)
    customer = 'airbnbprod'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(customer)

    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", "behaviour_score", input_file), engine='openpyxl', skiprows=2)
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", "behaviour_score", input_file)) 

    kpi['Recorded Date'] = pd.to_datetime(kpi['Recorded Date']).dt.strftime('%d-%m-%Y')
    kpi['Metrics'] = kpi['Metrics'].replace([np.nan], '')

    kpi = kpi.rename(columns={'Agent': 'airbnb_agent_id'})
    kpi['airbnb_agent_id'] = kpi['airbnb_agent_id'].apply(lambda x: re.search(r'\((\d+)\)', str(x)).group(1) if (isinstance(x, (str, int, float)) and re.search(r'\((\d+)\)', str(x)) is not None) else x)
    kpi['airbnb_agent_id'] = kpi['airbnb_agent_id'].astype(str)


    kpi = kpi.rename(columns={'Recorded Date': 'Recorded_Date'})
    kpi = kpi.rename(columns={'Unique Media Files (All Interactions)': 'Unique_Media_Files_All_Interactions'})
    kpi = kpi.rename(columns={'Avg Sentiment Score (Media Set Filtered)': 'Avg_Sentiment_Score_Media_Set_Filtered'})
    kpi = kpi.rename(columns={'Calls Be Empathetic': 'Calls_Be_Empathetic'})
    kpi = kpi.rename(columns={'Behavioral Score': 'Behavioral_Score'})
    kpi['Behavioral_Score'] = kpi['Behavioral_Score'].replace([np.nan], '')
    kpi = kpi.rename(columns={'Avg Indexed Set Expectations (All Interactions)': 'Avg_Indexed_Set_Expectations_All_Interactions'})
    kpi = kpi.rename(columns={'Avg Indexed Listen Actively (All Interactions)': 'Avg_Indexed_Actively_All_Interactions'})
    kpi = kpi.rename(columns={'Avg Indexed Innappropriate Action (All Interactions)': 'Avg_Indexed_Innappropriate_Action_All_Interactions'})
    kpi = kpi.rename(columns={'Avg Indexed Effective Questioning (All Interactions)': 'Avg_Indexed_Effective_Questioning_All_Interactions'})
    kpi = kpi.rename(columns={'Avg Indexed Demonstrate Ownership (All Interactions)': 'Avg_Indexed_Demonstrate_Ownership_All_Interactions'})
    kpi = kpi.rename(columns={'Avg Indexed Be Empathetic (All Interactions)': 'Avg_Indexed_Be_Empathetic_All_Interactions'})


    kpi = kpi.astype(str)
    kpi.insert(13, 'orgId', customer)
    kpi = spark.createDataFrame(kpi)
    kpi.createOrReplaceTempView("behaviour_score")

    spark.sql(f"""MERGE INTO {db_name}.day_wise_behaviour_score DB
                    USING behaviour_score A
                    ON A.Recorded_Date = DB.Recorded_Date
                    AND A.airbnb_agent_id = DB.airbnb_agent_id
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                     """)
  
