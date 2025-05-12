from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import argparse
import pandas as pd
import datetime
import os
import numpy as np
from pyspark.sql.functions import col, sum
from pyspark.sql.types import DecimalType
import pyspark.sql.functions as F
from pyspark.sql.functions import when

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
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", "conformance", input_file), engine='openpyxl', skiprows=1, sheet_name = 'Day wise')
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", "conformance", input_file)) 

    kpi = kpi.astype(str)

    kpi = kpi.rename(columns={'IST Date': 'IST_Date'})
    kpi['IST_Date'] = pd.to_datetime(kpi['IST_Date']).dt.strftime('%d-%m-%Y')
    kpi = kpi.rename(columns={'Team Leader': 'Team_Leader'})
    kpi = kpi.rename(columns={'Team Manager': 'Team_Manager'})
    kpi = kpi.rename(columns={'Attendance_Status': 'Attendance_Status'})
    kpi = kpi.rename(columns={'Login Target': 'Login_Target'})
  
    kpi = kpi.rename(columns={'Total Target': 'Total_Target'})
    kpi['Total_Target'] = kpi['Total_Target'].apply(lambda x: '00:00:00' if x == '1900-01-01 00:00:00' else x)
    kpi['Total_Target'] = pd.to_timedelta(kpi['Total_Target']) 
    kpi['Total_Target'] = kpi['Total_Target'].dt.total_seconds()
    kpi['Total_Target'] = kpi['Total_Target'].astype(int)

    kpi = kpi.rename(columns={'Break Target': 'Break_Target'})
    kpi = kpi.rename(columns={'No of Present days': 'No_of_Present_days'})
    kpi = kpi.rename(columns={'Frist Login time (IST)': 'Frist_Login_time_IST'})
    kpi['Frist_Login_time_IST'] = kpi['Frist_Login_time_IST'].replace(['-'], '')

    kpi = kpi.rename(columns={'Last Loged out time (IST)': 'Last_Loged_out_time_IST'})
    kpi['Last_Loged_out_time_IST'] = kpi['Last_Loged_out_time_IST'].replace(['-'], '')

    kpi = kpi.rename(columns={'NO REASON': 'NO_REASON'})
    kpi = kpi.rename(columns={'Net Login Hrs.': 'Net_Login_Hrs'})

    kpi = kpi.rename(columns={'Total Login Hrs.': 'Total_Login_Hrs'})
    kpi['Total_Login_Hrs'] = pd.to_timedelta(kpi['Total_Login_Hrs']) 
    kpi['Total_Login_Hrs'] = kpi['Total_Login_Hrs'].dt.total_seconds()
    kpi['Total_Login_Hrs'] = kpi['Total_Login_Hrs'].astype(int)

    kpi = kpi.rename(columns={'Break Hrs.': 'Break_Hrs'})
    kpi = kpi.rename(columns={'Actual Overtime': 'Actual_Overtime'})
    kpi = kpi.rename(columns={'Off Call Activity': 'Off_Call_Activity'})
    kpi = kpi.rename(columns={'Attendance Status': 'Attendance_Status'})
    kpi = kpi.rename(columns={'no-answer': 'no_answer'})

    kpi['Shift'] = kpi['Shift'].replace(['np.nan'], '')
    kpi['Attendance_Status'] = kpi['Attendance_Status'].replace(['np.nan'], '')
    kpi = kpi.drop_duplicates('ECN')
    Kpi = kpi.drop_duplicates('IST_Date')

    kpi.insert(45, 'orgId', customer)
    kpi = spark.createDataFrame(kpi)
    kpi.createOrReplaceTempView("Ambassador_wise")

    spark.sql(f"""MERGE INTO {db_name}.Ambassador_wise_conformance DB
                    USING Ambassador_wise A
                    ON A.ECN = DB.ECN
                    AND A.IST_Date = DB.IST_Date
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)