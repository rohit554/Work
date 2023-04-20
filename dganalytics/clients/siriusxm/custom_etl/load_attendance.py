from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import argparse
import pandas as pd
import datetime
import os
import numpy as np
import pyxlsb
from dateutil import parser
import re

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('attendance_data', tenant)
    customer = 'datagamz'
    tenant_path, db_path, log_path = get_path_vars(customer)

    
    if input_file.endswith(".xlsx"):
      attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='openpyxl', sheet_name = 'CSS Roster', skiprows=1)
        
    elif input_file.endswith(".csv"):
      attendance = pd.read_csv(os.path.join(tenant_path, "data", "raw", "attendance", input_file))
      
    elif input_file.endswith(".xlsb"):
        xls = pd.ExcelFile(os.path.join(tenant_path, "data", "raw", "attendance", input_file))
        attendance = pd.DataFrame()  
        
        for sheet in xls.sheet_names:
            sheet_df = pd.read_excel(xls, sheet_name='CSS Roster',  skiprows=1)
            attendance = pd.concat([attendance, sheet_df], ignore_index=True)
        
      
    attendance.drop(columns=attendance.columns[0], inplace=True)
    attendance = attendance.loc[:, :'-' ].drop('-', axis=1)
    
    attendance = attendance.astype(str) 
    date_cols = attendance.columns[14:]
    melted_df = pd.melt(attendance, id_vars=['CCMS'], value_vars=attendance.columns[14:], var_name='date', value_name='isPresent')
    melted_df['date'] = pd.to_datetime(melted_df['date'], format='%d-%m-%Y', errors='coerce')
    melted_df['date'] = melted_df['date'].fillna(pd.to_datetime(melted_df['date'], format='%d-%m-%Y', errors='coerce'))
    melted_df['date'] = melted_df['date'].dt.strftime('%d-%m-%Y')
    melted_df['isPresent'] = melted_df['isPresent'].apply(lambda x: False if x == 'Leave' or x == 'OFF' or x == 'JUMP' or x == '-' or x == '' else True)
    
    melted_df['recordInsertDate'] = datetime.datetime.now()
    melted_df['orgId'] = customer
    
    df = melted_df.rename(columns={
        "CCMS": "userId",
        "date": "reportDate",
        "isPresent": "isPresent"
    }, errors="raise")
    df = df.drop_duplicates()
    
    df= spark.createDataFrame(df)
    
    df.createOrReplaceTempView("siriusxm_attendance")
    attendance_data = spark.sql("SELECT * FROM siriusxm_attendance")
    
    newDF = spark.sql(f"""merge into dg_performance_management.siriusXm_attendance DB
                using siriusxm_attendance A
                on date_format(cast(A.reportdate as date), 'dd-MM-yyyy') = date_format(cast(DB.reportDate as date), 'dd-MM-yyyy')
                and A.userId = DB.userId
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)

    attendance = spark.sql("""
    SELECT DISTINCT userId,
           reportDate,
           isPresent 
    FROM dg_performance_management.siriusXm_attendance
""")
    
    export_powerbi_csv(customer, attendance, f"pm_attendance")