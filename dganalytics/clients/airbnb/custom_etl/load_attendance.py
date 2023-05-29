from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import argparse
import pandas as pd
import datetime
import os
import numpy as np

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('attendance_data', tenant)
    customer = 'airbnbprod'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(customer)

    #reading input file either csv or xlsx
    if input_file.endswith(".xlsx"):
        attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        attendance = pd.read_csv(os.path.join(tenant_path, "data", "raw", "attendance", input_file))
        
    attendance['Date'] = pd.to_datetime(attendance['Date'], format='%Y-%m-%d')
    attendance['Date'] = attendance['Date'].dt.strftime('%d-%m-%Y')
    
    attendance['Status'] = attendance['Status'].apply(lambda x: True if x == 'P' else False)
    attendance = attendance.rename(mapper={'Status': 'Is_Present', 'Status': 'Is_Present'}, axis=1)
    
    attendance = attendance.astype(str)
    
    attendance['recordInsertDate'] = datetime.datetime.now()
    attendance['orgId'] = customer
    attendance['loginTime'] = None
    attendance['logoutTime'] = None
    
    attendance = attendance.rename(columns={
        "Emp ID": "userId",
        "Date": "reportDate",
        "Is_Present": "isPresent"
    }, errors="raise")
    attendance = attendance.drop_duplicates()
    
    attendance['userId'] = pd.to_numeric(attendance['userId'], errors='coerce').fillna(-1).astype(np.int64)
    attendance['reportDate'] = attendance['reportDate'].astype('str').str.strip()
    
    attendance= spark.createDataFrame(attendance)
    
    attendance.createOrReplaceTempView("attendance")
    
    spark.sql(f"""merge into dg_performance_management.attendance DB
                using attendance A
                on date_format(cast(A.reportDate as date), 'dd-MM-yyyy') = date_format(cast(DB.reportDate as date), 'dd-MM-yyyy')
                AND A.userId = DB.userId
                AND A.orgId = DB.orgId
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)
        
      
    attendance = spark.sql(f"""
                         SELECT DISTINCT  
                         reportDate, 
                         isPresent, 
                         userId
                         FROM
                         (SELECT A.reportDate, 
                         A.isPresent, 
                         DB.user_id as userId
                         FROM dg_performance_management.attendance AS A
                         JOIN dg_airbnbprod.airbnb_user_data AS DB
                         ON A.userId = DB.Emp_code
                         WHERE DB.orgId = 'airbnbprod'
                         )
                         """)
    
    export_powerbi_csv(customer, attendance, f"pm_attendance")