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
    customer = 'tpindiait'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(customer)

    if input_file.endswith(".xlsx"):
        attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        attendance = pd.read_csv(os.path.join(tenant_path, "data", "raw", "attendance", input_file))

    attendance['Original Date'] = pd.to_datetime(attendance['Date'], format="%d-%m-%Y")
    attendance['Login Time'] = pd.to_datetime(attendance['Login Time'], format="%I:%M %p")
    attendance['Logout Time'] = pd.to_datetime(attendance['Logout Time'], format="%I:%M %p")

    attendance['Date'] = attendance['Original Date'].dt.strftime('%Y-%m-%d')
    attendance['Login Time'] = attendance['Login Time'].dt.strftime('%H:%M')
    attendance['Logout Time'] = attendance['Logout Time'].dt.strftime('%H:%M')

    attendance['Login Time'] = attendance['Date'] + 'T' + attendance['Login Time'] + ':00.000+0000'

    next_day_indices = attendance['Logout Time'] < attendance['Login Time']
    attendance.loc[next_day_indices, 'Original Date'] += pd.DateOffset(days=1)
    attendance['Logout Time'] = attendance['Original Date'].dt.strftime('%Y-%m-%d') + 'T' + attendance['Logout Time'] + ':00.000+0000'

    attendance['IsPresent'] = np.where(attendance['IsPresent'] == 'Yes', True, False)

    attendance['recordInsertDate'] = datetime.datetime.now()
    attendance['orgId'] = customer
    
    
    attendance = attendance.rename(columns={
        "User ID": "userId",
        "Date": "reportDate",
        "IsPresent": "isPresent",
        "Login Time":"Login_Time",
        "Logout Time":"Logout_Time"
    }, errors="raise")
    attendance = attendance.drop_duplicates()

    attendance= spark.createDataFrame(attendance)

    attendance= spark.createDataFrame(attendance)
    attendance = attendance.withColumn("loginTime",to_timestamp("Login_Time"))
    attendance = attendance.withColumn("logoutTime",to_timestamp("Logout_Time"))

    attendance = attendance[['userId', 'reportDate', 'loginTime', 'logoutTime', 'isPresent', 'recordInsertDate', 'orgId']]

    attendance.createOrReplaceTempView("attendance")

    spark.sql(f"""merge into dg_performance_management.attendance DB
                using attendance A
                on date_format(cast(A.reportDate as date), 'dd-MM-yyyy') = date_format(cast(DB.reportDate as date), 'dd-MM-yyyy')
                and A.userId = DB.userId
                and A.orgId = DB.orgId
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)
    
    attendance = spark.sql("""SELECT
                              DISTINCT 
                               DB.userId,
                               DB.reportDate,
                               DB.isPresent 
                              FROM 
                               dg_performance_management.attendance DB
                               where orgId = 'tpindiait'
                  """)

    export_powerbi_csv(customer, attendance, f"pm_attendance") 
