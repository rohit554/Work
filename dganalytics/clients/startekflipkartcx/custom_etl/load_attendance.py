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

    tenant = 'startek'
    spark = get_spark_session('kpi_data', tenant)
    customer = 'startek'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(tenant)

    if input_file.endswith(".xlsx"):
        attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='openpyxl', skiprows = 2)
    elif input_file.endswith(".csv"):
        attendance = pd.read_csv(os.path.join(tenant_path, "data", "raw", "attendance", input_file), skiprows = 2)

    attendance['Date'] = pd.to_datetime(attendance['Date'], format='%d-%b-%y').dt.strftime('%d-%m-%Y')

    attendance['IsPresent'] = np.where(attendance['IsPresent'] == 'Yes', True, False)

    attendance['recordInsertDate'] = datetime.datetime.now()
    attendance['orgId'] = 'startek'
    
    attendance = attendance.rename(columns={
        "UserId": "userId",
        "Date": "reportDate",
        "IsPresent": "isPresent"
    }, errors="raise")
    #attendance = attendance.drop_duplicates()

    attendance = attendance[['userId', 'reportDate', 'isPresent', 'recordInsertDate', 'orgId']]

    attendance= spark.createDataFrame(attendance)

    attendance.createOrReplaceTempView("startek_attendance")

    # newDF = spark.sql(f"""DELETE FROM {db_name}.startek_attendance""")

    newDF = spark.sql(f"""merge into dg_perfoarmance_management.attendance DB
                using startek_attendance A
                on date_format(cast(A.reportDate as date), 'dd-MM-yyyy') = date_format(cast(DB.reportDate as date), 'dd-MM-yyyy')
                and A.userId = DB.userId
                and A.orgId = DB.orgId
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)

    attendance = spark.sql("""SELECT DISTINCT userId,
                                            reportDate,
                                            isPresent 
                                            FROM 
                                            dg_perfoarmance_management.attendance
                                            where orgId = 'startek'
                                            """)

    export_powerbi_csv(customer, attendance, f"pm_attendance")


        