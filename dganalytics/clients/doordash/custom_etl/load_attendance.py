from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import argparse
import pandas as pd
import datetime
import os
import numpy as np
from pyspark.sql.functions import date_format, to_date
from pyspark.sql.functions import from_unixtime, when
from pyspark.sql.functions import concat_ws



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)
    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('attendance_data', tenant)
    customer = 'doordashprod'
    tenant_path, db_path, log_path = get_path_vars(customer)

    #reading input file either csv or xlsx
    if input_file.endswith(".xlsx"):
        attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        attendance = pd.read_csv(os.path.join(tenant_path, "data", "raw", "attendance", input_file))
    elif input_file.endswith(".xlsb"):
        attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='pyxlsb')

    #reading date columns
    DATE_COL = attendance.columns[13:]
    #transforming dates into columns and computing isPresent field
    melted_df = pd.melt(attendance, id_vars=['Employee ID'], value_vars= attendance.columns[13:], var_name = 'date', value_name = 'isPresent')
    melted_df['date'] = pd.to_numeric(melted_df['date'], errors='coerce')
    melted_df['date'] = pd.to_datetime(melted_df['date'], unit='d', origin='1899-12-30')
    melted_df['date'] = pd.to_datetime(melted_df['date'], format='%d/%m/%Y', errors='coerce')
    melted_df['date'] = melted_df['date'].fillna(pd.to_datetime(melted_df['date'], format='%d-%m-%Y', errors='coerce'))
    melted_df['date'] = pd.DatetimeIndex(melted_df['date']).strftime('%d-%m-%Y')
    melted_df['isPresent'] = melted_df['isPresent'].apply(lambda x: True if x == 'WFH' or x == 'P' else False)

    melted_df['orgId'] = customer
    melted_df['recordInsertDate'] = datetime.datetime.now()
    attendance['Login_Time'] = ''
    attendance['Logout_Time'] = ''


    df = melted_df.rename(columns={
        "Employee ID": "userId",
        "date": "reportDate",
        "isPresent": "isPresent"
    }, errors="raise") 
    df = df.drop_duplicates()
    df['userId'] = df['userId'].astype(np.int64)

    df = spark.createDataFrame(df)
    df.createOrReplaceTempView("attendance")

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
    
    attendance = spark.sql("""SELECT DISTINCT userId,
                                        to_date(reportDate, 'dd-MM-yyyy') reportDate,
                                        isPresent 
                                        FROM dg_performance_management.attendance
                                        where orgId = 'doordashprod' 
                                        """)

    attendance = attendance.withColumn("reportDate", date_format("reportDate", "dd-MM-yyyy"))

    export_powerbi_csv(customer, attendance, f"pm_attendance")