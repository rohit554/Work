from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import argparse
import pandas as pd
import datetime
import os


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('attendance_data', tenant)
    customer = 'datagamz'
    tenant_path, db_path, log_path = get_path_vars(customer)

    #reading input file either csv or xlsx
    if input_file.endswith(".xlsx"):
        attendance = pd.read_excel(os.path.join(tenant_path, "data", "raw", "attendance", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        attendance = pd.read_csv(os.path.join(tenant_path, "data", "raw", "attendance", input_file))

    #reading date columns
    #attendance = attendance.drop(columns=['HOD'])
    DATE_COL = attendance.columns[18:]
    #transforming dates into columns and computing isPresent field
    melted_df = pd.melt(attendance, id_vars=['Employee ID'], value_vars= attendance.columns[18:], var_name = 'date', value_name = 'isPresent')
    melted_df['date'] = pd.to_datetime(melted_df['date'], format='%d/%m/%Y', errors='coerce')
    melted_df['date'] = melted_df['date'].fillna(pd.to_datetime(melted_df['date'], format='%d-%m-%Y', errors='coerce'))
    melted_df['isPresent'] = melted_df['isPresent'].apply(lambda x: True if x == 'WFH' or x == 'P' else False)

    
    melted_df['orgId'] = customer
    melted_df['recordInsertDate'] = datetime.datetime.now()
    
    df = melted_df.rename(columns={
        "Employee ID": "userId",
        "date": "reportDate",
        "isPresent": "isPresent"
    }, errors="raise") 
    df = df.drop_duplicates()

    df['reportDate'] = df['reportDate'].astype('str').str.strip()
    
    df= spark.createDataFrame(df)
    
    df.createOrReplaceTempView("attendance") 
    
    newDF = spark.sql(f"""merge into dg_performance_management.attendance DB
                using attendance A
                on A.reportDate = DB.reportDate
                and A.userId = DB.userId
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)
    
    attendance = spark.sql("""SELECT DISTINCT userId,
                                            reportDate,
                                            isPresent 
                                            FROM 
                                            dg_performance_management.attendance
                                            """)
    
    
    export_powerbi_csv(customer, attendance, f"pm_attendance") 