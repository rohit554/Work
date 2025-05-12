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
    spark = get_spark_session('kpi_data', tenant)
    customer = 'airbnbprod'
    db_name = f"dg_{customer}"
    tenant_path, db_path, log_path = get_path_vars(customer)

    # reading input file either csv or xlsx
    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", "day_wise", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", "day_wise", input_file))

    columns_to_drop = ['Unnamed: 7', 'Unnamed: 8', 'Breakdown Selection 2', 'Breakdown Selection 3', 'Breakdown Selection 4']
    kpi.drop(columns=[col for col in columns_to_drop if col in kpi.columns], inplace=True)

    kpi['Column Breakdown Selection'] = pd.to_datetime(kpi['Column Breakdown Selection'], format='%d-%b-%y').dt.strftime('%d-%m-%Y')
    kpi.rename(columns={'Column Breakdown Selection': 'Date'}, inplace=True)

    kpi_pivot = pd.pivot(kpi, index=['Breakdown Selection 1', 'Date'], columns='Measure Names', values='Measure Values')
    kpi_pivot.columns = kpi_pivot.columns.str.strip()
    kpi_pivot.reset_index(inplace=True)

    kpi_pivot.fillna('', inplace=True)

    if 'Resolution Rate' not in kpi_pivot.columns:
        kpi_pivot['Resolution Rate'] = ''

    if 'Love Score' not in kpi_pivot.columns:
        kpi_pivot['Love Score'] = ''

    if 'Solves Per Day' not in kpi_pivot.columns:
        kpi_pivot['Solves Per Day'] = ''

    if 'Ambassador Occupancy' not in kpi_pivot.columns:
        kpi_pivot['Ambassador Occupancy'] = ''


    kpi_pivot.rename(columns={
        'Breakdown Selection 1': 'UserID',
        'Solves Per Day': 'SPD_logged',
        'Resolution Rate': 'Resolution_Rate',
        'NPS': 'NPS',
        'Love Score': 'Total_Crewbie_Love_Score',
        'Ambassador Occupancy': 'Ticket_Occupancy'
    }, inplace=True)

    if 'Escalation rate' in kpi_pivot.columns:
        kpi_pivot.rename(columns={'Escalation rate': 'Escalation_Rate'}, inplace=True)
        kpi_pivot['Escalation_Rate'] = kpi_pivot['Escalation_Rate'].replace([np.nan], '')
    else:
        kpi_pivot['Escalation_Rate'] = ''

    if 'Reopen Rate' in kpi_pivot.columns:
        kpi_pivot.rename(columns={'Reopen Rate': 'Reopen_Rate'}, inplace=True)
        kpi_pivot['Reopen_Rate'] = kpi_pivot['Reopen_Rate'].replace([np.nan], '')
    else:
        kpi_pivot['Reopen_Rate'] = ''

    if 'Schedule Adherence' in kpi_pivot.columns:
        kpi_pivot.rename(columns={'Schedule Adherence': 'Schedule_Adherence'}, inplace=True)
        kpi_pivot['Schedule_Adherence'] = kpi_pivot['Schedule_Adherence'].replace([np.nan], '')
    else:
        kpi_pivot['Schedule_Adherence'] = ''


    kpi_pivot.insert(10, 'orgId', customer)
    kpi_pivot = kpi_pivot.astype(str)
    kpi_spark = spark.createDataFrame(kpi_pivot)
    kpi_spark.createOrReplaceTempView("day_wise")

    spark.sql(f"""merge into {db_name}.airbnb_day_wise DB
                    USING day_wise A
                    ON A.UserID = DB.UserID
                    AND A.Date = DB.Date
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)