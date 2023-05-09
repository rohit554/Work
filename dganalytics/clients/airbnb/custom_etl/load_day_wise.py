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

    #reading input file either csv or xlsx
    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", "day_wise", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", "day_wise", input_file))

    kpi_pivot = pd.pivot_table(kpi, index=['OM Breakdown selection', 'Day of Chart_date'], columns='Measure Names', values='Measure Values')

    kpi_pivot.columns = kpi_pivot.columns.str.strip()

    kpi_pivot.reset_index(inplace=True)

    kpi_pivot['Date'] = pd.to_datetime(kpi_pivot['Day of Chart_date']).dt.strftime('%d-%m-%Y')
    kpi_pivot.drop('Day of Chart_date', axis=1, inplace=True)

    kpi = kpi_pivot[['OM Breakdown selection', 'Date'] + [col for col in kpi_pivot.columns if col not in ['OM Breakdown selection', 'Date']]]

    kpi.rename(columns={'OM Breakdown selection': 'UserID'}, inplace=True)

    kpi.rename(columns={'Escalation rate': 'Escalation_rate'}, inplace=True)
    kpi['Escalation_rate'] = kpi['Escalation_rate'].replace([np.nan], '')

    kpi.rename(columns={'Reopen Rate': 'Reopen_Rate'}, inplace=True)
    kpi['Reopen_Rate'] = kpi['Reopen_Rate'].replace([np.nan], '')

    kpi.rename(columns={'Resolution Rate': 'Resolution_Rate'}, inplace=True)
    kpi['Resolution_Rate'] = kpi['Resolution_Rate'].replace([np.nan], '')

    kpi.rename(columns={'SPD (logged)': 'SPD_logged'}, inplace=True)
    kpi['SPD_logged'] = kpi['SPD_logged'].replace([np.nan], '')

    kpi.rename(columns={'Schedule Adherence': 'Schedule_Adherence'}, inplace=True)
    kpi['Schedule_Adherence'] = kpi['Schedule_Adherence'].replace([np.nan], '')

    kpi.rename(columns={'Ticket Occupancy': 'Ticket_Occupancy'}, inplace=True)
    kpi['Ticket_Occupancy'] = kpi['Ticket_Occupancy'].replace([np.nan], '')

    kpi.rename(columns={'Total Crewbie Love Score': 'Total_Crewbie_Love_Score'}, inplace=True)
    kpi['Total_Crewbie_Love_Score'] = kpi['Total_Crewbie_Love_Score'].replace([np.nan], '')

    kpi['NPS'] = kpi['NPS'].replace([np.nan], '')

    kpi.insert(10, 'orgId', customer)
    kpi = kpi.astype(str)
    kpi = spark.createDataFrame(kpi)
    kpi.createOrReplaceTempView("day_wise")
    
    spark.sql(f"""merge into {db_name}.airbnb_day_wise DB
                    USING day_wise A
                    ON A.UserID = DB.UserID
                    AND A.Date = DB.Date
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)
