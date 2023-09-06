from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv
import pandas as pd
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, date_format, to_date
from pyspark.sql.types import DateType
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'skynz'
    db_name = f"dg_{tenant}"
    spark = get_spark_session('users', tenant, default_db = db_name)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    
    if input_file.endswith(".json"):
        users = pd.read_json(os.path.join(tenant_path, 'data', 'raw', 'users', input_file))

    else:
      raise Exception("File is not in the correct format. It should have a '.json' extension.")

    users = pd.DataFrame(users['values'].tolist())
    header = users.iloc[0]
    users = users[1:]
    users.columns = header

    users['orgId'] = tenant
    users = users.rename(columns={
        "Probe ID": "Probe_ID",
        "Sky ID": "Sky_ID",
        "Employee Name": "Employee_Name",
        "Sky Name": "Sky_Name",
        "Sky email": "Sky_email"
    }, errors="raise")

    users = users.drop_duplicates()

    users = users[['Probe_ID', 'Sky_ID', 'Employee_Name', 'Sky_Name', 'Sky_email', 'orgId']]
    users = spark.createDataFrame(users)
    users.createOrReplaceTempView("users")

    spark.sql(f"""merge into {db_name}.users DB
                using users A
                on A.Probe_ID = DB.Probe_ID
                and A.Sky_ID = DB.Sky_ID
                and A.Sky_email = DB.Sky_email
                and A.Sky_ID is not null
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)