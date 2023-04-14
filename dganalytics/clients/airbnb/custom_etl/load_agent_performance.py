from dganalytics.utils.utils import get_spark_session, get_path_vars, exec_powerbi_refresh, get_secret
import argparse
import pandas as pd
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file
    
    customer = 'airbnbprod'
    tenant_path, db_path, log_path = get_path_vars(customer)

    xls = pd.ExcelFile(os.path.join(tenant_path, "data", "raw", "agent_performance", input_file))
    df = pd.read_excel(xls, 'Emp List')
    df = spark.createDataFrame(df)
    df.createOrReplaceTempView("agent_performance")

    data = spark.sql(f"""
      SELECT SELECT CASE WHEN U.CCMS_ID = 'DNA' OR U.CCMS_ID IS NULL THEN U.Emp_code ELSE U.CCMS_ID end UserId,
            P.* FROM agent_performance P
      LEFT JOIN dg_airbnbprod.airbnb_user_data U
        ON P.`Emp ID` = U.Emp_code
    """)

    data.toPandas().to_csv(os.path.join(tenant_path, 'data', 'pbdatasets', 'pm_agent_performance', 'agent_performance.csv'),
                                       header=True, index=False)
    exec_powerbi_refresh(get_secret('pb-airbnb-agent-performance-workspace'), get_secret('pb-airbnb-agent-performance-dataset'))
