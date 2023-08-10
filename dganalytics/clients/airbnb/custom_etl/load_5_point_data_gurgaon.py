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

    df = pd.read_excel(os.path.join(tenant_path, "data", "raw", "5pointreport_gurgaon", input_file))

    df = df.astype(str)

    df.to_csv(os.path.join(tenant_path, 'data', 'pbdatasets', 'pm_5_point_gurgaon', '5_point_gurgaon.csv'),
                                       header=True, index=False)
    exec_powerbi_refresh(get_secret('pb-airbnb-5point-workspace'), get_secret('pb-airbnb-5point-dataset'))