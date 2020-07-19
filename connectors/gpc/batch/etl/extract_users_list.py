import argparse
from common_utils import get_key_vars
import requests as rq


def extract_api(access_token: str, )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)

    args = parser.parse_args()
    tenant = args.tenant
    run_id = args.run_id

    spark, dbutils, access_token = get_key_vars(tenant)
