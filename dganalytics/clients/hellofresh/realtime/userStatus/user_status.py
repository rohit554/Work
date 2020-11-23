import requests as rq
from datetime import datetime, timedelta
import json
import pandas as pd
import logging
import numpy as np
from pytz import timezone
from ..common_utils.utils import get_access_token, get_powerbi_token, invalidate_powerbi_token
from ..common_utils.utils import powerbi_group_id, powerbi_dataset_id, get_us_users

def get_user_status():
    logging.info("Get conversation aggregates from API")
    access_token = get_access_token()
    pb_access_token = get_powerbi_token()

    users_df = get_us_users()

    api_headers = {"Authorization": "Bearer {}".format(
        access_token), "Content-Type": "application/json"}

    tz = timezone("EST")

    user_details = []
    utc_now = datetime.utcnow()
    est_end = utc_now  # est_now
    est_start = est_end - timedelta(minutes=60)
    for u in range(0, users_df.shape[0], 100):
        print(u)
        users_predicates = [
            {"type": "dimension", "dimension": "userId",
                "operator": "matches", "value": q}
            for q in users_df[u:u + 100]["userId"]
        ]
        body = {
            "interval": "{}/{}".format(est_start.strftime("%Y-%m-%dT%H:%M:%S"), est_end.strftime("%Y-%m-%dT%H:%M:%S")),
            "order": "asc",
            "paging": {"pageSize": "100", "pageNumber": 1},
            "userFilters": [{"type": "or", "predicates": users_predicates}],
        }

        # retries
        empty_resp = False

        for j in range(1000):
            print(j)
            body["paging"]["pageNumber"] = j + 1
            for i in range(5):
                user_status = rq.post(
                    "https://api.mypurecloud.com/api/v2/analytics/users/details/query",
                    headers=api_headers,
                    data=json.dumps(body),
                )

                if user_status.status_code != 200:
                    logging.info("Real Time User Status API Failed")
                    logging.info(user_status.text)
                    if i == 4:
                        raise Exception
                else:
                    if user_status.json() == {}:
                        empty_resp = True
                        break
                    user_details = user_details + \
                        user_status.json()["userDetails"]
            if empty_resp:
                break

    df = pd.DataFrame(user_details)
    del user_details

    df = df.explode("primaryPresence").explode("routingStatus")
    df[["presenceStartTime", "presenceEndTime", "primaryPresence"]] = df["primaryPresence"].apply(
        lambda x: pd.Series(
            [
                x.get("startTime") if x is not np.NaN else np.NaN,
                x.get("endTime") if x is not np.NaN else np.NaN,
                x.get("systemPresence") if x is not np.NaN else np.NaN,
            ]
        )
    )
    df[["routingStartTime", "routingEndTime", "routingStatus"]] = df["routingStatus"].apply(
        lambda x: pd.Series(
            [
                x.get("startTime") if x is not np.NaN else np.NaN,
                x.get("endTime") if x is not np.NaN else np.NaN,
                x.get("routingStatus") if x is not np.NaN else np.NaN,
            ]
        )
    )
    df = (
        df.sort_values(by=["userId", "presenceStartTime",
                           "routingStartTime"], ascending=False)
        .groupby("userId")
        .first()
        .reset_index()
    )
    df = df.merge(users_df, on=["userId"], how="inner")
    df["presenceStartTime"] = df["presenceStartTime"].astype(
        "datetime64[ns]")  # - pd.Timedelta(hours=5)
    df["presenceEndTime"] = df["presenceEndTime"].astype(
        "datetime64[ns]")  # - pd.Timedelta(hours=5)
    df["routingStartTime"] = df["routingStartTime"].astype(
        "datetime64[ns]")  # - pd.Timedelta(hours=5)
    df["routingEndTime"] = df["routingEndTime"].astype(
        "datetime64[ns]")  # - pd.Timedelta(hours=5)
    # df['extractTime'] = datetime.utcnow()
    df = df[
        [
            "userId",
            "name",
            "groupName",
            "primaryPresence",
            "routingStatus",
            "presenceStartTime",
            "presenceEndTime",
            "routingStartTime",
            "routingEndTime",
            # "extractTime"
        ]
    ]

    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer {}".format(pb_access_token)}

    dataset_url = "https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/tables/USuserStatus/rows".format(
        powerbi_group_id, powerbi_dataset_id
    )

    delete_data = rq.delete(dataset_url, headers=headers)

    if delete_data.status_code != 200:
        logging.info(delete_data.text)
        if delete_data.json()['error']['code'] == 'TokenExpired':
            invalidate_powerbi_token()
        logging.info("Power Bi realtime Queue Stats - delete faled")
        raise Exception

    pb_data = json.loads(df.to_json(index=False, orient="table"))["data"]
    post_data = rq.post(dataset_url, headers=headers, data=json.dumps(pb_data))

    if post_data.status_code != 200:
        logging.info(post_data.text)
        if post_data.json()['error']['code'] == 'TokenExpired':
            invalidate_powerbi_token()
        raise Exception
        logging.info("Power BI Posting to dataset failed")

    logging.info("Realtime Report Extraction Completed")

    return True
