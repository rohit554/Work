pm_end_points = {
    " ": {
        "endpoint": "/api/trek-analytic/coaching/performance",
        "extract_type": "custom",
        "request_type": "POST",
        "paging": False,
        "cursor": True,
        "interval": False,
        "entity_name": "Coaching_Performance",
        "spark_partitions": {"max_records_per_partition": 5000},
        "table_name": "Coaching Performacne",
        "tbl_overwrite": False,
        "drop_duplicates": False,
        "raw_primary_key": ["campaign_id"]
    },
    "Coaching Insights": {
        "endpoint": "/api/trek-analytic/coaching/insight/tl",
        "extract_type": "custom",
        "request_type": "POST",
        "paging": True,
        "entity_name": "Coaching_Insights",
        "spark_partitions": {"max_records_per_partition": 5000},
        "table_name": "Coaching Insights",
        "tbl_overwrite": False,
        "drop_duplicates": False,
        "raw_primary_key": ["team_leader_id"]
    }
}