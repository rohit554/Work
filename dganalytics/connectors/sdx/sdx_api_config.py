sdx_end_points = {
    "interactions": {
        "endpoint": "/api/interactions",
        "request_type": "GET",
        "paging": True,
        "interval": True,
        "params": {
            "_include_responses": "true",
            "_include_data": "true",
            "_limit": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "questions": {
        "endpoint": "/api/questions",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "_limit": 1000
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "surveys": {
        "endpoint": "/api/surveys",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "_limit": 1000
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    }
}
