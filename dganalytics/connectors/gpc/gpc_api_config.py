gpc_end_points = {
    "users": {
        "endpoint": "/api/v2/users",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "state": "any",
            "expand": ["authorization", "geolocation", "station", "team", "profileSkills", "certifications",
                       "locations", "groups", "skills", "languages", "languagePreference", "employerInfo", "biography"],
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "routing_queues": {
        "endpoint": "/api/v2/routing/queues",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 50000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "groups": {
        "endpoint": "/api/v2/groups",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "users_details": {
        "endpoint": "/api/v2/analytics/users/details/query",
        "request_type": "POST",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 100
        },
        "entity_name": "userDetails",
        "spark_partitions": {"max_records_per_partition": 30000},
        "tbl_overwrite": False,
        "raw_primary_key": ["userId", "extractDate"]
    },
    "users_details_job": {
        "endpoint": "/api/v2/analytics/users/details/jobs",
        "extract_type": "custom",
        "paging": False,
        "cursor": True,
        "interval": False,
        "params": {
            "pageSize": 1000
        },
        "entity_name": "userDetails",
        "spark_partitions": {"max_records_per_partition": 30000},
        "table_name": "users_details",
        "tbl_overwrite": False,
        "raw_primary_key": ["userId", "extractDate"]
    },
    "wrapup_codes": {
        "endpoint": "/api/v2/routing/wrapupcodes",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 100
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 50000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "conversation_details": {
        "endpoint": "/api/v2/analytics/conversations/details/query",
        "request_type": "POST",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 100
        },
        "entity_name": "conversations",
        "spark_partitions": {"max_records_per_partition": 5000},
        "tbl_overwrite": False,
        "raw_primary_key": ["conversationId"]
    },
    "conversation_details_job": {
        "endpoint": "/api/v2/analytics/conversations/details/jobs",
        "extract_type": "custom",
        "request_type": "POST",
        "paging": False,
        "cursor": True,
        "interval": False,
        "params": {
            "pageSize": 1000
        },
        "entity_name": "conversations",
        "spark_partitions": {"max_records_per_partition": 5000},
        "table_name": "conversation_details",
        "tbl_overwrite": False,
        "raw_primary_key": ["conversationId"]
    },
    "wfm_adherence": {
        "endpoint": "/api/v2/workforcemanagement/adherence/historical",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 30000},
        "table_name": "wfm_adherence",
        "tbl_overwrite": False,
        "raw_primary_key": ["userId", "startDate", "endDate"]
    },
    "evaluations": {
        "endpoint": "/api/v2/quality/evaluations/query",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 10000},
        "table_name": "evaluations",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "evaluation_forms": {
        "endpoint": "/api/v2/quality/forms/evaluations/{}",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "evaluation_forms",
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    }
}
