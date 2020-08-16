gpc_base_url = "https://api.mypurecloud.com/"
gpc_end_points = {
    "users": {
        "endpoint": "api/v2/users",
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
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1
    },
    "routing_queues": {
        "endpoint": "api/v2/routing/queues",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1
    },
    "groups": {
        "endpoint": "api/v2/groups",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1
    },
    "users_details": {
        "endpoint": "api/v2/analytics/users/details/query",
        "request_type": "POST",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 100
        },
        "write_batch_size": None,
        "entity_name": "userDetails",
        "spark_partitions": 1
    },
    "users_details_job": {
        "endpoint": "api/v2/analytics/users/details/jobs",
        "extract_type": "custom",
        "entity_name": "userDetails",
        "spark_partitions": 1,
        "table_name": "users_details"
    },
    "wrapup_codes": {
        "endpoint": "api/v2/routing/wrapupcodes",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 100
        },
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1
    },
    "conversation_details": {
        "endpoint": "api/v2/analytics/conversations/details/query",
        "request_type": "POST",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 100
        },
        "write_batch_size": None,
        "entity_name": "conversations",
        "spark_partitions": 2
    },
    "conversation_details_job": {
        "endpoint": "api/v2/analytics/conversations/details/jobs",
        "extract_type": "custom",
        "entity_name": "conversations",
        "spark_partitions": 2,
        "table_name": "conversations_details"
    },
    "wfm_adherence": {
        "endpoint": "api/v2/workforcemanagement/adherence/historical",
        "extract_type": "custom",
        "spark_partitions": 2,
        "table_name": "wfm_adherence"
    },
    "evaluations": {
        "endpoint": "api/v2/quality/evaluations/query",
        "extract_type": "custom",
        "spark_partitions": 1,
        "table_name": "evaluations"
    }
}
