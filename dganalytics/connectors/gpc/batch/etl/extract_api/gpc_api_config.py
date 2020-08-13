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
        "params_to_replace": [],
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1,
        "raw_table_update": {
            "mode": "overwrite",
            "partition": None
        }
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
        "params_to_replace": [],
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1,
        "raw_table_update": {
            "mode": "overwrite",
            "partition": None
        }
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
        "params_to_replace": [],
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1,
        "raw_table_update": {
            "mode": "overwrite",
            "partition": None
        }
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
        "params_to_replace": [],
        "write_batch_size": None,
        "entity_name": "userDetails",
        "spark_partitions": 1,
        "raw_table_update": {
            "mode": "overwrite",
            "partition": ["extractDate"]
        }
    },
    "wrapupcodes": {
        "endpoint": "api/v2/routing/wrapupcodes",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 100
        },
        "params_to_replace": [],
        "write_batch_size": None,
        "entity_name": "entities",
        "spark_partitions": 1,
        "raw_table_update": {
            "mode": "overwrite",
            "partition": None
        }
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
        "params_to_replace": [],
        "write_batch_size": None,
        "entity_name": "conversations",
        "spark_partitions": 6,
        "raw_table_update": {
            "mode": "overwrite",
            "partition": ["extractDate"]
        }
    },
    "conversation_details_job": {
        "endpoint": "api/v2/analytics/conversations/details/jobs",
        "extract_type": "custom",
        "entity_name": "conversations",
        "spark_partitions": 6,
        "table_name": "conversations_details",
        "raw_table_update": {
            "mode": "overwrite",
            "partition": ["extractDate"]
        }
    }
}
