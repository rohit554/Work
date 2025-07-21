confluence_endpoints = {
    "pages_by_space_and_ancestor": {
    "endpoint": "/rest/api/content/search",
    "request_type": "GET",
    "paging": True,
    "cursor": False,
    "interval": False,
    "params": {
        "cql": 'space="SS" AND ancestor=1369733 AND type=page',
        "expand": "body.storage,version,history",
        "limit": 100
    },
    "entity_name": "results",
    "spark_partitions": {"max_records_per_partition": 5000},
    "tbl_overwrite": False,
    "raw_primary_key": ["id"]
},
    "page_details": {
        "endpoint": "/rest/api/content/{page_id}",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": False,
        "params": {
            "expand": "body.storage,version,history,ancestors"
        },
        "entity_name": "page",
        "spark_partitions": {"max_records_per_partition": 5000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "users": {
        "endpoint": "/rest/api/user",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": False,
        "params": {
            "accountId": "<ACCOUNT_ID>"
        },
        "entity_name": "user",
        "spark_partitions": {"max_records_per_partition": 1000},
        "tbl_overwrite": False,
        "raw_primary_key": ["accountId"]
    },
    "child_pages": {
        "endpoint": "/rest/api/content/{page_id}/child/page",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "limit": 100,
            "expand": "version,history"
        },
        "entity_name": "results",
        "spark_partitions": {"max_records_per_partition": 5000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "page_labels": {
        "endpoint": "/rest/api/content/{page_id}/label",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "limit": 100
        },
        "entity_name": "results",
        "spark_partitions": {"max_records_per_partition": 2000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id", "name"]
    }
}
