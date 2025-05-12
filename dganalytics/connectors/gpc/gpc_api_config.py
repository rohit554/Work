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
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 100
        },
        "spark_partitions": {"max_records_per_partition": 10000},
        "table_name": "evaluations",
        "entity_name": "entities",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "evaluation_forms": {
        "endpoint": "/api/v2/quality/forms/evaluations/{}",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "evaluation_forms",
        "tbl_overwrite": True,
		"entity_name": "entities",
        "raw_primary_key": ["id"]
    },
    "divisions": {
        "endpoint": "/api/v2/authorization/divisions",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 1000
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "business_units": {
        "endpoint": "/api/v2/workforcemanagement/businessunits/{}",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "management_units": {
        "endpoint": "/api/v2/workforcemanagement/businessunits/{b}/managementunits",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 100
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "management_unit_users": {
        "endpoint": "/api/v2/workforcemanagement/managementunits/{b}/users",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": False,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "activity_codes": {
        "endpoint": "/api/v2/workforcemanagement/businessunits/{buId}/activitycodes",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": False,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "presence_definitions": {
        "endpoint": "/api/v2/presencedefinitions",
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
    "wfm_forecast_meta": {
        "endpoint": "/api/v2/workforcemanagement/businessunits/{b}/weeks/{w}/shorttermforecasts/{sfi}",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "wfm_forecast_meta",
        "tbl_overwrite": False,
        "raw_primary_key": ["id", "weekDate"]
    },
    "wfm_forecast_data": {
        "endpoint": "/api/v2/workforcemanagement/businessunits/{b}/weeks/{w}/shorttermforecasts/{sfi}/data",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "wfm_forecast_data",
        "tbl_overwrite": False,
        "raw_primary_key": ["id", "weekDate"]
    },
    "wfm_planninggroups": {
        "endpoint": "/api/v2/workforcemanagement/businessunits/{b}/planninggroups",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "wfm_planninggroups",
        "tbl_overwrite": False,
		"entity_name": "entities",
        "raw_primary_key": ["id", "businessUnitId"]
    },
    "conversation_aggregates": {
        "endpoint": "/api/v2/analytics/conversations/aggregates/query",
        "request_type": "POST",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
            "granularity": "PT15M",
            "groupBy": ["userId", "mediaType", "messageType", "originatingDirection", "queueId", "wrapUpCode"],
            "metrics": ["nBlindTransferred", "nConnected", "nConsult", "nConsultTransferred",
                        "nError", "nOffered", "nOutbound", "nOutboundAbandoned", "nOutboundAttempted",
                        "nOutboundConnected", "nOverSla", "nStateTransitionError", "nTransferred",
                        "tAbandon", "tAcd", "tAcw", "tAgentResponseTime", "tAlert", "tAnswered",
                        "tContacting", "tDialing", "tFlowOut", "tHandle", "tHeld", "tHeldComplete", "tIvr",
                        "tMonitoring", "tNotResponding", "tShortAbandon", "tTalk", "tTalkComplete",
                        "tUserResponseTime", "tVoicemail", "tWait"]
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": False,
        "entity_name": "results",
    },
    "speechandtextanalytics": {
        "endpoint": "/api/v2/speechandtextanalytics/conversations/{conversation_id}",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "speechandtextanalytics",
        "tbl_overwrite": False,
        "drop_duplicates": True,
        "raw_primary_key": ["id"]
    },
    "speechandtextanalytics_topics": {
        "endpoint": "/api/v2/speechandtextanalytics/topics",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": False,
        "params": {
            "pageSize": 10000
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "drop_duplicates": True,
        "raw_primary_key": ["id"]
    },
    "speechandtextanalytics_transcript": {
        "endpoint": "api/v2/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl",
        "extract_type": "custom",
        "spark_partitions": {"max_records_per_partition": 100000},
        "table_name": "speechandtextanalytics_transcript",
        "tbl_overwrite": False,
        "drop_duplicates": True,
        "raw_primary_key": ["communicationId"]
    },
    "outbound_campaigns": {
        "endpoint": "/api/v2/outbound/campaigns",
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
        "drop_duplicates": True,
        "raw_primary_key": ["id"]
    }
}
