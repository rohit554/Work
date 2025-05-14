niceincontact_end_points={
    "agents":{
        "endpoint": "/agents",
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
    "agents_agnetId":{
        "endpoint": "/agents/{agentId}",
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
    "agents_groups":{
        "endpoint": "/agents/{agentId}/groups",
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
    "agents_skills":{
        "endpoint": "/agents/skills",
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
    "groups":{
        "endpoint": "/groups",
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
    "groups_agents":{
        "endpoint": "/groups/{groupId}/agents",
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
    "teams":{
        "endpoint": "/teams",
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
    "teams_teamId_agents":{
        "endpoint": "/teams/{teamId}/agents",
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
    "teams_agents":{
        "endpoint": "/teams/agents",
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
    "schedules_export":{
        "endpoint": "/schedules/export",
        "request_type": "POST",
        "paging": False,
        "cursor": False,
        "interval": True,
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "wfm_data_agents":{
        "endpoint": "/wfm-data/agents",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "wfm_data_agents_schedule_adherence":{
        "endpoint": "/wfm-data/agents/schedule-adherence",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "wfm_data_agents_scorecards":{
        "endpoint": "/wfm-data/agents/scorecards",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "wfm_data_skills_agent_performance":{
        "endpoint": "/wfm-data/skills/agent-performance",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "wfm_data_skills_contacts":{
        "endpoint": "/wfm-data/skills/contacts",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "wfm_data_skills_dialer_contacts":{
        "endpoint": "/wfm-data/skills/dialer-contacts",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "agents_interaction_history":{
        "endpoint": "/agents/interaction-history",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "agents_performance":{
        "endpoint": "/agents/performance",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "contacts":{
        "endpoint": "/contacts",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "contacts_sms_transcripts":{
        "endpoint": "/contacts/sms-transcripts",
        "request_type": "GET",  
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "contacts_completed":{
        "endpoint": "/contacts/completed",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "contacts_custom_data":{
        "endpoint": "/contacts/custom-data",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "skills_summary":{
        "endpoint": "/skills/summary",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "skills_sla_summary":{
        "endpoint": "/skills/sla-summary",
        "request_type": "GET",
        "paging": True,
        "cursor": False,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "teams_performance_total":{
        "endpoint": "/teams/performance-total",
        "request_type": "GET",
        "paging": False,
        "cursor": False,
        "interval": True,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "data_extraction_v1_jobs_jobId":{
        "endpoint": "/data-extraction/v1/jobs/{jobId}",
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
    "media_playback_v1_segments_segmentId":{
        "endpoint": "/media-playback/v1/segments/{segmentId}",
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
    "interaction_analytics_gateway_v2_segments_analyzed":{
        "endpoint": "/interaction-analytics-gateway/v2/segments/analyzed",
        "request_type": "GET",
        "paging": False,
        "cursor": True,
        "interval": False,
        "params": {
        },
        "entity_name": "entities",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": True,
        "raw_primary_key": ["id"]
    },
    "interaction_analytics_gateway_v2_segments_segmentId_analyzed_transcript":{
        "endpoint": "/interaction-analytics-gateway/v2/segments/{segmentId}/analyzed-transcript",
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
    }

}