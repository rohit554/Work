"""
Api configuration for Nice inContact
This module contains the API configuration for Nice inContact, including endpoints, request types
"""
niceincontact_end_points={
    "agents":{
        "endpoint": "/agents",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agents",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "agents_groups":{
        "endpoint": "/agents/{agentId}/groups",
        "request_type": "GET",
        "paging": False,
        "interval": False,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agentGroups",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "agents_skills":{
        "endpoint": "/agents/skills",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agentSkillAssignments",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "groups":{
        "endpoint": "/groups",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agents",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "groups_agents":{
        "endpoint": "/groups/{groupId}/agents",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agents",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "teams":{
        "endpoint": "/teams",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "teams",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "teams_Id_agents":{
        "endpoint": "/teams/{teamId}/agents",
        "request_type": "GET",
        "paging": True,
        "interval": False,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "teams",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "teams_agents":{
        "endpoint": "/teams/agents",
        "request_type": "GET",
        "paging": False,
        "interval": False,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "teams",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "schedules_export":{
        "endpoint": "/schedules/export",
        "request_type": "POST",
        "paging": False,
        "interval": True,
        "entity_name": "agentSchedules",
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "wfmdata_agents":{
        "endpoint": "/wfm-data/agents",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "wfM_Data_Agents",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "wfmdata_agents_schedule_adherence":{
        "endpoint": "/wfm-data/agents/schedule-adherence",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agentStateHistory",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "wfmdata_agents_scorecards":{
        "endpoint": "/wfm-data/agents/scorecards",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agentStateHistory",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "wfmdata_agent_performance_skills":{
        "endpoint": "/wfm-data/skills/agent-performance",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "wfmdata_contacts_skills":{
        "endpoint": "/wfm-data/skills/contacts",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "wfmdata_dialer_contacts_skills":{
        "endpoint": "/wfm-data/skills/dialer-contacts",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "wfM_OB_StatsV9",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "agents_interaction_history":{
        "endpoint": "/agents/interaction-history",
        "request_type": "GET",
        "paging": True,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "contactStates",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "agents_performance":{
        "endpoint": "/agents/performance",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "agentPerformance",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "contacts":{
        "endpoint": "/contacts",
        "request_type": "GET",
        "paging": True,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "data",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "contacts_smstranscripts":{
        "endpoint": "/contacts/sms-transcripts",
        "request_type": "GET",  
        "paging": True,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "smsTranscripts",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "contacts_completed":{
        "endpoint": "/contacts/completed",
        "request_type": "GET",
        "paging": True,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "completedContacts",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "contacts_customdata":{
        "endpoint": "/contacts/custom-data",
        "request_type": "GET",
        "paging": True,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "ContactCustomData",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "skills_summary":{
        "endpoint": "/skills/summary",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "skillSummaries",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "skills_slasummary":{
        "endpoint": "/skills/sla-summary",
        "request_type": "GET",
        "paging": True,
        "interval": True,
        "params": {
            "pageSize": 500
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "serviceLevelSummaries",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "teams_performancetotal":{
        "endpoint": "/teams/performance-total",
        "request_type": "GET",
        "paging": False,
        "interval": True,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "teamPerformanceTotal",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "data_extraction_jobs":{
        "endpoint": "/data-extraction/v1/jobs/{jobId}",
        "request_type": "GET",
        "paging": False,
        "interval": False,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "result",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "media_playback_segments":{
        "endpoint": "/media-playback/v1/segments/{segmentId}",
        "request_type": "GET",
        "paging": False,
        "interval": False,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "interactions",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "interaction_analytics_gateway_segments_analyzed":{
        "endpoint": "/interaction-analytics-gateway/v2/segments/analyzed",
        "request_type": "GET",
        "paging": False,
        "cursor": True,
        "interval": False,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "entity_name": "interactions",
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    },
    "interaction_analytics_gateway_segments_analyzed_transcript":{
        "endpoint": "/interaction-analytics-gateway/v2/segments/{segmentId}/analyzed-transcript",
        "request_type": "GET",
        "paging": False,
        "interval": False,
        "params": {
        },
        "spark_partitions": {"max_records_per_partition": 20000},
        "tbl_overwrite": False,
        "raw_primary_key": ["id"]
    }
}
