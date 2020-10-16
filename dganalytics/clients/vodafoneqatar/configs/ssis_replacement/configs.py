import json
# from dganalytics.clients.vodafoneqatar.analytics import mongo_pipelines as pipelines
from . import ( 
    mongo_pipelines as pipelines ,
    source_data_schemas as schemas
    )
from copy import deepcopy
import logging

database_name = {
    'dev': 'vodafone-qatar-prod',
    'uat': 'vodafone-qatar-prod',
    'prd': 'vodafone-qatar-prod',
}
config = {
    "run_window_type": "DYNAMIC",
    "find_min_rundate": {
        "collections": [
            'FCR',
            'CALL',
            'CALLSERVICE',
            'user_details',
            'TARGET',
            'SIEBELACTIVITY',
            'TNPS_Data',
            'Skill_List',
            'admin_login',
            'TTICKET',
            'FORECAST',
        ],
        "pipeline": pipelines.aaa_get_dates.aggr_pipeline,
        'load_timestamp_field': "insertion_timestamp",
    },
    "Raw_FCR": { #uniqu key is not available
        'output_collxn': "t_raw_fcr_data",
        'pipeline': pipelines.stage1_Raw_FCR.aggr_pipeline,
        'collection': "FCR",
        'load_timestamp_field': "insertion_timestamp",
        'output_columns': [
            'owner', 'account_type', 'agent', 
            # 'comments', 
            'journey', 'counts', 'created_month', 'description', 
            'check', 'activity_num', 'type', 'crm_segment', 'date_1', 
            'account_num', 'service_type', 'times', 'created_year', 'resolution_code',
            'user_id', 'location', 'state', 'category', 'date', 'status'
            ],
        'primary_key': ['activity_num'],
        'output_type': ['MERGED'],
        'partition_by': None,
        'schema': schemas.Raw_FCR.schema,
        'free_text_fields': [], #['comments']
    },
    "User_FCR_KPI_Data": {

        'output_collxn': "t_fcr_kpi_data",
        'pipeline': pipelines.stage2_User_FCR_KPI_Data.aggr_pipeline,
        'collection': "FCR",
        'load_timestamp_field': "insertion_timestamp",
        'output_columns': ['user_id', 'date',  'value1', 'value2', 'kpi','siebel_id' ],
        'primary_key': ['user_id', 'siebel_id' ,'date',  ],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.User_FCR_KPI_Data.schema,
        'free_text_fields': [],
    },
    "User_AHT_KPI_Data": {

        'output_collxn': 't_aht_kpi_data',
        'collection': 'CALL',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage3_User_AHT_KPI_Data.aggr_pipeline,
        'output_columns': ['date', 'kpi', 'user_id', 'value1', 'value2','siebel_id'],
        'primary_key': ['user_id','siebel_id', 'date','kpi', ],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.User_AHT_KPI_Data.schema,
        'free_text_fields': [],
    },
    "Call_Volume": {

        'output_collxn': 't_call_volume_data',
        'collection': 'CALLSERVICE',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage4_Call_Volume.aggr_pipeline,
        'output_columns': ['answd', 'answd_in_threshold', 'aban_in_5_secs', 'aban_calls', 'offered', 'report_date', 'location', 'lob', 'segment_type', 'language'],
        'primary_key': ['report_date', 'location', 'lob', 'segment_type', 'language'],
        'output_type': ['MERGED'],
        'partition_by': 'report_date',
        'schema': schemas.Call_Volume.schema,
        'free_text_fields': [],
    },
    "User_Details": {

        'output_collxn': 't_user_details',
        'collection': 'user_details',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage5_User_Details.aggr_pipeline,
        'output_columns': ['location', 'first_name', 'last_name', 'line_manager', 'role', 'sub_lob', 'user_id', 'lob', 'image', 'lm_user_id', 'cms_name', 'email_id', 'gender', 'team_leader', 'language'],
        'primary_key': [ 'user_id', 'cms_name', ],
        'output_type': ['MERGED'],
        'partition_by': None,
        'schema': schemas.User_Details.schema,
        'free_text_fields': [],
    },
    # "JSR_KPI_User_Team_Target": {  #No OutPUt
    #
    #     'output_collxn': 't_kpi_user_team_target',
    #     'load_timestamp_field': "insertion_timestamp",
    #     'collection': 'TARGET',
    #     'pipeline': pipelines.stage6_JSR_KPI_User_Team_Target.aggr_pipeline,
    #     'output_columns': ['kpi', 'team_id', 'target', 'weightage', 'user_id', 'date'],
    #     'primary_key': ['kpi', 'user_id', 'date'],
    #     'output_type': ['MERGED'],
    #     'partition_by': [],
    # },
    "JSR_Process_Forecast_Data": {
        
        'output_collxn': 't_forecast_data',
        'load_timestamp_field': "insertion_timestamp",
        'collection': 'CALLSERVICE',
        'pipeline': pipelines.stage7_JSR_Process_Forecast_Data.aggr_pipeline,
        'output_columns': ['forecast', 'report_date', 'lob', 'segment_type', 'location', 'language', 'actual'],
        'primary_key': [ 'report_date', 'lob', 'segment_type', 'location', 'language', ],
        'output_type': ['MERGED'],
        'partition_by': 'report_date',
        'schema': schemas.JSR_Process_Forecast_Data.schema,
        'free_text_fields': [],
    },
    "JSR_KPI_Team_Target": { 
        
        'output_collxn': 't_kpi_team_target',
        'collection': 'TARGET',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage8_JSR_KPI_Team_Target.aggr_pipeline,
        'output_columns': ['kpi', 'team_id', 'target', 'weightage', 'date'],
        'primary_key': ['kpi', 'team_id', 'date'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.JSR_KPI_Team_Target.schema,
        'free_text_fields': [],
    },
    "User_KPI_Data": {
        
        'output_collxn': 't_kpi_data',
        'collection': 'TNPS_Data',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage9_User_KPI_Data.aggr_pipeline,
        'output_columns': ['user_id', 'date', 'value1', 'value2', 'kpi','siebel_id'],
        'primary_key': ['user_id', 'date','siebel_id', 'kpi', ],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.User_KPI_Data.schema,
        'free_text_fields': [],
    },
    "JSR_Top_Reason_Data": {
        
        'output_collxn': 't_top_reason_data',
        'collection': 'SIEBELACTIVITY',
        'pipeline': pipelines.stage10_JSR_Top_Reason_Data.aggr_pipeline,
        'load_timestamp_field': "insertion_timestamp",
        'output_columns': ['activity_num','sub_reason', 'call_reason', 'type', 'call_type', 'created_by', 'segment_type', 'report_date', 'location', 'lob', 'fcr_tag', 'overall_tnps', 'fcr', 'nfcr'],
        'primary_key': ['activity_num'],
        'output_type': ['MERGED'],
        'partition_by': None,
        'schema': schemas.JSR_Top_Reason_Data.schema,
        'free_text_fields': [],
    },
    "Process_AHT_KPI_Data_to_All_Process" : {
        
        'output_collxn':'t_process_all_kpi_data',
        'collection': "CALLSERVICE",
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage12_Process_AHT_KPI_Data_to_All_Process.aggr_pipeline,
        'output_columns': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'value1', 'value2', 'kpi'],
        'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'kpi'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.Process_AHT_KPI_Data_to_All_Process.schema,
        'free_text_fields': [],
    },

    "JSR_Process_TNPS_KPI_Data_to_All_Process": {
        
        'output_collxn': 't_process_all_kpi_data',
        'collection': 'TNPS_Data',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage11_JSR_Process_TNPS_KPI_Data_to_All_Process.aggr_pipeline,
        'output_columns': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'value1', 'value2', 'kpi'],
        'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'kpi'],
        # 'primary_key': ['location', 'language', 'segment_type', 'services', 'date', 'kpi'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.JSR_Process_TNPS_KPI_Data_to_All_Process.schema,
        'free_text_fields': [],
    },

    "JSR_Process_KPI_Data_to_All_Process": {
        
        'output_collxn': 't_process_all_kpi_data',
        'collection': 'Skill_List',
        'pipeline': pipelines.stage13_JSR_Process_KPI_Data_to_All_Process.aggr_pipeline,
        'load_timestamp_field': "insertion_timestamp",
        'output_columns': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'value1', 'value2', 'kpi'],
        'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'kpi'],
        # 'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'kpi'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.JSR_Process_KPI_Data_to_All_Process.schema,
        'free_text_fields': [],
    },
    "JSR_Process_FCR_KPI_to_All_Process": {
        
        'output_collxn': 't_process_all_kpi_data',
        'collection': 'FCR',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage14_JSR_Process_FCR_KPI_to_All_Process.aggr_pipeline,
        'output_columns': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'value1', 'value2', 'kpi'],
        'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'kpi'],
        # 'primary_key': ['location', 'segment_type', 'services', 'date', 'kpi'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.JSR_Process_FCR_KPI_to_All_Process.schema,
        'free_text_fields': [],
    },
    "Admin_Login": {
        
        'output_collxn': 't_admin_login',
        'load_timestamp_field': "insertion_timestamp",
        'collection': 'admin_login',
        'pipeline': pipelines.stage15_Admin_Login.aggr_pipeline,
        'output_columns': ['email_id', 'avaya_name', 'first_name', 'last_name', 'siebel_id'],
        'primary_key': ['email_id', ],
        'output_type': ['MERGED'],
        'partition_by': None,
        'schema': schemas.Admin_Login.schema,
        'free_text_fields': [],
    },
    "JSR_AHT_KPI_Data": {
        
        'output_collxn': 't_process_aht_kpi_data',
        'collection': 'CALLSERVICE',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage16_JSR_AHT_KPI_Data.aggr_pipeline,
        'output_columns': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'value1', 'value2', 'kpi'],
        'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'kpi'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.JSR_AHT_KPI_Data.schema,
        'free_text_fields': [],
    },
    "JSR_Trouble_Ticket_Data": { 
        
        'output_collxn': 't_view_trouble_ticket_data',
        'collection': 'TTICKET',
        'pipeline': pipelines.stage17_JSR_Trouble_Ticket_Data.aggr_pipeline,
        'load_timestamp_field': "insertion_timestamp",
        'output_columns': ['call_type', 'created_by', 'call_reason', 'segment_type', 'report_date', 'location', 'lob'],
        'primary_key': ['call_type', 'created_by', 'call_reason', 'segment_type', 'report_date', 'location', 'lob'],
        'output_type': ['MERGED'],
        'partition_by': 'report_date',
        'schema': schemas.JSR_Trouble_Ticket_Data.schema,
        'free_text_fields': [],
    },
    "JSR_Process_KPI_Data": {
        
        'output_collxn': 't_process_kpi_data',
        'collection': 'Skill_List',
        'load_timestamp_field': "insertion_timestamp",
        'pipeline': pipelines.stage18_JSR_Process_KPI_Data.aggr_pipeline,
        'output_columns': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date', 'value1', 'value2', 'kpi'],
        'primary_key': ['location', 'lobs', 'language', 'arpu_segment', 'segment_type', 'services', 'site', 'date','kpi'],
        'output_type': ['MERGED'],
        'partition_by': 'date',
        'schema': schemas.JSR_Process_KPI_Data.schema,
        'free_text_fields': [],
    },
}
config['database_name']=database_name
