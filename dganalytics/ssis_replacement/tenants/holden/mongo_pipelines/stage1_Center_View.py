from datetime import datetime
aggr_pipeline = [
    {
      "$project": {
        "_id":0,
        "Date": "$report_date",
        "Buisness_Unit": '$business_unit',
        "ACD_Calls": "$acd_calls",
        "Direct_Agent_Calls": "$direct_agent_calls",
        "Ext_Out_Calls": "$ext_out_calls",
        "Total_ACD_Time_m": "$total_acd_time",
        "Max_Delay_m": "$max_delay",
        "Abn_Calls": "$abn_calls",
        "Avg_Outbound_Time_m": "$avg_outbound_time",
        "Trans_Out": "$trans_out",
        "Avg_Handle_Time_m": "$avg_handle_time",
        "ABN": "$abn_percentage",
        "Total_ACW_Time_m": "$total_acw_time",
        "OCCP_includes_AUX_7": "$occp_includes_aux_7",
        "occp_aux_7_plus_9": "$occp_aux_7_plus_9",
        "Total_Outbound_Time_min": "$total_outbound_time",
        "Avg_ACW_Time_m": "$avg_acw_time1",
        "AVG_ABN_Time_m": "$avg_abn_time",
        "Avg_ACD_Time_m": "$avg_acd_time",
        "Avg_Speed_Ans_s": "$avg_speed_ans",
        "Service_Level": "$service_level_percentage",
        "Total_Handle_Time_m": "$total_handle_time",
        "AUX_8_m": "$aux_8",
        "AUX_7_m": "$aux_7",
        "OCCP": "$occp",
        "AUX_9_m": "$aux_9",
        "Total_Avail_Time_m": "$total_avail_time",
        "Total_Hold_Time_m": "$total_hold_time",
        "AUX_6_m": "$aux_6",
        "AUX_5_m": "$aux_5",
        "Avg_Hold_Time_m": "$avg_hold_time1",
        "runID": "$runID",
        "org_id": '$org_id'
      }
    },
    {
      "$addFields": {
        "transport_time": datetime.utcnow()
      }
    }
  ]
  