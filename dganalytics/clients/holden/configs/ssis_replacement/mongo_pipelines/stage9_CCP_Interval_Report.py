from datetime import datetime
aggr_pipeline = [
  {
    "$project": {
      "_id": 0,
      'Login_ID': "$login_id",
      'Time': "$time",
      'date': "$report_date",
      'ACD_Calls': "$acd_calls",
      'Direct_Agent_Calls': "$direct_agent_calls",
      'DA_Abandon': "$da_abandon",
      'DA_Other': "$da_other",
      'Extout_Calls': "$extout_calls",
      'CARS_ACD_min': "$cars_acd_min",
      'CARS_Hold_min': "$cars_hold_min",
      'ACW_min': "$acw_min",
      'THT_min': "$tht_min",
      'Outbound_Time_min': "$ob_min",
      'True_ACW_min': "$true_acw_min",
      'Total_Stafftime_min': "$tot_stafftime_min",
      'Total_Auxtime_min': "$tot_auxtime_min",
      'Avail_Time': "$avail_time",
      'AUX_1_min': "$aux1",
      'AUX_2_min': "$aux2",
      'AUX_3_min': "$aux3",
      'AUX_4_min': "$aux4",
      'AUX_5_min': "$aux5",
      'AUX_6_min': "$aux6",
      'Aux_7_min': "$aux7",
      'AUX_8_min': "$aux8",
      'AUX_9_min': "$aux9",
    }
  },
  {
    "$addFields": {
      "transport_time": datetime.utcnow()
    }
  }
]
