aggr_pipeline = [
  {
    "$project": {
      "_id": 0,
      'Full_Name':'$full_name', 
      'Role':'$role', 
      "Buisness_Unit":'$bu', 
      'Litmus_ID':'$litmus_id', 
      'Avaya_ID':'$avaya_id', 
      'Last_Name':'$last_name', 
      'IEX_ID':'$iex_id', 
      'Team_Leader':'$team_leader', 
      'First_Name':'$first_name', 
      'Email':'$email', 
      'QA_ID':'$qa_id', 
      'Status':'$status',
    }
  }
]