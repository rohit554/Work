aggr_pipeline = [
  {
    "$project": {
      "_id": 0,
      "email_id": 1,
      "avaya_name": 1,
      "first_name": 1,
      "last_name": 1,
      "siebel_id": 1
    }
  }
]