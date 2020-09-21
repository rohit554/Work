aggr_pipeline = [
  {
    "$project": {
      "_id": 0,
      "owner": 1,
      "account_type": 1,
      "agent": 1,
      "comments": 1,
      "journey": 1,
      "counts": 1,
      "created_month": 1,
      "description": 1,
      "check": 1,
      "activity_num": 1,
      "type": 1,
      "crm_segment": 1,
      "date_1": 1,
      "account_num": 1,
      "service_type": 1,
      "times": 1,
      "created_year": 1,
      "resolution_code": 1,
      "user_id": 1,
      "location": 1,
      "state": 1,
      "category": 1,
      "date": {
        "$dateToString": {
          "format": "%Y-%m-%d",
          "date": "$report_date"
        }
      },
      "status": 1
    }
  }
]