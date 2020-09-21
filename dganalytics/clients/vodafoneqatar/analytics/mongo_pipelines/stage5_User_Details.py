aggr_pipeline = [
  {
    "$match": {
      "siebel_id": {
        "$nin": [
          "NA",
          "na",
          "",
          " ",
          "-"
        ]
      }
    }
  },
  {
    "$project": {
      "user_id": "$siebel_id",
      "cms_name": "$cms_name",
      "lm_user_id": {
        "$ifNull": [
          "$lm_siebel_id",
          ""
        ]
      },
      "_id": 0,
      "location": 1,
      "first_name": 1,
      "last_name": 1,
      "line_manager": 1,
      "role": 1,
      "sub_lob": 1,
      "lob": 1,
      "team_leader": 1,
      "email_id": 1,
      "gender": 1,
      "language": 1,
      "image": {
        "$ifNull": [
          "$image",
          ""
        ]
      }
    }
  }
]