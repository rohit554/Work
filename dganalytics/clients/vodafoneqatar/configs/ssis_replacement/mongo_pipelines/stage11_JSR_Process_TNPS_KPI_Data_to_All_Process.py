aggr_pipeline = [
  {
    "$match": {
      "touchpoint": "Contact Centre - Inbound Live Call"
    }
  },
  {
    "$addFields": {
      "TNPS_VAL": {
        "$switch": {
          "branches": [
            {
              "case": {
                "$and": [
                  {
                    "$gte": [
                      "$overall_tnps",
                      0
                    ]
                  },
                  {
                    "$lt": [
                      "$overall_tnps",
                      7
                    ]
                  }
                ]
              },
              "then": "Detractor"
            },
            {
              "case": {
                "$and": [
                  {
                    "$gte": [
                      "$overall_tnps",
                      9
                    ]
                  },
                  {
                    "$lt": [
                      "$overall_tnps",
                      11
                    ]
                  }
                ]
              },
              "then": "Promoter"
            },
            {
              "case": {
                "$and": [
                  {
                    "$gte": [
                      "$overall_tnps",
                      7
                    ]
                  },
                  {
                    "$lt": [
                      "$overall_tnps",
                      9
                    ]
                  }
                ]
              },
              "then": "Passive"
            }
          ],
          "default": ""
        }
      },
      "customer_type": {
        "$switch": {
          "branches": [
            {
              "case": {
                "$and": [
                  {
                    "$eq": [
                      "$type_of_product",
                      "Fixed"
                    ]
                  }
                ]
              },
              "then": "Fixed"
            }
          ],
          "default": "$customer_type"
        }
      }
    }
  },
  {
    "$group": {
      "_id": {
        "segment_type": "$customer_type",
        "report_date": {
          "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$report_date"
          }
        },
        "service_type": "$service_type",
        "location": "$location",
        "language": "$survey_language"
      },
      "Detractor": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$TNPS_VAL",
                "Detractor"
              ]
            },
            1,
            0
          ]
        }
      },
      "Promoter": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$TNPS_VAL",
                "Promoter"
              ]
            },
            1,
            0
          ]
        }
      },
      "Passive": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$TNPS_VAL",
                "Passive"
              ]
            },
            1,
            0
          ]
        }
      },
      "FCR": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$issue_resolved",
                "Yes, on the first time I contacted Vodafone"
              ]
            },
            1,
            0
          ]
        }
      },
      "NFCR": {
        "$sum": {
          "$cond": [
            {
              "$or": [
                {
                  "$eq": [
                    "$issue_resolved",
                    "No"
                  ]
                },
                {
                  "$eq": [
                    "$issue_resolved",
                    "Yes, but I had to contact Vodafone more than once"
                  ]
                }
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    "$facet": {
      "TNPS": [
        {
          "$project": {
            "location": "$_id.location",
            "segment_type": "$_id.segment_type",
            "service_type": "$_id.service_type",
            "language": "$_id.language",
            "date": "$_id.report_date",
            "value1": {
              "$subtract": [
                "$Promoter",
                "$Detractor"
              ]
            },
            "value2": {
              "$add": [
                "$Promoter",
                "$Detractor",
                "$Passive"
              ]
            },
            "kpi": "TNPS",
            "_id": 0
          }
        }
      ],
      "FTF": [
        {
          "$project": {
            "location": "$_id.location",
            "segment_type": "$_id.segment_type",
            "language": "$_id.language",
            "service_type": "$_id.service_type",
            "date": "$_id.report_date",
            "value1": "$FCR",
            "value2": {
              "$add": [
                "$FCR",
                "$NFCR"
              ]
            },
            "kpi": "FTF",
            "_id": 0
          }
        }
      ]
    }
  },
  {
    "$project": {
      "common": {
        "$concatArrays": [
          "$TNPS",
          "$FTF"
        ]
      }
    }
  },
  {
    "$unwind": {
      "path": "$common"
    }
  },
  {
    "$project": {
      "location": "$common.location",
      "lobs": None,
      "language": "$common.language",
      "arpu_segment": None,
      "segment_type": "$common.segment_type",
      "services": "$common.service_type",
      "site": None,
      "date": "$common.date",
      "value1": "$common.value1",
      "value2": "$common.value2",
      "kpi": "$common.kpi"
    }
  }
]