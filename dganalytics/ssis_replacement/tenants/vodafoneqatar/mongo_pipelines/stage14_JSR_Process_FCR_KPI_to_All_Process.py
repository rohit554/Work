aggr_pipeline  = [
  {
    "$lookup": {
      "from": "SIEBELACTIVITY",
      "localField": "activity_num",
      "foreignField": "activity_num",
      "as": "siebelactivity"
    }
  },
  {
    "$unwind": {
      "path": "$siebelactivity",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$addFields": {
      "fcr": {
        "$cond": {
          "if": {
            "$eq": [
              "$state",
              "FC"
            ]
          },
          "then": 1,
          "else": 0
        }
      },
      "total": 1,
      "account_type": {
        "$switch": {
          "branches": [
            {
              "case": {
                "$in": [
                  "$siebelactivity.major_product_name",
                  [
                    "10 Mbps Business Broadband and Phone Line",
                    "10 Mbps Enterprise Broadband",
                    "100 Mb Enterprise Broadband",
                    "100Mb Business Broadband and Phone Line",
                    "100Mb Superfast Broadband and Home Phone - New",
                    "100Mbps Superfast Broadband",
                    "10Mb Business Broadband and Phone Line",
                    "200Mbps Superfast Broadband",
                    "20Mb Superfast Broadband and Home Phone",
                    "20Mb Superfast Broadband and Home Phone - New",
                    "20Mbps Superfast Broadband",
                    "25 Mbps Business Broadband and Phone Line",
                    "25 Mbps Enterprise Broadband",
                    "300 Mbps Business Broadband and Phone Line",
                    "50 Mb Enterprise Broadband",
                    "50Mb Business Broadband and Phone Line",
                    "50Mb Superfast Broadband and Home Phone",
                    "50Mb Superfast Broadband and Home Phone - New",
                    "50Mbps Superfast Broadband",
                    "5Mb Superfast Broadband and Home Phone",
                    "5Mb Superfast Broadband and Home Phone - New",
                    "5Mbps Enterprise Broadband",
                    "Dedicated APN Unlimited",
                    "Giga Business 100",
                    "GigaHome Classic - F",
                    "GigaHome Essential - F",
                    "GigaHome Premium - F",
                    "GigaHome VIP - F",
                    "Home internet 250 -F",
                    "Home internet 350 -F",
                    "Home internet 500",
                    "Home internet 500 -F",
                    "IoT Shared Data Plan",
                    "IP VPN - Class of Services 1",
                    "Pearl Broadband 100Mbps Plan (3 Months Free)",
                    "Pearl Broadband 100Mbps Plan (Consumer)",
                    "Pearl Broadband 20Mbps Plan (3 Months Free)",
                    "Pearl Broadband 20Mbps Plan (Consumer)",
                    "Pearl Broadband 50Mbps Plan (3 Months Free)",
                    "Pearl Broadband 50Mbps Plan (Consumer)",
                    "Pearl Broadband 5Mbps Plan (3 Months Free)",
                    "Pearl Broadband 5Mbps Plan (Consumer)",
                    "Pearl Voice Consumer",
                    "Vodafone Corporate Internet 10Mbps",
                    "Vodafone Corporate Internet 15Mbps",
                    "Vodafone Corporate Internet 1Gbps",
                    "Vodafone Corporate Internet 1Mbps",
                    "Vodafone Corporate Internet 200Mbps",
                    "Vodafone Corporate Internet 20Mbps",
                    "Vodafone Corporate Internet 2Mbps",
                    "Vodafone Corporate Internet 50Mbps",
                    "Vodafone Corporate Internet 5Mbps"
                  ]
                ]
              },
              "then": "Fixed"
            },
            {
              "case": {
                "$and": [
                  {
                    "$eq": [
                      "$account_type",
                      "Residential"
                    ]
                  }
                ]
              },
              "then": "Consumer"
            },
            {
              "case": {
                "$and": [
                  {
                    "$eq": [
                      "$account_type",
                      "Business"
                    ]
                  }
                ]
              },
              "then": "Enterprise"
            }
          ],
          "default": "$account_type"
        }
      }
    }
  },
  {
    "$group": {
      "_id": {
        "segment_type": "$account_type",
        "services": "$service_type",
        "location": "$location",
        "report_date": {
          "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$report_date",
            "timezone": "+05:30"
          }
        }
      },
      "fcr_count": {
        "$sum": "$fcr"
      },
      "total_calls": {
        "$sum": "$total"
      }
    }
  },
  {
    "$project": {
      "location": "$_id.location",
      "lobs": None,
      "language": None,
      "arpu_segment": None,
      "site": None,
      "segment_type": "$_id.segment_type",
      "services": "$_id.services",
      "date": "$_id.report_date",
      "value1": "$fcr_count",
      "value2": "$total_calls",
      "kpi": "FCR",
      "_id": 0
    }
  }
]