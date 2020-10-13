from pytz import utc as tz_utc
from datetime import datetime
aggr_pipeline = [
    {
      "$match": {
        "report_date": {
          '$gte' : tz_utc.localize(datetime.strptime('2020-02-25','%Y-%m-%d') )
        }
      }
    },
  {
    "$project": {
      '_id': 0,
      'Q5_c_Goodwill_Has_this_section_and_relevant_activities_been_completed_correctly': '$goodwill',
      'Q5_d_Attachments_Have_the_all_critical_attachments_and_approvals_been_attached_to_the_front_of_th': '$attachments',
      'Agent_Name': '$agent_name',
      'Advisor_Status': '$advisor_status',
      'Channel': '$channel',
      'Q2_Interaction_Notes_Transcript_Link': '$intrction_notes',
      'Q3_b_Were_all_written_comms_to_the_customer_accurate_easily_understandable_address_the_customer': '$accuracy',
      'Q3_g_Did_the_advisor_send_a_CSAT_to_the_customer_if_appropriate': '$csat_sent',
      'Evaluation_Method': '$evaluation_method',
      'Q4_b_Did_the_advisor_engage_the_appropriate_stakeholders_and_record_provide_all_relevant_informatio': '$provide_info',
      'Q3_c_Did_the_advisor_demonstrate_the_usage_of_appropriate_soft_skills': '$soft_skill_demo',
      'Q3_Customer_Interactions': '$custm_intrctns',
      'Q4_c_Did_the_advisor_summarise_all_case_notes_templates_accurately_to_reflect_the_discussion_on': '$soft_skills',
      'Q1_Reporting_Detail': '$reporting_detail',
      'General_Data': '$general_data',
      'Evaluator_Name': '$evaluator_name',
      'Evaluation_Date': '$report_date',
      'Q3_d_Did_the_advisor_advocate_for_the_brand_provide_a_good_customer_experience': '$advocate_brand',
      'Proportional_Score': '$prop_score',
      'Comments': '$comments',
      'Q3_e_Did_the_advisor_make_commitments_respond_to_all_the_customers_interactions_within_the_requir': '$process_knowledge',
      'Q1_a_Capture_case_number_Enter_in_pop_up_notes_field': '$capture_case_no',
      'Q3_a_Did_the_advisor_understand_the_customer_s_problem_to_solve_as_well_as_their_desired_outcome': '$problem_understanding',
      'Q3_f_Was_all_the_required_information_outcome_provided_to_the_customer_accurate': '$accurate_info',
      'Case_Type': '$case_type',
      'Interaction_ID': '$interaction_id',
      'Sections_Questions': '$sections',
      'Q5_b_Rental_Requests_Has_this_section_and_relevant_activities_been_completed_correctly': '$rental_req',
      'Orig_Calib': '$orig',
      'Evaluation_Score': '$eval_score',
      'Q6_a_Has_the_advisor_followed_all_company_policies_and_ensured_that_Holden_is_compliant_with_the_re': '$compliant',
      'Q5_a_Have_the_SR_Customer_Vehicle_Dealer_Additional_Information_fields_been_filled_out_and_ar': '$system_knowledge',
      'Q4_a_Did_the_advisor_do_everything_in_their_control_to_investigate_progress_the_customers_concern': '$actions_in_their_control',
      'User_ID': '$user_id',
      'Q4_Investigation_Case_Notes': None,
      'Q5_Service_Request_Admin': None,
      'Q6_Overall': None
    }
  }
]