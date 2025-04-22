load_id_expr = '''SUBSTR(shpmt_id,-9)'''

carr_desc_expr = '''
    CASE 
        WHEN carr_num = 15329857 THEN 'CIRCLE 8 LOGISTICS INC'
        WHEN carr_num = 15322444 THEN 'GREYPOINT INC'
        WHEN carr_num = 15327462 THEN 'DUPRE LOGISTICS, LLC'
        WHEN carr_num = 15169031 THEN 'TOTAL QUALITY LOGISTICS LLC'
        WHEN carr_num = 15306674 THEN 'UNIVERSAL TRUCKLOAD INC'
        WHEN carr_num = 15164435 THEN 'US XPRESS INC'
        WHEN carr_num = 20512523 THEN 'US XPRESS INC'
        ELSE carr_desc
    END
'''

true_frt_type_desc_expr = '''
    CASE 
        WHEN CAST(COALESCE(REGEXP_EXTRACT(ship_to_party_code, '^[0-9]', 0), '-1') AS INT) >= 0 THEN 'CUSTOMER'
        ELSE 'INTERPLANT'
    END
'''

customer_desc_expr = '''
    CASE 
        WHEN customer_code LIKE 'CARDINAL%' THEN customer_lvl3_desc
        WHEN customer_code LIKE 'WALMART%' THEN 'WALMART'
        WHEN customer_code LIKE 'COSTCO COMPANIES%%' THEN 'COSTCO'
        WHEN customer_code LIKE 'JETRO%' THEN 'JETRO'
        WHEN customer_code LIKE 'TIENDAS SINDICALES%' THEN 'TIENDAS SINDICALES'
        ELSE customer_code
    END
'''

origin_zone_ship_from_code_expr = '''
    CASE
        WHEN substr(coalesce(origin_zone_code, ''), 1, 3) <> "SF_" THEN sl_origin_zone_ship_from_code
        ELSE origin_zone_name 
    END
'''

actual_ship_datetm_expr = '''
    CASE 
        WHEN SUBSTR(actual_ship_datetm, 3, 1) = ":" THEN actual_ship_datetm 
        ELSE CONCAT(SUBSTR(actual_ship_datetm,1,2), ":", SUBSTR(actual_ship_datetm,3,2), ":", SUBSTR(actual_ship_datetm,5,2)) 
    END'''

true_fa_flag_expr = '''
    CASE
        WHEN freight_auction_val = "YES" THEN "Y"
        ELSE "N"
    END'''

carr_flag_expr = '''
    CASE
        WHEN avg_award_weekly_vol_qty > 0.01 THEN 1
        ELSE 0
    END'''

min_rn_code_expr = '''
    CASE
        WHEN min_event_datetm_rn = 1 THEN plan_shpmt_end_tmstp_calc
        ELSE NULL
    END'''

max_rn_code_expr = '''
    CASE
        WHEN max_event_datetm_rn = 1 THEN plan_shpmt_end_tmstp_calc
        ELSE NULL
    END'''

count_expr = '''
    CASE
        WHEN ship_cond_code = "" THEN 0
        ELSE otd_cnt
    END +
    CASE
        WHEN lot_ontime_status_last_appt_val = "" THEN 0
        ELSE tat_late_counter_val
    END'''

tms_code_expr = '''
    CASE
        WHEN max_event_datetm_rn = 1 THEN service_tms_code
        ELSE NULL
    END'''

fa_flag_expr = '''
    CASE
        WHEN max_frt_auction_code = "NO" THEN max_frt_auction_code
        WHEN max_frt_auction_code = "YES" THEN max_frt_auction_code
        ELSE NULL
    END'''

pct_lot_expr = '''max_otd_cnt / max_shpmt_cnt * 100'''

ci_code_expr = '''
    CASE
        WHEN frt_type_code = "C" THEN "CUSTOMER"
        WHEN frt_type_code = "I" THEN "INTERPLANT"
        WHEN frt_type_code = "E" THEN "EXPORT"
        ELSE ""
    END'''

bin_code_expr = '''
    CASE
        WHEN distance_qty < 250  THEN "< 250"
        WHEN distance_qty < 500  THEN "250 - 499"
        WHEN distance_qty < 1000 THEN "500 - 1000"
        ELSE "> 1000"
    END'''

flag_case_code_expr = '''
    CASE
        WHEN child_shpmt_num = "" THEN ""
        ELSE shpmt_id
    END'''

no_shpmt_expr = '''
    CASE
        WHEN ship_cond_code = "" THEN 0
        ELSE otd_cnt
    END +
    CASE
        WHEN lot_ontime_status_last_appt_val = "" THEN 0
        ELSE tat_late_counter_val
    END'''

aot_meas_expr = '''
    CASE
        WHEN first_appt_dlvry_tmstp = ""
            AND actual_dlvry_tmstp         = ""
                THEN 0
        WHEN request_dlvry_to_tmstp        = ""
            AND actual_dlvry_tmstp         = ""
                THEN 0
        ELSE 1
    END'''

iot_meas_expr = '''
    CASE
        WHEN frt_type_desc                    = "INTERPLANT"
            OR frt_type_desc                  = "EXPORT"
            AND first_appt_dlvry_tmstp = ""
            AND actual_dlvry_tmstp            = ""
                THEN 0
        WHEN frt_type_desc                    = "INTERPLANT"
            OR frt_type_desc                  = "EXPORT"
            AND request_dlvry_to_tmstp        = ""
            AND actual_dlvry_tmstp            = ""
                THEN 0
        WHEN frt_type_desc = "INTERPLANT"
            OR frt_type_desc                  = "EXPORT"
            THEN 1
        ELSE 0
    END'''

lot_meas_expr = '''
    CASE
        WHEN final_lrdt_tmstp         = ""
            AND actual_load_end_tmstp = "" 
                THEN 0
        ELSE 1
    END'''

csot_pos_expr = '''
    CASE
        WHEN UPPER(measrbl_flag) = "Y" 
            AND UPPER(customer_desc) IN ("CVS", "RITE AID CORPORATION US", "WALGREEN ()")
                THEN load_id
        WHEN UPPER(measrbl_flag) = "Y"
            THEN cust_po_num
        ELSE ""
    END'''

aot_loads_expr = '''
    CASE
        WHEN aot_measrbl_flag = 1 THEN load_id
        ELSE NULL
    END'''

aot_loads_on_time_expr = '''
    CASE
        WHEN aot_measrbl_flag                                 = 1
            AND COALESCE(first_appt_dlvry_tmstp,"") != ""
            AND actual_dlvry_tmstp <= first_appt_dlvry_tmstp
                THEN load_id
        WHEN aot_measrbl_flag                                = 1
            AND COALESCE(first_appt_dlvry_tmstp,"") = ""
            AND actual_dlvry_tmstp <= request_dlvry_to_tmstp
                THEN load_id
        ELSE NULL
    END'''

aot_late_loads_expr = '''
    CASE
        WHEN aot_measrbl_flag                                 = 1
            AND COALESCE(first_appt_dlvry_tmstp,"") != ""
            AND actual_dlvry_tmstp > first_appt_dlvry_tmstp
                THEN load_id
        WHEN aot_measrbl_flag                                = 1
            AND COALESCE(first_appt_dlvry_tmstp,"") = ""
            AND actual_dlvry_tmstp > request_dlvry_to_tmstp
                THEN load_id
        ELSE NULL
    END'''

iot_loads_expr = '''
    CASE
        WHEN iot_measrbl_flag = 1 THEN load_id
        ELSE NULL
    END'''

iot_loads_on_time_expr = '''
    CASE
        WHEN max_event_datetm_rn_iot                                 = 1
            AND csot_failure_reason_bucket_name = "On Time"
                THEN load_id
        ELSE NULL
    END'''

iot_late_loads_expr = '''
    CASE
        WHEN max_event_datetm_rn_iot                                 = 1
            AND csot_failure_reason_bucket_name != "On Time"
                THEN load_id
        ELSE NULL
    END'''

lot_loads_expr = '''
    CASE
        WHEN lot_measrbl_flag = 1 THEN load_id
        ELSE NULL
    END'''

lot_loads_on_time_expr = '''
    CASE
        WHEN lot_measrbl_flag      = 1
            AND load_on_time_pct = 100 
                THEN load_id
        ELSE NULL
    END'''

lot_late_loads_expr = '''
    CASE
        WHEN lot_measrbl_flag      = 1
            AND load_on_time_pct < 100 
                THEN load_id
        ELSE NULL
    END'''

csot_intermediate_failure_reason_bucket_updated_expr = '''
    CASE
        WHEN customer_desc = "CVS"
            AND str_carr_num IN ("0015302264", "0010033473")
                THEN "On Time"
        WHEN customer_desc IN ("BIG LOTS, INC ()", "HASKEL TRADING US", "OLLIES BARGAIN OUTLET US")
                THEN "On Time"
        WHEN customer_lvl4_desc   LIKE "AMELIA\'S GROCERY OUTLET"
            OR customer_lvl4_desc LIKE "GROCERY OUTLET 95"
            OR customer_lvl4_desc LIKE "GROCERY OUTLET 97"
            OR customer_lvl4_desc LIKE "GROCERY OUTLETS"
                THEN "On Time"
        ELSE csot_scrubs_value
    END'''

csot_failure_reason_bucket_updated_expr = '''
    COALESCE(csot_intrmdt_failure_reason_bucket_updated_name, csot_failure_reason_bucket_name)'''

csot_on_time_expr = '''
    CASE
        WHEN UPPER(measrbl_flag) = "Y"
            THEN
            CASE
                WHEN COALESCE(csot_intrmdt_failure_reason_bucket_updated_name, 
                csot_failure_reason_bucket_name) = "On Time"
                    THEN
                    CASE
                        WHEN customer_desc   LIKE "CVS"
                            OR customer_desc LIKE "RITE AID CORPORATION US"
                            OR customer_desc LIKE "WALGREEN ()"
                            THEN load_id
                            ELSE cust_po_num
                    END
                WHEN COALESCE(csot_intrmdt_failure_reason_bucket_updated_name, 
                csot_failure_reason_bucket_name) = NULL
                    AND csot_failure_reason_bucket_name = "On Time"
                    THEN
                    CASE
                        WHEN customer_desc   LIKE "CVS"
                            OR customer_desc LIKE "RITE AID CORPORATION US"
                            OR customer_desc LIKE "WALGREEN ()"
                            THEN load_id
                            ELSE cust_po_num
                    END
                    ELSE NULL
            END
            ELSE NULL
    END'''

csot_not_on_time_expr = '''
CASE
        WHEN UPPER(measrbl_flag) = "Y"
            THEN
            CASE
                WHEN COALESCE(csot_intrmdt_failure_reason_bucket_updated_name, 
                csot_failure_reason_bucket_name) != NULL
                    OR COALESCE(csot_intrmdt_failure_reason_bucket_updated_name, 
                    csot_failure_reason_bucket_name) != "On Time"
                    THEN
                    CASE
                        WHEN customer_desc   LIKE "CVS"
                            OR customer_desc LIKE "RITE AID CORPORATION US"
                            OR customer_desc LIKE "WALGREEN ()"
                            THEN load_id
                            ELSE cust_po_num
                    END
                WHEN csot_failure_reason_bucket_name   != NULL
                    OR csot_failure_reason_bucket_name != "On Time"
                    THEN
                    CASE
                        WHEN customer_desc   LIKE "CVS"
                            OR customer_desc LIKE "RITE AID CORPORATION US"
                            OR customer_desc LIKE "WALGREEN ()"
                            THEN load_id
                            ELSE cust_po_num
                    END
                    ELSE NULL
            END
            ELSE NULL
    END'''

csot_update_reason_code_expr = '''
    COALESCE(csot_update_reason_code1, csot_update_reason_code2, csot_update_reason_code3)'''

reason_code_expr = '''
    COALESCE(reason_code1, reason_code2, reason_code3)'''

aot_reason_code_expr = '''
    COALESCE(aot_reason_code1, aot_reason_code2, aot_reason_code3)'''

#dest_ship_from_code_expr = '''
#    CASE
#        WHEN frt_type_desc = "CUSTOMER" AND length(ship_to_party_code)=10
#                THEN ship_to_party_code
#        WHEN frt_type_desc = "CUSTOMER" AND length(ship_to_party_code)<10
#                THEN sl_origin_zone_ship_from_code
#        WHEN frt_type_desc = "INTERPLANT"
#                OR frt_type_desc = "EXPORT"
#                THEN sl_origin_zone_ship_from_code
#        ELSE NULL
#    END'''
dest_ship_from_code_expr = '''
    CASE
        WHEN true_frt_type_desc = "CUSTOMER"
                THEN ship_to_party_code
        WHEN true_frt_type_desc = "INTERPLANT"
                THEN customer_desc
        ELSE NULL
    END'''
