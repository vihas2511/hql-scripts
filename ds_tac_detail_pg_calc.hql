USE ${hivevar:database};

CREATE temp view operational_tariff AS
select 
`source`.`lane_origin_zone_code` as `lane origin zone`,
`source`.`lane_dstn_zone_id` as `lane destination zone`,
`source`.`origin_corp_code` as `origin corporate id 2`,
`source`.`dest_zip_code` as `destination zip`,
`source`.`dest_loc_desc` as `dest location description`,
`source`.`dest_corp_code` as `dest corporate id2`,
`source`.`exclude_from_optimization_flag` as `exclude from optimization`,
`source`.`exclude_from_cst_flag` as `exclude from cst`,
`source`.`tariff_id` as `tariff number`,
`source`.`carrier_id` as `carrier id`,
`source`.`carrier_name` as `carrier description`,
`source`.`service_code` as `service`,
`source`.`service_desc` as `service description`,
`source`.`cea_profile_code` as `cea profile id`,
`source`.`train_operating_day_qty` as `number of train op days`,
`source`.`train_day_of_week_list` as `train days of week`,
`source`.`transit_time_val` as `transit time`,
`source`.`charge_code` as `charge code`,
`source`.`charge_desc` as `charge description`,
`source`.`mile_contract_rate` as `rate`,
`source`.`payment_crncy_code` as `payment currency`,
`source`.`rate_unit_code` as `rating unit`,
`source`.`base_charge_amt` as `base charge`,
`source`.`min_charge_amt` as `minimum charge`,
`source`.`rate_eff_date` as `rate effective date`,
`source`.`rate_exp_date` as `rate exp date`,
`source`.`min_weight_qty` as `minimum weight`,
`source`.`min_floor_postn_cnt` as `minimum floor positions`,
`source`.`max_floor_postn_cnt` as `maximum floor positions`,
`source`.`max_weight_qty` as `maximum weight`,
`source`.`auto_accept_tender_override_flag` as `auto-accept tender override`,
`source`.`freight_auction_eligibility_code` as `freight auction eligibility`,
`source`.`alloc_type_code` as `allocation type`,
`source`.`alloc_profile_val` as `allocation profile basis`,
`source`.`award_rate` as `awards`,
`source`.`weekly_alloc_pct` as `percent weekly allocation`,
`source`.`mon_min_load_qty` as `mon min (loads)`,
`source`.`mon_max_load_qty` as `mon max (loads)`,
`source`.`tue_min_load_qty` as `tue min (loads)`,
`source`.`tue_max_load_qty` as `tue max (loads)`,
`source`.`wed_min_load_qty` as `wed min (loads)`,
`source`.`wed_max_load_qty` as `wed max (loads)`,
`source`.`thu_min_load_qty` as `thu min (loads)`,
`source`.`thu_max_load_qty` as `thu max (loads)`,
`source`.`fri_min_load_qty` as `fri min (loads)`,
`source`.`fri_max_load_qty` as `fri max (loads)`,
`source`.`sat_min_load_qty` as `sat min (loads)`,
`source`.`sat_max_load_qty` as `sat max (loads)`,
`source`.`sun_min_load_qty` as `sun min (loads)`,
`source`.`sun_max_load_qty` as `sun max (loads)`,
`source`.`dlvry_schedule_code` as `delivery schedule id`,
`source`.`equip_type_code` as `equipment type`,
`source`.`status_code` as `status`,
`source`.`rate_code` as `rate code`,
`source`.`service_grade_val` as `service grade`,
`source`.`tariff_desc` as `tariff id`,
`source`.`max_no_of_shpmt_cnt` as `maximum no of shipments`,
`source`.`cust_id` as `customer id`,
`source`.`cust_desc` as `customer description`,
`source`.`equip_code` as `equipment code`,
`source`.`resource_project_code` as `resource project id`,
`source`.`report_date` as `report date`,
case
  when report_date is not null and week_day_code !='Mon'
  Then week_begin_date
  else report_date
end as TAC_report_date
from dp_trans_vsblt_bw.operational_tariff_star as source 
left join tac_calendar_lkp as lkp
on lkp.actual_goods_issue_date = source.`report_date` 
;

CREATE temp view operational_tariff_stage_CR16 as
select
  `lane origin zone`,
  `lane destination zone`,
  case
    when (`origin corporate id 2` is null or `origin corporate id 2` = '') then `lane origin zone`
    else `origin corporate id 2`
  end as `origin corporate id 2`,
  case
    when (`dest corporate id2` is null or `dest corporate id2` = '') then `lane destination zone`
    else `dest corporate id2`
  end as `dest corporate id2`,
  `exclude from optimization`,
  `exclude from cst`,
  `tariff number`,
  `carrier id`,
  `carrier description`,
  `service`,
  `service description`,
  `cea profile id`,
  `number of train op days`,
  `train days of week`,
  `transit time`,
  `charge code`,
  `charge description`,
  `rate`,
  `payment currency`,
  `rating unit`,
  `base charge`,
  `minimum charge`,
  `rate effective date`,
  `rate exp date`,
  `minimum weight`,
  `minimum floor positions`,
  `maximum floor positions`,
  `maximum weight`,
  `auto-accept tender override`,
  `freight auction eligibility`,
  `allocation type`,
  `allocation profile basis`,
  `awards`,
  `percent weekly allocation`,
  `mon min (loads)`,
  `mon max (loads)`,
  `tue min (loads)`,
  `tue max (loads)`,
  `wed min (loads)`,
  `wed max (loads)`,
  `thu min (loads)`,
  `thu max (loads)`,
  `fri min (loads)`,
  `fri max (loads)`,
  `sat min (loads)`,
  `sat max (loads)`,
  `sun min (loads)`,
  `sun max (loads)`,
  `delivery schedule id`,
  `equipment type`,
  `status`,
  `rate code`,
  `service grade`,
  `tariff id`,
  `maximum no of shipments`,
  `customer id`,
  `customer description`,
  `equipment code`,
  `resource project id`,
  `report date`,
   TAC_report_date
from
  operational_tariff
;
  
CREATE temp view operational_tariff_Zone_Awards_CR16 as
select `report date`, 
       `lane origin zone`, 
       `lane destination zone`, 
        replace(`origin corporate id 2`,'&','') as `origin corporate id 2`, 
        replace(`dest corporate id2`,'&','') as `dest corporate id2`, 
       `carrier id`, 
       `service`, 
       Max(awards) as awards,
       TAC_report_date
from operational_tariff_stage_CR16
group by TAC_report_date, 
        `report date`,
        `lane origin zone`, 
        `lane destination zone`, 
        `origin corporate id 2`, 
        `dest corporate id2`, 
        `carrier id`, 
        `service`
;
		
CREATE TEMP VIEW operational_tariff_SF_Awards_CR16 as
select  concat (rtrim(left(`origin corporate id 2`,20)),'-',left(`dest corporate id2`,20)) as `SF Lane`,
        TAC_report_date, 
        `report date`, 
        `origin corporate id 2`, 
        `dest corporate id2`, 
        `carrier id`, 
        `service`, 
        SUM(awards) as SF_awards 
from operational_tariff_Zone_Awards_CR16
group by `SF Lane`, 
          `report date`, 
          TAC_report_date, 
          `origin corporate id 2`, 
          `dest corporate id2`, 
          `carrier id`, 
          `service`
;


CREATE TEMPORARY VIEW tmp_tac_tender_pg_1_CR16 AS
SELECT
       actual_ship_date
     , tac_day_of_year
     , workweekday
     , week_begin_date
     , monthtype445
     , calendar_year
     , month_date
     , week_number
     , day_of_week
     , load_id
     , origin_sf
     , origin_location_id
     , origin_zone
     , destination_sf
     , destination_zone
     , destination_location_id
     , customer_id
     , customer_id_description
     , carrier_id
     , carrier_description
     , mode
     , tender_event_type_code
     , tender_reason_code
     , tender_event_date
     , tender_event_time
     , tender_event_date_a_time
     , transportation_planning_point
     , departure_country_code
     , sap_original_shipdate
     , original_tendered_ship_date
     , day_of_the_week_of_original_tendered_shipdate
     , actual_ship_time
     , actual_ship_date_and_time
     , sold_to_n
     , no_of_days_in_week
     , no_of_days_in_month
     , scac
     , carrier_mode_description
     , tariff_id
     , schedule_id
     , tender_event_type_description
     , tender_acceptance_key
     , tender_reason_code_description
     , scheduled_date
     , scheduled_time
     , average_awarded_weekly_volume
     , daily_award
     , day_of_the_week
     , allocation_type
     , sun_max_loads
     , mon_max_loads
     , tue_max_loads
     , wed_max_loads
     , thu_max_loads
     , fri_max_loads
     , sat_max_loads
     , gbu_per_shipping_site
     , shipping_conditions
     , postal_code_raw_tms
     , postal_code_final_stop
     , country_from
     , country_to
     , freight_auction_flag
     , true_fa_flag
     , freight_type
     , operational_freight_type
     , pre_tms_upgrade_flag
     , data_structure_version
     , primary_carrier_flag
     , tendered_back_to_primary_carrier_with_no_fa_adjustment
     , tendered_back_to_primary_carrier_with__fa_adjustment
     , tender_accepted_loads_with_no_fa
     , tender_accepted_loads_with_fa
     , tender_rejected_loads
     , previous_tender_date_a_time
     , time_between_tender_events
     , canceled_due_to_no_response
     , customer
     , customer_level_1_id
     , customer_level_1_description
     , customer_level_2_id
     , customer_level_2_description
     , customer_level_3_id
     , customer_level_3_description
     , customer_level_4_id
     , customer_level_4_description
     , customer_level_5_id
     , customer_level_5_description
     , customer_level_6_id
     , customer_level_6_description
     , customer_level_7_id
     , customer_level_7_description
     , customer_level_8_id
     , customer_level_8_description
     , customer_level_9_id
     , customer_level_9_description
     , customer_level_10_id
     , customer_level_10_description
     , customer_level_11_id
     , customer_level_11_description
     , customer_level_12_id
     , customer_level_12_description
     , actual_carrier_id
     , actual_carrier_description
     , actual_carrier_total_transportation_cost_usd
     , linehaul
     , incremental_fa
     , cnc_carrier_mix
     , unsourced
     , fuel
     , accessorial
     , appliances
     , baby_care
     , chemicals
     , fabric_care
     , family_care
     , fem_care
     , hair_care
     , home_care
     , oral_care
     , phc
     , shave_care
     , skin_a_personal_care
     , other
     , tfts_load_tmstp
     , load_from_file
     , bd_mod_tmstp
     , historical_data_structure_flag
     , tms_service_code
     , scotts_magical_field_of_dreams
     , concat(carrier_id,'-',`tms_service_code`, '-', `load_id`, '-', scotts_magical_field_of_dreams) AS cassell_key
     , to_date(`tender_event_date_a_time`)                                                            AS tender_date_time_type
     , TO_DATE(current_timestamp())                                                                   AS `current_date`
--     , MAX(week_begin_date) OVER(ORDER BY workweekday)                                                AS max_begin_date
FROM
       (
                  SELECT
                             table_569288147_1.actual_goods_issue_date AS actual_ship_date
                           , table_569288147_1.day_of_year_num         AS tac_day_of_year
                           , table_569288147_1.work_weekday_num        AS workweekday
                           , table_569288147_1.week_begin_date         AS week_begin_date
                           , table_569288147_1.month_type_val          AS monthtype445
                           , table_569288147_1.cal_year_num            AS calendar_year
                           , table_569288147_1.month_date              AS month_date
                           , table_569288147_1.week_num                AS week_number
                           , table_569288147_1.week_day_code           AS day_of_week
                           , table_569288147_2.load_id                 AS load_id
                           , table_569288147_2.origin_sf               AS origin_sf
                           , table_569288147_2.origin_location_id      AS origin_location_id
                           , table_569288147_2.origin_zone             AS origin_zone
                           , table_569288147_2.destination_sf          AS destination_sf
                           , table_569288147_2.destination_zone        AS destination_zone
                           , table_569288147_2.destination_location_id AS destination_location_id
                           , table_569288147_2.customer_id             AS customer_id
                           , table_569288147_2.customer_id_description AS customer_id_description
                           , table_569288147_2.carrier_id              AS carrier_id
                           , table_569288147_2.carrier_description     AS carrier_description
                             --, table_569288147_2.tms_service_code                                       AS tms_service_code
                           , table_569288147_2.mode                                                   AS mode
                           , table_569288147_2.tender_event_type_code                                 AS tender_event_type_code
                           , table_569288147_2.tender_reason_code                                     AS tender_reason_code
                           , table_569288147_2.tender_event_date                                      AS tender_event_date
                           , table_569288147_2.tender_event_time                                      AS tender_event_time
                           , table_569288147_2.tender_event_date_a_time                               AS tender_event_date_a_time
                           , table_569288147_2.transportation_planning_point                          AS transportation_planning_point
                           , table_569288147_2.departure_country_code                                 AS departure_country_code
                           , table_569288147_2.sap_original_shipdate                                  AS sap_original_shipdate
                           , table_569288147_2.original_tendered_ship_date                            AS original_tendered_ship_date
                           , table_569288147_2.day_of_the_week_of_original_tendered_shipdate          AS day_of_the_week_of_original_tendered_shipdate
                           , table_569288147_2.actual_ship_time                                       AS actual_ship_time
                           , table_569288147_2.actual_ship_date_and_time                              AS actual_ship_date_and_time
                           , table_569288147_2.sold_to_n                                              AS sold_to_n
                           , table_569288147_2.no_of_days_in_week                                     AS no_of_days_in_week
                           , table_569288147_2.no_of_days_in_month                                    AS no_of_days_in_month
                           , table_569288147_2.scac                                                   AS scac
                           , table_569288147_2.carrier_mode_description                               AS carrier_mode_description
                           , table_569288147_2.tariff_id                                              AS tariff_id
                           , table_569288147_2.schedule_id                                            AS schedule_id
                           , table_569288147_2.tender_event_type_description                          AS tender_event_type_description
                           , table_569288147_2.tender_acceptance_key                                  AS tender_acceptance_key
                           , table_569288147_2.tender_reason_code_description                         AS tender_reason_code_description
                           , table_569288147_2.scheduled_date                                         AS scheduled_date
                           , table_569288147_2.scheduled_time                                         AS scheduled_time
                           , table_569288147_2.average_awarded_weekly_volume                          AS average_awarded_weekly_volume
                           , table_569288147_2.daily_award                                            AS daily_award
                           , table_569288147_2.day_of_the_week                                        AS day_of_the_week
                           , table_569288147_2.allocation_type                                        AS allocation_type
                           , table_569288147_2.sun_max_loads                                        AS sun_max_loads
                           , table_569288147_2.mon_max_loads                                        AS mon_max_loads
                           , table_569288147_2.tue_max_loads                                        AS tue_max_loads
                           , table_569288147_2.wed_max_loads                                        AS wed_max_loads
                           , table_569288147_2.thu_max_loads                                        AS thu_max_loads
                           , table_569288147_2.fri_max_loads                                        AS fri_max_loads
                           , table_569288147_2.sat_max_loads                                        AS sat_max_loads
                           , table_569288147_2.gbu_per_shipping_site                                  AS gbu_per_shipping_site
                           , table_569288147_2.shipping_conditions                                    AS shipping_conditions
                           , table_569288147_2.postal_code_raw_tms                                    AS postal_code_raw_tms
                           , table_569288147_2.postal_code_final_stop                                 AS postal_code_final_stop
                           , table_569288147_2.country_from                                           AS country_from
                           , table_569288147_2.country_to                                             AS country_to
                           , table_569288147_2.freight_auction_flag                                   AS freight_auction_flag
                           , table_569288147_2.true_fa_flag                                           AS true_fa_flag
                           , table_569288147_2.freight_type                                           AS freight_type
                           , table_569288147_2.operational_freight_type                               AS operational_freight_type
                           , table_569288147_2.pre_tms_upgrade_flag                                   AS pre_tms_upgrade_flag
                           , table_569288147_2.data_structure_version                                 AS data_structure_version
                           , table_569288147_2.primary_carrier_flag                                   AS primary_carrier_flag
                           , table_569288147_2.tendered_back_to_primary_carrier_with_no_fa_adjustment AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                           , table_569288147_2.tendered_back_to_primary_carrier_with__fa_adjustment   AS tendered_back_to_primary_carrier_with__fa_adjustment
                           , table_569288147_2.tender_accepted_loads_with_no_fa                       AS tender_accepted_loads_with_no_fa
                           , table_569288147_2.tender_accepted_loads_with_fa                          AS tender_accepted_loads_with_fa
                           , table_569288147_2.tender_rejected_loads                                  AS tender_rejected_loads
                           , table_569288147_2.previous_tender_date_a_time                            AS previous_tender_date_a_time
                           , table_569288147_2.time_between_tender_events                             AS time_between_tender_events
                           , table_569288147_2.canceled_due_to_no_response                            AS canceled_due_to_no_response
                           , table_569288147_2.customer                                               AS customer
                           , table_569288147_2.customer_level_1_id                                    AS customer_level_1_id
                           , table_569288147_2.customer_level_1_description                           AS customer_level_1_description
                           , table_569288147_2.customer_level_2_id                                    AS customer_level_2_id
                           , table_569288147_2.customer_level_2_description                           AS customer_level_2_description
                           , table_569288147_2.customer_level_3_id                                    AS customer_level_3_id
                           , table_569288147_2.customer_level_3_description                           AS customer_level_3_description
                           , table_569288147_2.customer_level_4_id                                    AS customer_level_4_id
                           , table_569288147_2.customer_level_4_description                           AS customer_level_4_description
                           , table_569288147_2.customer_level_5_id                                    AS customer_level_5_id
                           , table_569288147_2.customer_level_5_description                           AS customer_level_5_description
                           , table_569288147_2.customer_level_6_id                                    AS customer_level_6_id
                           , table_569288147_2.customer_level_6_description                           AS customer_level_6_description
                           , table_569288147_2.customer_level_7_id                                    AS customer_level_7_id
                           , table_569288147_2.customer_level_7_description                           AS customer_level_7_description
                           , table_569288147_2.customer_level_8_id                                    AS customer_level_8_id
                           , table_569288147_2.customer_level_8_description                           AS customer_level_8_description
                           , table_569288147_2.customer_level_9_id                                    AS customer_level_9_id
                           , table_569288147_2.customer_level_9_description                           AS customer_level_9_description
                           , table_569288147_2.customer_level_10_id                                   AS customer_level_10_id
                           , table_569288147_2.customer_level_10_description                          AS customer_level_10_description
                           , table_569288147_2.customer_level_11_id                                   AS customer_level_11_id
                           , table_569288147_2.customer_level_11_description                          AS customer_level_11_description
                           , table_569288147_2.customer_level_12_id                                   AS customer_level_12_id
                           , table_569288147_2.customer_level_12_description                          AS customer_level_12_description
                           , table_569288147_2.actual_carrier_id                                      AS actual_carrier_id
                           , table_569288147_2.actual_carrier_description                             AS actual_carrier_description
                           , table_569288147_2.actual_carrier_total_transportation_cost_usd           AS actual_carrier_total_transportation_cost_usd
                           , table_569288147_2.linehaul                                               AS linehaul
                           , table_569288147_2.incremental_fa                                         AS incremental_fa
                           , table_569288147_2.cnc_carrier_mix                                        AS cnc_carrier_mix
                           , table_569288147_2.unsourced                                              AS unsourced
                           , table_569288147_2.fuel                                                   AS fuel
                           , table_569288147_2.accessorial                                           AS accessorial
                           , table_569288147_2.appliances                                             AS appliances
                           , table_569288147_2.baby_care                                              AS baby_care
                           , table_569288147_2.chemicals                                              AS chemicals
                           , table_569288147_2.fabric_care                                            AS fabric_care
                           , table_569288147_2.family_care                                            AS family_care
                           , table_569288147_2.fem_care                                               AS fem_care
                           , table_569288147_2.hair_care                                              AS hair_care
                           , table_569288147_2.home_care                                              AS home_care
                           , table_569288147_2.oral_care                                              AS oral_care
                           , table_569288147_2.phc                                                    AS phc
                           , table_569288147_2.shave_care                                             AS shave_care
                           , table_569288147_2.skin_a_personal_care                                   AS skin_a_personal_care
                           , table_569288147_2.other                                                  AS other
                           , table_569288147_2.tfts_load_tmstp                                        AS tfts_load_tmstp
                           , table_569288147_2.load_from_file                                         AS load_from_file
                           , table_569288147_2.bd_mod_tmstp                                           AS bd_mod_tmstp
                           , table_569288147_2.historical_data_structure_flag                         AS historical_data_structure_flag
                           , CASE
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'METL'
                                                   then 'W28T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'PCD6'
                                                   then 'WCP6'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'PCD7'
                                                   then 'WCP7'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'PIM6'
                                                   then 'WAP6'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'SIM5'
                                                   then 'WAS5'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'SIM6'
                                                   then 'WAS6'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'SIM7'
                                                   then 'WAS7'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'SLD7'
                                                   then 'WFS7'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TECO'
                                                   then 'W15T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TELO'
                                                   then 'W17T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TL'
                                                   then 'W01T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TL2R'
                                                   then 'W23T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLBR'
                                                   then 'W03T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLCD'
                                                   then 'W18T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLCG'
                                                   then 'W04T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLDD'
                                                   then 'W25T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLDE'
                                                   then 'W06T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLDS'
                                                   then 'W19T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLEO'
                                                   then 'W13T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLEV'
                                                   then 'W12T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLHT'
                                                   then 'W09T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLHZ'
                                                   then 'W22T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLJ1'
                                                   then 'W26T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLLE'
                                                   then 'W16T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLLG'
                                                   then 'W07T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLLH'
                                                   then 'W11T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLNO'
                                                   then 'W05T'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLOC'
                                                   then 'TOC'
                                        when to_date(table_569288147_1.actual_goods_issue_date) >= to_date('2018-05-28')
                                                   AND table_569288147_2.tms_service_code        = 'TLTO'
                                                   then 'W02T'
                                                   ELSE table_569288147_2.tms_service_code
                             END AS tms_service_code -- tms_service_code_new
                           , CASE
                                        WHEN table_569288147_2.`tender_event_type_code` = "TENDACC"
                                                   THEN "Carrier Response"
                                        WHEN table_569288147_2.`tender_event_type_code` = "TENDCNCL"
                                                   THEN "Carrier Response"
                                        WHEN table_569288147_2.`tender_event_type_code` = "TENDFRST"
                                                   THEN "P&G Tender"
                                        WHEN table_569288147_2.`tender_event_type_code` = "TENDOTHER"
                                                   THEN "P&G Tender"
                                                   ELSE "Carrier Response"
                             END AS scotts_magical_field_of_dreams
                  FROM
                             tac_calendar_lkp -- ds_tac_calendar_snappy
                             AS table_569288147_1
                             RIGHT JOIN
                                        (
                                               SELECT
                                                      `load id`                                                as load_id
                                                    ,  replace(`origin sf`,'&','')                             as origin_sf
                                                    , `origin location id`                                     as origin_location_id
                                                    , `origin zone`                                            as origin_zone
                                                    ,  replace(`destination sf`,'&','')                        as destination_sf
                                                    , `destination zone`                                       as destination_zone
                                                    , `destination location id`                                as destination_location_id
                                                    , `customer id`                                            as customer_id
                                                    , `customer id description`                                as customer_id_description
                                                    , `carrier id`                                             as carrier_id
                                                    , `carrier description`                                    as carrier_description
                                                    , `tms service code`                                       as tms_service_code
                                                    , `mode`                                                   as mode
                                                    , `tender event type code`                                 as tender_event_type_code
                                                    , `tender reason code`                                     as tender_reason_code
                                                    , `tender event date`                                      as tender_event_date
                                                    , `tender event time`                                      as tender_event_time
                                                    , `tender event date & time`                               as tender_event_date_a_time
                                                    , `transportation planning point`                          as transportation_planning_point
                                                    , `departure country code`                                 as departure_country_code
                                                    , `sap original shipdate`                                  as sap_original_shipdate
                                                    , `original tendered ship date`                            as original_tendered_ship_date
                                                    , `actual ship date`                                       as actual_ship_date
                                                    , `day of the week of original tendered shipdate`          as day_of_the_week_of_original_tendered_shipdate
                                                    , `actual ship time`                                       as actual_ship_time
                                                    , `actual ship date and time`                              as actual_ship_date_and_time
                                                    , `sold to #`                                              as sold_to_n
                                                    , `no.of days in week`                                     as no_of_days_in_week
                                                    , `no.of days in month`                                    as no_of_days_in_month
                                                    , `scac`                                                   as scac
                                                    , `carrier mode description`                               as carrier_mode_description
                                                    , `tariff id`                                              as tariff_id
                                                    , `schedule id`                                            as schedule_id
                                                    , `tender event type description`                          as tender_event_type_description
                                                    , `tender acceptance key`                                  as tender_acceptance_key
                                                    , `tender reason code description`                         as tender_reason_code_description
                                                    , `scheduled date`                                         as scheduled_date
                                                    , `scheduled time`                                         as scheduled_time
                                                    , `average awarded weekly volume`                          as average_awarded_weekly_volume
                                                    , `daily award`                                            as daily_award
                                                    , `day of the week`                                        as day_of_the_week
                                                    , `allocation type`                                        as allocation_type
                                                    , `sun max (loads)`                                        as sun_max_loads
                                                    , `mon max (loads)`                                        as mon_max_loads
                                                    , `tue max (loads)`                                        as tue_max_loads
                                                    , `wed max (loads)`                                        as wed_max_loads
                                                    , `thu max (loads)`                                        as thu_max_loads
                                                    , `fri max (loads)`                                        as fri_max_loads
                                                    , `sat max (loads)`                                        as sat_max_loads
                                                    ,`gbu per shipping site`                                   as gbu_per_shipping_site
                                                    , `shipping conditions`                                    as shipping_conditions
                                                    , `postal code raw tms`                                    as postal_code_raw_tms
                                                    , `postal code final stop`                                 as postal_code_final_stop
                                                    , `country from`                                           as country_from
                                                    , `country to`                                             as country_to
                                                    , `freight auction flag`                                   as freight_auction_flag
                                                    , `true fa flag`                                           as true_fa_flag
                                                    , `freight type`                                           as freight_type
                                                    , `operational freight type`                               as operational_freight_type
                                                    , `pre tms upgrade flag`                                   as pre_tms_upgrade_flag
                                                    , `data structure version`                                 as data_structure_version
                                                    , `primary carrier flag`                                   as primary_carrier_flag
                                                    , `tendered back to primary carrier with no fa adjustment` as tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                    , `tendered back to primary carrier with  fa adjustment`   as tendered_back_to_primary_carrier_with__fa_adjustment
                                                    , `tender accepted loads with no fa`                       as tender_accepted_loads_with_no_fa
                                                    , `tender accepted loads with fa`                          as tender_accepted_loads_with_fa
                                                    , `tender rejected loads`                                  as tender_rejected_loads
                                                    , `previous tender date & time`                            as previous_tender_date_a_time
                                                    , `time between tender events`                             as time_between_tender_events
                                                    , `canceled due to no response`                            as canceled_due_to_no_response
                                                    , `customer`                                               as customer
                                                    , `customer level 1 id`                                    as customer_level_1_id
                                                    , `customer level 1 description`                           as customer_level_1_description
                                                    , `customer level 2 id`                                    as customer_level_2_id
                                                    , `customer level 2 description`                           as customer_level_2_description
                                                    , `customer level 3 id`                                    as customer_level_3_id
                                                    , `customer level 3 description`                           as customer_level_3_description
                                                    , `customer level 4 id`                                    as customer_level_4_id
                                                    , `customer level 4 description`                           as customer_level_4_description
                                                    , `customer level 5 id`                                    as customer_level_5_id
                                                    , `customer level 5 description`                           as customer_level_5_description
                                                    , `customer level 6 id`                                    as customer_level_6_id
                                                    , `customer level 6 description`                           as customer_level_6_description
                                                    , `customer level 7 id`                                    as customer_level_7_id
                                                    , `customer level 7 description`                           as customer_level_7_description
                                                    , `customer level 8 id`                                    as customer_level_8_id
                                                    , `customer level 8 description`                           as customer_level_8_description
                                                    , `customer level 9 id`                                    as customer_level_9_id
                                                    , `customer level 9 description`                           as customer_level_9_description
                                                    , `customer level 10 id`                                   as customer_level_10_id
                                                    , `customer level 10 description`                          as customer_level_10_description
                                                    , `customer level 11 id`                                   as customer_level_11_id
                                                    , `customer level 11 description`                          as customer_level_11_description
                                                    , `customer level 12 id`                                   as customer_level_12_id
                                                    , `customer level 12 description`                          as customer_level_12_description
                                                    , `actual carrier id`                                      as actual_carrier_id
                                                    , `actual carrier description`                             as actual_carrier_description
                                                    , `actual carrier total transportation cost usd`           as actual_carrier_total_transportation_cost_usd
                                                    , `linehaul`                                               as linehaul
                                                    , `incremental fa`                                         as incremental_fa
                                                    , `cnc_carrier mix`                                        as cnc_carrier_mix
                                                    , `unsourced`                                              as unsourced
                                                    , `fuel`                                                   as fuel
                                                    , `accessorial`                                            as accessorial
                                                    , `appliances`                                             as appliances
                                                    , `baby care`                                              as baby_care
                                                    , `chemicals`                                              as chemicals
                                                    , `fabric care`                                            as fabric_care
                                                    , `family care`                                            as family_care
                                                    , `fem care`                                               as fem_care
                                                    , `hair care`                                              as hair_care
                                                    , `home care`                                              as home_care
                                                    , `oral care`                                              as oral_care
                                                    , `phc`                                                    as phc
                                                    , `shave care`                                             as shave_care
                                                    , `skin & personal care`                                   as skin_a_personal_care
                                                    , `other`                                                  as other
                                                    , `tfts load tmstp`                                        as tfts_load_tmstp
                                                    , `load from file`                                         as load_from_file
                                                    , `bd mod tmstp`                                           as bd_mod_tmstp
                                                    , `historical data structure flag`                         as historical_data_structure_flag
                                               FROM
                                                      tac
                                        )
                                        table_569288147_2
                                        ON
                                                   table_569288147_1.actual_goods_issue_date=table_569288147_2.actual_ship_date
       )
       concatkey
;

CREATE TEMPORARY VIEW tmp_tac_tender_pg_CR16_not_equal_pgtender AS
SELECT
          table_1257076127_1.cassell_key                                            AS cassell_key
        , table_1257076127_1.tender_event_date_a_time                               AS tender_event_date_a_time
        , table_1257076127_2.actual_ship_date                                       AS actual_ship_date
        , table_1257076127_2.tac_day_of_year                                        AS tac_day_of_year
        , table_1257076127_2.workweekday                                            AS workweekday
        , table_1257076127_2.week_begin_date                                        AS week_begin_date
        , table_1257076127_2.monthtype445                                           AS monthtype445
        , table_1257076127_2.calendar_year                                          AS calendar_year
        , table_1257076127_2.month_date                                             AS month_date
        , table_1257076127_2.week_number                                            AS week_number
        , table_1257076127_2.day_of_week                                            AS day_of_week
        , table_1257076127_2.load_id                                                AS load_id
        , table_1257076127_2.origin_sf                                              AS origin_sf
        , table_1257076127_2.origin_location_id                                     AS origin_location_id
        , table_1257076127_2.origin_zone                                            AS origin_zone
        , table_1257076127_2.destination_sf                                         AS destination_sf
        , table_1257076127_2.destination_zone                                       AS destination_zone
        , table_1257076127_2.destination_location_id                                AS destination_location_id
        , table_1257076127_2.customer_id                                            AS customer_id
        , table_1257076127_2.customer_id_description                                AS customer_id_description
        , table_1257076127_2.carrier_id                                             AS carrier_id
        , table_1257076127_2.carrier_description                                    AS carrier_description
        , table_1257076127_2.mode                                                   AS mode
        , table_1257076127_2.tender_event_type_code                                 AS tender_event_type_code
        , table_1257076127_2.tender_reason_code                                     AS tender_reason_code
        , table_1257076127_2.tender_event_date                                      AS tender_event_date
        , table_1257076127_2.tender_event_time                                      AS tender_event_time
        , table_1257076127_2.transportation_planning_point                          AS transportation_planning_point
        , table_1257076127_2.departure_country_code                                 AS departure_country_code
        , table_1257076127_2.sap_original_shipdate                                  AS sap_original_shipdate
        , table_1257076127_2.original_tendered_ship_date                            AS original_tendered_ship_date
        , table_1257076127_2.day_of_the_week_of_original_tendered_shipdate          AS day_of_the_week_of_original_tendered_shipdate
        , table_1257076127_2.actual_ship_time                                       AS actual_ship_time
        , table_1257076127_2.actual_ship_date_and_time                              AS actual_ship_date_and_time
        , table_1257076127_2.sold_to_n                                              AS sold_to_n
        , table_1257076127_2.no_of_days_in_week                                     AS no_of_days_in_week
        , table_1257076127_2.no_of_days_in_month                                    AS no_of_days_in_month
        , table_1257076127_2.scac                                                   AS scac
        , table_1257076127_2.carrier_mode_description                               AS carrier_mode_description
        , table_1257076127_2.tariff_id                                              AS tariff_id
        , table_1257076127_2.schedule_id                                            AS schedule_id
        , table_1257076127_2.tender_event_type_description                          AS tender_event_type_description
        , table_1257076127_2.tender_acceptance_key                                  AS tender_acceptance_key
        , table_1257076127_2.tender_reason_code_description                         AS tender_reason_code_description
        , table_1257076127_2.scheduled_date                                         AS scheduled_date
        , table_1257076127_2.scheduled_time                                         AS scheduled_time
        , table_1257076127_2.average_awarded_weekly_volume                          AS average_awarded_weekly_volume
        , table_1257076127_2.daily_award                                            AS daily_award
        , table_1257076127_2.day_of_the_week                                        AS day_of_the_week
        , table_1257076127_2.allocation_type                                        AS allocation_type
        , table_1257076127_2.sun_max_loads                                        AS sun_max_loads
        , table_1257076127_2.mon_max_loads                                        AS mon_max_loads
        , table_1257076127_2.tue_max_loads                                        AS tue_max_loads
        , table_1257076127_2.wed_max_loads                                        AS wed_max_loads
        , table_1257076127_2.thu_max_loads                                        AS thu_max_loads
        , table_1257076127_2.fri_max_loads                                        AS fri_max_loads
        , table_1257076127_2.sat_max_loads                                        AS sat_max_loads
        , table_1257076127_2.gbu_per_shipping_site                                  AS gbu_per_shipping_site
        , table_1257076127_2.shipping_conditions                                    AS shipping_conditions
        , table_1257076127_2.postal_code_raw_tms                                    AS postal_code_raw_tms
        , table_1257076127_2.postal_code_final_stop                                 AS postal_code_final_stop
        , table_1257076127_2.country_from                                           AS country_from
        , table_1257076127_2.country_to                                             AS country_to
        , table_1257076127_2.freight_auction_flag                                   AS freight_auction_flag
        , table_1257076127_2.true_fa_flag                                           AS true_fa_flag
        , table_1257076127_2.freight_type                                           AS freight_type
        , table_1257076127_2.operational_freight_type                               AS operational_freight_type
        , table_1257076127_2.pre_tms_upgrade_flag                                   AS pre_tms_upgrade_flag
        , table_1257076127_2.data_structure_version                                 AS data_structure_version
        , table_1257076127_2.primary_carrier_flag                                   AS primary_carrier_flag
        , table_1257076127_2.tendered_back_to_primary_carrier_with_no_fa_adjustment AS tendered_back_to_primary_carrier_with_no_fa_adjustment
        , table_1257076127_2.tendered_back_to_primary_carrier_with__fa_adjustment   AS tendered_back_to_primary_carrier_with__fa_adjustment
        , table_1257076127_2.tender_accepted_loads_with_no_fa                       AS tender_accepted_loads_with_no_fa
        , table_1257076127_2.tender_accepted_loads_with_fa                          AS tender_accepted_loads_with_fa
        , table_1257076127_2.tender_rejected_loads                                  AS tender_rejected_loads
        , table_1257076127_2.previous_tender_date_a_time                            AS previous_tender_date_a_time
        , table_1257076127_2.time_between_tender_events                             AS time_between_tender_events
        , table_1257076127_2.canceled_due_to_no_response                            AS canceled_due_to_no_response
        , table_1257076127_2.customer                                               AS customer
        , table_1257076127_2.customer_level_1_id                                    AS customer_level_1_id
        , table_1257076127_2.customer_level_1_description                           AS customer_level_1_description
        , table_1257076127_2.customer_level_2_id                                    AS customer_level_2_id
        , table_1257076127_2.customer_level_2_description                           AS customer_level_2_description
        , table_1257076127_2.customer_level_3_id                                    AS customer_level_3_id
        , table_1257076127_2.customer_level_3_description                           AS customer_level_3_description
        , table_1257076127_2.customer_level_4_id                                    AS customer_level_4_id
        , table_1257076127_2.customer_level_4_description                           AS customer_level_4_description
        , table_1257076127_2.customer_level_5_id                                    AS customer_level_5_id
        , table_1257076127_2.customer_level_5_description                           AS customer_level_5_description
        , table_1257076127_2.customer_level_6_id                                    AS customer_level_6_id
        , table_1257076127_2.customer_level_6_description                           AS customer_level_6_description
        , table_1257076127_2.customer_level_7_id                                    AS customer_level_7_id
        , table_1257076127_2.customer_level_7_description                           AS customer_level_7_description
        , table_1257076127_2.customer_level_8_id                                    AS customer_level_8_id
        , table_1257076127_2.customer_level_8_description                           AS customer_level_8_description
        , table_1257076127_2.customer_level_9_id                                    AS customer_level_9_id
        , table_1257076127_2.customer_level_9_description                           AS customer_level_9_description
        , table_1257076127_2.customer_level_10_id                                   AS customer_level_10_id
        , table_1257076127_2.customer_level_10_description                          AS customer_level_10_description
        , table_1257076127_2.customer_level_11_id                                   AS customer_level_11_id
        , table_1257076127_2.customer_level_11_description                          AS customer_level_11_description
        , table_1257076127_2.customer_level_12_id                                   AS customer_level_12_id
        , table_1257076127_2.customer_level_12_description                          AS customer_level_12_description
        , table_1257076127_2.actual_carrier_id                                      AS actual_carrier_id
        , table_1257076127_2.actual_carrier_description                             AS actual_carrier_description
        , table_1257076127_2.actual_carrier_total_transportation_cost_usd           AS actual_carrier_total_transportation_cost_usd
        , table_1257076127_2.linehaul                                               AS linehaul
        , table_1257076127_2.incremental_fa                                         AS incremental_fa
        , table_1257076127_2.cnc_carrier_mix                                        AS cnc_carrier_mix
        , table_1257076127_2.unsourced                                              AS unsourced
        , table_1257076127_2.fuel                                                   AS fuel
        , table_1257076127_2.accessorial                                          AS accessorial
        , table_1257076127_2.appliances                                             AS appliances
        , table_1257076127_2.baby_care                                              AS baby_care
        , table_1257076127_2.chemicals                                              AS chemicals
        , table_1257076127_2.fabric_care                                            AS fabric_care
        , table_1257076127_2.family_care                                            AS family_care
        , table_1257076127_2.fem_care                                               AS fem_care
        , table_1257076127_2.hair_care                                              AS hair_care
        , table_1257076127_2.home_care                                              AS home_care
        , table_1257076127_2.oral_care                                              AS oral_care
        , table_1257076127_2.phc                                                    AS phc
        , table_1257076127_2.shave_care                                             AS shave_care
        , table_1257076127_2.skin_a_personal_care                                   AS skin_a_personal_care
        , table_1257076127_2.other                                                  AS other
        , table_1257076127_2.tfts_load_tmstp                                        AS tfts_load_tmstp
        , table_1257076127_2.load_from_file                                         AS load_from_file
        , table_1257076127_2.bd_mod_tmstp                                           AS bd_mod_tmstp
        , table_1257076127_2.historical_data_structure_flag                         AS historical_data_structure_flag
        , table_1257076127_2.tms_service_code                                       AS tms_service_code
        , table_1257076127_2.scotts_magical_field_of_dreams                         AS scotts_magical_field_of_dreams
        , table_1257076127_2.tender_date_time_type                                  AS tender_date_time_type
        --, table_1257076127_2.max_begin_date                                         AS max_begin_date
        , table_1257076127_2.`current_date`                                         AS `current_date`
        , dt.max_begin_date                                                         AS max_begin_date
FROM
          (
                   SELECT
                            cassell_key
                          , MAX(table_491629510.tender_event_date_a_time) AS tender_event_date_a_time
                   FROM
                            tmp_tac_tender_pg_1_CR16 AS table_491629510
                   WHERE table_491629510.scotts_magical_field_of_dreams <> "P&G Tender" 
                   GROUP BY
                            cassell_key
          )
          table_1257076127_1
          LEFT JOIN
                    tmp_tac_tender_pg_1_CR16 AS table_1257076127_2
                    ON
                              table_1257076127_1.cassell_key                 =table_1257076127_2.cassell_key
                              AND table_1257076127_1.tender_event_date_a_time=table_1257076127_2.tender_event_date_a_time
          CROSS JOIN (
               SELECT MAX(week_begin_date) AS max_begin_date
               FROM tmp_tac_tender_pg_1_CR16
          ) AS dt
WHERE table_1257076127_2.scotts_magical_field_of_dreams <> "P&G Tender" 
;


CREATE TEMPORARY VIEW tmp_tac_tender_pg_CR16_equalto_pgtender AS
SELECT
          table_1257076127_1.cassell_key                                            AS cassell_key
        , table_1257076127_1.tender_event_date_a_time                               AS tender_event_date_a_time
        , table_1257076127_2.actual_ship_date                                       AS actual_ship_date
        , table_1257076127_2.tac_day_of_year                                        AS tac_day_of_year
        , table_1257076127_2.workweekday                                            AS workweekday
        , table_1257076127_2.week_begin_date                                        AS week_begin_date
        , table_1257076127_2.monthtype445                                           AS monthtype445
        , table_1257076127_2.calendar_year                                          AS calendar_year
        , table_1257076127_2.month_date                                             AS month_date
        , table_1257076127_2.week_number                                            AS week_number
        , table_1257076127_2.day_of_week                                            AS day_of_week
        , table_1257076127_2.load_id                                                AS load_id
        , table_1257076127_2.origin_sf                                              AS origin_sf
        , table_1257076127_2.origin_location_id                                     AS origin_location_id
        , table_1257076127_2.origin_zone                                            AS origin_zone
        , table_1257076127_2.destination_sf                                         AS destination_sf
        , table_1257076127_2.destination_zone                                       AS destination_zone
        , table_1257076127_2.destination_location_id                                AS destination_location_id
        , table_1257076127_2.customer_id                                            AS customer_id
        , table_1257076127_2.customer_id_description                                AS customer_id_description
        , table_1257076127_2.carrier_id                                             AS carrier_id
        , table_1257076127_2.carrier_description                                    AS carrier_description
        , table_1257076127_2.mode                                                   AS mode
        , table_1257076127_2.tender_event_type_code                                 AS tender_event_type_code
        , table_1257076127_2.tender_reason_code                                     AS tender_reason_code
        , table_1257076127_2.tender_event_date                                      AS tender_event_date
        , table_1257076127_2.tender_event_time                                      AS tender_event_time
        , table_1257076127_2.transportation_planning_point                          AS transportation_planning_point
        , table_1257076127_2.departure_country_code                                 AS departure_country_code
        , table_1257076127_2.sap_original_shipdate                                  AS sap_original_shipdate
        , table_1257076127_2.original_tendered_ship_date                            AS original_tendered_ship_date
        , table_1257076127_2.day_of_the_week_of_original_tendered_shipdate          AS day_of_the_week_of_original_tendered_shipdate
        , table_1257076127_2.actual_ship_time                                       AS actual_ship_time
        , table_1257076127_2.actual_ship_date_and_time                              AS actual_ship_date_and_time
        , table_1257076127_2.sold_to_n                                              AS sold_to_n
        , table_1257076127_2.no_of_days_in_week                                     AS no_of_days_in_week
        , table_1257076127_2.no_of_days_in_month                                    AS no_of_days_in_month
        , table_1257076127_2.scac                                                   AS scac
        , table_1257076127_2.carrier_mode_description                               AS carrier_mode_description
        , table_1257076127_2.tariff_id                                              AS tariff_id
        , table_1257076127_2.schedule_id                                            AS schedule_id
        , table_1257076127_2.tender_event_type_description                          AS tender_event_type_description
        , table_1257076127_2.tender_acceptance_key                                  AS tender_acceptance_key
        , table_1257076127_2.tender_reason_code_description                         AS tender_reason_code_description
        , table_1257076127_2.scheduled_date                                         AS scheduled_date
        , table_1257076127_2.scheduled_time                                         AS scheduled_time
        , table_1257076127_2.average_awarded_weekly_volume                          AS average_awarded_weekly_volume
        , table_1257076127_2.daily_award                                            AS daily_award
        , table_1257076127_2.day_of_the_week                                        AS day_of_the_week
        , table_1257076127_2.allocation_type                                        AS allocation_type
        , table_1257076127_2.sun_max_loads                                        AS sun_max_loads
        , table_1257076127_2.mon_max_loads                                        AS mon_max_loads
        , table_1257076127_2.tue_max_loads                                        AS tue_max_loads
        , table_1257076127_2.wed_max_loads                                        AS wed_max_loads
        , table_1257076127_2.thu_max_loads                                        AS thu_max_loads
        , table_1257076127_2.fri_max_loads                                        AS fri_max_loads
        , table_1257076127_2.sat_max_loads                                        AS sat_max_loads
        , table_1257076127_2.gbu_per_shipping_site                                  AS gbu_per_shipping_site
        , table_1257076127_2.shipping_conditions                                    AS shipping_conditions
        , table_1257076127_2.postal_code_raw_tms                                    AS postal_code_raw_tms
        , table_1257076127_2.postal_code_final_stop                                 AS postal_code_final_stop
        , table_1257076127_2.country_from                                           AS country_from
        , table_1257076127_2.country_to                                             AS country_to
        , table_1257076127_2.freight_auction_flag                                   AS freight_auction_flag
        , table_1257076127_2.true_fa_flag                                           AS true_fa_flag
        , table_1257076127_2.freight_type                                           AS freight_type
        , table_1257076127_2.operational_freight_type                               AS operational_freight_type
        , table_1257076127_2.pre_tms_upgrade_flag                                   AS pre_tms_upgrade_flag
        , table_1257076127_2.data_structure_version                                 AS data_structure_version
        , table_1257076127_2.primary_carrier_flag                                   AS primary_carrier_flag
        , table_1257076127_2.tendered_back_to_primary_carrier_with_no_fa_adjustment AS tendered_back_to_primary_carrier_with_no_fa_adjustment
        , table_1257076127_2.tendered_back_to_primary_carrier_with__fa_adjustment   AS tendered_back_to_primary_carrier_with__fa_adjustment
        , table_1257076127_2.tender_accepted_loads_with_no_fa                       AS tender_accepted_loads_with_no_fa
        , table_1257076127_2.tender_accepted_loads_with_fa                          AS tender_accepted_loads_with_fa
        , table_1257076127_2.tender_rejected_loads                                  AS tender_rejected_loads
        , table_1257076127_2.previous_tender_date_a_time                            AS previous_tender_date_a_time
        , table_1257076127_2.time_between_tender_events                             AS time_between_tender_events
        , table_1257076127_2.canceled_due_to_no_response                            AS canceled_due_to_no_response
        , table_1257076127_2.customer                                               AS customer
        , table_1257076127_2.customer_level_1_id                                    AS customer_level_1_id
        , table_1257076127_2.customer_level_1_description                           AS customer_level_1_description
        , table_1257076127_2.customer_level_2_id                                    AS customer_level_2_id
        , table_1257076127_2.customer_level_2_description                           AS customer_level_2_description
        , table_1257076127_2.customer_level_3_id                                    AS customer_level_3_id
        , table_1257076127_2.customer_level_3_description                           AS customer_level_3_description
        , table_1257076127_2.customer_level_4_id                                    AS customer_level_4_id
        , table_1257076127_2.customer_level_4_description                           AS customer_level_4_description
        , table_1257076127_2.customer_level_5_id                                    AS customer_level_5_id
        , table_1257076127_2.customer_level_5_description                           AS customer_level_5_description
        , table_1257076127_2.customer_level_6_id                                    AS customer_level_6_id
        , table_1257076127_2.customer_level_6_description                           AS customer_level_6_description
        , table_1257076127_2.customer_level_7_id                                    AS customer_level_7_id
        , table_1257076127_2.customer_level_7_description                           AS customer_level_7_description
        , table_1257076127_2.customer_level_8_id                                    AS customer_level_8_id
        , table_1257076127_2.customer_level_8_description                           AS customer_level_8_description
        , table_1257076127_2.customer_level_9_id                                    AS customer_level_9_id
        , table_1257076127_2.customer_level_9_description                           AS customer_level_9_description
        , table_1257076127_2.customer_level_10_id                                   AS customer_level_10_id
        , table_1257076127_2.customer_level_10_description                          AS customer_level_10_description
        , table_1257076127_2.customer_level_11_id                                   AS customer_level_11_id
        , table_1257076127_2.customer_level_11_description                          AS customer_level_11_description
        , table_1257076127_2.customer_level_12_id                                   AS customer_level_12_id
        , table_1257076127_2.customer_level_12_description                          AS customer_level_12_description
        , table_1257076127_2.actual_carrier_id                                      AS actual_carrier_id
        , table_1257076127_2.actual_carrier_description                             AS actual_carrier_description
        , table_1257076127_2.actual_carrier_total_transportation_cost_usd           AS actual_carrier_total_transportation_cost_usd
        , table_1257076127_2.linehaul                                               AS linehaul
        , table_1257076127_2.incremental_fa                                         AS incremental_fa
        , table_1257076127_2.cnc_carrier_mix                                        AS cnc_carrier_mix
        , table_1257076127_2.unsourced                                              AS unsourced
        , table_1257076127_2.fuel                                                   AS fuel
        , table_1257076127_2.accessorial                                            AS accessorial
        , table_1257076127_2.appliances                                             AS appliances
        , table_1257076127_2.baby_care                                              AS baby_care
        , table_1257076127_2.chemicals                                              AS chemicals
        , table_1257076127_2.fabric_care                                            AS fabric_care
        , table_1257076127_2.family_care                                            AS family_care
        , table_1257076127_2.fem_care                                               AS fem_care
        , table_1257076127_2.hair_care                                              AS hair_care
        , table_1257076127_2.home_care                                              AS home_care
        , table_1257076127_2.oral_care                                              AS oral_care
        , table_1257076127_2.phc                                                    AS phc
        , table_1257076127_2.shave_care                                             AS shave_care
        , table_1257076127_2.skin_a_personal_care                                   AS skin_a_personal_care
        , table_1257076127_2.other                                                  AS other
        , table_1257076127_2.tfts_load_tmstp                                        AS tfts_load_tmstp
        , table_1257076127_2.load_from_file                                         AS load_from_file
        , table_1257076127_2.bd_mod_tmstp                                           AS bd_mod_tmstp
        , table_1257076127_2.historical_data_structure_flag                         AS historical_data_structure_flag
        , table_1257076127_2.tms_service_code                                       AS tms_service_code
        , table_1257076127_2.scotts_magical_field_of_dreams                         AS scotts_magical_field_of_dreams
        , table_1257076127_2.tender_date_time_type                                  AS tender_date_time_type
        --, table_1257076127_2.max_begin_date                                         AS max_begin_date
        , table_1257076127_2.`current_date`                                         AS `current_date`
        , dt.max_begin_date                                                         AS max_begin_date
FROM
          (
                   SELECT
                            cassell_key
                          , MIN(table_491629510.tender_event_date_a_time) AS tender_event_date_a_time
                   FROM
                            tmp_tac_tender_pg_1_CR16 AS table_491629510
                   WHERE table_491629510.scotts_magical_field_of_dreams = "P&G Tender" 

                   GROUP BY
                            cassell_key
          )
          table_1257076127_1
          LEFT JOIN
                    tmp_tac_tender_pg_1_CR16 AS table_1257076127_2
                    ON
                              table_1257076127_1.cassell_key                 =table_1257076127_2.cassell_key
                              AND table_1257076127_1.tender_event_date_a_time=table_1257076127_2.tender_event_date_a_time
          CROSS JOIN (
               SELECT MAX(week_begin_date) AS max_begin_date
               FROM tmp_tac_tender_pg_1_CR16
          ) AS dt
WHERE table_1257076127_2.scotts_magical_field_of_dreams = "P&G Tender"
;


CREATE TEMPORARY VIEW tac_response2021 AS
SELECT *
      , case
              when `freight_type` = 'INTERPLANT'
                  then concat(left(`origin_sf`,20), '-', left(`destination_sf`,20))
                  else concat(left(`origin_sf`,20), '-', left(`destination_location_id`,20))
       end as SF_Lane     
     , case
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'INTERPLANT'
                     then concat(origin_sf, '-', destination_sf)
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'CUSTOMER'
                     then concat(origin_sf, '-', destination_zip)
                     else concat(origin_sf, '-', destination_sf)
       end as lane
     , case
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'INTERPLANT'
                     then concat(origin_sf, '-', destination_sf)
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'CUSTOMER'
                     then concat(origin_sf, '-', destination_zip)
              else concat(origin_location_id, '-', destination_location_id)
       end as lane_new
     , case
              when freight_type = 'INTERPLANT'
                     then concat(origin_sf, '-', destination_sf)
              when freight_type = 'CUSTOMER'
                     then concat(origin_sf, '-', destination_zip)
       end                                             as historical_lane
     , concat(origin_sf, '-', destination_location_id) as customer_specific_lane
     , concat(week_number, '/', calendar_year)         as calendar_year_week_tac
-- For CR16    , case
--              when to_date(actual_ship_date) >= '2018-09-10'
--                     then destination_sf
--                     else postal_code_final_stop
--       end as destination
FROM
       (
              SELECT *
                   , case
                            when tender_event_type_code = "TENDACC"
                                   THEN 1
                                   else 0
                     end as accept_count
                   , case
                            when tender_event_type_code IN ("TENDFRST"
                                                          , "TENDOTHER")
                                   THEN 1
                                   else 0
                     end                       as total_count
                   , to_date(actual_ship_date) as actual_ship_date_format
                   , case
                            when country_to = "CA"
                                   then substr(postal_code_raw_tms, 1, 6)
                            when country_to = "US"
                                   then substr(postal_code_raw_tms, 1, 5)
                     end as destination_zip
              FROM
                     (
                              SELECT
                                       T.cassell_key
                                     , T.tender_event_date_a_time
                                     , T.actual_ship_date
                                     , T.tac_day_of_year
                                     , T.workweekday
                                     , T.week_begin_date
                                     , T.monthtype445
                                     , T.calendar_year
                                     , T.month_date
                                     , T.week_number
                                     , T.day_of_week
                                     , T.load_id
                                     , T.origin_sf
                                     , T.origin_location_id
                                     , T.origin_zone
                                     , T.destination_sf
                                     , T.destination_zone
                                     , T.destination_location_id
                                     , T.customer_id
                                     , T.customer_id_description
                                     , T.carrier_id
                                     , T.carrier_description
                                     , T.mode
                                     , T.tender_event_type_code
                                     , T.tender_reason_code
                                     , T.tender_event_date
                                     , T.tender_event_time
                                     , T.transportation_planning_point
                                     , T.departure_country_code
                                     , T.sap_original_shipdate
                                     , T.original_tendered_ship_date
                                     , T.day_of_the_week_of_original_tendered_shipdate
                                     , T.actual_ship_time
                                     , T.actual_ship_date_and_time
                                     , T.sold_to_n
                                     , T.no_of_days_in_week
                                     , T.no_of_days_in_month
                                     , T.scac
                                     , T.carrier_mode_description
                                     , T.tariff_id
                                     , T.schedule_id
                                     , T.tender_event_type_description
                                     , T.tender_acceptance_key
                                     , T.tender_reason_code_description
                                     , T.scheduled_date
                                     , T.scheduled_time
                                     , T.average_awarded_weekly_volume
                                     , T.daily_award
                                     , T.day_of_the_week
                                     , T.allocation_type
                                     , T.sun_max_loads
                                     , T.mon_max_loads
                                     , T.tue_max_loads
                                     , T.wed_max_loads
                                     , T.thu_max_loads
                                     , T.fri_max_loads
                                     , T.sat_max_loads
                                     , T.gbu_per_shipping_site
                                     , T.shipping_conditions
                                     , T.postal_code_raw_tms
                                     , T.postal_code_final_stop
                                     , T.country_from
                                     , T.country_to
                                     , T.freight_auction_flag
                                     , T.true_fa_flag
                                     , T.freight_type
                                     , T.operational_freight_type
                                     , T.pre_tms_upgrade_flag
                                     , T.data_structure_version
                                     , T.primary_carrier_flag
                                     , T.tendered_back_to_primary_carrier_with_no_fa_adjustment
                                     , T.tendered_back_to_primary_carrier_with__fa_adjustment
                                     , T.tender_accepted_loads_with_no_fa
                                     , T.tender_accepted_loads_with_fa
                                     , T.tender_rejected_loads
                                     , T.previous_tender_date_a_time
                                     , T.time_between_tender_events
                                     , T.canceled_due_to_no_response
                                     , T.customer
                                     , T.customer_level_1_id
                                     , T.customer_level_1_description
                                     , T.customer_level_2_id
                                     , T.customer_level_2_description
                                     , T.customer_level_3_id
                                     , T.customer_level_3_description
                                     , T.customer_level_4_id
                                     , T.customer_level_4_description
                                     , T.customer_level_5_id
                                     , T.customer_level_5_description
                                     , T.customer_level_6_id
                                     , T.customer_level_6_description
                                     , T.customer_level_7_id
                                     , T.customer_level_7_description
                                     , T.customer_level_8_id
                                     , T.customer_level_8_description
                                     , T.customer_level_9_id
                                     , T.customer_level_9_description
                                     , T.customer_level_10_id
                                     , T.customer_level_10_description
                                     , T.customer_level_11_id
                                     , T.customer_level_11_description
                                     , T.customer_level_12_id
                                     , T.customer_level_12_description
                                     , T.actual_carrier_id
                                     , T.actual_carrier_description
                                     , T.actual_carrier_total_transportation_cost_usd
                                     , T.linehaul
                                     , T.incremental_fa
                                     , T.cnc_carrier_mix
                                     , T.unsourced
                                     , T.fuel
                                     , T.accessorial
                                     , T.appliances
                                     , T.baby_care
                                     , T.chemicals
                                     , T.fabric_care
                                     , T.family_care
                                     , T.fem_care
                                     , T.hair_care
                                     , T.home_care
                                     , T.oral_care
                                     , T.phc
                                     , T.shave_care
                                     , T.skin_a_personal_care
                                     , T.other
                                     , T.tfts_load_tmstp
                                     , T.load_from_file
                                     , T.bd_mod_tmstp
                                     , T.historical_data_structure_flag
                                     , T.tms_service_code
                                     , T.scotts_magical_field_of_dreams
                                     , T.tender_date_time_type
                                     , T.state_province
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Monday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Monday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Monday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Monday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Thursday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Thursday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Thursday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Thursday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Friday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Friday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Friday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Friday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Sunday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Sunday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Sunday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Sunday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Wednesday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Wednesday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Wednesday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Wednesday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Tuesday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Tuesday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Tuesday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Tuesday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Saturday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Saturday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Saturday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Saturday_total_count
                              FROM
                                       (
                                                 SELECT
                                                           table_324461333_1.cassell_key                                            AS cassell_key
                                                         , table_324461333_1.tender_event_date_a_time                               AS tender_event_date_a_time
                                                         , table_324461333_1.actual_ship_date                                       AS actual_ship_date
                                                         , table_324461333_1.tac_day_of_year                                        AS tac_day_of_year
                                                         , table_324461333_1.workweekday                                            AS workweekday
                                                         , table_324461333_1.week_begin_date                                        AS week_begin_date
                                                         , table_324461333_1.monthtype445                                           AS monthtype445
                                                         , table_324461333_1.calendar_year                                          AS calendar_year
                                                         , table_324461333_1.month_date                                             AS month_date
                                                         , table_324461333_1.week_number                                            AS week_number
                                                         , table_324461333_1.day_of_week                                            AS day_of_week
                                                         , table_324461333_1.load_id                                                AS load_id
                                                         , table_324461333_1.origin_sf                                              AS origin_sf
                                                         , table_324461333_1.origin_location_id                                     AS origin_location_id
                                                         , table_324461333_1.origin_zone                                            AS origin_zone
                                                         , table_324461333_1.destination_sf                                         AS destination_sf
                                                         , table_324461333_1.destination_zone                                       AS destination_zone
                                                         , table_324461333_1.destination_location_id                                AS destination_location_id
                                                         , table_324461333_1.customer_id                                            AS customer_id
                                                         , table_324461333_1.customer_id_description                                AS customer_id_description
                                                         , table_324461333_1.carrier_id                                             AS carrier_id
                                                         , table_324461333_1.carrier_description                                    AS carrier_description
                                                         , table_324461333_1.mode                                                   AS mode
                                                         , table_324461333_1.tender_event_type_code                                 AS tender_event_type_code
                                                         , table_324461333_1.tender_reason_code                                     AS tender_reason_code
                                                         , table_324461333_1.tender_event_date                                      AS tender_event_date
                                                         , table_324461333_1.tender_event_time                                      AS tender_event_time
                                                         , table_324461333_1.transportation_planning_point                          AS transportation_planning_point
                                                         , table_324461333_1.departure_country_code                                 AS departure_country_code
                                                         , table_324461333_1.sap_original_shipdate                                  AS sap_original_shipdate
                                                         , table_324461333_1.original_tendered_ship_date                            AS original_tendered_ship_date
                                                         , table_324461333_1.day_of_the_week_of_original_tendered_shipdate          AS day_of_the_week_of_original_tendered_shipdate
                                                         , table_324461333_1.actual_ship_time                                       AS actual_ship_time
                                                         , table_324461333_1.actual_ship_date_and_time                              AS actual_ship_date_and_time
                                                         , table_324461333_1.sold_to_n                                              AS sold_to_n
                                                         , table_324461333_1.no_of_days_in_week                                     AS no_of_days_in_week
                                                         , table_324461333_1.no_of_days_in_month                                    AS no_of_days_in_month
                                                         , table_324461333_1.scac                                                   AS scac
                                                         , table_324461333_1.carrier_mode_description                               AS carrier_mode_description
                                                         , table_324461333_1.tariff_id                                              AS tariff_id
                                                         , table_324461333_1.schedule_id                                            AS schedule_id
                                                         , table_324461333_1.tender_event_type_description                          AS tender_event_type_description
                                                         , table_324461333_1.tender_acceptance_key                                  AS tender_acceptance_key
                                                         , table_324461333_1.tender_reason_code_description                         AS tender_reason_code_description
                                                         , table_324461333_1.scheduled_date                                         AS scheduled_date
                                                         , table_324461333_1.scheduled_time                                         AS scheduled_time
                                                         , table_324461333_1.average_awarded_weekly_volume                          AS average_awarded_weekly_volume
                                                         , table_324461333_1.daily_award                                            AS daily_award
                                                         , table_324461333_1.day_of_the_week                                        AS day_of_the_week
                                                         , table_324461333_1.allocation_type                                        AS allocation_type
                                                         , table_324461333_1.sun_max_loads                                        AS sun_max_loads
                                                         , table_324461333_1.mon_max_loads                                        AS mon_max_loads
                                                         , table_324461333_1.tue_max_loads                                        AS tue_max_loads
                                                         , table_324461333_1.wed_max_loads                                        AS wed_max_loads
                                                         , table_324461333_1.thu_max_loads                                        AS thu_max_loads
                                                         , table_324461333_1.fri_max_loads                                        AS fri_max_loads
                                                         , table_324461333_1.sat_max_loads                                        AS sat_max_loads
                                                         , table_324461333_1.gbu_per_shipping_site                                  AS gbu_per_shipping_site
                                                         , table_324461333_1.shipping_conditions                                    AS shipping_conditions
                                                         , table_324461333_1.postal_code_raw_tms                                    AS postal_code_raw_tms
                                                         , table_324461333_1.postal_code_final_stop                                 AS postal_code_final_stop
                                                         , table_324461333_1.country_from                                           AS country_from
                                                         , table_324461333_1.country_to                                             AS country_to
                                                         , table_324461333_1.freight_auction_flag                                   AS freight_auction_flag
                                                         , table_324461333_1.true_fa_flag                                           AS true_fa_flag
                                                         , table_324461333_1.freight_type                                           AS freight_type
                                                         , table_324461333_1.operational_freight_type                               AS operational_freight_type
                                                         , table_324461333_1.pre_tms_upgrade_flag                                   AS pre_tms_upgrade_flag
                                                         , table_324461333_1.data_structure_version                                 AS data_structure_version
                                                         , table_324461333_1.primary_carrier_flag                                   AS primary_carrier_flag
                                                         , table_324461333_1.tendered_back_to_primary_carrier_with_no_fa_adjustment AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                         , table_324461333_1.tendered_back_to_primary_carrier_with__fa_adjustment   AS tendered_back_to_primary_carrier_with__fa_adjustment
                                                         , table_324461333_1.tender_accepted_loads_with_no_fa                       AS tender_accepted_loads_with_no_fa
                                                         , table_324461333_1.tender_accepted_loads_with_fa                          AS tender_accepted_loads_with_fa
                                                         , table_324461333_1.tender_rejected_loads                                  AS tender_rejected_loads
                                                         , table_324461333_1.previous_tender_date_a_time                            AS previous_tender_date_a_time
                                                         , table_324461333_1.time_between_tender_events                             AS time_between_tender_events
                                                         , table_324461333_1.canceled_due_to_no_response                            AS canceled_due_to_no_response
                                                         , table_324461333_1.customer                                               AS customer
                                                         , table_324461333_1.customer_level_1_id                                    AS customer_level_1_id
                                                         , table_324461333_1.customer_level_1_description                           AS customer_level_1_description
                                                         , table_324461333_1.customer_level_2_id                                    AS customer_level_2_id
                                                         , table_324461333_1.customer_level_2_description                           AS customer_level_2_description
                                                         , table_324461333_1.customer_level_3_id                                    AS customer_level_3_id
                                                         , table_324461333_1.customer_level_3_description                           AS customer_level_3_description
                                                         , table_324461333_1.customer_level_4_id                                    AS customer_level_4_id
                                                         , table_324461333_1.customer_level_4_description                           AS customer_level_4_description
                                                         , table_324461333_1.customer_level_5_id                                    AS customer_level_5_id
                                                         , table_324461333_1.customer_level_5_description                           AS customer_level_5_description
                                                         , table_324461333_1.customer_level_6_id                                    AS customer_level_6_id
                                                         , table_324461333_1.customer_level_6_description                           AS customer_level_6_description
                                                         , table_324461333_1.customer_level_7_id                                    AS customer_level_7_id
                                                         , table_324461333_1.customer_level_7_description                           AS customer_level_7_description
                                                         , table_324461333_1.customer_level_8_id                                    AS customer_level_8_id
                                                         , table_324461333_1.customer_level_8_description                           AS customer_level_8_description
                                                         , table_324461333_1.customer_level_9_id                                    AS customer_level_9_id
                                                         , table_324461333_1.customer_level_9_description                           AS customer_level_9_description
                                                         , table_324461333_1.customer_level_10_id                                   AS customer_level_10_id
                                                         , table_324461333_1.customer_level_10_description                          AS customer_level_10_description
                                                         , table_324461333_1.customer_level_11_id                                   AS customer_level_11_id
                                                         , table_324461333_1.customer_level_11_description                          AS customer_level_11_description
                                                         , table_324461333_1.customer_level_12_id                                   AS customer_level_12_id
                                                         , table_324461333_1.customer_level_12_description                          AS customer_level_12_description
                                                         , table_324461333_1.actual_carrier_id                                      AS actual_carrier_id
                                                         , table_324461333_1.actual_carrier_description                             AS actual_carrier_description
                                                         , table_324461333_1.actual_carrier_total_transportation_cost_usd           AS actual_carrier_total_transportation_cost_usd
                                                         , table_324461333_1.linehaul                                               AS linehaul
                                                         , table_324461333_1.incremental_fa                                         AS incremental_fa
                                                         , table_324461333_1.cnc_carrier_mix                                        AS cnc_carrier_mix
                                                         , table_324461333_1.unsourced                                              AS unsourced
                                                         , table_324461333_1.fuel                                                   AS fuel
                                                         , table_324461333_1.accessorial                                            AS accessorial
                                                         , table_324461333_1.appliances                                             AS appliances
                                                         , table_324461333_1.baby_care                                              AS baby_care
                                                         , table_324461333_1.chemicals                                              AS chemicals
                                                         , table_324461333_1.fabric_care                                            AS fabric_care
                                                         , table_324461333_1.family_care                                            AS family_care
                                                         , table_324461333_1.fem_care                                               AS fem_care
                                                         , table_324461333_1.hair_care                                              AS hair_care
                                                         , table_324461333_1.home_care                                              AS home_care
                                                         , table_324461333_1.oral_care                                              AS oral_care
                                                         , table_324461333_1.phc                                                    AS phc
                                                         , table_324461333_1.shave_care                                             AS shave_care
                                                         , table_324461333_1.skin_a_personal_care                                   AS skin_a_personal_care
                                                         , table_324461333_1.other                                                  AS other
                                                         , table_324461333_1.tfts_load_tmstp                                        AS tfts_load_tmstp
                                                         , table_324461333_1.load_from_file                                         AS load_from_file
                                                         , table_324461333_1.bd_mod_tmstp                                           AS bd_mod_tmstp
                                                         , table_324461333_1.historical_data_structure_flag                         AS historical_data_structure_flag
                                                         , table_324461333_1.tms_service_code                                       AS tms_service_code
                                                         , table_324461333_1.scotts_magical_field_of_dreams                         AS scotts_magical_field_of_dreams
                                                         , table_324461333_1.tender_date_time_type                                  AS tender_date_time_type
                                                         , table_324461333_1.max_begin_date                                         AS max_begin_date
                                                         , table_324461333_1.`current_date`                                         AS `current_date`
                                                         , table_324461333_2.`state/province`                                       AS state_province
 -- For CR16                                                       , case
 --                                                                    when table_324461333_1.max_begin_date = table_324461333_1.`current_date`
--                                                                               then '1'
--                                                                     when table_324461333_1.week_begin_date               < table_324461333_1.max_begin_date
--                                                                               OR table_324461333_1.week_begin_date IS NULL
--                                                                               then '1'
--                                                                               else '0'
--                                                           end as filter_partial_weeks
                                                         , case
                                                                     when table_324461333_1.tender_event_type_code = "TENDACC"
                                                                               THEN 1
                                                                               else 0
                                                           end as accept_count
                                                         , case
                                                                     when table_324461333_1.tender_event_type_code IN ("TENDFRST"
                                                                                                                     , "TENDOTHER")
                                                                               THEN 1
                                                                               else 0
                                                           end as total_count
                                                 FROM
                                                           tmp_tac_tender_pg_CR16_not_equal_pgtender -- ds_tac_tender_pg_mxdt
                                                           AS table_324461333_1
                                                           LEFT JOIN
                                                                     shipping_location AS table_324461333_2
                                                                     ON
                                                                               table_324461333_1.destination_location_id=table_324461333_2.`location id`
                                       )
                                       T
--                              WHERE
--                                       filter_partial_weeks = '1'
                              GROUP BY
                                       T.cassell_key
                                     , T.tender_event_date_a_time
                                     , T.actual_ship_date
                                     , T.tac_day_of_year
                                     , T.workweekday
                                     , T.week_begin_date
                                     , T.monthtype445
                                     , T.calendar_year
                                     , T.month_date
                                     , T.week_number
                                     , T.day_of_week
                                     , T.load_id
                                     , T.origin_sf
                                     , T.origin_location_id
                                     , T.origin_zone
                                     , T.destination_sf
                                     , T.destination_zone
                                     , T.destination_location_id
                                     , T.customer_id
                                     , T.customer_id_description
                                     , T.carrier_id
                                     , T.carrier_description
                                     , T.mode
                                     , T.tender_event_type_code
                                     , T.tender_reason_code
                                     , T.tender_event_date
                                     , T.tender_event_time
                                     , T.transportation_planning_point
                                     , T.departure_country_code
                                     , T.sap_original_shipdate
                                     , T.original_tendered_ship_date
                                     , T.day_of_the_week_of_original_tendered_shipdate
                                     , T.actual_ship_time
                                     , T.actual_ship_date_and_time
                                     , T.sold_to_n
                                     , T.no_of_days_in_week
                                     , T.no_of_days_in_month
                                     , T.scac
                                     , T.carrier_mode_description
                                     , T.tariff_id
                                     , T.schedule_id
                                     , T.tender_event_type_description
                                     , T.tender_acceptance_key
                                     , T.tender_reason_code_description
                                     , T.scheduled_date
                                     , T.scheduled_time
                                     , T.average_awarded_weekly_volume
                                     , T.daily_award
                                     , T.day_of_the_week
                                     , T.allocation_type
                                     , T.sun_max_loads
                                     , T.mon_max_loads
                                     , T.tue_max_loads
                                     , T.wed_max_loads
                                     , T.thu_max_loads
                                     , T.fri_max_loads
                                     , T.sat_max_loads
                                     , T.gbu_per_shipping_site
                                     , T.shipping_conditions
                                     , T.postal_code_raw_tms
                                     , T.postal_code_final_stop
                                     , T.country_from
                                     , T.country_to
                                     , T.freight_auction_flag
                                     , T.true_fa_flag
                                     , T.freight_type
                                     , T.operational_freight_type
                                     , T.pre_tms_upgrade_flag
                                     , T.data_structure_version
                                     , T.primary_carrier_flag
                                     , T.tendered_back_to_primary_carrier_with_no_fa_adjustment
                                     , T.tendered_back_to_primary_carrier_with__fa_adjustment
                                     , T.tender_accepted_loads_with_no_fa
                                     , T.tender_accepted_loads_with_fa
                                     , T.tender_rejected_loads
                                     , T.previous_tender_date_a_time
                                     , T.time_between_tender_events
                                     , T.canceled_due_to_no_response
                                     , T.customer
                                     , T.customer_level_1_id
                                     , T.customer_level_1_description
                                     , T.customer_level_2_id
                                     , T.customer_level_2_description
                                     , T.customer_level_3_id
                                     , T.customer_level_3_description
                                     , T.customer_level_4_id
                                     , T.customer_level_4_description
                                     , T.customer_level_5_id
                                     , T.customer_level_5_description
                                     , T.customer_level_6_id
                                     , T.customer_level_6_description
                                     , T.customer_level_7_id
                                     , T.customer_level_7_description
                                     , T.customer_level_8_id
                                     , T.customer_level_8_description
                                     , T.customer_level_9_id
                                     , T.customer_level_9_description
                                     , T.customer_level_10_id
                                     , T.customer_level_10_description
                                     , T.customer_level_11_id
                                     , T.customer_level_11_description
                                     , T.customer_level_12_id
                                     , T.customer_level_12_description
                                     , T.actual_carrier_id
                                     , T.actual_carrier_description
                                     , T.actual_carrier_total_transportation_cost_usd
                                     , T.linehaul
                                     , T.incremental_fa
                                     , T.cnc_carrier_mix
                                     , T.unsourced
                                     , T.fuel
                                     , T.accessorial
                                     , T.appliances
                                     , T.baby_care
                                     , T.chemicals
                                     , T.fabric_care
                                     , T.family_care
                                     , T.fem_care
                                     , T.hair_care
                                     , T.home_care
                                     , T.oral_care
                                     , T.phc
                                     , T.shave_care
                                     , T.skin_a_personal_care
                                     , T.other
                                     , T.tfts_load_tmstp
                                     , T.load_from_file
                                     , T.bd_mod_tmstp
                                     , T.historical_data_structure_flag
                                     , T.tms_service_code
                                     , T.scotts_magical_field_of_dreams
                                     , T.tender_date_time_type
                                     , T.state_province
                     )
                     newcolscount
       )
       calendar_year_week_new
;


CREATE TEMPORARY VIEW ds_tac_tender_pg_tenders_only_CR16 AS
SELECT *
      , case
              when `freight_type` = 'INTERPLANT'
                  then concat(left(`origin_sf`,20), '-', left(`destination_sf`,20))
                  else concat(left(`origin_sf`,20), '-', left(`destination_location_id`,20))
       end as SF_Lane     
     , case
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'INTERPLANT'
                     then concat(origin_sf, '-', destination_sf)
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'CUSTOMER'
                     then concat(origin_sf, '-', destination_zip)
                     else concat(origin_sf, '-', destination_sf)
       end as lane
     , case
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'INTERPLANT'
                     then concat(origin_sf, '-', destination_sf)
              when to_date(actual_ship_date) < '2018-09-10'
                     AND freight_type        = 'CUSTOMER'
                     then concat(origin_sf, '-', destination_zip)
              else concat(origin_location_id, '-', destination_location_id)
       end as lane_new
     , case
              when freight_type = 'INTERPLANT'
                     then concat(origin_sf, '-', destination_sf)
              when freight_type = 'CUSTOMER'
                     then concat(origin_sf, '-', destination_zip)
       end                                             as historical_lane
     , concat(origin_sf, '-', destination_location_id) as customer_specific_lane
     , concat(week_number, '/', calendar_year)         as calendar_year_week_tac
-- For CR16    , case
--              when to_date(actual_ship_date) >= '2018-09-10'
--                     then destination_sf
--                     else postal_code_final_stop
--       end as destination
FROM
       (
              SELECT *
                   , case
                            when tender_event_type_code = "TENDACC"
                                   THEN 1
                                   else 0
                     end as accept_count
                   , case
                            when tender_event_type_code IN ("TENDFRST"
                                                          , "TENDOTHER")
                                   THEN 1
                                   else 0
                     end                       as total_count
                   , to_date(actual_ship_date) as actual_ship_date_format
                   , case
                            when country_to = "CA"
                                   then substr(postal_code_raw_tms, 1, 6)
                            when country_to = "US"
                                   then substr(postal_code_raw_tms, 1, 5)
                     end as destination_zip
              FROM
                     (
                              SELECT
                                       T.cassell_key
                                     , T.tender_event_date_a_time
                                     , T.actual_ship_date
                                     , T.tac_day_of_year
                                     , T.workweekday
                                     , T.week_begin_date
                                     , T.monthtype445
                                     , T.calendar_year
                                     , T.month_date
                                     , T.week_number
                                     , T.day_of_week
                                     , T.load_id
                                     , T.origin_sf
                                     , T.origin_location_id
                                     , T.origin_zone
                                     , T.destination_sf
                                     , T.destination_zone
                                     , T.destination_location_id
                                     , T.customer_id
                                     , T.customer_id_description
                                     , T.carrier_id
                                     , T.carrier_description
                                     , T.mode
                                     , T.tender_event_type_code
                                     , T.tender_reason_code
                                     , T.tender_event_date
                                     , T.tender_event_time
                                     , T.transportation_planning_point
                                     , T.departure_country_code
                                     , T.sap_original_shipdate
                                     , T.original_tendered_ship_date
                                     , T.day_of_the_week_of_original_tendered_shipdate
                                     , T.actual_ship_time
                                     , T.actual_ship_date_and_time
                                     , T.sold_to_n
                                     , T.no_of_days_in_week
                                     , T.no_of_days_in_month
                                     , T.scac
                                     , T.carrier_mode_description
                                     , T.tariff_id
                                     , T.schedule_id
                                     , T.tender_event_type_description
                                     , T.tender_acceptance_key
                                     , T.tender_reason_code_description
                                     , T.scheduled_date
                                     , T.scheduled_time
                                     , T.average_awarded_weekly_volume
                                     , T.daily_award
                                     , T.day_of_the_week
                                     , T.allocation_type
                                     , T.sun_max_loads
                                     , T.mon_max_loads
                                     , T.tue_max_loads
                                     , T.wed_max_loads
                                     , T.thu_max_loads
                                     , T.fri_max_loads
                                     , T.sat_max_loads
                                     , T.gbu_per_shipping_site
                                     , T.shipping_conditions
                                     , T.postal_code_raw_tms
                                     , T.postal_code_final_stop
                                     , T.country_from
                                     , T.country_to
                                     , T.freight_auction_flag
                                     , T.true_fa_flag
                                     , T.freight_type
                                     , T.operational_freight_type
                                     , T.pre_tms_upgrade_flag
                                     , T.data_structure_version
                                     , T.primary_carrier_flag
                                     , T.tendered_back_to_primary_carrier_with_no_fa_adjustment
                                     , T.tendered_back_to_primary_carrier_with__fa_adjustment
                                     , T.tender_accepted_loads_with_no_fa
                                     , T.tender_accepted_loads_with_fa
                                     , T.tender_rejected_loads
                                     , T.previous_tender_date_a_time
                                     , T.time_between_tender_events
                                     , T.canceled_due_to_no_response
                                     , T.customer
                                     , T.customer_level_1_id
                                     , T.customer_level_1_description
                                     , T.customer_level_2_id
                                     , T.customer_level_2_description
                                     , T.customer_level_3_id
                                     , T.customer_level_3_description
                                     , T.customer_level_4_id
                                     , T.customer_level_4_description
                                     , T.customer_level_5_id
                                     , T.customer_level_5_description
                                     , T.customer_level_6_id
                                     , T.customer_level_6_description
                                     , T.customer_level_7_id
                                     , T.customer_level_7_description
                                     , T.customer_level_8_id
                                     , T.customer_level_8_description
                                     , T.customer_level_9_id
                                     , T.customer_level_9_description
                                     , T.customer_level_10_id
                                     , T.customer_level_10_description
                                     , T.customer_level_11_id
                                     , T.customer_level_11_description
                                     , T.customer_level_12_id
                                     , T.customer_level_12_description
                                     , T.actual_carrier_id
                                     , T.actual_carrier_description
                                     , T.actual_carrier_total_transportation_cost_usd
                                     , T.linehaul
                                     , T.incremental_fa
                                     , T.cnc_carrier_mix
                                     , T.unsourced
                                     , T.fuel
                                     , T.accessorial
                                     , T.appliances
                                     , T.baby_care
                                     , T.chemicals
                                     , T.fabric_care
                                     , T.family_care
                                     , T.fem_care
                                     , T.hair_care
                                     , T.home_care
                                     , T.oral_care
                                     , T.phc
                                     , T.shave_care
                                     , T.skin_a_personal_care
                                     , T.other
                                     , T.tfts_load_tmstp
                                     , T.load_from_file
                                     , T.bd_mod_tmstp
                                     , T.historical_data_structure_flag
                                     , T.tms_service_code
                                     , T.scotts_magical_field_of_dreams
                                     , T.tender_date_time_type
                                     , T.state_province
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Monday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Monday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Monday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Monday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Thursday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Thursday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Thursday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Thursday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Friday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Friday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Friday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Friday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Sunday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Sunday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Sunday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Sunday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Wednesday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Wednesday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Wednesday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Wednesday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Tuesday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Tuesday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Tuesday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Tuesday_total_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Saturday'
                                                                           THEN accept_count
                                                         END
                                                )
                                       Saturday_accept_count
                                     , SUM
                                                (
                                                         CASE
                                                                  WHEN day_of_the_week='Saturday'
                                                                           THEN total_count
                                                         END
                                                )
                                       Saturday_total_count
                              FROM
                                       (
                                                 SELECT
                                                           table_324461333_1.cassell_key                                            AS cassell_key
                                                         , table_324461333_1.tender_event_date_a_time                               AS tender_event_date_a_time
                                                         , table_324461333_1.actual_ship_date                                       AS actual_ship_date
                                                         , table_324461333_1.tac_day_of_year                                        AS tac_day_of_year
                                                         , table_324461333_1.workweekday                                            AS workweekday
                                                         , table_324461333_1.week_begin_date                                        AS week_begin_date
                                                         , table_324461333_1.monthtype445                                           AS monthtype445
                                                         , table_324461333_1.calendar_year                                          AS calendar_year
                                                         , table_324461333_1.month_date                                             AS month_date
                                                         , table_324461333_1.week_number                                            AS week_number
                                                         , table_324461333_1.day_of_week                                            AS day_of_week
                                                         , table_324461333_1.load_id                                                AS load_id
                                                         , table_324461333_1.origin_sf                                              AS origin_sf
                                                         , table_324461333_1.origin_location_id                                     AS origin_location_id
                                                         , table_324461333_1.origin_zone                                            AS origin_zone
                                                         , table_324461333_1.destination_sf                                         AS destination_sf
                                                         , table_324461333_1.destination_zone                                       AS destination_zone
                                                         , table_324461333_1.destination_location_id                                AS destination_location_id
                                                         , table_324461333_1.customer_id                                            AS customer_id
                                                         , table_324461333_1.customer_id_description                                AS customer_id_description
                                                         , table_324461333_1.carrier_id                                             AS carrier_id
                                                         , table_324461333_1.carrier_description                                    AS carrier_description
                                                         , table_324461333_1.mode                                                   AS mode
                                                         , table_324461333_1.tender_event_type_code                                 AS tender_event_type_code
                                                         , table_324461333_1.tender_reason_code                                     AS tender_reason_code
                                                         , table_324461333_1.tender_event_date                                      AS tender_event_date
                                                         , table_324461333_1.tender_event_time                                      AS tender_event_time
                                                         , table_324461333_1.transportation_planning_point                          AS transportation_planning_point
                                                         , table_324461333_1.departure_country_code                                 AS departure_country_code
                                                         , table_324461333_1.sap_original_shipdate                                  AS sap_original_shipdate
                                                         , table_324461333_1.original_tendered_ship_date                            AS original_tendered_ship_date
                                                         , table_324461333_1.day_of_the_week_of_original_tendered_shipdate          AS day_of_the_week_of_original_tendered_shipdate
                                                         , table_324461333_1.actual_ship_time                                       AS actual_ship_time
                                                         , table_324461333_1.actual_ship_date_and_time                              AS actual_ship_date_and_time
                                                         , table_324461333_1.sold_to_n                                              AS sold_to_n
                                                         , table_324461333_1.no_of_days_in_week                                     AS no_of_days_in_week
                                                         , table_324461333_1.no_of_days_in_month                                    AS no_of_days_in_month
                                                         , table_324461333_1.scac                                                   AS scac
                                                         , table_324461333_1.carrier_mode_description                               AS carrier_mode_description
                                                         , table_324461333_1.tariff_id                                              AS tariff_id
                                                         , table_324461333_1.schedule_id                                            AS schedule_id
                                                         , table_324461333_1.tender_event_type_description                          AS tender_event_type_description
                                                         , table_324461333_1.tender_acceptance_key                                  AS tender_acceptance_key
                                                         , table_324461333_1.tender_reason_code_description                         AS tender_reason_code_description
                                                         , table_324461333_1.scheduled_date                                         AS scheduled_date
                                                         , table_324461333_1.scheduled_time                                         AS scheduled_time
                                                         , table_324461333_1.average_awarded_weekly_volume                          AS average_awarded_weekly_volume
                                                         , table_324461333_1.daily_award                                            AS daily_award
                                                         , table_324461333_1.day_of_the_week                                        AS day_of_the_week
                                                         , table_324461333_1.allocation_type                                        AS allocation_type
                                                         , table_324461333_1.sun_max_loads                                        AS sun_max_loads
                                                         , table_324461333_1.mon_max_loads                                        AS mon_max_loads
                                                         , table_324461333_1.tue_max_loads                                        AS tue_max_loads
                                                         , table_324461333_1.wed_max_loads                                        AS wed_max_loads
                                                         , table_324461333_1.thu_max_loads                                        AS thu_max_loads
                                                         , table_324461333_1.fri_max_loads                                        AS fri_max_loads
                                                         , table_324461333_1.sat_max_loads                                        AS sat_max_loads
                                                         , table_324461333_1.gbu_per_shipping_site                                  AS gbu_per_shipping_site
                                                         , table_324461333_1.shipping_conditions                                    AS shipping_conditions
                                                         , table_324461333_1.postal_code_raw_tms                                    AS postal_code_raw_tms
                                                         , table_324461333_1.postal_code_final_stop                                 AS postal_code_final_stop
                                                         , table_324461333_1.country_from                                           AS country_from
                                                         , table_324461333_1.country_to                                             AS country_to
                                                         , table_324461333_1.freight_auction_flag                                   AS freight_auction_flag
                                                         , table_324461333_1.true_fa_flag                                           AS true_fa_flag
                                                         , table_324461333_1.freight_type                                           AS freight_type
                                                         , table_324461333_1.operational_freight_type                               AS operational_freight_type
                                                         , table_324461333_1.pre_tms_upgrade_flag                                   AS pre_tms_upgrade_flag
                                                         , table_324461333_1.data_structure_version                                 AS data_structure_version
                                                         , table_324461333_1.primary_carrier_flag                                   AS primary_carrier_flag
                                                         , table_324461333_1.tendered_back_to_primary_carrier_with_no_fa_adjustment AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                         , table_324461333_1.tendered_back_to_primary_carrier_with__fa_adjustment   AS tendered_back_to_primary_carrier_with__fa_adjustment
                                                         , table_324461333_1.tender_accepted_loads_with_no_fa                       AS tender_accepted_loads_with_no_fa
                                                         , table_324461333_1.tender_accepted_loads_with_fa                          AS tender_accepted_loads_with_fa
                                                         , table_324461333_1.tender_rejected_loads                                  AS tender_rejected_loads
                                                         , table_324461333_1.previous_tender_date_a_time                            AS previous_tender_date_a_time
                                                         , table_324461333_1.time_between_tender_events                             AS time_between_tender_events
                                                         , table_324461333_1.canceled_due_to_no_response                            AS canceled_due_to_no_response
                                                         , table_324461333_1.customer                                               AS customer
                                                         , table_324461333_1.customer_level_1_id                                    AS customer_level_1_id
                                                         , table_324461333_1.customer_level_1_description                           AS customer_level_1_description
                                                         , table_324461333_1.customer_level_2_id                                    AS customer_level_2_id
                                                         , table_324461333_1.customer_level_2_description                           AS customer_level_2_description
                                                         , table_324461333_1.customer_level_3_id                                    AS customer_level_3_id
                                                         , table_324461333_1.customer_level_3_description                           AS customer_level_3_description
                                                         , table_324461333_1.customer_level_4_id                                    AS customer_level_4_id
                                                         , table_324461333_1.customer_level_4_description                           AS customer_level_4_description
                                                         , table_324461333_1.customer_level_5_id                                    AS customer_level_5_id
                                                         , table_324461333_1.customer_level_5_description                           AS customer_level_5_description
                                                         , table_324461333_1.customer_level_6_id                                    AS customer_level_6_id
                                                         , table_324461333_1.customer_level_6_description                           AS customer_level_6_description
                                                         , table_324461333_1.customer_level_7_id                                    AS customer_level_7_id
                                                         , table_324461333_1.customer_level_7_description                           AS customer_level_7_description
                                                         , table_324461333_1.customer_level_8_id                                    AS customer_level_8_id
                                                         , table_324461333_1.customer_level_8_description                           AS customer_level_8_description
                                                         , table_324461333_1.customer_level_9_id                                    AS customer_level_9_id
                                                         , table_324461333_1.customer_level_9_description                           AS customer_level_9_description
                                                         , table_324461333_1.customer_level_10_id                                   AS customer_level_10_id
                                                         , table_324461333_1.customer_level_10_description                          AS customer_level_10_description
                                                         , table_324461333_1.customer_level_11_id                                   AS customer_level_11_id
                                                         , table_324461333_1.customer_level_11_description                          AS customer_level_11_description
                                                         , table_324461333_1.customer_level_12_id                                   AS customer_level_12_id
                                                         , table_324461333_1.customer_level_12_description                          AS customer_level_12_description
                                                         , table_324461333_1.actual_carrier_id                                      AS actual_carrier_id
                                                         , table_324461333_1.actual_carrier_description                             AS actual_carrier_description
                                                         , table_324461333_1.actual_carrier_total_transportation_cost_usd           AS actual_carrier_total_transportation_cost_usd
                                                         , table_324461333_1.linehaul                                               AS linehaul
                                                         , table_324461333_1.incremental_fa                                         AS incremental_fa
                                                         , table_324461333_1.cnc_carrier_mix                                        AS cnc_carrier_mix
                                                         , table_324461333_1.unsourced                                              AS unsourced
                                                         , table_324461333_1.fuel                                                   AS fuel
                                                         , table_324461333_1.accessorial                                            AS accessorial
                                                         , table_324461333_1.appliances                                             AS appliances
                                                         , table_324461333_1.baby_care                                              AS baby_care
                                                         , table_324461333_1.chemicals                                              AS chemicals
                                                         , table_324461333_1.fabric_care                                            AS fabric_care
                                                         , table_324461333_1.family_care                                            AS family_care
                                                         , table_324461333_1.fem_care                                               AS fem_care
                                                         , table_324461333_1.hair_care                                              AS hair_care
                                                         , table_324461333_1.home_care                                              AS home_care
                                                         , table_324461333_1.oral_care                                              AS oral_care
                                                         , table_324461333_1.phc                                                    AS phc
                                                         , table_324461333_1.shave_care                                             AS shave_care
                                                         , table_324461333_1.skin_a_personal_care                                   AS skin_a_personal_care
                                                         , table_324461333_1.other                                                  AS other
                                                         , table_324461333_1.tfts_load_tmstp                                        AS tfts_load_tmstp
                                                         , table_324461333_1.load_from_file                                         AS load_from_file
                                                         , table_324461333_1.bd_mod_tmstp                                           AS bd_mod_tmstp
                                                         , table_324461333_1.historical_data_structure_flag                         AS historical_data_structure_flag
                                                         , table_324461333_1.tms_service_code                                       AS tms_service_code
                                                         , table_324461333_1.scotts_magical_field_of_dreams                         AS scotts_magical_field_of_dreams
                                                         , table_324461333_1.tender_date_time_type                                  AS tender_date_time_type
                                                         , table_324461333_1.max_begin_date                                         AS max_begin_date
                                                         , table_324461333_1.`current_date`                                         AS `current_date`
                                                         , table_324461333_2.`state/province`                                       AS state_province
 -- For CR16                                                       , case
 --                                                                    when table_324461333_1.max_begin_date = table_324461333_1.`current_date`
--                                                                               then '1'
--                                                                     when table_324461333_1.week_begin_date               < table_324461333_1.max_begin_date
--                                                                               OR table_324461333_1.week_begin_date IS NULL
--                                                                               then '1'
--                                                                               else '0'
--                                                           end as filter_partial_weeks
                                                         , case
                                                                     when table_324461333_1.tender_event_type_code = "TENDACC"
                                                                               THEN 1
                                                                               else 0
                                                           end as accept_count
                                                         , case
                                                                     when table_324461333_1.tender_event_type_code IN ("TENDFRST"
                                                                                                                     , "TENDOTHER")
                                                                               THEN 1
                                                                               else 0
                                                           end as total_count
                                                 FROM
                                                           tmp_tac_tender_pg_CR16_equalto_pgtender -- ds_tac_tender_pg_mxdt
                                                           AS table_324461333_1
                                                           LEFT JOIN
                                                                     shipping_location AS table_324461333_2
                                                                     ON
                                                                               table_324461333_1.destination_location_id=table_324461333_2.`location id`
                                       )
                                       T
--                              WHERE
--                                       filter_partial_weeks = '1'
                              GROUP BY
                                       T.cassell_key
                                     , T.tender_event_date_a_time
                                     , T.actual_ship_date
                                     , T.tac_day_of_year
                                     , T.workweekday
                                     , T.week_begin_date
                                     , T.monthtype445
                                     , T.calendar_year
                                     , T.month_date
                                     , T.week_number
                                     , T.day_of_week
                                     , T.load_id
                                     , T.origin_sf
                                     , T.origin_location_id
                                     , T.origin_zone
                                     , T.destination_sf
                                     , T.destination_zone
                                     , T.destination_location_id
                                     , T.customer_id
                                     , T.customer_id_description
                                     , T.carrier_id
                                     , T.carrier_description
                                     , T.mode
                                     , T.tender_event_type_code
                                     , T.tender_reason_code
                                     , T.tender_event_date
                                     , T.tender_event_time
                                     , T.transportation_planning_point
                                     , T.departure_country_code
                                     , T.sap_original_shipdate
                                     , T.original_tendered_ship_date
                                     , T.day_of_the_week_of_original_tendered_shipdate
                                     , T.actual_ship_time
                                     , T.actual_ship_date_and_time
                                     , T.sold_to_n
                                     , T.no_of_days_in_week
                                     , T.no_of_days_in_month
                                     , T.scac
                                     , T.carrier_mode_description
                                     , T.tariff_id
                                     , T.schedule_id
                                     , T.tender_event_type_description
                                     , T.tender_acceptance_key
                                     , T.tender_reason_code_description
                                     , T.scheduled_date
                                     , T.scheduled_time
                                     , T.average_awarded_weekly_volume
                                     , T.daily_award
                                     , T.day_of_the_week
                                     , T.allocation_type
                                     , T.sun_max_loads
                                     , T.mon_max_loads
                                     , T.tue_max_loads
                                     , T.wed_max_loads
                                     , T.thu_max_loads
                                     , T.fri_max_loads
                                     , T.sat_max_loads
                                     , T.gbu_per_shipping_site
                                     , T.shipping_conditions
                                     , T.postal_code_raw_tms
                                     , T.postal_code_final_stop
                                     , T.country_from
                                     , T.country_to
                                     , T.freight_auction_flag
                                     , T.true_fa_flag
                                     , T.freight_type
                                     , T.operational_freight_type
                                     , T.pre_tms_upgrade_flag
                                     , T.data_structure_version
                                     , T.primary_carrier_flag
                                     , T.tendered_back_to_primary_carrier_with_no_fa_adjustment
                                     , T.tendered_back_to_primary_carrier_with__fa_adjustment
                                     , T.tender_accepted_loads_with_no_fa
                                     , T.tender_accepted_loads_with_fa
                                     , T.tender_rejected_loads
                                     , T.previous_tender_date_a_time
                                     , T.time_between_tender_events
                                     , T.canceled_due_to_no_response
                                     , T.customer
                                     , T.customer_level_1_id
                                     , T.customer_level_1_description
                                     , T.customer_level_2_id
                                     , T.customer_level_2_description
                                     , T.customer_level_3_id
                                     , T.customer_level_3_description
                                     , T.customer_level_4_id
                                     , T.customer_level_4_description
                                     , T.customer_level_5_id
                                     , T.customer_level_5_description
                                     , T.customer_level_6_id
                                     , T.customer_level_6_description
                                     , T.customer_level_7_id
                                     , T.customer_level_7_description
                                     , T.customer_level_8_id
                                     , T.customer_level_8_description
                                     , T.customer_level_9_id
                                     , T.customer_level_9_description
                                     , T.customer_level_10_id
                                     , T.customer_level_10_description
                                     , T.customer_level_11_id
                                     , T.customer_level_11_description
                                     , T.customer_level_12_id
                                     , T.customer_level_12_description
                                     , T.actual_carrier_id
                                     , T.actual_carrier_description
                                     , T.actual_carrier_total_transportation_cost_usd
                                     , T.linehaul
                                     , T.incremental_fa
                                     , T.cnc_carrier_mix
                                     , T.unsourced
                                     , T.fuel
                                     , T.accessorial
                                     , T.appliances
                                     , T.baby_care
                                     , T.chemicals
                                     , T.fabric_care
                                     , T.family_care
                                     , T.fem_care
                                     , T.hair_care
                                     , T.home_care
                                     , T.oral_care
                                     , T.phc
                                     , T.shave_care
                                     , T.skin_a_personal_care
                                     , T.other
                                     , T.tfts_load_tmstp
                                     , T.load_from_file
                                     , T.bd_mod_tmstp
                                     , T.historical_data_structure_flag
                                     , T.tms_service_code
                                     , T.scotts_magical_field_of_dreams
                                     , T.tender_date_time_type
                                     , T.state_province
                     )
                     newcolscount
       )
       calendar_year_week_new
;

CREATE TEMPORARY VIEW TAC_Response_Tender_union  AS
select * from tac_response2021
union all
select * from ds_tac_tender_pg_tenders_only_CR16
;


CREATE TEMPORARY VIEW TAC_Detail_Only_temp
AS
select a.cassell_key,a.tender_event_date_a_time,a.actual_ship_date,a.tac_day_of_year,a.workweekday,a.week_begin_date,a.monthtype445,a.calendar_year,a.month_date,a.week_number,a.day_of_week,a.load_id,a.origin_sf,a.origin_location_id,a.origin_zone,a.destination_sf,a.destination_zone,a.destination_location_id,a.customer_id,a.customer_id_description,a.carrier_id,a.carrier_description,a.mode,a.tender_event_type_code,a.tender_reason_code,a.tender_event_date,a.tender_event_time,a.transportation_planning_point,a.departure_country_code,a.sap_original_shipdate,a.original_tendered_ship_date,a.day_of_the_week_of_original_tendered_shipdate,a.actual_ship_time,a.actual_ship_date_and_time,a.sold_to_n,a.no_of_days_in_week,a.no_of_days_in_month,a.scac,a.carrier_mode_description,a.tariff_id,a.schedule_id,a.tender_event_type_description,a.tender_acceptance_key,a.tender_reason_code_description,a.scheduled_date,a.scheduled_time,a.average_awarded_weekly_volume,a.daily_award,a.day_of_the_week,a.allocation_type,a.sun_max_loads,a.mon_max_loads,a.tue_max_loads,a.wed_max_loads,a.thu_max_loads,a.fri_max_loads,a.sat_max_loads,a.gbu_per_shipping_site,a.shipping_conditions,a.postal_code_raw_tms,a.postal_code_final_stop,a.country_from,a.country_to,a.freight_auction_flag,a.true_fa_flag,a.freight_type,a.operational_freight_type,a.pre_tms_upgrade_flag,a.data_structure_version,a.primary_carrier_flag,a.tendered_back_to_primary_carrier_with_no_fa_adjustment,a.tendered_back_to_primary_carrier_with__fa_adjustment,a.tender_accepted_loads_with_no_fa,a.tender_accepted_loads_with_fa,a.tender_rejected_loads,a.previous_tender_date_a_time,a.time_between_tender_events,a.canceled_due_to_no_response,a.customer_level_1_id,a.customer_level_1_description,a.customer_level_2_id,a.customer_level_2_description,a.customer_level_3_id,a.customer_level_3_description,a.customer_level_4_id,a.customer_level_4_description,a.customer_level_5_id,a.customer_level_5_description,a.customer_level_6_id,a.customer_level_6_description,a.customer_level_7_id,a.customer_level_7_description,a.customer_level_8_id,a.customer_level_8_description,a.customer_level_9_id,a.customer_level_9_description,a.customer_level_10_id,a.customer_level_10_description,a.customer_level_11_id,a.customer_level_11_description,a.customer_level_12_id,a.customer_level_12_description,a.actual_carrier_id,a.actual_carrier_description,a.actual_carrier_total_transportation_cost_usd as actual_carrier_total_transportation_cost_usd_temp,a.linehaul as linehaul_temp,a.incremental_fa as incremental_fa_temp,a.cnc_carrier_mix as cnc_carrier_mix_temp,a.unsourced as unsourced_temp,a.fuel as fuel_temp,a.accessorial as accessorial_temp,a.appliances,a.baby_care,a.chemicals,a.fabric_care,a.family_care,a.fem_care,a.hair_care,a.home_care,a.oral_care,a.phc,a.shave_care,a.skin_a_personal_care,a.other,a.tfts_load_tmstp,a.load_from_file,a.bd_mod_tmstp,a.historical_data_structure_flag,a.tms_service_code,a.scotts_magical_field_of_dreams,a.tender_date_time_type,a.state_province,a.Monday_accept_count,a.Monday_total_count,a.Thursday_accept_count,a.Thursday_total_count,a.Friday_accept_count,a.Friday_total_count,a.Sunday_accept_count,a.Sunday_total_count,a.Wednesday_accept_count,a.Wednesday_total_count,a.Tuesday_accept_count,a.Tuesday_total_count,a.Saturday_accept_count,a.Saturday_total_count,a.accept_count as accept_count_temp ,a.total_count,a.actual_ship_date_format,a.destination_zip,a.SF_Lane,a.lane,a.lane_new,a.historical_lane,a.customer_specific_lane,a.calendar_year_week_tac,b.SF_awards,b.TAC_report_date
from TAC_Response_Tender_union a
left join operational_tariff_SF_Awards_CR16 b
on a.carrier_id = substring (b.`carrier id`, 3,8)
and a.week_begin_date = b.TAC_report_date
and a.SF_Lane = b.`SF Lane`
and a.tms_service_code = b.service
;

INSERT OVERWRITE TABLE TAC_DETAIL_ONLY_STAR
select *, 
Case
when scotts_magical_field_of_dreams = 'Carrier Response' Then
	case 
		when 	`tender_event_type_description` = 'Tender Accepted' Then
			case 
				when carrier_id != actual_carrier_id Then
					'Carrier didn’t actually ship the load'				
				when carrier_id = actual_carrier_id  Then
					case 
                        when true_fa_flag = 'Y' Then
                            'Freight Auction Rate Adjustment'
                        when tms_service_code = 'TDCP' Then
                             'Dynamic Pricing'
                        when SF_awards < 0.02 then
                             'No Award on Lane'
                        else 'This row counts'
					end 
				
			end
		else 
			tender_event_type_description
		end 
Else 
	'Tender Event'
end as `Why_row_not_counted_toward_award`,

Case
when scotts_magical_field_of_dreams = 'Carrier Response' Then
	case 
		when `tender_event_type_description` = 'Tender Accepted' Then
			case 
				when carrier_id != actual_carrier_id Then
                       0 		
				when carrier_id = actual_carrier_id  Then
					case 
					when true_fa_flag = 'Y' Then
						  0
					when tms_service_code = 'TDCP' Then
						  0
					when SF_awards < 0.02 then
						  0
					else  1
					end 
				
			end
		else 
			 0
		end 
Else 
	 0
end as accept_count,
Case
when scotts_magical_field_of_dreams = 'Carrier Response' and `tender_event_type_description` = 'Tender Accepted'  and carrier_id != actual_carrier_id then
   1 
  else (total_count-accept_count_temp) 
end as reject_count,
case
  when tender_event_type_code = 'TENDFRST' then
    accessorial_temp
  when tender_event_type_code != 'TENDFRST' then
  0
  else 
   accessorial_temp
end as accessorial,
case
  when tender_event_type_code = 'TENDFRST' then
  actual_carrier_total_transportation_cost_usd_temp
  when tender_event_type_code != 'TENDFRST' then
  0
  else 
  actual_carrier_total_transportation_cost_usd_temp
end as actual_carrier_total_transportation_cost_usd,
case
  when tender_event_type_code = 'TENDFRST' then
  cnc_carrier_mix_temp
  when tender_event_type_code != 'TENDFRST' then
  0
  else 
  cnc_carrier_mix_temp
end as cnc_carrier_mix,
case
  when tender_event_type_code = 'TENDFRST' then
  fuel_temp
  when tender_event_type_code != 'TENDFRST' then
  0
  else 
  fuel_temp
end as fuel,
case
  when tender_event_type_code = 'TENDFRST' then
  incremental_fa_temp
  when tender_event_type_code != 'TENDFRST' then
  0
  else 
  incremental_fa_temp
end as incremental_fa,
case
  when tender_event_type_code = 'TENDFRST' then
  linehaul_temp
  when tender_event_type_code != 'TENDFRST' then
  0
  else 
  linehaul_temp
end as linehaul,
case
  when tender_event_type_code = 'TENDFRST' then
  unsourced_temp
  when tender_event_type_code != 'TENDFRST' then 
  0
  else unsourced_temp
end as unsourced,
concat(SF_Lane , `load_id` , tender_event_type_code , week_begin_date, carrier_id , tms_service_code) As MattKey,
case
when customer_level_12_description LIKE '%SF_%'
then customer_level_12_description
when country_to like '%CA%' AND customer_level_12_description LIKE '%LOBLAWS%'
then 'LOBLAWS INC'
when country_to like '%CA%' AND customer_level_12_description LIKE '%COSTCO%'
then 'COSTCO CANADA'
when country_to like '%CA%' AND customer_level_12_description LIKE '%WALMART / MONARCH %'
then 'WAL-MART CANADA CORP'
when country_to like '%CA%' AND customer_level_12_description LIKE '%WAL-MART %'
then 'WAL-MART CANADA CORP'
when country_to like '%CA%' AND customer_level_12_description LIKE '%WALMART %'
then 'WAL-MART CANADA CORP'
when country_to like '%CA%' AND customer_level_12_description LIKE '%SUPPLY CHAIN %'
then 'WAL-MART CANADA CORP'
when country_to not like '%CA%' AND customer_level_12_description LIKE '%WALMART.COM%'
then 'WAL-MART.COM'
when country_to not like '%CA%' and customer_level_12_description LIKE 'SAMS %'
then 'WMRT - TOTAL SAMS'
when country_to not like '%CA%' and customer_level_12_description LIKE 'SAM\'S %'
then 'WMRT - TOTAL SAMS'
when country_to not like '%CA%' and customer_level_12_description LIKE '%Kroger%'
then 'THE KROGER COMPANY'
when country_to not like '%CA%' and customer_level_12_description LIKE '%Kroger%'
then 'THE KROGER COMPANY'
else customer_level_4_description
end as customer
from TAC_Detail_Only_temp where tender_event_date >= '2019-07-01'
;

REFRESH TABLE TAC_DETAIL_ONLY_STAR;

CREATE TEMPORARY VIEW Tac_GroupbySFOnly_tmp_1
AS 
select *, 
case
    when allocation_type ='Daily' then
	case
         when sun_max_loads <= Sunday_total_count 
         then  sun_max_loads
         else Sunday_total_count
    end 
end as Sunday_expected_count,

case
    when allocation_type ='Daily' then
	case
         when mon_max_loads <= Monday_total_count 
         then  mon_max_loads
         else Monday_total_count
    end 
end as Monday_expected_count,

case
    when allocation_type ='Daily' then
	case
         when tue_max_loads <= Tuesday_total_count 
         then  tue_max_loads
         else Tuesday_total_count
    end 
end as Tuesday_expected_count,

case
    when allocation_type ='Daily' then
	case
         when wed_max_loads <= Wednesday_total_count 
         then  wed_max_loads
         else Wednesday_total_count
    end 
end as Wednesday_expected_count,

case
    when allocation_type ='Daily' then
	case
         when thu_max_loads <= Thursday_total_count 
         then  thu_max_loads
         else Thursday_total_count
    end 
end as Thursday_expected_count,

case
    when allocation_type ='Daily' then
	case
         when fri_max_loads <= Friday_total_count 
         then  fri_max_loads
         else Friday_total_count
    end 
end as Friday_expected_count,

case
    when allocation_type ='Daily' then
	case
         when sat_max_loads <= Saturday_total_count 
         then  sat_max_loads
         else Saturday_total_count
    end 
end as Saturday_expected_count
    from 
(select SF_Lane, 
week_begin_date,
carrier_id, 
tms_service_code, 
min (MattKey) as Matt_Key,
--min(load_id) as First_load, 
---min (tender_event_type_code) as tender_event_type_code , 
sum(accept_count) as SF_accept_count, 
Sum( Total_count) as SF_total_count, 
sum ((total_count-accept_count)) as SF_reject_count,
max (SF_awards) as SF_Awards, 
min (allocation_type) as allocation_type,
max(sun_max_loads) as sun_max_loads ,
max (mon_max_loads) as mon_max_loads, 
max(tue_max_loads) as tue_max_loads ,
max(wed_max_loads) as wed_max_loads,
max(thu_max_loads) as thu_max_loads,
max(fri_max_loads) as fri_max_loads,
max(sat_max_loads) as sat_max_loads ,
Sum(Sunday_total_count) as Sunday_total_count,
sum(Monday_total_count) as Monday_total_count,
sum(Tuesday_total_count) as Tuesday_total_count,
sum (Wednesday_total_count) as Wednesday_total_count,
sum(Thursday_total_count) as Thursday_total_count,
sum(Friday_total_count) as  Friday_total_count,
sum(Saturday_total_count) as Saturday_total_count
from TAC_DETAIL_ONLY_STAR
group by SF_Lane, week_begin_date,carrier_id, tms_service_code)
;

CREATE TEMPORARY VIEW Tac_GroupbySFOnly_tmp_2
AS 
select SF_Lane,
week_begin_date,
carrier_id,
tms_service_code,
Matt_Key,
SF_accept_count,
SF_total_count,
SF_reject_count,
SF_Awards,
allocation_type,
sun_max_loads,
mon_max_loads,
tue_max_loads,
wed_max_loads,
thu_max_loads,
fri_max_loads,
sat_max_loads,
Sunday_total_count,
Monday_total_count,
Tuesday_total_count,
Wednesday_total_count,
Thursday_total_count,
Friday_total_count,
Saturday_total_count,
Sunday_expected_count,
Monday_expected_count,
Tuesday_expected_count,
Wednesday_expected_count,
Thursday_expected_count,
Friday_expected_count,
Saturday_expected_count,
expected_volume 
from 
(select *, 
case
   when allocation_type ='Daily' then
   daily_count
   when allocation_type !='Daily' then
    case 
      when SF_awards > SF_total_count									
        then SF_total_count									
      when SF_awards <SF_total_count									
        then SF_awards									
      else SF_total_count									
  end  
end as expected_volume
from 
(select  
(COALESCE(SUM(Sunday_expected_count),0) + COALESCE(SUM(Monday_expected_count),0) +COALESCE(SUM(Tuesday_expected_count),0) +COALESCE(SUM(Wednesday_expected_count),0) + COALESCE(SUM(Thursday_expected_count),0) + COALESCE(SUM(Friday_expected_count),0) + COALESCE(SUM(Saturday_expected_count),0))  as daily_count, *
from Tac_GroupbySFOnly_tmp_1
group by SF_Lane, 
week_begin_date,
carrier_id, 
tms_service_code,
Matt_Key,
--First_load, 
--tender_event_type_code , 
SF_accept_count, 
SF_total_count, 
SF_reject_count,
SF_Awards, 
allocation_type,
sun_max_loads ,
mon_max_loads, 
tue_max_loads ,
wed_max_loads,
thu_max_loads,
fri_max_loads,
sat_max_loads ,
Sunday_total_count,
Monday_total_count,
Tuesday_total_count,
Wednesday_total_count,
Thursday_total_count,
Friday_total_count,
Saturday_total_count,
Sunday_expected_count,
Monday_expected_count,
Tuesday_expected_count,
Wednesday_expected_count,
Thursday_expected_count,
Friday_expected_count,
Saturday_expected_count))
;


INSERT OVERWRITE TABLE TAC_GROUPBYSFONLY_STAR
select *, 
case
  when allocation_type !='Daily' then
    case
     when 0 > (expected_volume-SF_accept_count) then
     0
     else (expected_volume-SF_accept_count)	
    end  
end as rejects_below_award	
from Tac_GroupbySFOnly_tmp_2
;

REFRESH TABLE TAC_GROUPBYSFONLY_STAR;

INSERT OVERWRITE TABLE TAC_DETAILANDSUMMARY_STAR
select a.SF_Lane,a.carrier_description,a.tms_service_code,a.customer,b.SF_accept_count,b.SF_total_count,b.SF_Awards,b.expected_volume,b.rejects_below_award,a.actual_ship_date,a.load_id,a.tender_event_date_a_time,a.tender_event_type_description,a.accept_count,a.allocation_type,a.cassell_key,a.tac_day_of_year,a.workweekday,a.week_begin_date,a.calendar_year,a.month_date,a.week_number,a.day_of_week,a.origin_sf,a.origin_location_id,a.origin_zone,a.destination_sf,a.destination_zone,a.destination_location_id,a.customer_id,a.customer_id_description,a.carrier_id,a.mode,a.tender_event_type_code,a.tender_reason_code,a.tender_event_date,a.tender_event_time,a.transportation_planning_point,a.departure_country_code,a.sap_original_shipdate,a.original_tendered_ship_date,a.day_of_the_week_of_original_tendered_shipdate,a.actual_ship_time,a.actual_ship_date_and_time,a.sold_to_n,a.scac,a.carrier_mode_description,a.tariff_id,a.schedule_id,a.tender_acceptance_key,a.tender_reason_code_description,a.scheduled_date,a.scheduled_time,a.average_awarded_weekly_volume,a.daily_award,a.day_of_the_week,a.sun_max_loads,a.mon_max_loads,a.tue_max_loads,a.wed_max_loads,a.thu_max_loads,a.fri_max_loads,a.sat_max_loads,a.gbu_per_shipping_site,a.shipping_conditions,a.postal_code_raw_tms,a.postal_code_final_stop,a.country_from,a.country_to,a.true_fa_flag,a.freight_type,a.operational_freight_type,a.pre_tms_upgrade_flag,a.data_structure_version,a.primary_carrier_flag,a.tendered_back_to_primary_carrier_with_no_fa_adjustment,a.tendered_back_to_primary_carrier_with__fa_adjustment,a.tender_accepted_loads_with_no_fa,a.tender_accepted_loads_with_fa,a.tender_rejected_loads,a.previous_tender_date_a_time,a.time_between_tender_events,a.canceled_due_to_no_response,a.customer_level_1_id,a.customer_level_1_description,a.customer_level_2_id,a.customer_level_2_description,a.customer_level_3_id,a.customer_level_3_description,a.customer_level_4_id,a.customer_level_4_description,a.customer_level_5_id,a.customer_level_5_description,a.customer_level_6_id,a.customer_level_6_description,a.customer_level_7_id,a.customer_level_7_description,a.customer_level_8_id,a.customer_level_8_description,a.customer_level_9_id,a.customer_level_9_description,a.customer_level_10_id,a.customer_level_10_description,a.customer_level_11_id,a.customer_level_11_description,a.customer_level_12_id,a.customer_level_12_description,a.actual_carrier_id,a.actual_carrier_description,a.appliances,a.baby_care,a.chemicals,a.fabric_care,a.family_care,a.fem_care,a.hair_care,a.home_care,a.oral_care,a.phc,a.shave_care,a.skin_a_personal_care,a.other,a.tfts_load_tmstp,a.load_from_file,a.bd_mod_tmstp,a.historical_data_structure_flag,a.tender_date_time_type,a.state_province,a.Monday_total_count,a.Thursday_total_count,a.Friday_total_count,a.Sunday_total_count,a.Wednesday_total_count,a.Tuesday_total_count,a.Saturday_total_count,a.total_count,a.actual_ship_date_format,a.destination_zip,a.lane,a.lane_new,a.historical_lane,a.customer_specific_lane,a.calendar_year_week_tac,
a.`Why_row_not_counted_toward_award`, a.reject_count,a.accessorial,a.actual_carrier_total_transportation_cost_usd,a.cnc_carrier_mix,a.fuel,a.incremental_fa,a.linehaul,a.unsourced,b.SF_reject_count,
case
  when b.expected_volume is not null then
        b.SF_Awards
    Else null
End as SF_Awards_1row
from TAC_DETAIL_ONLY_STAR a
left join TAC_GROUPBYSFONLY_STAR b
on a.MattKey = b.Matt_Key
and b.SF_Awards > 0.02;
;
