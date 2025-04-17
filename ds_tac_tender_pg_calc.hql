USE ${hivevar:database};

DROP TABLE IF EXISTS tmp_tac_tender_pg_1
;

CREATE TEMPORARY VIEW tmp_tac_tender_pg_1 AS
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
     , sun_max__loads_
     , mon_max__loads_
     , tue_max__loads_
     , wed_max__loads_
     , thu_max__loads_
     , fri_max__loads_
     , sat_max__loads_
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
                           , table_569288147_2.sun_max__loads_                                        AS sun_max__loads_
                           , table_569288147_2.mon_max__loads_                                        AS mon_max__loads_
                           , table_569288147_2.tue_max__loads_                                        AS tue_max__loads_
                           , table_569288147_2.wed_max__loads_                                        AS wed_max__loads_
                           , table_569288147_2.thu_max__loads_                                        AS thu_max__loads_
                           , table_569288147_2.fri_max__loads_                                        AS fri_max__loads_
                           , table_569288147_2.sat_max__loads_                                        AS sat_max__loads_
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
                           , table_569288147_2.accessorial                                            AS accessorial
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
                                                    , `origin sf`                                              as origin_sf
                                                    , `origin location id`                                     as origin_location_id
                                                    , `origin zone`                                            as origin_zone
                                                    , `destination sf`                                         as destination_sf
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
                                                    , `sun max (loads)`                                        as sun_max__loads_
                                                    , `mon max (loads)`                                        as mon_max__loads_
                                                    , `tue max (loads)`                                        as tue_max__loads_
                                                    , `wed max (loads)`                                        as wed_max__loads_
                                                    , `thu max (loads)`                                        as thu_max__loads_
                                                    , `fri max (loads)`                                        as fri_max__loads_
                                                    , `sat max (loads)`                                        as sat_max__loads_
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
----WHERE
----       scotts_magical_field_of_dreams = "P&G Tender"
;


DROP TABLE IF EXISTS tmp_tac_tender_pg
;

CREATE TABLE tmp_tac_tender_pg AS
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
        , table_1257076127_2.sun_max__loads_                                        AS sun_max__loads_
        , table_1257076127_2.mon_max__loads_                                        AS mon_max__loads_
        , table_1257076127_2.tue_max__loads_                                        AS tue_max__loads_
        , table_1257076127_2.wed_max__loads_                                        AS wed_max__loads_
        , table_1257076127_2.thu_max__loads_                                        AS thu_max__loads_
        , table_1257076127_2.fri_max__loads_                                        AS fri_max__loads_
        , table_1257076127_2.sat_max__loads_                                        AS sat_max__loads_
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
                            tmp_tac_tender_pg_1 AS table_491629510
                   WHERE table_491629510.scotts_magical_field_of_dreams = "P&G Tender"
                   GROUP BY
                            cassell_key
          )
          table_1257076127_1
          LEFT JOIN
                    tmp_tac_tender_pg_1 AS table_1257076127_2
                    ON
                              table_1257076127_1.cassell_key                 =table_1257076127_2.cassell_key
                              AND table_1257076127_1.tender_event_date_a_time=table_1257076127_2.tender_event_date_a_time
          CROSS JOIN (
               SELECT MAX(week_begin_date) AS max_begin_date
               FROM tmp_tac_tender_pg_1
          ) AS dt
WHERE table_1257076127_2.scotts_magical_field_of_dreams = "P&G Tender"
;


INSERT INTO tmp_tac_tender_pg
SELECT
          table_102901341_1.cassell_key                                            AS cassell_key
        , table_102901341_1.tender_event_date_a_time                               AS tender_event_date_a_time
        , table_102901341_2.actual_ship_date                                       AS actual_ship_date
        , table_102901341_2.tac_day_of_year                                        AS tac_day_of_year
        , table_102901341_2.workweekday                                            AS workweekday
        , table_102901341_2.week_begin_date                                        AS week_begin_date
        , table_102901341_2.monthtype445                                           AS monthtype445
        , table_102901341_2.calendar_year                                          AS calendar_year
        , table_102901341_2.month_date                                             AS month_date
        , table_102901341_2.week_number                                            AS week_number
        , table_102901341_2.day_of_week                                            AS day_of_week
        , table_102901341_2.load_id                                                AS load_id
        , table_102901341_2.origin_sf                                              AS origin_sf
        , table_102901341_2.origin_location_id                                     AS origin_location_id
        , table_102901341_2.origin_zone                                            AS origin_zone
        , table_102901341_2.destination_sf                                         AS destination_sf
        , table_102901341_2.destination_zone                                       AS destination_zone
        , table_102901341_2.destination_location_id                                AS destination_location_id
        , table_102901341_2.customer_id                                            AS customer_id
        , table_102901341_2.customer_id_description                                AS customer_id_description
        , table_102901341_2.carrier_id                                             AS carrier_id
        , table_102901341_2.carrier_description                                    AS carrier_description
        , table_102901341_2.mode                                                   AS mode
        , table_102901341_2.tender_event_type_code                                 AS tender_event_type_code
        , table_102901341_2.tender_reason_code                                     AS tender_reason_code
        , table_102901341_2.tender_event_date                                      AS tender_event_date
        , table_102901341_2.tender_event_time                                      AS tender_event_time
        , table_102901341_2.transportation_planning_point                          AS transportation_planning_point
        , table_102901341_2.departure_country_code                                 AS departure_country_code
        , table_102901341_2.sap_original_shipdate                                  AS sap_original_shipdate
        , table_102901341_2.original_tendered_ship_date                            AS original_tendered_ship_date
        , table_102901341_2.day_of_the_week_of_original_tendered_shipdate          AS day_of_the_week_of_original_tendered_shipdate
        , table_102901341_2.actual_ship_time                                       AS actual_ship_time
        , table_102901341_2.actual_ship_date_and_time                              AS actual_ship_date_and_time
        , table_102901341_2.sold_to_n                                              AS sold_to_n
        , table_102901341_2.no_of_days_in_week                                     AS no_of_days_in_week
        , table_102901341_2.no_of_days_in_month                                    AS no_of_days_in_month
        , table_102901341_2.scac                                                   AS scac
        , table_102901341_2.carrier_mode_description                               AS carrier_mode_description
        , table_102901341_2.tariff_id                                              AS tariff_id
        , table_102901341_2.schedule_id                                            AS schedule_id
        , table_102901341_2.tender_event_type_description                          AS tender_event_type_description
        , table_102901341_2.tender_acceptance_key                                  AS tender_acceptance_key
        , table_102901341_2.tender_reason_code_description                         AS tender_reason_code_description
        , table_102901341_2.scheduled_date                                         AS scheduled_date
        , table_102901341_2.scheduled_time                                         AS scheduled_time
        , table_102901341_2.average_awarded_weekly_volume                          AS average_awarded_weekly_volume
        , table_102901341_2.daily_award                                            AS daily_award
        , table_102901341_2.day_of_the_week                                        AS day_of_the_week
        , table_102901341_2.allocation_type                                        AS allocation_type
        , table_102901341_2.sun_max__loads_                                        AS sun_max__loads_
        , table_102901341_2.mon_max__loads_                                        AS mon_max__loads_
        , table_102901341_2.tue_max__loads_                                        AS tue_max__loads_
        , table_102901341_2.wed_max__loads_                                        AS wed_max__loads_
        , table_102901341_2.thu_max__loads_                                        AS thu_max__loads_
        , table_102901341_2.fri_max__loads_                                        AS fri_max__loads_
        , table_102901341_2.sat_max__loads_                                        AS sat_max__loads_
        , table_102901341_2.gbu_per_shipping_site                                  AS gbu_per_shipping_site
        , table_102901341_2.shipping_conditions                                    AS shipping_conditions
        , table_102901341_2.postal_code_raw_tms                                    AS postal_code_raw_tms
        , table_102901341_2.postal_code_final_stop                                 AS postal_code_final_stop
        , table_102901341_2.country_from                                           AS country_from
        , table_102901341_2.country_to                                             AS country_to
        , table_102901341_2.freight_auction_flag                                   AS freight_auction_flag
        , table_102901341_2.true_fa_flag                                           AS true_fa_flag
        , table_102901341_2.freight_type                                           AS freight_type
        , table_102901341_2.operational_freight_type                               AS operational_freight_type
        , table_102901341_2.pre_tms_upgrade_flag                                   AS pre_tms_upgrade_flag
        , table_102901341_2.data_structure_version                                 AS data_structure_version
        , table_102901341_2.primary_carrier_flag                                   AS primary_carrier_flag
        , table_102901341_2.tendered_back_to_primary_carrier_with_no_fa_adjustment AS tendered_back_to_primary_carrier_with_no_fa_adjustment
        , table_102901341_2.tendered_back_to_primary_carrier_with__fa_adjustment   AS tendered_back_to_primary_carrier_with__fa_adjustment
        , table_102901341_2.tender_accepted_loads_with_no_fa                       AS tender_accepted_loads_with_no_fa
        , table_102901341_2.tender_accepted_loads_with_fa                          AS tender_accepted_loads_with_fa
        , table_102901341_2.tender_rejected_loads                                  AS tender_rejected_loads
        , table_102901341_2.previous_tender_date_a_time                            AS previous_tender_date_a_time
        , table_102901341_2.time_between_tender_events                             AS time_between_tender_events
        , table_102901341_2.canceled_due_to_no_response                            AS canceled_due_to_no_response
        , table_102901341_2.customer                                               AS customer
        , table_102901341_2.customer_level_1_id                                    AS customer_level_1_id
        , table_102901341_2.customer_level_1_description                           AS customer_level_1_description
        , table_102901341_2.customer_level_2_id                                    AS customer_level_2_id
        , table_102901341_2.customer_level_2_description                           AS customer_level_2_description
        , table_102901341_2.customer_level_3_id                                    AS customer_level_3_id
        , table_102901341_2.customer_level_3_description                           AS customer_level_3_description
        , table_102901341_2.customer_level_4_id                                    AS customer_level_4_id
        , table_102901341_2.customer_level_4_description                           AS customer_level_4_description
        , table_102901341_2.customer_level_5_id                                    AS customer_level_5_id
        , table_102901341_2.customer_level_5_description                           AS customer_level_5_description
        , table_102901341_2.customer_level_6_id                                    AS customer_level_6_id
        , table_102901341_2.customer_level_6_description                           AS customer_level_6_description
        , table_102901341_2.customer_level_7_id                                    AS customer_level_7_id
        , table_102901341_2.customer_level_7_description                           AS customer_level_7_description
        , table_102901341_2.customer_level_8_id                                    AS customer_level_8_id
        , table_102901341_2.customer_level_8_description                           AS customer_level_8_description
        , table_102901341_2.customer_level_9_id                                    AS customer_level_9_id
        , table_102901341_2.customer_level_9_description                           AS customer_level_9_description
        , table_102901341_2.customer_level_10_id                                   AS customer_level_10_id
        , table_102901341_2.customer_level_10_description                          AS customer_level_10_description
        , table_102901341_2.customer_level_11_id                                   AS customer_level_11_id
        , table_102901341_2.customer_level_11_description                          AS customer_level_11_description
        , table_102901341_2.customer_level_12_id                                   AS customer_level_12_id
        , table_102901341_2.customer_level_12_description                          AS customer_level_12_description
        , table_102901341_2.actual_carrier_id                                      AS actual_carrier_id
        , table_102901341_2.actual_carrier_description                             AS actual_carrier_description
        , table_102901341_2.actual_carrier_total_transportation_cost_usd           AS actual_carrier_total_transportation_cost_usd
        , table_102901341_2.linehaul                                               AS linehaul
        , table_102901341_2.incremental_fa                                         AS incremental_fa
        , table_102901341_2.cnc_carrier_mix                                        AS cnc_carrier_mix
        , table_102901341_2.unsourced                                              AS unsourced
        , table_102901341_2.fuel                                                   AS fuel
        , table_102901341_2.accessorial                                            AS accessorial
        , table_102901341_2.appliances                                             AS appliances
        , table_102901341_2.baby_care                                              AS baby_care
        , table_102901341_2.chemicals                                              AS chemicals
        , table_102901341_2.fabric_care                                            AS fabric_care
        , table_102901341_2.family_care                                            AS family_care
        , table_102901341_2.fem_care                                               AS fem_care
        , table_102901341_2.hair_care                                              AS hair_care
        , table_102901341_2.home_care                                              AS home_care
        , table_102901341_2.oral_care                                              AS oral_care
        , table_102901341_2.phc                                                    AS phc
        , table_102901341_2.shave_care                                             AS shave_care
        , table_102901341_2.skin_a_personal_care                                   AS skin_a_personal_care
        , table_102901341_2.other                                                  AS other
        , table_102901341_2.tfts_load_tmstp                                        AS tfts_load_tmstp
        , table_102901341_2.load_from_file                                         AS load_from_file
        , table_102901341_2.bd_mod_tmstp                                           AS bd_mod_tmstp
        , table_102901341_2.historical_data_structure_flag                         AS historical_data_structure_flag
        , table_102901341_2.tms_service_code                                       AS tms_service_code
        , table_102901341_2.scotts_magical_field_of_dreams                         AS scotts_magical_field_of_dreams
        , table_102901341_2.tender_date_time_type                                  AS tender_date_time_type
        , table_102901341_2.`current_date`                                         AS `current_date`
        , dt.max_begin_date                                                        AS max_begin_date
--        , (SELECT MAX(week_begin_date) FROM tmp_tac_tender_pg_1)                   AS max_begin_date
--        , TO_DATE(current_timestamp())                                             AS `current_date`
FROM
          (
                   SELECT
                            cassell_key
                          , MAX(table_1306472724.tender_event_date_a_time) AS tender_event_date_a_time
                   FROM
                            tmp_tac_tender_pg_1 AS table_1306472724
                   WHERE
                            table_1306472724.scotts_magical_field_of_dreams <> "P&G Tender"
                   GROUP BY
                            cassell_key
          )
          table_102901341_1
          LEFT JOIN
                    tmp_tac_tender_pg_1 AS table_102901341_2
                    ON
                              table_102901341_1.cassell_key                 =table_102901341_2.cassell_key
                              AND table_102901341_1.tender_event_date_a_time=table_102901341_2.tender_event_date_a_time
          CROSS JOIN (
               SELECT MAX(week_begin_date) AS max_begin_date
               FROM tmp_tac_tender_pg_1
          ) AS dt
WHERE
          table_102901341_2.scotts_magical_field_of_dreams <> "P&G Tender"
;


DROP TABLE IF EXISTS ds_tac_tender_pg_tenders_only
;

CREATE TEMPORARY VIEW ds_tac_tender_pg_tenders_only AS
SELECT
         carrier_id
       , tms_service_code
       , lane
       , lane_new
       , calendar_year_week_tac
       , SUM(actual_carrier_total_transportation_cost_usd) AS actual_carrier_total_transportation_cost_usd
       , SUM(linehaul)                                     AS linehaul
       , SUM(incremental_fa)                               AS incremental_fa
       , SUM(cnc_carrier_mix)                              AS cnc_carrier_mix
       , SUM(unsourced)                                    AS unsourced
       , SUM(fuel)                                         AS fuel
       , SUM(accessorial)                                  AS accessorial
       , MAX(week_begin_date)                              AS week_begin_date
FROM
         (
                   SELECT
                             table_1257076127_2.carrier_id       AS carrier_id
                           , table_1257076127_2.tms_service_code AS tms_service_code
                           , CASE
                                       WHEN TO_DATE(table_1257076127_2.actual_ship_date) < '2018-09-10'
                                                 AND table_1257076127_2.freight_type     = 'INTERPLANT'
                                                 THEN CONCAT(table_1257076127_2.origin_sf, '-', table_1257076127_2.destination_sf)
                                       WHEN TO_DATE(table_1257076127_2.actual_ship_date) < '2018-09-10'
                                                 AND table_1257076127_2.freight_type     = 'CUSTOMER'
                                                 THEN CONCAT(table_1257076127_2.origin_sf, '-', CASE
                                                           WHEN table_1257076127_2.country_to = "CA"
                                                                     THEN SUBSTR(table_1257076127_2.postal_code_raw_tms, 1, 6)
                                                           WHEN table_1257076127_2.country_to = "US"
                                                                     THEN SUBSTR(table_1257076127_2.postal_code_raw_tms, 1, 5)
                                                 END)
                                                 ELSE CONCAT(table_1257076127_2.origin_sf, '-', table_1257076127_2.destination_sf)
                             END                                                                           AS lane
                           , CASE
                                       WHEN TO_DATE(table_1257076127_2.actual_ship_date) < '2018-09-10'
                                                 AND table_1257076127_2.freight_type     = 'INTERPLANT'
                                                 THEN CONCAT(table_1257076127_2.origin_sf, '-', table_1257076127_2.destination_sf)
                                       WHEN TO_DATE(table_1257076127_2.actual_ship_date) < '2018-09-10'
                                                 AND table_1257076127_2.freight_type     = 'CUSTOMER'
                                                 THEN CONCAT(table_1257076127_2.origin_sf, '-', CASE
                                                           WHEN table_1257076127_2.country_to = "CA"
                                                                     THEN SUBSTR(table_1257076127_2.postal_code_raw_tms, 1, 6)
                                                           WHEN table_1257076127_2.country_to = "US"
                                                                     THEN SUBSTR(table_1257076127_2.postal_code_raw_tms, 1, 5)
                                                 END)
                                    else concat(origin_location_id, '-', destination_location_id)
                             end as lane_new
                           , CONCAT(table_1257076127_2.week_number, '/', table_1257076127_2.calendar_year) AS calendar_year_week_tac
                           , table_1257076127_2.actual_carrier_total_transportation_cost_usd               AS actual_carrier_total_transportation_cost_usd
                           , table_1257076127_2.linehaul                                                   AS linehaul
                           , table_1257076127_2.incremental_fa                                             AS incremental_fa
                           , table_1257076127_2.cnc_carrier_mix                                            AS cnc_carrier_mix
                           , table_1257076127_2.unsourced                                                  AS unsourced
                           , table_1257076127_2.fuel                                                       AS fuel
                           , table_1257076127_2.accessorial                                                AS accessorial
                           , table_1257076127_2.week_begin_date                                            AS week_begin_date
                   FROM
                             (
                                      SELECT
                                               cassell_key
                                             , MIN(table_491629510.tender_event_date_a_time) AS tender_event_date_a_time
                                      FROM
                                               tmp_tac_tender_pg_1 AS table_491629510
                                      WHERE
                                               table_491629510.scotts_magical_field_of_dreams = "P&G Tender"
                                      GROUP BY
                                               cassell_key
                             )
                             table_1257076127_1
                             LEFT JOIN
                                       tmp_tac_tender_pg_1 AS table_1257076127_2
                                       ON
                                                 table_1257076127_1.cassell_key                 =table_1257076127_2.cassell_key
                                                 AND table_1257076127_1.tender_event_date_a_time=table_1257076127_2.tender_event_date_a_time
--                   WHERE
--                             table_1257076127_2.tender_event_type_code = "TENDFRST"  -- to raczej nie to
         )
         grp
GROUP BY
         carrier_id
       , tms_service_code
       , lane
       , lane_new
       , calendar_year_week_tac
;


INSERT OVERWRITE TABLE tac_tender_star
SELECT
       cassell_key
     , tender_event_date_a_time
     , actual_ship_date
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
     , sun_max__loads_
     , mon_max__loads_
     , tue_max__loads_
     , wed_max__loads_
     , thu_max__loads_
     , fri_max__loads_
     , sat_max__loads_
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
     , tender_date_time_type
FROM
       tmp_tac_tender_pg
;

DROP TABLE IF EXISTS tac_tender_pg_summary_1
;

CREATE TEMPORARY VIEW tac_tender_pg_summary_1 AS
SELECT *
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
     , case
              when to_date(actual_ship_date) >= '2018-09-10'
                     then destination_sf
                     else postal_code_final_stop
       end as destination
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
                                     , T.sun_max__loads_
                                     , T.mon_max__loads_
                                     , T.tue_max__loads_
                                     , T.wed_max__loads_
                                     , T.thu_max__loads_
                                     , T.fri_max__loads_
                                     , T.sat_max__loads_
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
                                                         , table_324461333_1.sun_max__loads_                                        AS sun_max__loads_
                                                         , table_324461333_1.mon_max__loads_                                        AS mon_max__loads_
                                                         , table_324461333_1.tue_max__loads_                                        AS tue_max__loads_
                                                         , table_324461333_1.wed_max__loads_                                        AS wed_max__loads_
                                                         , table_324461333_1.thu_max__loads_                                        AS thu_max__loads_
                                                         , table_324461333_1.fri_max__loads_                                        AS fri_max__loads_
                                                         , table_324461333_1.sat_max__loads_                                        AS sat_max__loads_
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
                                                         , case
                                                                     when table_324461333_1.max_begin_date = table_324461333_1.`current_date`
                                                                               then '1'
                                                                     when table_324461333_1.week_begin_date               < table_324461333_1.max_begin_date
                                                                               OR table_324461333_1.week_begin_date IS NULL
                                                                               then '1'
                                                                               else '0'
                                                           end as filter_partial_weeks
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
                                                           tmp_tac_tender_pg -- ds_tac_tender_pg_mxdt
                                                           AS table_324461333_1
                                                           LEFT JOIN
                                                                     shipping_location AS table_324461333_2
                                                                     ON
                                                                               table_324461333_1.destination_location_id=table_324461333_2.`location id`
                                       )
                                       T
-- As part of Req-id RITM2746877, below where clause is disabled to publish data for all days of the week.
-- If this is enabled, week_begin_date will only available on every Monday and rest of the days previous week_begin_date will be populated.
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
                                     , T.sun_max__loads_
                                     , T.mon_max__loads_
                                     , T.tue_max__loads_
                                     , T.wed_max__loads_
                                     , T.thu_max__loads_
                                     , T.fri_max__loads_
                                     , T.sat_max__loads_
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

--CREATE TABLE tac_tender_pg_summary AS
INSERT OVERWRITE TABLE tac_tender_summary_star
SELECT
       table_717798636_1.actual_carrier_total_transportation_cost_usd AS actual_carrier_total_transportation_cost_usd
     , table_717798636_1.linehaul                                     AS linehaul
     , table_717798636_1.incremental_fa                               AS incremental_fa
     , table_717798636_1.cnc_carrier_mix                              AS cnc_carrier_mix
     , table_717798636_1.unsourced                                    AS unsourced
     , table_717798636_1.fuel                                         AS fuel
     , table_717798636_1.accessorial                                  AS accessorial
     , table_717798636_2.carrier_id                                   AS carrier_id
     , table_717798636_2.tms_service_code                             AS tms_service_code
     , table_717798636_2.lane                                         AS lane
     , table_717798636_2.calendar_year_week_tac                       AS calendar_year_week_tac
     , table_717798636_2.origin_sf                                    AS origin_sf
     , table_717798636_2.destination_sf                               AS destination_sf
     , table_717798636_2.carrier_description                          AS carrier_description
     , table_717798636_2.sold_to_n                                    AS sold_to_n
     , table_717798636_2.average_awarded_weekly_volume                AS average_awarded_weekly_volume
     , table_717798636_2.sun_max__loads_                              AS sun_max__loads_
     , table_717798636_2.mon_max__loads_                              AS mon_max__loads_
     , table_717798636_2.tue_max__loads_                              AS tue_max__loads_
     , table_717798636_2.wed_max__loads_                              AS wed_max__loads_
     , table_717798636_2.thu_max__loads_                              AS thu_max__loads_
     , table_717798636_2.fri_max__loads_                              AS fri_max__loads_
     , table_717798636_2.sat_max__loads_                              AS sat_max__loads_
     , table_717798636_2.shipping_conditions                          AS shipping_conditions
     , table_717798636_2.postal_code_raw_tms                          AS postal_code_raw_tms
     , table_717798636_2.postal_code_final_stop                       AS postal_code_final_stop
     , table_717798636_2.country_from                                 AS country_from
     , table_717798636_2.country_to                                   AS country_to
     , table_717798636_2.freight_type                                 AS freight_type
     , table_717798636_2.pre_tms_upgrade_flag                         AS pre_tms_upgrade_flag
     , table_717798636_2.data_structure_version                       AS data_structure_version
     , table_717798636_2.primary_carrier_flag                         AS primary_carrier_flag
     , table_717798636_2.week_begin_date                              AS week_begin_date
     , table_717798636_2.monthtype445                                 AS monthtype445
     , table_717798636_2.calendar_year                                AS calendar_year
     , table_717798636_2.month_date                                   AS month_date
     , table_717798636_2.week_number                                  AS week_number
     , table_717798636_2.state_province                               AS state_province
     , table_717798636_2.monday_accept_count                          AS monday_accept_count
     , table_717798636_2.monday_total_count                           AS monday_total_count
     , table_717798636_2.thursday_accept_count                        AS thursday_accept_count
     , table_717798636_2.thursday_total_count                         AS thursday_total_count
     , table_717798636_2.friday_accept_count                          AS friday_accept_count
     , table_717798636_2.friday_total_count                           AS friday_total_count
     , table_717798636_2.sunday_accept_count                          AS sunday_accept_count
     , table_717798636_2.sunday_total_count                           AS sunday_total_count
     , table_717798636_2.wednesday_accept_count                       AS wednesday_accept_count
     , table_717798636_2.wednesday_total_count                        AS wednesday_total_count
     , table_717798636_2.tuesday_accept_count                         AS tuesday_accept_count
     , table_717798636_2.tuesday_total_count                          AS tuesday_total_count
     , table_717798636_2.saturday_accept_count                        AS saturday_accept_count
     , table_717798636_2.saturday_total_count                         AS saturday_total_count
     , table_717798636_2.accept_count                                 AS accept_count
     , table_717798636_2.total_count                                  AS total_count
     , table_717798636_2.destination_zip                              AS destination_zip
     , table_717798636_2.historical_lane                              AS historical_lane
     , table_717798636_2.reject_count                                 AS reject_count
     , table_717798636_2.accept_percent                               AS accept_percent
     , table_717798636_2.reject_percent                               AS reject_percent
     , table_717798636_2.expected_volume                              AS expected_volume
     , table_717798636_2.rejects_below_award                          AS rejects_below_award
     , table_717798636_2.weekly_tac                                   AS weekly_tac
     , table_717798636_2.customer_id_description                      AS customer_id_description
     , table_717798636_2.customer                                     AS customer
     , table_717798636_2.customer_3_description                       AS customer_3_description
     , table_717798636_2.customer_level_5_description                 AS customer_level_5_description
     , table_717798636_2.customer_level_6_description                 AS customer_level_6_description
     , cast(table_717798636_2.customer_level_12_description as decimal(38,16))                AS customer_level_12_description
     , cast(table_717798636_2.customer_specific_lane as decimal(38,16))                       AS customer_specific_lane
FROM
       ds_tac_tender_pg_tenders_only AS table_717798636_1
       JOIN
              (
                     SELECT
                            distinct_count_customers_customer_level_12_description_ AS distinct_cust
                          , carrier_id
                          , tms_service_code
                          , lane
                          , calendar_year_week_tac
                          , week_begin_date
                          , monthtype445
                          , calendar_year
                          , month_date
                          , week_number
                          , origin_sf
                          , destination_sf
                          , customer_id_description AS customer_id_description_dup
                          , carrier_description
                          , sold_to_n
                          , average_awarded_weekly_volume
                          , sun_max__loads_
                          , mon_max__loads_
                          , tue_max__loads_
                          , wed_max__loads_
                          , thu_max__loads_
                          , fri_max__loads_
                          , sat_max__loads_
                          , shipping_conditions
                          , postal_code_raw_tms
                          , postal_code_final_stop
                          , country_from
                          , country_to
                          , freight_type
                          , pre_tms_upgrade_flag
                          , data_structure_version
                          , primary_carrier_flag
                          , customer                      AS customer_dup
                          , customer_level_3_description  AS customer_level_3_description_dup
                          , customer_level_5_description  AS customer_level_5_description_dup
                          , customer_level_6_description  AS customer_level_6_description_dup
                          , customer_level_12_id          AS customer_level_12_id_dup
                          , customer_level_12_description AS customer_level_12_description_dup
                          , state_province
                          , monday_accept_count
                          , monday_total_count
                          , thursday_accept_count
                          , thursday_total_count
                          , friday_accept_count
                          , friday_total_count
                          , sunday_accept_count
                          , sunday_total_count
                          , wednesday_accept_count
                          , wednesday_total_count
                          , tuesday_accept_count
                          , tuesday_total_count
                          , saturday_accept_count
                          , saturday_total_count
                          , accept_count
                          , total_count
                          , destination_zip
                          , historical_lane
                          , customer_specific_lane AS customer_specific_lane_dup
                          , destination
                          , reject_count
                          , accept_percent
                          , reject_percent
                          , expected_volume
                          , rejects_below_award
                          , weekly_tac
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_id_description
                            end as customer_id_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer
                            end as customer
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_3_description
                            end as customer_3_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_5_description
                            end as customer_level_5_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_6_description
                            end as customer_level_6_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_12_description
                            end as customer_level_12_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_specific_lane
                            end as customer_specific_lane
                     FROM
                            (
                                   SELECT *
                                        , case
                                                 when 0 > (expected_volume-accept_count)
                                                        then 0
                                                        else expected_volume-accept_count
                                          end                          as rejects_below_award
                                        , accept_count/expected_volume as weekly_tac
                                   FROM
                                          (
                                                 SELECT *
                                                      , accept_count/total_count as accept_percent
                                                      , reject_count/total_count as reject_percent
                                                      , case
                                                               when average_awarded_weekly_volume > total_count
                                                                      then total_count
                                                               when average_awarded_weekly_volume < total_count
                                                                      then average_awarded_weekly_volume
                                                                      else total_count
                                                        end as expected_volume
                                                 FROM
                                                        (
                                                                   SELECT
                                                                              table_696478142_1.distinct_count_customers_customer_level_12_description_ AS distinct_count_customers_customer_level_12_description_
                                                                            , table_696478142_2.carrier_id                                              AS carrier_id
                                                                            , table_696478142_2.tms_service_code                                        AS tms_service_code
                                                                            , table_696478142_2.lane                                                    AS lane
                                                                            , table_696478142_2.calendar_year_week_tac                                  AS calendar_year_week_tac
                                                                            , table_696478142_2.cassell_key                                             AS cassell_key
                                                                            , table_696478142_2.tender_event_date_a_time                                AS tender_event_date_a_time
                                                                            , table_696478142_2.actual_ship_date                                        AS actual_ship_date
                                                                            , table_696478142_2.tac_day_of_year                                         AS tac_day_of_year
                                                                            , table_696478142_2.workweekday                                             AS workweekday
                                                                            , table_696478142_2.week_begin_date                                         AS week_begin_date
                                                                            , table_696478142_2.monthtype445                                            AS monthtype445
                                                                            , table_696478142_2.calendar_year                                           AS calendar_year
                                                                            , table_696478142_2.month_date                                              AS month_date
                                                                            , table_696478142_2.week_number                                             AS week_number
                                                                            , table_696478142_2.day_of_week                                             AS day_of_week
                                                                            , table_696478142_2.load_id                                                 AS load_id
                                                                            , table_696478142_2.origin_sf                                               AS origin_sf
                                                                            , table_696478142_2.origin_location_id                                      AS origin_location_id
                                                                            , table_696478142_2.origin_zone                                             AS origin_zone
                                                                            , table_696478142_2.destination_sf                                          AS destination_sf
                                                                            , table_696478142_2.destination_zone                                        AS destination_zone
                                                                            , table_696478142_2.destination_location_id                                 AS destination_location_id
                                                                            , table_696478142_2.customer_id                                             AS customer_id
                                                                            , table_696478142_2.customer_id_description                                 AS customer_id_description
                                                                            , table_696478142_2.carrier_description                                     AS carrier_description
                                                                            , table_696478142_2.mode                                                    AS mode
                                                                            , table_696478142_2.tender_event_type_code                                  AS tender_event_type_code
                                                                            , table_696478142_2.tender_reason_code                                      AS tender_reason_code
                                                                            , table_696478142_2.tender_event_date                                       AS tender_event_date
                                                                            , table_696478142_2.tender_event_time                                       AS tender_event_time
                                                                            , table_696478142_2.transportation_planning_point                           AS transportation_planning_point
                                                                            , table_696478142_2.departure_country_code                                  AS departure_country_code
                                                                            , table_696478142_2.sap_original_shipdate                                   AS sap_original_shipdate
                                                                            , table_696478142_2.original_tendered_ship_date                             AS original_tendered_ship_date
                                                                            , table_696478142_2.day_of_the_week_of_original_tendered_shipdate           AS day_of_the_week_of_original_tendered_shipdate
                                                                            , table_696478142_2.actual_ship_time                                        AS actual_ship_time
                                                                            , table_696478142_2.actual_ship_date_and_time                               AS actual_ship_date_and_time
                                                                            , table_696478142_2.sold_to_n                                               AS sold_to_n
                                                                            , table_696478142_2.no_of_days_in_week                                      AS no_of_days_in_week
                                                                            , table_696478142_2.no_of_days_in_month                                     AS no_of_days_in_month
                                                                            , table_696478142_2.scac                                                    AS scac
                                                                            , table_696478142_2.carrier_mode_description                                AS carrier_mode_description
                                                                            , table_696478142_2.tariff_id                                               AS tariff_id
                                                                            , table_696478142_2.schedule_id                                             AS schedule_id
                                                                            , table_696478142_2.tender_event_type_description                           AS tender_event_type_description
                                                                            , table_696478142_2.tender_acceptance_key                                   AS tender_acceptance_key
                                                                            , table_696478142_2.tender_reason_code_description                          AS tender_reason_code_description
                                                                            , table_696478142_2.scheduled_date                                          AS scheduled_date
                                                                            , table_696478142_2.scheduled_time                                          AS scheduled_time
                                                                            , table_696478142_2.average_awarded_weekly_volume                           AS average_awarded_weekly_volume
                                                                            , table_696478142_2.daily_award                                             AS daily_award
                                                                            , table_696478142_2.day_of_the_week                                         AS day_of_the_week
                                                                            , table_696478142_2.allocation_type                                         AS allocation_type
                                                                            , table_696478142_2.sun_max__loads_                                         AS sun_max__loads_
                                                                            , table_696478142_2.mon_max__loads_                                         AS mon_max__loads_
                                                                            , table_696478142_2.tue_max__loads_                                         AS tue_max__loads_
                                                                            , table_696478142_2.wed_max__loads_                                         AS wed_max__loads_
                                                                            , table_696478142_2.thu_max__loads_                                         AS thu_max__loads_
                                                                            , table_696478142_2.fri_max__loads_                                         AS fri_max__loads_
                                                                            , table_696478142_2.sat_max__loads_                                         AS sat_max__loads_
                                                                            , table_696478142_2.gbu_per_shipping_site                                   AS gbu_per_shipping_site
                                                                            , table_696478142_2.shipping_conditions                                     AS shipping_conditions
                                                                            , table_696478142_2.postal_code_raw_tms                                     AS postal_code_raw_tms
                                                                            , table_696478142_2.postal_code_final_stop                                  AS postal_code_final_stop
                                                                            , table_696478142_2.country_from                                            AS country_from
                                                                            , table_696478142_2.country_to                                              AS country_to
                                                                            , table_696478142_2.freight_auction_flag                                    AS freight_auction_flag
                                                                            , table_696478142_2.true_fa_flag                                            AS true_fa_flag
                                                                            , table_696478142_2.freight_type                                            AS freight_type
                                                                            , table_696478142_2.operational_freight_type                                AS operational_freight_type
                                                                            , table_696478142_2.pre_tms_upgrade_flag                                    AS pre_tms_upgrade_flag
                                                                            , table_696478142_2.data_structure_version                                  AS data_structure_version
                                                                            , table_696478142_2.primary_carrier_flag                                    AS primary_carrier_flag
                                                                            , table_696478142_2.tendered_back_to_primary_carrier_with_no_fa_adjustment  AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                                            , table_696478142_2.tendered_back_to_primary_carrier_with__fa_adjustment    AS tendered_back_to_primary_carrier_with__fa_adjustment
                                                                            , table_696478142_2.tender_accepted_loads_with_no_fa                        AS tender_accepted_loads_with_no_fa
                                                                            , table_696478142_2.tender_accepted_loads_with_fa                           AS tender_accepted_loads_with_fa
                                                                            , table_696478142_2.tender_rejected_loads                                   AS tender_rejected_loads
                                                                            , table_696478142_2.previous_tender_date_a_time                             AS previous_tender_date_a_time
                                                                            , table_696478142_2.time_between_tender_events                              AS time_between_tender_events
                                                                            , table_696478142_2.canceled_due_to_no_response                             AS canceled_due_to_no_response
                                                                            , table_696478142_2.customer                                                AS customer
                                                                            , table_696478142_2.customer_level_1_id                                     AS customer_level_1_id
                                                                            , table_696478142_2.customer_level_1_description                            AS customer_level_1_description
                                                                            , table_696478142_2.customer_level_2_id                                     AS customer_level_2_id
                                                                            , table_696478142_2.customer_level_2_description                            AS customer_level_2_description
                                                                            , table_696478142_2.customer_level_3_id                                     AS customer_level_3_id
                                                                            , table_696478142_2.customer_level_3_description                            AS customer_level_3_description
                                                                            , table_696478142_2.customer_level_4_id                                     AS customer_level_4_id
                                                                            , table_696478142_2.customer_level_4_description                            AS customer_level_4_description
                                                                            , table_696478142_2.customer_level_5_id                                     AS customer_level_5_id
                                                                            , table_696478142_2.customer_level_5_description                            AS customer_level_5_description
                                                                            , table_696478142_2.customer_level_6_id                                     AS customer_level_6_id
                                                                            , table_696478142_2.customer_level_6_description                            AS customer_level_6_description
                                                                            , table_696478142_2.customer_level_7_id                                     AS customer_level_7_id
                                                                            , table_696478142_2.customer_level_7_description                            AS customer_level_7_description
                                                                            , table_696478142_2.customer_level_8_id                                     AS customer_level_8_id
                                                                            , table_696478142_2.customer_level_8_description                            AS customer_level_8_description
                                                                            , table_696478142_2.customer_level_9_id                                     AS customer_level_9_id
                                                                            , table_696478142_2.customer_level_9_description                            AS customer_level_9_description
                                                                            , table_696478142_2.customer_level_10_id                                    AS customer_level_10_id
                                                                            , table_696478142_2.customer_level_10_description                           AS customer_level_10_description
                                                                            , table_696478142_2.customer_level_11_id                                    AS customer_level_11_id
                                                                            , table_696478142_2.customer_level_11_description                           AS customer_level_11_description
                                                                            , table_696478142_2.customer_level_12_id                                    AS customer_level_12_id
                                                                            , table_696478142_2.customer_level_12_description                           AS customer_level_12_description
                                                                            , table_696478142_2.actual_carrier_id                                       AS actual_carrier_id
                                                                            , table_696478142_2.actual_carrier_description                              AS actual_carrier_description
                                                                            , table_696478142_2.actual_carrier_total_transportation_cost_usd            AS actual_carrier_total_transportation_cost_usd
                                                                            , table_696478142_2.linehaul                                                AS linehaul
                                                                            , table_696478142_2.incremental_fa                                          AS incremental_fa
                                                                            , table_696478142_2.cnc_carrier_mix                                         AS cnc_carrier_mix
                                                                            , table_696478142_2.unsourced                                               AS unsourced
                                                                            , table_696478142_2.fuel                                                    AS fuel
                                                                            , table_696478142_2.accessorial                                             AS accessorial
                                                                            , table_696478142_2.appliances                                              AS appliances
                                                                            , table_696478142_2.baby_care                                               AS baby_care
                                                                            , table_696478142_2.chemicals                                               AS chemicals
                                                                            , table_696478142_2.fabric_care                                             AS fabric_care
                                                                            , table_696478142_2.family_care                                             AS family_care
                                                                            , table_696478142_2.fem_care                                                AS fem_care
                                                                            , table_696478142_2.hair_care                                               AS hair_care
                                                                            , table_696478142_2.home_care                                               AS home_care
                                                                            , table_696478142_2.oral_care                                               AS oral_care
                                                                            , table_696478142_2.phc                                                     AS phc
                                                                            , table_696478142_2.shave_care                                              AS shave_care
                                                                            , table_696478142_2.skin_a_personal_care                                    AS skin_a_personal_care
                                                                            , table_696478142_2.other                                                   AS other
                                                                            , table_696478142_2.tfts_load_tmstp                                         AS tfts_load_tmstp
                                                                            , table_696478142_2.load_from_file                                          AS load_from_file
                                                                            , table_696478142_2.bd_mod_tmstp                                            AS bd_mod_tmstp
                                                                            , table_696478142_2.historical_data_structure_flag                          AS historical_data_structure_flag
                                                                            , table_696478142_2.scotts_magical_field_of_dreams                          AS scotts_magical_field_of_dreams
                                                                            , table_696478142_2.tender_date_time_type                                   AS tender_date_time_type
                                                                            , table_696478142_2.state_province                                          AS state_province
                                                                            , table_696478142_2.monday_accept_count                                     AS monday_accept_count
                                                                            , table_696478142_2.monday_total_count                                      AS monday_total_count
                                                                            , table_696478142_2.thursday_accept_count                                   AS thursday_accept_count
                                                                            , table_696478142_2.thursday_total_count                                    AS thursday_total_count
                                                                            , table_696478142_2.friday_accept_count                                     AS friday_accept_count
                                                                            , table_696478142_2.friday_total_count                                      AS friday_total_count
                                                                            , table_696478142_2.sunday_accept_count                                     AS sunday_accept_count
                                                                            , table_696478142_2.sunday_total_count                                      AS sunday_total_count
                                                                            , table_696478142_2.wednesday_accept_count                                  AS wednesday_accept_count
                                                                            , table_696478142_2.wednesday_total_count                                   AS wednesday_total_count
                                                                            , table_696478142_2.tuesday_accept_count                                    AS tuesday_accept_count
                                                                            , table_696478142_2.tuesday_total_count                                     AS tuesday_total_count
                                                                            , table_696478142_2.saturday_accept_count                                   AS saturday_accept_count
                                                                            , table_696478142_2.saturday_total_count                                    AS saturday_total_count
                                                                            , table_696478142_2.accept_count                                            AS accept_count
                                                                            , table_696478142_2.total_count                                             AS total_count
                                                                            , table_696478142_2.actual_ship_date_format                                 AS actual_ship_date_format
                                                                            , table_696478142_2.destination_zip                                         AS destination_zip
                                                                            , table_696478142_2.historical_lane                                         AS historical_lane
                                                                            , table_696478142_2.customer_specific_lane                                  AS customer_specific_lane
                                                                            , table_696478142_2.destination                                             AS destination
                                                                            , (table_696478142_2.total_count-table_696478142_2.accept_count)            AS reject_count
                                                                   FROM
                                                                              (
                                                                                       SELECT
                                                                                                carrier_id
                                                                                              , tms_service_code
                                                                                              , lane
                                                                                              , calendar_year_week_tac
                                                                                              , count(DISTINCT table_1742270680.customer_level_12_description) AS distinct_count_customers_customer_level_12_description_
                                                                                       FROM
                                                                                                tac_tender_pg_summary_1 AS table_1742270680
                                                                                       GROUP BY
                                                                                                carrier_id
                                                                                              , tms_service_code
                                                                                              , lane
                                                                                              , calendar_year_week_tac
                                                                              )
                                                                              table_696478142_1
                                                                              RIGHT JOIN
                                                                                         (
                                                                                                  SELECT
                                                                                                           carrier_id
                                                                                                         , tms_service_code
                                                                                                         , lane
                                                                                                         , calendar_year_week_tac
                                                                                                         , MIN(table_1644172437.cassell_key)                                            AS cassell_key
                                                                                                         , MIN(table_1644172437.tender_event_date_a_time)                               AS tender_event_date_a_time
                                                                                                         , MIN(table_1644172437.actual_ship_date)                                       AS actual_ship_date
                                                                                                         , MIN(table_1644172437.tac_day_of_year)                                        AS tac_day_of_year
                                                                                                         , MIN(table_1644172437.workweekday)                                            AS workweekday
                                                                                                         , MIN(table_1644172437.week_begin_date)                                        AS week_begin_date
                                                                                                         , MIN(table_1644172437.monthtype445)                                           AS monthtype445
                                                                                                         , MIN(table_1644172437.calendar_year)                                          AS calendar_year
                                                                                                         , MIN(table_1644172437.month_date)                                             AS month_date
                                                                                                         , MIN(table_1644172437.week_number)                                            AS week_number
                                                                                                         , MIN(table_1644172437.day_of_week)                                            AS day_of_week
                                                                                                         , MIN(table_1644172437.load_id)                                                AS load_id
                                                                                                         , MIN(table_1644172437.origin_sf)                                              AS origin_sf
                                                                                                         , MIN(table_1644172437.origin_location_id)                                     AS origin_location_id
                                                                                                         , MIN(table_1644172437.origin_zone)                                            AS origin_zone
                                                                                                         , MIN(table_1644172437.destination_sf)                                         AS destination_sf
                                                                                                         , MIN(table_1644172437.destination_zone)                                       AS destination_zone
                                                                                                         , MIN(table_1644172437.destination_location_id)                                AS destination_location_id
                                                                                                         , MIN(table_1644172437.customer_id)                                            AS customer_id
                                                                                                         , MIN(table_1644172437.customer_id_description)                                AS customer_id_description
                                                                                                         , MIN(table_1644172437.carrier_description)                                    AS carrier_description
                                                                                                         , MIN(table_1644172437.mode)                                                   AS mode
                                                                                                         , MIN(table_1644172437.tender_event_type_code)                                 AS tender_event_type_code
                                                                                                         , MIN(table_1644172437.tender_reason_code)                                     AS tender_reason_code
                                                                                                         , MIN(table_1644172437.tender_event_date)                                      AS tender_event_date
                                                                                                         , MIN(table_1644172437.tender_event_time)                                      AS tender_event_time
                                                                                                         , MIN(table_1644172437.transportation_planning_point)                          AS transportation_planning_point
                                                                                                         , MIN(table_1644172437.departure_country_code)                                 AS departure_country_code
                                                                                                         , MIN(table_1644172437.sap_original_shipdate)                                  AS sap_original_shipdate
                                                                                                         , MIN(table_1644172437.original_tendered_ship_date)                            AS original_tendered_ship_date
                                                                                                         , MIN(table_1644172437.day_of_the_week_of_original_tendered_shipdate)          AS day_of_the_week_of_original_tendered_shipdate
                                                                                                         , MIN(table_1644172437.actual_ship_time)                                       AS actual_ship_time
                                                                                                         , MIN(table_1644172437.actual_ship_date_and_time)                              AS actual_ship_date_and_time
                                                                                                         , MIN(table_1644172437.sold_to_n)                                              AS sold_to_n
                                                                                                         , MIN(table_1644172437.no_of_days_in_week)                                     AS no_of_days_in_week
                                                                                                         , MIN(table_1644172437.no_of_days_in_month)                                    AS no_of_days_in_month
                                                                                                         , MIN(table_1644172437.scac)                                                   AS scac
                                                                                                         , MIN(table_1644172437.carrier_mode_description)                               AS carrier_mode_description
                                                                                                         , MIN(table_1644172437.tariff_id)                                              AS tariff_id
                                                                                                         , MIN(table_1644172437.schedule_id)                                            AS schedule_id
                                                                                                         , MIN(table_1644172437.tender_event_type_description)                          AS tender_event_type_description
                                                                                                         , MIN(table_1644172437.tender_acceptance_key)                                  AS tender_acceptance_key
                                                                                                         , MIN(table_1644172437.tender_reason_code_description)                         AS tender_reason_code_description
                                                                                                         , MIN(table_1644172437.scheduled_date)                                         AS scheduled_date
                                                                                                         , MIN(table_1644172437.scheduled_time)                                         AS scheduled_time
                                                                                                         , MIN(table_1644172437.average_awarded_weekly_volume)                          AS average_awarded_weekly_volume
                                                                                                         , MIN(table_1644172437.daily_award)                                            AS daily_award
                                                                                                         , MIN(table_1644172437.day_of_the_week)                                        AS day_of_the_week
                                                                                                         , MIN(table_1644172437.allocation_type)                                        AS allocation_type
                                                                                                         , MIN(table_1644172437.sun_max__loads_)                                        AS sun_max__loads_
                                                                                                         , MIN(table_1644172437.mon_max__loads_)                                        AS mon_max__loads_
                                                                                                         , MIN(table_1644172437.tue_max__loads_)                                        AS tue_max__loads_
                                                                                                         , MIN(table_1644172437.wed_max__loads_)                                        AS wed_max__loads_
                                                                                                         , MIN(table_1644172437.thu_max__loads_)                                        AS thu_max__loads_
                                                                                                         , MIN(table_1644172437.fri_max__loads_)                                        AS fri_max__loads_
                                                                                                         , MIN(table_1644172437.sat_max__loads_)                                        AS sat_max__loads_
                                                                                                         , MIN(table_1644172437.gbu_per_shipping_site)                                  AS gbu_per_shipping_site
                                                                                                         , MIN(table_1644172437.shipping_conditions)                                    AS shipping_conditions
                                                                                                         , MIN(table_1644172437.postal_code_raw_tms)                                    AS postal_code_raw_tms
                                                                                                         , MIN(table_1644172437.postal_code_final_stop)                                 AS postal_code_final_stop
                                                                                                         , MIN(table_1644172437.country_from)                                           AS country_from
                                                                                                         , MIN(table_1644172437.country_to)                                             AS country_to
                                                                                                         , MIN(table_1644172437.freight_auction_flag)                                   AS freight_auction_flag
                                                                                                         , MIN(table_1644172437.true_fa_flag)                                           AS true_fa_flag
                                                                                                         , MIN(table_1644172437.freight_type)                                           AS freight_type
                                                                                                         , MIN(table_1644172437.operational_freight_type)                               AS operational_freight_type
                                                                                                         , MIN(table_1644172437.pre_tms_upgrade_flag)                                   AS pre_tms_upgrade_flag
                                                                                                         , MIN(table_1644172437.data_structure_version)                                 AS data_structure_version
                                                                                                         , MIN(table_1644172437.primary_carrier_flag)                                   AS primary_carrier_flag
                                                                                                         , MIN(table_1644172437.tendered_back_to_primary_carrier_with_no_fa_adjustment) AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                                                                         , MIN(table_1644172437.tendered_back_to_primary_carrier_with__fa_adjustment)   AS tendered_back_to_primary_carrier_with__fa_adjustment
                                                                                                         , MIN(table_1644172437.tender_accepted_loads_with_no_fa)                       AS tender_accepted_loads_with_no_fa
                                                                                                         , MIN(table_1644172437.tender_accepted_loads_with_fa)                          AS tender_accepted_loads_with_fa
                                                                                                         , MIN(table_1644172437.tender_rejected_loads)                                  AS tender_rejected_loads
                                                                                                         , MIN(table_1644172437.previous_tender_date_a_time)                            AS previous_tender_date_a_time
                                                                                                         , MIN(table_1644172437.time_between_tender_events)                             AS time_between_tender_events
                                                                                                         , MIN(table_1644172437.canceled_due_to_no_response)                            AS canceled_due_to_no_response
                                                                                                         , MIN(table_1644172437.customer)                                               AS customer
                                                                                                         , MIN(table_1644172437.customer_level_1_id)                                    AS customer_level_1_id
                                                                                                         , MIN(table_1644172437.customer_level_1_description)                           AS customer_level_1_description
                                                                                                         , MIN(table_1644172437.customer_level_2_id)                                    AS customer_level_2_id
                                                                                                         , MIN(table_1644172437.customer_level_2_description)                           AS customer_level_2_description
                                                                                                         , MIN(table_1644172437.customer_level_3_id)                                    AS customer_level_3_id
                                                                                                         , MIN(table_1644172437.customer_level_3_description)                           AS customer_level_3_description
                                                                                                         , MIN(table_1644172437.customer_level_4_id)                                    AS customer_level_4_id
                                                                                                         , MIN(table_1644172437.customer_level_4_description)                           AS customer_level_4_description
                                                                                                         , MIN(table_1644172437.customer_level_5_id)                                    AS customer_level_5_id
                                                                                                         , MIN(table_1644172437.customer_level_5_description)                           AS customer_level_5_description
                                                                                                         , MIN(table_1644172437.customer_level_6_id)                                    AS customer_level_6_id
                                                                                                         , MIN(table_1644172437.customer_level_6_description)                           AS customer_level_6_description
                                                                                                         , MIN(table_1644172437.customer_level_7_id)                                    AS customer_level_7_id
                                                                                                         , MIN(table_1644172437.customer_level_7_description)                           AS customer_level_7_description
                                                                                                         , MIN(table_1644172437.customer_level_8_id)                                    AS customer_level_8_id
                                                                                                         , MIN(table_1644172437.customer_level_8_description)                           AS customer_level_8_description
                                                                                                         , MIN(table_1644172437.customer_level_9_id)                                    AS customer_level_9_id
                                                                                                         , MIN(table_1644172437.customer_level_9_description)                           AS customer_level_9_description
                                                                                                         , MIN(table_1644172437.customer_level_10_id)                                   AS customer_level_10_id
                                                                                                         , MIN(table_1644172437.customer_level_10_description)                          AS customer_level_10_description
                                                                                                         , MIN(table_1644172437.customer_level_11_id)                                   AS customer_level_11_id
                                                                                                         , MIN(table_1644172437.customer_level_11_description)                          AS customer_level_11_description
                                                                                                         , MIN(table_1644172437.customer_level_12_id)                                   AS customer_level_12_id
                                                                                                         , MIN(table_1644172437.customer_level_12_description)                          AS customer_level_12_description
                                                                                                         , MIN(table_1644172437.actual_carrier_id)                                      AS actual_carrier_id
                                                                                                         , MIN(table_1644172437.actual_carrier_description)                             AS actual_carrier_description
                                                                                                         , MIN(table_1644172437.actual_carrier_total_transportation_cost_usd)           AS actual_carrier_total_transportation_cost_usd
                                                                                                         , MIN(table_1644172437.linehaul)                                               AS linehaul
                                                                                                         , MIN(table_1644172437.incremental_fa)                                         AS incremental_fa
                                                                                                         , MIN(table_1644172437.cnc_carrier_mix)                                        AS cnc_carrier_mix
                                                                                                         , MIN(table_1644172437.unsourced)                                              AS unsourced
                                                                                                         , MIN(table_1644172437.fuel)                                                   AS fuel
                                                                                                         , MIN(table_1644172437.accessorial)                                            AS accessorial
                                                                                                         , MIN(table_1644172437.appliances)                                             AS appliances
                                                                                                         , MIN(table_1644172437.baby_care)                                              AS baby_care
                                                                                                         , MIN(table_1644172437.chemicals)                                              AS chemicals
                                                                                                         , MIN(table_1644172437.fabric_care)                                            AS fabric_care
                                                                                                         , MIN(table_1644172437.family_care)                                            AS family_care
                                                                                                         , MIN(table_1644172437.fem_care)                                               AS fem_care
                                                                                                         , MIN(table_1644172437.hair_care)                                              AS hair_care
                                                                                                         , MIN(table_1644172437.home_care)                                              AS home_care
                                                                                                         , MIN(table_1644172437.oral_care)                                              AS oral_care
                                                                                                         , MIN(table_1644172437.phc)                                                    AS phc
                                                                                                         , MIN(table_1644172437.shave_care)                                             AS shave_care
                                                                                                         , MIN(table_1644172437.skin_a_personal_care)                                   AS skin_a_personal_care
                                                                                                         , MIN(table_1644172437.other)                                                  AS other
                                                                                                         , MIN(table_1644172437.tfts_load_tmstp)                                        AS tfts_load_tmstp
                                                                                                         , MIN(table_1644172437.load_from_file)                                         AS load_from_file
                                                                                                         , MIN(table_1644172437.bd_mod_tmstp)                                           AS bd_mod_tmstp
                                                                                                         , MIN(table_1644172437.historical_data_structure_flag)                         AS historical_data_structure_flag
                                                                                                         , MIN(table_1644172437.scotts_magical_field_of_dreams)                         AS scotts_magical_field_of_dreams
                                                                                                         , MIN(table_1644172437.tender_date_time_type)                                  AS tender_date_time_type
                                                                                                         , MIN(table_1644172437.state_province)                                         AS state_province
                                                                                                         , SUM(table_1644172437.monday_accept_count)                                    AS monday_accept_count
                                                                                                         , SUM(table_1644172437.monday_total_count)                                     AS monday_total_count
                                                                                                         , SUM(table_1644172437.thursday_accept_count)                                  AS thursday_accept_count
                                                                                                         , SUM(table_1644172437.thursday_total_count)                                   AS thursday_total_count
                                                                                                         , SUM(table_1644172437.friday_accept_count)                                    AS friday_accept_count
                                                                                                         , SUM(table_1644172437.friday_total_count)                                     AS friday_total_count
                                                                                                         , SUM(table_1644172437.sunday_accept_count)                                    AS sunday_accept_count
                                                                                                         , SUM(table_1644172437.sunday_total_count)                                     AS sunday_total_count
                                                                                                         , SUM(table_1644172437.wednesday_accept_count)                                 AS wednesday_accept_count
                                                                                                         , SUM(table_1644172437.wednesday_total_count)                                  AS wednesday_total_count
                                                                                                         , SUM(table_1644172437.tuesday_accept_count)                                   AS tuesday_accept_count
                                                                                                         , SUM(table_1644172437.tuesday_total_count)                                    AS tuesday_total_count
                                                                                                         , SUM(table_1644172437.saturday_accept_count)                                  AS saturday_accept_count
                                                                                                         , SUM(table_1644172437.saturday_total_count)                                   AS saturday_total_count
                                                                                                         , SUM(table_1644172437.accept_count)                                           AS accept_count
                                                                                                         , SUM(table_1644172437.total_count)                                            AS total_count
                                                                                                         , MIN(table_1644172437.actual_ship_date_format)                                AS actual_ship_date_format
                                                                                                         , MIN(table_1644172437.destination_zip)                                        AS destination_zip
                                                                                                         , MIN(table_1644172437.historical_lane)                                        AS historical_lane
                                                                                                         , MIN(table_1644172437.customer_specific_lane)                                 AS customer_specific_lane
                                                                                                         , MIN(table_1644172437.destination)                                            AS destination
                                                                                                  FROM
                                                                                                           tac_tender_pg_summary_1 AS table_1644172437
                                                                                                  GROUP BY
                                                                                                           carrier_id
                                                                                                         , tms_service_code
                                                                                                         , lane
                                                                                                         , calendar_year_week_tac
                                                                                         )
                                                                                         table_696478142_2
                                                                                         ON
                                                                                                    table_696478142_1.carrier_id                =table_696478142_2.carrier_id
                                                                                                    AND table_696478142_1.tms_service_code      =table_696478142_2.tms_service_code
                                                                                                    AND table_696478142_1.lane                  =table_696478142_2.lane
                                                                                                    AND table_696478142_1.calendar_year_week_tac=table_696478142_2.calendar_year_week_tac
                                                        )
                                                        percents_expected_vol
                                          )
                                          rej_below_award
                            )
                            table_1443454242
--                     WHERE
--                            average_awarded_weekly_volume >= 0.02
              )
              table_717798636_2
              ON
                     table_717798636_1.carrier_id                =table_717798636_2.carrier_id
                     AND table_717798636_1.lane                  =table_717798636_2.lane
                     AND table_717798636_1.tms_service_code      =table_717798636_2.tms_service_code
                     AND table_717798636_1.calendar_year_week_tac=table_717798636_2.calendar_year_week_tac
;


INSERT OVERWRITE TABLE tac_tender_summary_new_star
SELECT
       table_717798636_1.actual_carrier_total_transportation_cost_usd AS actual_carrier_total_transportation_cost_usd
     , table_717798636_1.linehaul                                     AS linehaul
     , table_717798636_1.incremental_fa                               AS incremental_fa
     , table_717798636_1.cnc_carrier_mix                              AS cnc_carrier_mix
     , table_717798636_1.unsourced                                    AS unsourced
     , table_717798636_1.fuel                                         AS fuel
     , table_717798636_1.accessorial                                  AS accessorial
     , table_717798636_2.carrier_id                                   AS carrier_id
     , table_717798636_2.tms_service_code                             AS tms_service_code
     , table_717798636_2.lane                                         AS lane
     , table_717798636_2.lane_new                                     AS lane_detail_name
     , table_717798636_2.calendar_year_week_tac                       AS calendar_year_week_tac
     , table_717798636_2.origin_location_id                           AS origin_loc_id
     , table_717798636_2.origin_sf                                    AS origin_sf
     , table_717798636_2.destination_sf                               AS destination_sf
     , table_717798636_2.carrier_description                          AS carrier_description
     , table_717798636_2.sold_to_n                                    AS sold_to_n
     , table_717798636_2.average_awarded_weekly_volume                AS average_awarded_weekly_volume
     , table_717798636_2.sun_max__loads_                              AS sun_max__loads_
     , table_717798636_2.mon_max__loads_                              AS mon_max__loads_
     , table_717798636_2.tue_max__loads_                              AS tue_max__loads_
     , table_717798636_2.wed_max__loads_                              AS wed_max__loads_
     , table_717798636_2.thu_max__loads_                              AS thu_max__loads_
     , table_717798636_2.fri_max__loads_                              AS fri_max__loads_
     , table_717798636_2.sat_max__loads_                              AS sat_max__loads_
     , table_717798636_2.shipping_conditions                          AS shipping_conditions
     , table_717798636_2.postal_code_raw_tms                          AS postal_code_raw_tms
     , table_717798636_2.postal_code_final_stop                       AS postal_code_final_stop
     , table_717798636_2.country_from                                 AS country_from
     , table_717798636_2.country_to                                   AS country_to
     , table_717798636_2.freight_type                                 AS freight_type
     , table_717798636_2.pre_tms_upgrade_flag                         AS pre_tms_upgrade_flag
     , table_717798636_2.data_structure_version                       AS data_structure_version
     , table_717798636_2.primary_carrier_flag                         AS primary_carrier_flag
     , table_717798636_2.week_begin_date                              AS week_begin_date
     , table_717798636_2.monthtype445                                 AS monthtype445
     , table_717798636_2.calendar_year                                AS calendar_year
     , table_717798636_2.month_date                                   AS month_date
     , table_717798636_2.week_number                                  AS week_number
     , table_717798636_2.state_province                               AS state_province
     , table_717798636_2.monday_accept_count                          AS monday_accept_count
     , table_717798636_2.monday_total_count                           AS monday_total_count
     , table_717798636_2.thursday_accept_count                        AS thursday_accept_count
     , table_717798636_2.thursday_total_count                         AS thursday_total_count
     , table_717798636_2.friday_accept_count                          AS friday_accept_count
     , table_717798636_2.friday_total_count                           AS friday_total_count
     , table_717798636_2.sunday_accept_count                          AS sunday_accept_count
     , table_717798636_2.sunday_total_count                           AS sunday_total_count
     , table_717798636_2.wednesday_accept_count                       AS wednesday_accept_count
     , table_717798636_2.wednesday_total_count                        AS wednesday_total_count
     , table_717798636_2.tuesday_accept_count                         AS tuesday_accept_count
     , table_717798636_2.tuesday_total_count                          AS tuesday_total_count
     , table_717798636_2.saturday_accept_count                        AS saturday_accept_count
     , table_717798636_2.saturday_total_count                         AS saturday_total_count
     , table_717798636_2.accept_count                                 AS accept_count
     , table_717798636_2.total_count                                  AS total_count
     , table_717798636_2.destination_zip                              AS destination_zip
     , table_717798636_2.historical_lane                              AS historical_lane
     , table_717798636_2.reject_count                                 AS reject_count
     , table_717798636_2.accept_percent                               AS accept_percent
     , table_717798636_2.reject_percent                               AS reject_percent
     , table_717798636_2.expected_volume                              AS expected_volume
     , table_717798636_2.rejects_below_award                          AS rejects_below_award
     , table_717798636_2.weekly_tac                                   AS weekly_tac
     , table_717798636_2.customer_id_description                      AS customer_id_description
     , table_717798636_2.customer                                     AS customer
     , table_717798636_2.customer_3_description                       AS customer_3_description
	 , table_717798636_2.customer_level_4_description                 AS customer_level_4_description
     , table_717798636_2.customer_level_5_description                 AS customer_level_5_description
     , table_717798636_2.customer_level_6_description                 AS customer_level_6_description
     , cast(table_717798636_2.customer_level_12_description as decimal(38,16))                AS customer_level_12_description
     , cast(table_717798636_2.customer_specific_lane as decimal(38,16))                       AS customer_specific_lane
FROM
       ds_tac_tender_pg_tenders_only AS table_717798636_1
       JOIN
              (
                     SELECT
                            distinct_count_customers_customer_level_12_description_ AS distinct_cust
                          , carrier_id
                          , tms_service_code
                          , lane
                          , lane_new
                          , calendar_year_week_tac
                          , week_begin_date
                          , monthtype445
                          , calendar_year
                          , month_date
                          , week_number
                          , origin_location_id
                          , origin_sf
                          , destination_sf
                          , customer_id_description AS customer_id_description_dup
                          , carrier_description
                          , sold_to_n
                          , average_awarded_weekly_volume
                          , sun_max__loads_
                          , mon_max__loads_
                          , tue_max__loads_
                          , wed_max__loads_
                          , thu_max__loads_
                          , fri_max__loads_
                          , sat_max__loads_
                          , shipping_conditions
                          , postal_code_raw_tms
                          , postal_code_final_stop
                          , country_from
                          , country_to
                          , freight_type
                          , pre_tms_upgrade_flag
                          , data_structure_version
                          , primary_carrier_flag
                          , customer                      AS customer_dup
                          , customer_level_3_description  AS customer_level_3_description_dup
						  , customer_level_4_description  AS customer_level_4_description_dup
                          , customer_level_5_description  AS customer_level_5_description_dup
                          , customer_level_6_description  AS customer_level_6_description_dup
                          , customer_level_12_id          AS customer_level_12_id_dup
                          , customer_level_12_description AS customer_level_12_description_dup
                          , state_province
                          , monday_accept_count
                          , monday_total_count
                          , thursday_accept_count
                          , thursday_total_count
                          , friday_accept_count
                          , friday_total_count
                          , sunday_accept_count
                          , sunday_total_count
                          , wednesday_accept_count
                          , wednesday_total_count
                          , tuesday_accept_count
                          , tuesday_total_count
                          , saturday_accept_count
                          , saturday_total_count
                          , accept_count
                          , total_count
                          , destination_zip
                          , historical_lane
                          , customer_specific_lane AS customer_specific_lane_dup
                          , destination
                          , reject_count
                          , accept_percent
                          , reject_percent
                          , expected_volume
                          , rejects_below_award
                          , weekly_tac
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_id_description
                            end as customer_id_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer
                            end as customer
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_3_description
                            end as customer_3_description
						  , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_4_description
                            end as customer_level_4_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_5_description
                            end as customer_level_5_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_6_description
                            end as customer_level_6_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_level_12_description
                            end as customer_level_12_description
                          , case
                                   when distinct_count_customers_customer_level_12_description_ > 1
                                          then '*'
                                          else customer_specific_lane
                            end as customer_specific_lane
                     FROM
                            (
                                   SELECT *
                                        , case
                                                 when 0 > (expected_volume-accept_count)
                                                        then 0
                                                        else expected_volume-accept_count
                                          end                          as rejects_below_award
                                        , accept_count/expected_volume as weekly_tac
                                   FROM
                                          (
                                                 SELECT *
                                                      , accept_count/total_count as accept_percent
                                                      , reject_count/total_count as reject_percent
                                                      , case
                                                               when average_awarded_weekly_volume > total_count
                                                                      then total_count
                                                               when average_awarded_weekly_volume < total_count
                                                                      then average_awarded_weekly_volume
                                                                      else total_count
                                                        end as expected_volume
                                                 FROM
                                                        (
                                                                   SELECT
                                                                              table_696478142_1.distinct_count_customers_customer_level_12_description_ AS distinct_count_customers_customer_level_12_description_
                                                                            , table_696478142_2.carrier_id                                              AS carrier_id
                                                                            , table_696478142_2.tms_service_code                                        AS tms_service_code
                                                                            , table_696478142_2.lane                                                    AS lane
                                                                            , table_696478142_2.lane_new                                                AS lane_new
                                                                            , table_696478142_2.calendar_year_week_tac                                  AS calendar_year_week_tac
                                                                            , table_696478142_2.cassell_key                                             AS cassell_key
                                                                            , table_696478142_2.tender_event_date_a_time                                AS tender_event_date_a_time
                                                                            , table_696478142_2.actual_ship_date                                        AS actual_ship_date
                                                                            , table_696478142_2.tac_day_of_year                                         AS tac_day_of_year
                                                                            , table_696478142_2.workweekday                                             AS workweekday
                                                                            , table_696478142_2.week_begin_date                                         AS week_begin_date
                                                                            , table_696478142_2.monthtype445                                            AS monthtype445
                                                                            , table_696478142_2.calendar_year                                           AS calendar_year
                                                                            , table_696478142_2.month_date                                              AS month_date
                                                                            , table_696478142_2.week_number                                             AS week_number
                                                                            , table_696478142_2.day_of_week                                             AS day_of_week
                                                                            , table_696478142_2.load_id                                                 AS load_id
                                                                            , table_696478142_2.origin_sf                                               AS origin_sf
                                                                            , table_696478142_2.origin_location_id                                      AS origin_location_id
                                                                            , table_696478142_2.origin_zone                                             AS origin_zone
                                                                            , table_696478142_2.destination_sf                                          AS destination_sf
                                                                            , table_696478142_2.destination_zone                                        AS destination_zone
                                                                            , table_696478142_2.destination_location_id                                 AS destination_location_id
                                                                            , table_696478142_2.customer_id                                             AS customer_id
                                                                            , table_696478142_2.customer_id_description                                 AS customer_id_description
                                                                            , table_696478142_2.carrier_description                                     AS carrier_description
                                                                            , table_696478142_2.mode                                                    AS mode
                                                                            , table_696478142_2.tender_event_type_code                                  AS tender_event_type_code
                                                                            , table_696478142_2.tender_reason_code                                      AS tender_reason_code
                                                                            , table_696478142_2.tender_event_date                                       AS tender_event_date
                                                                            , table_696478142_2.tender_event_time                                       AS tender_event_time
                                                                            , table_696478142_2.transportation_planning_point                           AS transportation_planning_point
                                                                            , table_696478142_2.departure_country_code                                  AS departure_country_code
                                                                            , table_696478142_2.sap_original_shipdate                                   AS sap_original_shipdate
                                                                            , table_696478142_2.original_tendered_ship_date                             AS original_tendered_ship_date
                                                                            , table_696478142_2.day_of_the_week_of_original_tendered_shipdate           AS day_of_the_week_of_original_tendered_shipdate
                                                                            , table_696478142_2.actual_ship_time                                        AS actual_ship_time
                                                                            , table_696478142_2.actual_ship_date_and_time                               AS actual_ship_date_and_time
                                                                            , table_696478142_2.sold_to_n                                               AS sold_to_n
                                                                            , table_696478142_2.no_of_days_in_week                                      AS no_of_days_in_week
                                                                            , table_696478142_2.no_of_days_in_month                                     AS no_of_days_in_month
                                                                            , table_696478142_2.scac                                                    AS scac
                                                                            , table_696478142_2.carrier_mode_description                                AS carrier_mode_description
                                                                            , table_696478142_2.tariff_id                                               AS tariff_id
                                                                            , table_696478142_2.schedule_id                                             AS schedule_id
                                                                            , table_696478142_2.tender_event_type_description                           AS tender_event_type_description
                                                                            , table_696478142_2.tender_acceptance_key                                   AS tender_acceptance_key
                                                                            , table_696478142_2.tender_reason_code_description                          AS tender_reason_code_description
                                                                            , table_696478142_2.scheduled_date                                          AS scheduled_date
                                                                            , table_696478142_2.scheduled_time                                          AS scheduled_time
                                                                            , table_696478142_2.average_awarded_weekly_volume                           AS average_awarded_weekly_volume
                                                                            , table_696478142_2.daily_award                                             AS daily_award
                                                                            , table_696478142_2.day_of_the_week                                         AS day_of_the_week
                                                                            , table_696478142_2.allocation_type                                         AS allocation_type
                                                                            , table_696478142_2.sun_max__loads_                                         AS sun_max__loads_
                                                                            , table_696478142_2.mon_max__loads_                                         AS mon_max__loads_
                                                                            , table_696478142_2.tue_max__loads_                                         AS tue_max__loads_
                                                                            , table_696478142_2.wed_max__loads_                                         AS wed_max__loads_
                                                                            , table_696478142_2.thu_max__loads_                                         AS thu_max__loads_
                                                                            , table_696478142_2.fri_max__loads_                                         AS fri_max__loads_
                                                                            , table_696478142_2.sat_max__loads_                                         AS sat_max__loads_
                                                                            , table_696478142_2.gbu_per_shipping_site                                   AS gbu_per_shipping_site
                                                                            , table_696478142_2.shipping_conditions                                     AS shipping_conditions
                                                                            , table_696478142_2.postal_code_raw_tms                                     AS postal_code_raw_tms
                                                                            , table_696478142_2.postal_code_final_stop                                  AS postal_code_final_stop
                                                                            , table_696478142_2.country_from                                            AS country_from
                                                                            , table_696478142_2.country_to                                              AS country_to
                                                                            , table_696478142_2.freight_auction_flag                                    AS freight_auction_flag
                                                                            , table_696478142_2.true_fa_flag                                            AS true_fa_flag
                                                                            , table_696478142_2.freight_type                                            AS freight_type
                                                                            , table_696478142_2.operational_freight_type                                AS operational_freight_type
                                                                            , table_696478142_2.pre_tms_upgrade_flag                                    AS pre_tms_upgrade_flag
                                                                            , table_696478142_2.data_structure_version                                  AS data_structure_version
                                                                            , table_696478142_2.primary_carrier_flag                                    AS primary_carrier_flag
                                                                            , table_696478142_2.tendered_back_to_primary_carrier_with_no_fa_adjustment  AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                                            , table_696478142_2.tendered_back_to_primary_carrier_with__fa_adjustment    AS tendered_back_to_primary_carrier_with__fa_adjustment
                                                                            , table_696478142_2.tender_accepted_loads_with_no_fa                        AS tender_accepted_loads_with_no_fa
                                                                            , table_696478142_2.tender_accepted_loads_with_fa                           AS tender_accepted_loads_with_fa
                                                                            , table_696478142_2.tender_rejected_loads                                   AS tender_rejected_loads
                                                                            , table_696478142_2.previous_tender_date_a_time                             AS previous_tender_date_a_time
                                                                            , table_696478142_2.time_between_tender_events                              AS time_between_tender_events
                                                                            , table_696478142_2.canceled_due_to_no_response                             AS canceled_due_to_no_response
                                                                            , table_696478142_2.customer                                                AS customer
                                                                            , table_696478142_2.customer_level_1_id                                     AS customer_level_1_id
                                                                            , table_696478142_2.customer_level_1_description                            AS customer_level_1_description
                                                                            , table_696478142_2.customer_level_2_id                                     AS customer_level_2_id
                                                                            , table_696478142_2.customer_level_2_description                            AS customer_level_2_description
                                                                            , table_696478142_2.customer_level_3_id                                     AS customer_level_3_id
                                                                            , table_696478142_2.customer_level_3_description                            AS customer_level_3_description
                                                                            , table_696478142_2.customer_level_4_id                                     AS customer_level_4_id
                                                                            , table_696478142_2.customer_level_4_description                            AS customer_level_4_description
                                                                            , table_696478142_2.customer_level_5_id                                     AS customer_level_5_id
                                                                            , table_696478142_2.customer_level_5_description                            AS customer_level_5_description
                                                                            , table_696478142_2.customer_level_6_id                                     AS customer_level_6_id
                                                                            , table_696478142_2.customer_level_6_description                            AS customer_level_6_description
                                                                            , table_696478142_2.customer_level_7_id                                     AS customer_level_7_id
                                                                            , table_696478142_2.customer_level_7_description                            AS customer_level_7_description
                                                                            , table_696478142_2.customer_level_8_id                                     AS customer_level_8_id
                                                                            , table_696478142_2.customer_level_8_description                            AS customer_level_8_description
                                                                            , table_696478142_2.customer_level_9_id                                     AS customer_level_9_id
                                                                            , table_696478142_2.customer_level_9_description                            AS customer_level_9_description
                                                                            , table_696478142_2.customer_level_10_id                                    AS customer_level_10_id
                                                                            , table_696478142_2.customer_level_10_description                           AS customer_level_10_description
                                                                            , table_696478142_2.customer_level_11_id                                    AS customer_level_11_id
                                                                            , table_696478142_2.customer_level_11_description                           AS customer_level_11_description
                                                                            , table_696478142_2.customer_level_12_id                                    AS customer_level_12_id
                                                                            , table_696478142_2.customer_level_12_description                           AS customer_level_12_description
                                                                            , table_696478142_2.actual_carrier_id                                       AS actual_carrier_id
                                                                            , table_696478142_2.actual_carrier_description                              AS actual_carrier_description
                                                                            , table_696478142_2.actual_carrier_total_transportation_cost_usd            AS actual_carrier_total_transportation_cost_usd
                                                                            , table_696478142_2.linehaul                                                AS linehaul
                                                                            , table_696478142_2.incremental_fa                                          AS incremental_fa
                                                                            , table_696478142_2.cnc_carrier_mix                                         AS cnc_carrier_mix
                                                                            , table_696478142_2.unsourced                                               AS unsourced
                                                                            , table_696478142_2.fuel                                                    AS fuel
                                                                            , table_696478142_2.accessorial                                             AS accessorial
                                                                            , table_696478142_2.appliances                                              AS appliances
                                                                            , table_696478142_2.baby_care                                               AS baby_care
                                                                            , table_696478142_2.chemicals                                               AS chemicals
                                                                            , table_696478142_2.fabric_care                                             AS fabric_care
                                                                            , table_696478142_2.family_care                                             AS family_care
                                                                            , table_696478142_2.fem_care                                                AS fem_care
                                                                            , table_696478142_2.hair_care                                               AS hair_care
                                                                            , table_696478142_2.home_care                                               AS home_care
                                                                            , table_696478142_2.oral_care                                               AS oral_care
                                                                            , table_696478142_2.phc                                                     AS phc
                                                                            , table_696478142_2.shave_care                                              AS shave_care
                                                                            , table_696478142_2.skin_a_personal_care                                    AS skin_a_personal_care
                                                                            , table_696478142_2.other                                                   AS other
                                                                            , table_696478142_2.tfts_load_tmstp                                         AS tfts_load_tmstp
                                                                            , table_696478142_2.load_from_file                                          AS load_from_file
                                                                            , table_696478142_2.bd_mod_tmstp                                            AS bd_mod_tmstp
                                                                            , table_696478142_2.historical_data_structure_flag                          AS historical_data_structure_flag
                                                                            , table_696478142_2.scotts_magical_field_of_dreams                          AS scotts_magical_field_of_dreams
                                                                            , table_696478142_2.tender_date_time_type                                   AS tender_date_time_type
                                                                            , table_696478142_2.state_province                                          AS state_province
                                                                            , table_696478142_2.monday_accept_count                                     AS monday_accept_count
                                                                            , table_696478142_2.monday_total_count                                      AS monday_total_count
                                                                            , table_696478142_2.thursday_accept_count                                   AS thursday_accept_count
                                                                            , table_696478142_2.thursday_total_count                                    AS thursday_total_count
                                                                            , table_696478142_2.friday_accept_count                                     AS friday_accept_count
                                                                            , table_696478142_2.friday_total_count                                      AS friday_total_count
                                                                            , table_696478142_2.sunday_accept_count                                     AS sunday_accept_count
                                                                            , table_696478142_2.sunday_total_count                                      AS sunday_total_count
                                                                            , table_696478142_2.wednesday_accept_count                                  AS wednesday_accept_count
                                                                            , table_696478142_2.wednesday_total_count                                   AS wednesday_total_count
                                                                            , table_696478142_2.tuesday_accept_count                                    AS tuesday_accept_count
                                                                            , table_696478142_2.tuesday_total_count                                     AS tuesday_total_count
                                                                            , table_696478142_2.saturday_accept_count                                   AS saturday_accept_count
                                                                            , table_696478142_2.saturday_total_count                                    AS saturday_total_count
                                                                            , table_696478142_2.accept_count                                            AS accept_count
                                                                            , table_696478142_2.total_count                                             AS total_count
                                                                            , table_696478142_2.actual_ship_date_format                                 AS actual_ship_date_format
                                                                            , table_696478142_2.destination_zip                                         AS destination_zip
                                                                            , table_696478142_2.historical_lane                                         AS historical_lane
                                                                            , table_696478142_2.customer_specific_lane                                  AS customer_specific_lane
                                                                            , table_696478142_2.destination                                             AS destination
                                                                            , (table_696478142_2.total_count-table_696478142_2.accept_count)            AS reject_count
                                                                   FROM
                                                                              (
                                                                                       SELECT
                                                                                                carrier_id
                                                                                              , tms_service_code
                                                                                              , lane
                                                                                              , lane_new
                                                                                              , calendar_year_week_tac
                                                                                              , count(DISTINCT table_1742270680.customer_level_12_description) AS distinct_count_customers_customer_level_12_description_
                                                                                       FROM
                                                                                                tac_tender_pg_summary_1 AS table_1742270680
                                                                                       GROUP BY
                                                                                                carrier_id
                                                                                              , tms_service_code
                                                                                              , lane
                                                                                              , lane_new
                                                                                              , calendar_year_week_tac
                                                                              )
                                                                              table_696478142_1
                                                                              RIGHT JOIN
                                                                                         (
                                                                                                  SELECT
                                                                                                           table_1644172437.carrier_id
                                                                                                         , table_1644172437.tms_service_code
                                                                                                         , table_1644172437.lane
                                                                                                         , table_1644172437.lane_new
                                                                                                         , table_1644172437.calendar_year_week_tac
                                                                                                         , MIN(table_1644172437.cassell_key)                                            AS cassell_key
                                                                                                         , MIN(table_1644172437.tender_event_date_a_time)                               AS tender_event_date_a_time
                                                                                                         , MIN(table_1644172437.actual_ship_date)                                       AS actual_ship_date
                                                                                                         , MIN(table_1644172437.tac_day_of_year)                                        AS tac_day_of_year
                                                                                                         , MIN(table_1644172437.workweekday)                                            AS workweekday
                                                                                                         , MIN(table_1644172437.week_begin_date)                                        AS week_begin_date
                                                                                                         , MIN(table_1644172437.monthtype445)                                           AS monthtype445
                                                                                                         , MIN(table_1644172437.calendar_year)                                          AS calendar_year
                                                                                                         , MIN(table_1644172437.month_date)                                             AS month_date
                                                                                                         , MIN(table_1644172437.week_number)                                            AS week_number
                                                                                                         , MIN(table_1644172437.day_of_week)                                            AS day_of_week
                                                                                                         , MIN(table_1644172437.load_id)                                                AS load_id
                                                                                                         , MIN(table_1644172437.origin_sf)                                              AS origin_sf
                                                                                                         , MIN(table_1644172437.origin_location_id)                                     AS origin_location_id
                                                                                                         , MIN(table_1644172437.origin_zone)                                            AS origin_zone
                                                                                                         , MIN(table_1644172437.destination_sf)                                         AS destination_sf
                                                                                                         , MIN(table_1644172437.destination_zone)                                       AS destination_zone
                                                                                                         , MIN(table_1644172437.destination_location_id)                                AS destination_location_id
                                                                                                         , MIN(table_1644172437.customer_id)                                            AS customer_id
                                                                                                         , MIN(table_1644172437.customer_id_description)                                AS customer_id_description
                                                                                                         , MIN(table_1644172437.carrier_description)                                    AS carrier_description
                                                                                                         , MIN(table_1644172437.mode)                                                   AS mode
                                                                                                         , MIN(table_1644172437.tender_event_type_code)                                 AS tender_event_type_code
                                                                                                         , MIN(table_1644172437.tender_reason_code)                                     AS tender_reason_code
                                                                                                         , MIN(table_1644172437.tender_event_date)                                      AS tender_event_date
                                                                                                         , MIN(table_1644172437.tender_event_time)                                      AS tender_event_time
                                                                                                         , MIN(table_1644172437.transportation_planning_point)                          AS transportation_planning_point
                                                                                                         , MIN(table_1644172437.departure_country_code)                                 AS departure_country_code
                                                                                                         , MIN(table_1644172437.sap_original_shipdate)                                  AS sap_original_shipdate
                                                                                                         , MIN(table_1644172437.original_tendered_ship_date)                            AS original_tendered_ship_date
                                                                                                         , MIN(table_1644172437.day_of_the_week_of_original_tendered_shipdate)          AS day_of_the_week_of_original_tendered_shipdate
                                                                                                         , MIN(table_1644172437.actual_ship_time)                                       AS actual_ship_time
                                                                                                         , MIN(table_1644172437.actual_ship_date_and_time)                              AS actual_ship_date_and_time
                                                                                                         , MIN(table_1644172437.sold_to_n)                                              AS sold_to_n
                                                                                                         , MIN(table_1644172437.no_of_days_in_week)                                     AS no_of_days_in_week
                                                                                                         , MIN(table_1644172437.no_of_days_in_month)                                    AS no_of_days_in_month
                                                                                                         , MIN(table_1644172437.scac)                                                   AS scac
                                                                                                         , MIN(table_1644172437.carrier_mode_description)                               AS carrier_mode_description
                                                                                                         , MIN(table_1644172437.tariff_id)                                              AS tariff_id
                                                                                                         , MIN(table_1644172437.schedule_id)                                            AS schedule_id
                                                                                                         , MIN(table_1644172437.tender_event_type_description)                          AS tender_event_type_description
                                                                                                         , MIN(table_1644172437.tender_acceptance_key)                                  AS tender_acceptance_key
                                                                                                         , MIN(table_1644172437.tender_reason_code_description)                         AS tender_reason_code_description
                                                                                                         , MIN(table_1644172437.scheduled_date)                                         AS scheduled_date
                                                                                                         , MIN(table_1644172437.scheduled_time)                                         AS scheduled_time
                                                                                                         --, MIN(table_1644172437.average_awarded_weekly_volume)                          AS average_awarded_weekly_volume
                                                                                                         , MIN(table_1644172437.daily_award)                                            AS daily_award
                                                                                                         , MIN(table_1644172437.day_of_the_week)                                        AS day_of_the_week
                                                                                                         , MIN(table_1644172437.allocation_type)                                        AS allocation_type
                                                                                                         , MIN(table_1644172437.sun_max__loads_)                                        AS sun_max__loads_
                                                                                                         , MIN(table_1644172437.mon_max__loads_)                                        AS mon_max__loads_
                                                                                                         , MIN(table_1644172437.tue_max__loads_)                                        AS tue_max__loads_
                                                                                                         , MIN(table_1644172437.wed_max__loads_)                                        AS wed_max__loads_
                                                                                                         , MIN(table_1644172437.thu_max__loads_)                                        AS thu_max__loads_
                                                                                                         , MIN(table_1644172437.fri_max__loads_)                                        AS fri_max__loads_
                                                                                                         , MIN(table_1644172437.sat_max__loads_)                                        AS sat_max__loads_
                                                                                                         , MIN(table_1644172437.gbu_per_shipping_site)                                  AS gbu_per_shipping_site
                                                                                                         , MIN(table_1644172437.shipping_conditions)                                    AS shipping_conditions
                                                                                                         , MIN(table_1644172437.postal_code_raw_tms)                                    AS postal_code_raw_tms
                                                                                                         , MIN(table_1644172437.postal_code_final_stop)                                 AS postal_code_final_stop
                                                                                                         , MIN(table_1644172437.country_from)                                           AS country_from
                                                                                                         , MIN(table_1644172437.country_to)                                             AS country_to
                                                                                                         , MIN(table_1644172437.freight_auction_flag)                                   AS freight_auction_flag
                                                                                                         , MIN(table_1644172437.true_fa_flag)                                           AS true_fa_flag
                                                                                                         , MIN(table_1644172437.freight_type)                                           AS freight_type
                                                                                                         , MIN(table_1644172437.operational_freight_type)                               AS operational_freight_type
                                                                                                         , MIN(table_1644172437.pre_tms_upgrade_flag)                                   AS pre_tms_upgrade_flag
                                                                                                         , MIN(table_1644172437.data_structure_version)                                 AS data_structure_version
                                                                                                         , MIN(table_1644172437.primary_carrier_flag)                                   AS primary_carrier_flag
                                                                                                         , MIN(table_1644172437.tendered_back_to_primary_carrier_with_no_fa_adjustment) AS tendered_back_to_primary_carrier_with_no_fa_adjustment
                                                                                                         , MIN(table_1644172437.tendered_back_to_primary_carrier_with__fa_adjustment)   AS tendered_back_to_primary_carrier_with__fa_adjustment
                                                                                                         , MIN(table_1644172437.tender_accepted_loads_with_no_fa)                       AS tender_accepted_loads_with_no_fa
                                                                                                         , MIN(table_1644172437.tender_accepted_loads_with_fa)                          AS tender_accepted_loads_with_fa
                                                                                                         , MIN(table_1644172437.tender_rejected_loads)                                  AS tender_rejected_loads
                                                                                                         , MIN(table_1644172437.previous_tender_date_a_time)                            AS previous_tender_date_a_time
                                                                                                         , MIN(table_1644172437.time_between_tender_events)                             AS time_between_tender_events
                                                                                                         , MIN(table_1644172437.canceled_due_to_no_response)                            AS canceled_due_to_no_response
                                                                                                         , MIN(table_1644172437.customer)                                               AS customer
                                                                                                         , MIN(table_1644172437.customer_level_1_id)                                    AS customer_level_1_id
                                                                                                         , MIN(table_1644172437.customer_level_1_description)                           AS customer_level_1_description
                                                                                                         , MIN(table_1644172437.customer_level_2_id)                                    AS customer_level_2_id
                                                                                                         , MIN(table_1644172437.customer_level_2_description)                           AS customer_level_2_description
                                                                                                         , MIN(table_1644172437.customer_level_3_id)                                    AS customer_level_3_id
                                                                                                         , MIN(table_1644172437.customer_level_3_description)                           AS customer_level_3_description
                                                                                                         , MIN(table_1644172437.customer_level_4_id)                                    AS customer_level_4_id
                                                                                                         , MIN(table_1644172437.customer_level_4_description)                           AS customer_level_4_description
                                                                                                         , MIN(table_1644172437.customer_level_5_id)                                    AS customer_level_5_id
                                                                                                         , MIN(table_1644172437.customer_level_5_description)                           AS customer_level_5_description
                                                                                                         , MIN(table_1644172437.customer_level_6_id)                                    AS customer_level_6_id
                                                                                                         , MIN(table_1644172437.customer_level_6_description)                           AS customer_level_6_description
                                                                                                         , MIN(table_1644172437.customer_level_7_id)                                    AS customer_level_7_id
                                                                                                         , MIN(table_1644172437.customer_level_7_description)                           AS customer_level_7_description
                                                                                                         , MIN(table_1644172437.customer_level_8_id)                                    AS customer_level_8_id
                                                                                                         , MIN(table_1644172437.customer_level_8_description)                           AS customer_level_8_description
                                                                                                         , MIN(table_1644172437.customer_level_9_id)                                    AS customer_level_9_id
                                                                                                         , MIN(table_1644172437.customer_level_9_description)                           AS customer_level_9_description
                                                                                                         , MIN(table_1644172437.customer_level_10_id)                                   AS customer_level_10_id
                                                                                                         , MIN(table_1644172437.customer_level_10_description)                          AS customer_level_10_description
                                                                                                         , MIN(table_1644172437.customer_level_11_id)                                   AS customer_level_11_id
                                                                                                         , MIN(table_1644172437.customer_level_11_description)                          AS customer_level_11_description
                                                                                                         , MIN(table_1644172437.customer_level_12_id)                                   AS customer_level_12_id
                                                                                                         , MIN(table_1644172437.customer_level_12_description)                          AS customer_level_12_description
                                                                                                         , MIN(table_1644172437.actual_carrier_id)                                      AS actual_carrier_id
                                                                                                         , MIN(table_1644172437.actual_carrier_description)                             AS actual_carrier_description
                                                                                                         , MIN(table_1644172437.actual_carrier_total_transportation_cost_usd)           AS actual_carrier_total_transportation_cost_usd
                                                                                                         , MIN(table_1644172437.linehaul)                                               AS linehaul
                                                                                                         , MIN(table_1644172437.incremental_fa)                                         AS incremental_fa
                                                                                                         , MIN(table_1644172437.cnc_carrier_mix)                                        AS cnc_carrier_mix
                                                                                                         , MIN(table_1644172437.unsourced)                                              AS unsourced
                                                                                                         , MIN(table_1644172437.fuel)                                                   AS fuel
                                                                                                         , MIN(table_1644172437.accessorial)                                            AS accessorial
                                                                                                         , MIN(table_1644172437.appliances)                                             AS appliances
                                                                                                         , MIN(table_1644172437.baby_care)                                              AS baby_care
                                                                                                         , MIN(table_1644172437.chemicals)                                              AS chemicals
                                                                                                         , MIN(table_1644172437.fabric_care)                                            AS fabric_care
                                                                                                         , MIN(table_1644172437.family_care)                                            AS family_care
                                                                                                         , MIN(table_1644172437.fem_care)                                               AS fem_care
                                                                                                         , MIN(table_1644172437.hair_care)                                              AS hair_care
                                                                                                         , MIN(table_1644172437.home_care)                                              AS home_care
                                                                                                         , MIN(table_1644172437.oral_care)                                              AS oral_care
                                                                                                         , MIN(table_1644172437.phc)                                                    AS phc
                                                                                                         , MIN(table_1644172437.shave_care)                                             AS shave_care
                                                                                                         , MIN(table_1644172437.skin_a_personal_care)                                   AS skin_a_personal_care
                                                                                                         , MIN(table_1644172437.other)                                                  AS other
                                                                                                         , MIN(table_1644172437.tfts_load_tmstp)                                        AS tfts_load_tmstp
                                                                                                         , MIN(table_1644172437.load_from_file)                                         AS load_from_file
                                                                                                         , MIN(table_1644172437.bd_mod_tmstp)                                           AS bd_mod_tmstp
                                                                                                         , MIN(table_1644172437.historical_data_structure_flag)                         AS historical_data_structure_flag
                                                                                                         , MIN(table_1644172437.scotts_magical_field_of_dreams)                         AS scotts_magical_field_of_dreams
                                                                                                         , MIN(table_1644172437.tender_date_time_type)                                  AS tender_date_time_type
                                                                                                         , MIN(table_1644172437.state_province)                                         AS state_province
                                                                                                         , SUM(table_1644172437.monday_accept_count)                                    AS monday_accept_count
                                                                                                         , SUM(table_1644172437.monday_total_count)                                     AS monday_total_count
                                                                                                         , SUM(table_1644172437.thursday_accept_count)                                  AS thursday_accept_count
                                                                                                         , SUM(table_1644172437.thursday_total_count)                                   AS thursday_total_count
                                                                                                         , SUM(table_1644172437.friday_accept_count)                                    AS friday_accept_count
                                                                                                         , SUM(table_1644172437.friday_total_count)                                     AS friday_total_count
                                                                                                         , SUM(table_1644172437.sunday_accept_count)                                    AS sunday_accept_count
                                                                                                         , SUM(table_1644172437.sunday_total_count)                                     AS sunday_total_count
                                                                                                         , SUM(table_1644172437.wednesday_accept_count)                                 AS wednesday_accept_count
                                                                                                         , SUM(table_1644172437.wednesday_total_count)                                  AS wednesday_total_count
                                                                                                         , SUM(table_1644172437.tuesday_accept_count)                                   AS tuesday_accept_count
                                                                                                         , SUM(table_1644172437.tuesday_total_count)                                    AS tuesday_total_count
                                                                                                         , SUM(table_1644172437.saturday_accept_count)                                  AS saturday_accept_count
                                                                                                         , SUM(table_1644172437.saturday_total_count)                                   AS saturday_total_count
                                                                                                         , SUM(table_1644172437.accept_count)                                           AS accept_count
                                                                                                         , SUM(table_1644172437.total_count)                                            AS total_count
                                                                                                         , MIN(table_1644172437.actual_ship_date_format)                                AS actual_ship_date_format
                                                                                                         , MIN(table_1644172437.destination_zip)                                        AS destination_zip
                                                                                                         , MIN(table_1644172437.historical_lane)                                        AS historical_lane
                                                                                                         , MIN(table_1644172437.customer_specific_lane)                                 AS customer_specific_lane
                                                                                                         , MIN(table_1644172437.destination)                                            AS destination
                                                                                                         , MIN(temp_tab.average_awarded_weekly_volume_new)                              AS average_awarded_weekly_volume
                                                                                                        FROM
                                                                                                           tac_tender_pg_summary_1 AS table_1644172437
                                                                                                           join (
                                                                                                           
                                                                                                            SELECT
                                                                                                           carrier_id
                                                                                                         , tms_service_code
                                                                                                         , lane
                                                                                                         , lane_new
                                                                                                         , calendar_year_week_tac
                                                                                                         , MIN(tab1.average_awarded_weekly_volume) AS average_awarded_weekly_volume_new
                                                                                                  FROM
                                                                                                           tac_tender_pg_summary_1 AS tab1
                                                                                                           where tab1.tender_reason_code like 'TNRD'
                                                                                                           
                                                                                                  GROUP BY
                                                                                                           carrier_id
                                                                                                         , tms_service_code
                                                                                                         , lane
                                                                                                         , lane_new
                                                                                                         , calendar_year_week_tac
                                                                                                         ) temp_tab
                                                                                                         
                                                                                   ON
                                                                                                    temp_tab.carrier_id                =table_1644172437.carrier_id
                                                                                                    AND temp_tab.tms_service_code      =table_1644172437.tms_service_code
                                                                                                    AND temp_tab.lane_new              =table_1644172437.lane_new
                                                                                                    AND temp_tab.lane                  =table_1644172437.lane
                                                                                                    AND temp_tab.calendar_year_week_tac=table_1644172437.calendar_year_week_tac
                                                                                                 
                                                                                                  GROUP BY
                                                                                                           table_1644172437.carrier_id
                                                                                                         , table_1644172437.tms_service_code
                                                                                                         , table_1644172437.lane
                                                                                                         , table_1644172437.lane_new
                                                                                                         , table_1644172437.calendar_year_week_tac
                                                                                                         )
                                                                                         table_696478142_2
                                                                                         ON
                                                                                                    table_696478142_1.carrier_id                =table_696478142_2.carrier_id
                                                                                                    AND table_696478142_1.tms_service_code      =table_696478142_2.tms_service_code
                                                                                                    AND table_696478142_1.lane_new              =table_696478142_2.lane_new
                                                                                                    AND table_696478142_1.lane                  =table_696478142_2.lane
                                                                                                    AND table_696478142_1.calendar_year_week_tac=table_696478142_2.calendar_year_week_tac
                                                        )
                                                        percents_expected_vol
                                          )
                                          rej_below_award
                            )
                            table_1443454242
              )
              table_717798636_2
              ON
                     table_717798636_1.carrier_id                =table_717798636_2.carrier_id
                     AND table_717798636_1.lane                  =table_717798636_2.lane
                     AND table_717798636_1.lane_new              =table_717798636_2.lane_new
                     AND table_717798636_1.tms_service_code      =table_717798636_2.tms_service_code
                     AND table_717798636_1.calendar_year_week_tac=table_717798636_2.calendar_year_week_tac
;
