
-- Step 1

USE ${hivevar:database};

---- Step 2
DROP TABLE IF EXISTS distinct_shpmt_id_tfs;
CREATE TEMPORARY VIEW distinct_shpmt_id_tfs AS
SELECT DISTINCT shpmt_id AS `shpmt_id` FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star;

---- Step 3
-- CAD has leading zeros in load_id column. Those will cause match fail if compared to TFS's shpmt_id
---- Confirmed that there i s only 1 destination_zone per load_id in CAD
DROP TABLE IF EXISTS distinct_load_id_cad;
CREATE TEMPORARY VIEW distinct_load_id_cad AS
SELECT load_id_cad, destination_zone, cntry_to_code, cust_id FROM (
SELECT IF(substr(load_id,1,4)='0000',substr(load_id,5),load_id) AS `load_id_cad`, first_tender_dest_zone_code AS `destination_zone`, cntry_to_code AS `cntry_to_code`, `cust_id` AS `cust_id`, 
ROW_NUMBER() OVER(PARTITION BY load_id ORDER BY IF(tfts_load_tmstp="Historical load", 1, tfts_load_tmstp) DESC) as `rank_no`
FROM ${hivevar:dbTransVsbltBw}.contract_adherence_na_merged_star) a
WHERE rank_no=1;

---- Step 4
DROP TABLE IF EXISTS distinct_load_id_frt_auction_ind_tac;
CREATE TEMPORARY VIEW distinct_load_id_frt_auction_ind_tac AS
SELECT load_id as `load_id_tac2`, frt_auction_ind as `frt_auction_ind` FROM (
SELECT load_id, frt_auction_ind, ROW_NUMBER() OVER (PARTITION BY load_id ORDER BY frt_auction_ind DESC) as `number` FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star ) a
WHERE number=1;

---- Step 5
DROP TABLE IF EXISTS distinct_shpmt_id_count_tdc_val_tfs;
CREATE TEMPORARY VIEW distinct_shpmt_id_count_tdc_val_tfs AS
SELECT shpmt_id AS `shpmt_id_tdc`, COUNT(DISTINCT(tdc_val_code)) AS `tdc_val_code_count`
FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star
GROUP BY shpmt_id;

---- Step 6
DROP TABLE IF EXISTS distinct_ship_to_num_tfs;
CREATE TEMPORARY VIEW distinct_ship_to_num_tfs AS
SELECT DISTINCT ship_to_num FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star;

-----step 7


DROP TABLE IF EXISTS distinct_cust_656;
CREATE TEMPORARY VIEW distinct_cust_656 
AS
SELECT 
	customer_id as CUST_ID,
	l1_global_name AS CUST_1_NAME,   
	COALESCE(l2_key_customer_group_name, l1_global_name) AS CUST_2_NAME, 
	COALESCE(l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_3_NAME,
	COALESCE(l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_4_NAME,
	COALESCE(l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_5_NAME,
	COALESCE(l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_6_NAME,
	COALESCE(l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_7_NAME,
	COALESCE(l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_8_NAME,
	COALESCE(l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_9_NAME,
	COALESCE(l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_10_NAME,
	COALESCE(l11_intrmdt5_name, l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_11_NAME,
	COALESCE(l12_ship_to_name, l11_intrmdt5_name, l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS CUST_12_NAME
FROM 
(
	SELECT 
		customer_id, 
		l1_global_name,   
		IF(l2_key_customer_group_name='', NULL, l2_key_customer_group_name) AS l2_key_customer_group_name,
		IF(l3_vary1_name='', NULL, l3_vary1_name) AS l3_vary1_name,		
		IF(l4_vary2_name='', NULL, l4_vary2_name) AS l4_vary2_name,		
		IF(l5_country_top_account_name='', NULL, l5_country_top_account_name) AS l5_country_top_account_name,		
		IF(l6_vary6_name='', NULL, l6_vary6_name) AS l6_vary6_name,		
		IF(l7_intrmdt_name='', NULL, l7_intrmdt_name) AS l7_intrmdt_name,		
		IF(l8_intrmdt2_name='', NULL, l8_intrmdt2_name) AS l8_intrmdt2_name,		
		IF(l9_intrmdt3_name='', NULL, l9_intrmdt3_name) AS l9_intrmdt3_name,		
		IF(l10_intrmdt4_name='', NULL, l10_intrmdt4_name) AS l10_intrmdt4_name,		
		IF(l11_intrmdt5_name='', NULL, l11_intrmdt5_name) AS l11_intrmdt5_name,	
		IF(l12_ship_to_name='', NULL, l12_ship_to_name) AS l12_ship_to_name
	FROM cust_hierarchy656_na_lkp
) a;


---- Step 8
--
DROP TABLE IF EXISTS postal_codes;
CREATE TEMPORARY VIEW `postal_codes` AS
SELECT 
    customer_id AS cust_id, 
    CASE 
        WHEN country_code LIKE '%US%' AND INSTR(`postal_code`, '-') = 6 THEN SUBSTR(`postal_code`, 1, 5) -- 06010-4714
        WHEN country_code LIKE '%US%' THEN REGEXP_EXTRACT(`postal_code`,'([0-9]*)(-*[0-9]*)', 1)
        WHEN country_code LIKE '%CA%' AND LENGTH(`postal_code`) = 7 AND LOCATE(' ', `postal_code`) > 0 THEN CONCAT(SUBSTR(`postal_code`, 1, 3), SUBSTR(`postal_code`, 5, 3))  -- Canada Zip Code with space in it
        WHEN country_code LIKE '%CA%' AND TRIM(postal_code) = '' THEN NULL
        WHEN country_code LIKE '%CA%' THEN SUBSTRING(postal_code, 1, 3)
        ELSE `postal_code`
    END AS `postal_code` 
FROM (
    SELECT DISTINCT customer_id, country_code, postal_code FROM ${hivevar:dbOsiNa}.customer_dim WHERE country_code IN ('US','CA')

) a;

---- Step 9 
DROP TABLE IF EXISTS cust_656_postal_codes;
CREATE TEMPORARY VIEW cust_656_postal_codes AS
SELECT distinct distinct_ship_to_num_tfs.ship_to_num, distinct_cust_656.cust_id AS `656_cust`, distinct_cust_656.cust_1_name, distinct_cust_656.cust_2_name, distinct_cust_656.cust_3_name, distinct_cust_656.cust_4_name, distinct_cust_656.cust_5_name, distinct_cust_656.cust_6_name, distinct_cust_656.cust_7_name, distinct_cust_656.cust_8_name, distinct_cust_656.cust_9_name, distinct_cust_656.cust_10_name, distinct_cust_656.cust_11_name, distinct_cust_656.cust_12_name, postal_codes.cust_id AS `postal_codes_cust`, postal_codes.postal_code
FROM distinct_ship_to_num_tfs
LEFT OUTER JOIN distinct_cust_656
ON distinct_ship_to_num_tfs.ship_to_num=distinct_cust_656.cust_id
LEFT OUTER JOIN postal_codes
ON distinct_ship_to_num_tfs.ship_to_num=postal_codes.cust_id
where distinct_ship_to_num_tfs.ship_to_num <> ('') ;

---- Step 10
---- for fields: 5 Digit Lane Name / Regional 2_3 Digit Lane Name / Customer Spcific Lane Name
DROP TABLE IF EXISTS load_id_orign_zone_frt_type_name_tac;
CREATE TEMPORARY VIEW load_id_orign_zone_frt_type_name_tac AS
SELECT load_id, orign_zone_code, frt_type_name FROM (
SELECT load_id, orign_zone_code, frt_type_name, ROW_NUMBER() OVER(PARTITION BY load_id ORDER BY tender_date DESC, tender_datetm DESC, orign_zone_code DESC) as `rank_no`
FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star
) a
WHERE rank_no=1;

---- Step 11
DROP TABLE IF EXISTS truckload_intermodal_1;
CREATE TEMPORARY VIEW truckload_intermodal_1 AS
SELECT shpmt_num, serv_tms_code, dest_zone_go_name, dest_loc_code, origin_zone_name  FROM (
SELECT shpmt_num, serv_tms_code, dest_zone_go_name, dest_loc_code, origin_zone_name, ROW_NUMBER() OVER (PARTITION BY shpmt_num ORDER BY serv_tms_code DESC) as rank_num FROM ${hivevar:dbTransVsbltBw}.on_time_arriv_shpmt_custshpmt_na_merged_star
) a 
WHERE rank_num=1;

---- Step 12
DROP TABLE IF EXISTS tms_service_codes_map;
CREATE TABLE `tms_service_codes_map` (
`code_a` STRING,
`code_g` STRING,
`truckload_intermodal` STRING);

----Step 13 after TMS Upgrade

INSERT INTO TABLE `tms_service_codes_map` 
SELECT "TLDF" AS code_a,"DO NOT USE AFTER BID" AS code_g,"Truckload" AS truckload_intermodal
UNION ALL
SELECT "TLSC" AS code_a,"DO NOT USE AFTER BID" AS code_g,"Truckload" AS truckload_intermodal
UNION ALL
SELECT "TLSD" AS code_a,"DO NOT USE AFTER BID" AS code_g,"Truckload" AS truckload_intermodal
UNION ALL
SELECT "HCP"  AS code_a,"HCPT" AS code_g,"Ocean" AS truckload_intermodal
UNION ALL
SELECT "TOC"  AS code_a,"TOCT" AS code_g,"Ocean" AS truckload_intermodal
UNION ALL
SELECT "TL"   AS code_a,"W01T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLTO" AS code_a,"W02T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLBR" AS code_a,"W03T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLCG" AS code_a,"W04T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLNO" AS code_a,"W05T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLDE" AS code_a,"W06T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLLG" AS code_a,"W07T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLLT" AS code_a,"W08T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLHT" AS code_a,"W09T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLHC" AS code_a,"W10T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLLH" AS code_a,"W11T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLEV" AS code_a,"W12T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLEO" AS code_a,"W13T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLEC" AS code_a,"W14T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TECO" AS code_a,"W15T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLLE" AS code_a,"W16T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TELO" AS code_a,"W17T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLCD" AS code_a,"W18T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLDS" AS code_a,"W19T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLDC" AS code_a,"W20T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLHZ" AS code_a,"W22T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TL2R" AS code_a,"W23T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLDD" AS code_a,"W25T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLJ1" AS code_a,"W26T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "TLEX" AS code_a,"W27T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "W28T" AS code_a,"Y28T" AS code_g,"Truckload"  AS truckload_intermodal
UNION ALL
SELECT "PIM4" AS code_a,"WAP4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PIM5" AS code_a,"WAP5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PIM6" AS code_a,"WAP6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PIM7" AS code_a,"WAP7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SIM2" AS code_a,"WAS2" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SIM3" AS code_a,"WAS3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SIM4" AS code_a,"WAS4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SIM5" AS code_a,"WAS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SIM6" AS code_a,"WAS6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SIM7" AS code_a,"WAS7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBS6" AS code_a,"YBS6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBS7" AS code_a,"YBS7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PCD6" AS code_a,"WCP6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SCD5" AS code_a,"WCS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDC6" AS code_a,"WDP6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDC6" AS code_a,"WDS6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SLD5" AS code_a,"WFS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SLD7" AS code_a,"WFS7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SHZ5" AS code_a,"WGS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SHZ7" AS code_a,"WGS7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "TLLD" AS code_a,"W21T" AS code_g,"Truckload" AS truckload_intermodal
UNION ALL
SELECT "TL2C" AS code_a,"W24T" AS code_g,"Truckload" AS truckload_intermodal
UNION ALL
SELECT "PIM3" AS code_a,"WAP3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBP3" AS code_a,"YBP3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBP4" AS code_a,"YBP4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBP5" AS code_a,"YBP5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBP6" AS code_a,"YBP6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBP7" AS code_a,"YBP7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBS3" AS code_a,"YBS3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBS4" AS code_a,"YBS4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "WBS5" AS code_a,"YBS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PCD3" AS code_a,"WCP3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PCD4" AS code_a,"WCP4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PCD5" AS code_a,"WCP5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PCD7" AS code_a,"WCP7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SCD3" AS code_a,"WCS3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SCD4" AS code_a,"WCS4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SCD5" AS code_a,"WCS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SCD6" AS code_a,"WCS6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SCD7" AS code_a,"WCS7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDC3" AS code_a,"WDP3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDC4" AS code_a,"WDP4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDC5" AS code_a,"WDP5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDC7" AS code_a,"WDP7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDC3" AS code_a,"WDS3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDC4" AS code_a,"WDS4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDC5" AS code_a,"WDS5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDC7" AS code_a,"WDS7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDL3" AS code_a,"WEP3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDL4" AS code_a,"WEP4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDL5" AS code_a,"WEP5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDL6" AS code_a,"WEP6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PDL7" AS code_a,"WEP7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDL3" AS code_a,"WES3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDL4" AS code_a,"WES4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDL5" AS code_a,"WES5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDL6" AS code_a,"WES6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SDL7" AS code_a,"WES7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PLD3" AS code_a,"WFP3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PLD4" AS code_a,"WFP4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PLD5" AS code_a,"WFP5" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PLD6" AS code_a,"WFP6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "PLD7" AS code_a,"WFP7" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SLD3" AS code_a,"WFS3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SLD4" AS code_a,"WFS4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SLD6" AS code_a,"WFS6" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SHZ3" AS code_a,"WGS3" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SHZ4" AS code_a,"WGS4" AS code_g,"Intermodal" AS truckload_intermodal
UNION ALL
SELECT "SHZ6" AS code_a,"WGS6" AS code_g,"Intermodal" AS truckload_intermodal
;

---- Step 14
DROP TABLE IF EXISTS truckload_intermodal_2;
CREATE TEMPORARY VIEW truckload_intermodal_2 AS
SELECT truckload_intermodal_1.shpmt_num, truckload_intermodal_1.serv_tms_code, truckload_intermodal_1.dest_zone_go_name, truckload_intermodal_1.dest_loc_code, truckload_intermodal_1.origin_zone_name, tms_service_codes_map.code_a, tms_service_codes_map.truckload_intermodal as `truckload_intermodal_a`
FROM truckload_intermodal_1
LEFT OUTER JOIN tms_service_codes_map
ON truckload_intermodal_1.serv_tms_code=tms_service_codes_map.code_a;

--- Step 15
DROP TABLE IF EXISTS truckload_intermodal_3;
CREATE TEMPORARY VIEW truckload_intermodal_3 AS
SELECT truckload_intermodal_2.shpmt_num, truckload_intermodal_2.serv_tms_code, truckload_intermodal_2.dest_zone_go_name, truckload_intermodal_2.dest_loc_code, truckload_intermodal_2.origin_zone_name, truckload_intermodal_2.truckload_intermodal_a, tms_service_codes_map.code_g, tms_service_codes_map.truckload_intermodal as `truckload_intermodal_g`
FROM truckload_intermodal_2
LEFT OUTER JOIN tms_service_codes_map
ON truckload_intermodal_2.serv_tms_code=tms_service_codes_map.code_g;

---- Step 16
DROP TABLE IF EXISTS truckload_intermodal_final;
CREATE TEMPORARY VIEW truckload_intermodal_final AS
SELECT shpmt_num, serv_tms_code, dest_zone_go_name, dest_loc_code, origin_zone_name,
IF(truckload_intermodal_a IS NOT NULL, truckload_intermodal_a, IF(truckload_intermodal_g IS NOT NULL, truckload_intermodal_g, 'Service code not found')) AS `truckload_vs_intermodal`
FROM truckload_intermodal_3;

---- Step 17
---- for fields: `1st tendered carrier`
DROP TABLE IF EXISTS vendor_masterdata_carrier_description;
CREATE TEMPORARY VIEW vendor_masterdata_carrier_description AS
SELECT lifnr AS `carrier_id`, name1 AS `carrier_desc` FROM ${hivevar:dbMasterDataG11}.lfa1;

---- Step 18
---- for fields: `1st tendered carrier`
DROP TABLE IF EXISTS tac_first_tender_carrier_id;
CREATE TEMPORARY VIEW tac_first_tender_carrier_id AS
SELECT load_id, frwrd_agent_id FROM (
SELECT load_id, tender_first_carr_ind, frwrd_agent_id, ROW_NUMBER() OVER (PARTITION BY load_id ORDER BY tender_first_carr_ind DESC) as `row_num` FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star ) a
WHERE `row_num`=1;

---- Step 19
---- for fields: `1st tendered carrier`
DROP TABLE IF EXISTS load_id_carrier_description;
CREATE TEMPORARY VIEW load_id_carrier_description AS
SELECT tac_first_tender_carrier_id.load_id, vendor_masterdata_carrier_description.carrier_desc
FROM tac_first_tender_carrier_id
LEFT OUTER JOIN vendor_masterdata_carrier_description
ON tac_first_tender_carrier_id.frwrd_agent_id=vendor_masterdata_carrier_description.carrier_id;

---- Step
---- Workaround for getting Distance fields
DROP TABLE IF EXISTS shpmt_num_dstnc_qty_aotsotcsot;
CREATE TEMPORARY VIEW `shpmt_num_dstnc_qty_aotsotcsot` AS 
SELECT DISTINCT shpmt_num, dstnc_qty 
FROM (SELECT DISTINCT shpmt_num, dstnc_qty FROM ${hivevar:dbTransVsbltBw}.on_time_arriv_shpmt_custshpmt_na_merged_star  
UNION ALL
SELECT concat('0', cast(load_id as string)) as shpmt_num, cast(distance_qty as DECIMAL(38,4)) as dstnc_qty FROM tfs_distance_hist_lkp) a ;

---- Step 20
DROP TABLE IF EXISTS joined_id_tfs_cad_tac;
--CREATE TEMPORARY VIEW joined_id_tfs_cad_tac AS
CREATE TABLE joined_id_tfs_cad_tac AS
SELECT distinct_shpmt_id_tfs.*, distinct_load_id_cad.*, distinct_load_id_frt_auction_ind_tac.frt_auction_ind, distinct_shpmt_id_count_tdc_val_tfs.tdc_val_code_count, load_id_orign_zone_frt_type_name_tac.orign_zone_code, load_id_orign_zone_frt_type_name_tac.frt_type_name, truckload_intermodal_final.`truckload_vs_intermodal`, truckload_intermodal_final.`serv_tms_code`, 
truckload_intermodal_final.`dest_zone_go_name`,truckload_intermodal_final.`dest_loc_code`,truckload_intermodal_final.`origin_zone_name`,
load_id_carrier_description.`carrier_desc`, `shpmt_num_dstnc_qty_aotsotcsot`.`dstnc_qty`
FROM distinct_shpmt_id_tfs
LEFT OUTER JOIN distinct_load_id_cad
ON distinct_shpmt_id_tfs.shpmt_id = distinct_load_id_cad.load_id_cad
LEFT OUTER JOIN distinct_load_id_frt_auction_ind_tac
ON distinct_shpmt_id_tfs.shpmt_id = distinct_load_id_frt_auction_ind_tac.load_id_tac2
LEFT OUTER JOIN distinct_shpmt_id_count_tdc_val_tfs
ON distinct_shpmt_id_tfs.shpmt_id=distinct_shpmt_id_count_tdc_val_tfs.shpmt_id_tdc
LEFT OUTER JOIN load_id_orign_zone_frt_type_name_tac
ON distinct_shpmt_id_tfs.shpmt_id=load_id_orign_zone_frt_type_name_tac.load_id
LEFT OUTER JOIN truckload_intermodal_final
ON distinct_shpmt_id_tfs.shpmt_id=truckload_intermodal_final.shpmt_num
LEFT OUTER JOIN load_id_carrier_description
ON distinct_shpmt_id_tfs.shpmt_id=load_id_carrier_description.load_id
LEFT OUTER JOIN shpmt_num_dstnc_qty_aotsotcsot
ON distinct_shpmt_id_tfs.shpmt_id=shpmt_num_dstnc_qty_aotsotcsot.shpmt_num;

---- Step 21
---- Info: date format in exchg_rate_eff_date is YYYY-MM-DD. Date format in tfs.gr_posting_date which connects to it, is DD/MM/YYYY
DROP TABLE IF EXISTS month_exchng_rate_rds;
CREATE TEMPORARY VIEW month_exchng_rate_rds AS
SELECT DATE_FORMAT(exchg_rate_eff_date,'y-MM') AS `year_month`, exchg_rate  
FROM ${hivevar:dbRds}.exchg_rate_fct 
WHERE srce_iso_crncy_code='CAD' AND trgt_iso_crncy_code='USD' AND exchg_rate_type_id='11' AND YEAR(exchg_rate_eff_date)>'2013';

---- Step 22
DROP TABLE IF EXISTS tms_charge_id;
CREATE TABLE `tms_charge_id` (
`fcc` STRING,
`fcc_desc` STRING,
`freight_charge_description_2` STRING,
`flow_reason` STRING,
`accessorial_reason` STRING);

---- Step 23
INSERT INTO TABLE tms_charge_id 
SELECT "0000" AS fcc,"1 of VAT/Offshore/CAD currency exchange/inland transportation" AS fcc_desc,"Base Price - Incorrect Rate"                           AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "150"  AS fcc,"Canadian Currency Exchange"                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "450"  AS fcc,"Inland Transportation"                                         AS fcc_desc,""                                                      AS freight_charge_description_2,""                      AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "510"  AS fcc,"Offshore-Alaska/Hawaii"                                        AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "750"  AS fcc,"VAT - VALUE ADDED TAX"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "ADET" AS fcc,"Authorizatrion to destroy."                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "ADH"  AS fcc,"ADH"                                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "ADR"  AS fcc,"Temperature Protect"                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "ADRA" AS fcc,"Dangerous for Air"                                             AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "BB1"  AS fcc,"Atlanta BB1 X Dock"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "BB2"  AS fcc,"CAROL STRM BB2 X Dock"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "BCF"  AS fcc,"Border Crossing Fee"                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "BOB"  AS fcc,"Bobtail"                                                       AS fcc_desc,"Bobtail charge "                                       AS freight_charge_description_2,"Surge"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "BROK" AS fcc,"Brokerage fee or duty fee"                                     AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "BSC"  AS fcc,"BUNKER SURCHARGE"                                              AS fcc_desc,"Bunker Surcharge"                                      AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "BTCH" AS fcc,"Bobtail charge on promotaionl trailers"                        AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "BYD"  AS fcc,"Beyond Charge"                                                 AS fcc_desc,"Beyond Charge"                                         AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "CAA"  AS fcc,"Cancelled Expedite"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "CASE" AS fcc,"Case Based Charge"                                             AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CFC"  AS fcc,"Customs Fee"                                                   AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "CLN"  AS fcc,"CONTAINER CLEANING"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "COD"  AS fcc,"COD"                                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "COF"  AS fcc,"Ocean Freight"                                                 AS fcc_desc,"Ocean Freight"                                         AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "COL"  AS fcc,"Fee for Collecting COD Charge"                                 AS fcc_desc,"Collect on Delivery"                                   AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "CPU"  AS fcc,"CPU"                                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CPU5" AS fcc,"CPU 5 day"                                                     AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CPU7" AS fcc,"CPU 7 days"                                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CPUC" AS fcc,"CPU (Condition)"                                               AS fcc_desc,"CPU (Condition)"                                       AS freight_charge_description_2,"Base Price"            AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CPUI" AS fcc,"CPUIM"                                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CPUT" AS fcc,"CPUT"                                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CSCT" AS fcc,"Location Needs Customer Specific Carrier Tariff"               AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CSE"  AS fcc,"CASE"                                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "CUA"  AS fcc,"Currency Adjustments"                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "CUD"  AS fcc,"Currency Adjustment Debit"                                     AS fcc_desc,"Currency Adjustment Debit - Should Not Be Used."       AS freight_charge_description_2,"Other"                 AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "CUF"  AS fcc,"Currency Adjustment Credit"                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "DAM"  AS fcc,"AUTHORIZATION TO DESTROY"                                      AS fcc_desc,"Authorization to Destroy"                              AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "DBAL" AS fcc,"Balance Due."                                                  AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "DEL"  AS fcc,"Delivery Charge"                                               AS fcc_desc,"Dual Driver"                                           AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "DEM"  AS fcc,"Demurrage"                                                     AS fcc_desc,"Demurrage"                                             AS freight_charge_description_2,"Origin Detention"      AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DEW"  AS fcc,"Detention without Power Unit"                                  AS fcc_desc,"Detention without Power"                               AS freight_charge_description_2,"Destination Detention" AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DG"   AS fcc,"Dangerous Goods"                                               AS fcc_desc,"Dangerous Goods not paid on Initial proposal"          AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "DGC"  AS fcc,"Dangerous Goods (Condition)"                                   AS fcc_desc,"TMS Code - Should Not Be Used"                         AS freight_charge_description_2,"Other"                 AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "DGS"  AS fcc,"DANGEROUS GOODS"                                               AS fcc_desc,"Dangerous Goods not paid on Initial proposal - HAZMAT" AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "DIST" AS fcc,"Distance Based Charge"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DLTL" AS fcc,"DISCOUNTS ON LTL"                                              AS fcc_desc,"Discounts on LTL"                                      AS freight_charge_description_2,"Base Price"            AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DPD"  AS fcc,"PRE-CARRIAGE CHARGE"                                           AS fcc_desc,"PRE-CARRIAGE CHARGE (Drayage)"                         AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "DPE"  AS fcc,"POST-CARRIAGE CHARGE"                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "DPU"  AS fcc,"Detention"                                                     AS fcc_desc,"Detention with Power"                                  AS freight_charge_description_2,"Destination Detention" AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DROP" AS fcc,"DROP"                                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "DSCS" AS fcc,"Discount - Shipment Level"                                     AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DSD"  AS fcc,"Direct Store Delivery"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "DSDC" AS fcc,"Direct Store Delivery (Condition)"                             AS fcc_desc,"Direct Store Delivery (Condition)"                     AS freight_charge_description_2,"Base Price"            AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "DSPL" AS fcc,"DS Parcel Test Condition - Longest Side"                       AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DSPS" AS fcc,"DS Parcel Test Condition - Shortest Side"                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DSPW" AS fcc,"DS Parcel Test Condition - Weight"                             AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DSSL" AS fcc,"DS Parcel Test Condition - Second Longest Side"                AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DSSS" AS fcc,"DS Parcel Test Condition - Size"                               AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DST"  AS fcc,"distance based charge"                                         AS fcc_desc,"Used for balance due on non LTL loads"                 AS freight_charge_description_2,"Base Price"            AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "DTC"  AS fcc,"DESTINATION CHARGE"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "DTL"  AS fcc,"Detention Loading"                                             AS fcc_desc,"Detention Loading Minutes"                             AS freight_charge_description_2,"Origin Detention"      AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DTU"  AS fcc,"Driver Detention Unloading (Dwell Time)"                       AS fcc_desc,"Driver wait time at delivery"                          AS freight_charge_description_2,"Destination Detention" AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DTV"  AS fcc,"Trailer Detention"                                             AS fcc_desc,"Trailer Detention Interplant Loads only"               AS freight_charge_description_2,"Destination Detention" AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DULD" AS fcc,"Driver Detention Unloading"                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "DWEL" AS fcc,"Dwell Time"                                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "EBD"  AS fcc,"Exhibition Delivery"                                           AS fcc_desc,"Convention Exhibition Delivery"                        AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "EBP"  AS fcc,"Exhibition Pickup Charge"                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "EMT"  AS fcc,"SITE CAUSED TONU"                                              AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "EVEN" AS fcc,"Events"                                                        AS fcc_desc,"Event"                                                 AS freight_charge_description_2,"Base Price"            AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "EXC"  AS fcc,"Exclusive Use"                                                 AS fcc_desc,"Exclusive Use"                                         AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "EXM"  AS fcc,"Empty miles/Repositioning/Surge Charge"                        AS fcc_desc,"Out of route miles"                                    AS freight_charge_description_2,"Surge"                 AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "EXP"  AS fcc,"EXPEDITE"                                                      AS fcc_desc,"Expedite"                                              AS freight_charge_description_2,"Expedite"              AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "FA_A" AS fcc,"Freight Auction Adjustment"                                    AS fcc_desc,"Freight Auction Rate Adjustment"                       AS freight_charge_description_2,"Surge"                 AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "FCHG" AS fcc,"fuel surcharge LTL"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Fuel" AS accessorial_reason
UNION ALL
SELECT "FFLT" AS fcc,"Fuel Flat Charge"                                              AS fcc_desc,"Fuel Flat Charge for Domestic Ocean"                   AS freight_charge_description_2,"Base Price"            AS flow_reason,"Fuel" AS accessorial_reason
UNION ALL
SELECT "FLAT" AS fcc,"Flat Charge"                                                   AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "FLT"  AS fcc,"FLAT CHARGE"                                                   AS fcc_desc,"Flat Charge Mexico"                                    AS freight_charge_description_2,"Base Price"            AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "FLTL" AS fcc,"LTL FUEL SURCHARGE"                                            AS fcc_desc,"LTL Fuel Flat Charge Domestic Ocean"                   AS freight_charge_description_2,"Base Price"            AS flow_reason,"Fuel" AS accessorial_reason
UNION ALL
SELECT "FSUR" AS fcc,"BTF Fuel Surcharge"                                            AS fcc_desc,"BTF Fuel surcharge for TL and IM"                      AS freight_charge_description_2,"Base Price"            AS flow_reason,"Fuel" AS accessorial_reason
UNION ALL
SELECT "FTP"  AS fcc,"Fleet Tiered Pricing"                                          AS fcc_desc,"Carriers manage above award + surge"                   AS freight_charge_description_2,"Surge"                 AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "FU_S" AS fcc,"FUEL SURCHARGE"                                                AS fcc_desc,"Pegged Fuel for IM"                                    AS freight_charge_description_2,"Base Price"            AS flow_reason,"Fuel" AS accessorial_reason
UNION ALL
SELECT "FULD" AS fcc,"Full Unload/Sort"                                              AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "FUSU" AS fcc,"Fuel Surcharge"                                                AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Fuel" AS accessorial_reason
UNION ALL
SELECT "GD"   AS fcc,"GUARANTEED DELIVERY"                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "GLD"  AS fcc,"Special Customer Event"                                        AS fcc_desc,"Initiative/Surge Extra Costs"                          AS freight_charge_description_2,"Surge"                 AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "GST"  AS fcc,"Goods and Services Tax Charge"                                 AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "HAZ"  AS fcc,"HAZARDOUS SURCHARGE"                                           AS fcc_desc,"HAZ MAT"                                               AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "HEAV" AS fcc,"Heavy Equipment"                                               AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "HLPR" AS fcc,"Helper/Lumper"                                                 AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "HOT"  AS fcc,"HOT"                                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "HOTC" AS fcc,"HOT (Condition)"                                               AS fcc_desc,"Hot Condition"                                         AS freight_charge_description_2,"Base Price"            AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "HUR"  AS fcc,"Hurricane Orders"                                              AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "HURC" AS fcc,"HURRICANE (Condition)"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "IDL"  AS fcc,"Inside Pick Up and Delivery"                                   AS fcc_desc,"Inside Delivery"                                       AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "IDLR" AS fcc,"Inside Delivery"                                               AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "IIA"  AS fcc,"Balance Due"                                                   AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "IPDR" AS fcc,"Interplant DRP 2nd Run"                                        AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "LAB"  AS fcc,"Helper/Lumper Charge"                                          AS fcc_desc,"Extra Labor (Helper Service)"                          AS freight_charge_description_2,"Helper"                AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "LAY"  AS fcc,"Layover"                                                       AS fcc_desc,"Layover Charge"                                        AS freight_charge_description_2,"Other"                 AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "LFLT" AS fcc,"Linehaul Flat Charge"                                          AS fcc_desc,"Line haul flat charge"                                 AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "LFT"  AS fcc,"Liftgate Charges"                                              AS fcc_desc,"Lift Gate (Truck) or Forklift Service"                 AS freight_charge_description_2,"Other"                 AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "LIN"  AS fcc,"Linear Foot"                                                   AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "LOVR" AS fcc,"Layover"                                                       AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "LTL"  AS fcc,"LTL CHARGE"                                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "LTL_" AS fcc,"LTL Tariff"                                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "MEA"  AS fcc,"Layover at the Ship Site"                                      AS fcc_desc,""                                                      AS freight_charge_description_2,"Other"                 AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "MOO"  AS fcc,"Milk Runs for Canada"                                          AS fcc_desc,"Canada Milk Runs"                                      AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "MSG"  AS fcc,"MISCELLANEOUS CHARGE"                                          AS fcc_desc,"Miscellaneous charge"                                  AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "NA"   AS fcc,"Misc Accessorial"                                              AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "NCBI" AS fcc,"NO CB TRANSPORTATION"                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NCBN" AS fcc,"NO CB TRANSPORTATION"                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NKNI" AS fcc,"NO KNIGHT TRANSPORTATION"                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NLLW" AS fcc,"NO LIPSEY LOGISTICS"                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NMTR" AS fcc,"NO METROPOLITAN TRUCKING"                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NO A" AS fcc,"NO AIR"                                                        AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NOAI" AS fcc,"Not Fit for Air Transport"                                     AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NOFR" AS fcc,"No Freight"                                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NRBI" AS fcc,"NO CH ROBINSON"                                                AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "NYD"  AS fcc,"New York Delivery Charge"                                      AS fcc_desc,"New York Delivery Charge"                              AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "ODLR" AS fcc,"Off Hours Delivery"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "OORM" AS fcc,"Empty miles/ Repositioning/Surge"                              AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "ORM"  AS fcc,"OUT OF ROUTE MILES"                                            AS fcc_desc,"Out of route miles"                                    AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "OVR"  AS fcc,"Overweight Return"                                             AS fcc_desc,"Overweight Return"                                     AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "PALL" AS fcc,"PALL"                                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "PCH"  AS fcc,"Temperature Protective Service"                                AS fcc_desc,"Temperature Protect Surcharge"                         AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "PDT"  AS fcc,"Promotional Drop Trailer"                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "PMS"  AS fcc,"Same Day Service"                                              AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "PMT"  AS fcc,"After Hours Pick-Up and/or Delivery"                           AS fcc_desc,"After Hours Pick-Up and/or Delivery"                   AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "PPC"  AS fcc,"Pre Purchase Capacity"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "PPCH" AS fcc,"Pre-purchase ( Unused Pre Purchase )"                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "PPU"  AS fcc,"Pre Purchase Capacity"                                         AS fcc_desc,"Unused PrePurchased Capacity"                          AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "PRCH" AS fcc,"Puerto Rico Charge"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "PRDT" AS fcc,"Promotional Drop Trailer"                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "PSC-" AS fcc,"PSC-Cold"                                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "RCC"  AS fcc,"Reconsignment Charge"                                          AS fcc_desc,"Reconsignment"                                         AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "RCCH" AS fcc,"Reconsignment Charge"                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "RCL"  AS fcc,"Redelivery Charge"                                             AS fcc_desc,"Redelivery"                                            AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "RDLR" AS fcc,"Redelivery"                                                    AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "REP"  AS fcc,"Residential Pick-Up"                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "RES"  AS fcc,"Residential Delivery"                                          AS fcc_desc,"Residential Delivery"                                  AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "RESI" AS fcc,"Residential Charge"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "RET"  AS fcc,"Refused and Return"                                            AS fcc_desc,"Refusal & Return"                                      AS freight_charge_description_2,"Returns/Refusals"      AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "RNR"  AS fcc,"Refused and Return"                                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "RRC"  AS fcc,"Reconsignment/Diversion of Freight"                            AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "RRN"  AS fcc,"Rental of equipment"                                           AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "RTC"  AS fcc,"Spot Rate"                                                     AS fcc_desc,"Spot Rate"                                             AS freight_charge_description_2,"Returns/Refusals"      AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "SAT"  AS fcc,"Saturday Delivery"                                             AS fcc_desc,"Saturday Delivery"                                     AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "SCS"  AS fcc,"Schedule C Shortfall"                                          AS fcc_desc,"Schedule C Shortfall"                                  AS freight_charge_description_2,"Base Price"            AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "SEC"  AS fcc,"NAT GAS PLANNED ACCESSORIAL"                                   AS fcc_desc,"Special Equipment - Nat Gas Planned Accessorial"       AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "SEG"  AS fcc,"Sorting and Segregating"                                       AS fcc_desc,"Segregating (Sorting)"                                 AS freight_charge_description_2,"Other"                 AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "SER"  AS fcc,"SECURITY CHARGE"                                               AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "SOC"  AS fcc,"STOP OFF CHARGE CONDITION"                                     AS fcc_desc,"Stop Off Charge"                                       AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "SOCH" AS fcc,"Stop Off Charge"                                               AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "SPA"  AS fcc,"Special Allowance"                                             AS fcc_desc,"Special Allowance"                                     AS freight_charge_description_2,"Surge"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "SPOT" AS fcc,"SPOT RATE"                                                     AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "SRG"  AS fcc,"Promotional Drop Trailer"                                      AS fcc_desc,"Promotional Drop Trailer"                              AS freight_charge_description_2,"Other"                 AS flow_reason,"Event" AS accessorial_reason
UNION ALL
SELECT "STOP" AS fcc,"STOP CHARGE CONDITION"                                         AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "STOR" AS fcc,"Storage"                                                       AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "STR"  AS fcc,"Storage"                                                       AS fcc_desc,"Storage In Transit"                                    AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "SUF"  AS fcc,"Sufferance Warehouse Charge"                                   AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "SUR"  AS fcc,"SURCHARGE- Other than fuel-Scanning Charges"                   AS fcc_desc,"Surcharge - NonFuel Scanning Charges"                  AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "TAY"  AS fcc,"GOVERNMENTAL TAX"                                              AS fcc_desc,"Government Tax"                                        AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "TDRV" AS fcc,"Team Driver Charge"                                            AS fcc_desc,"Team Driver"                                           AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "TEMP" AS fcc,"Temperature Protect Charge"                                    AS fcc_desc,"Protective Service (Heat or Freezing)"                 AS freight_charge_description_2,"Base Price"            AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "TER"  AS fcc,"TERMINAL CHARGE"                                               AS fcc_desc,"Terminal Charge"                                       AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "TEST" AS fcc,"TEST"                                                          AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "TPS"  AS fcc,"Management Fee"                                                AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Opt Only" AS accessorial_reason
UNION ALL
SELECT "TSC"  AS fcc,"Tiered Surge Cost"                                             AS fcc_desc,"Tiered Surcharge"                                      AS freight_charge_description_2,"Base Price"            AS flow_reason,"Line Haul" AS accessorial_reason
UNION ALL
SELECT "UND"  AS fcc,"PARTIAL UNLOAD"                                                AS fcc_desc,"Partial Unload"                                        AS freight_charge_description_2,"Other"                 AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "UNL"  AS fcc,"Full Unload/Sort Charge"                                       AS fcc_desc,"Full Unload"                                           AS freight_charge_description_2,"Other"                 AS flow_reason,"Labor" AS accessorial_reason
UNION ALL
SELECT "VFN"  AS fcc,"Vehicle Furnished Not Used/Truck Ordered Not Used"             AS fcc_desc,"Vehicle Furnished Not Used/Truck Ordered Not Used"     AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "VOLM" AS fcc,"Volume"                                                        AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "WAIT" AS fcc,"Wait Time - Total Wait Time"                                   AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Detention" AS accessorial_reason
UNION ALL
SELECT "WFG"  AS fcc,"Wharfage"                                                      AS fcc_desc,"Wharfage"                                              AS freight_charge_description_2,"Base Price"            AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "WGHT" AS fcc,"WEIGHT BASED LTL CHARGE"                                       AS fcc_desc,"Unknown"                                               AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "WGTC" AS fcc,"Weight - Container Level"                                      AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "WGTS" AS fcc,"Weight - Shipment Level"                                       AS fcc_desc," "                                                     AS freight_charge_description_2," "                     AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "WHC"  AS fcc,"Warehouse Charge"                                              AS fcc_desc,"Warehouse Charge"                                      AS freight_charge_description_2,"Other"                 AS flow_reason,"Special Needs" AS accessorial_reason
UNION ALL
SELECT "WRC"  AS fcc,"Weight and Inspection Charge"                                  AS fcc_desc,"Weight and Inspection Charge"                          AS freight_charge_description_2,"Other"                 AS flow_reason,"Unproductive" AS accessorial_reason
UNION ALL
SELECT "WTG"  AS fcc,"BORDER DELAY"                                                  AS fcc_desc,"Border Delay"                                          AS freight_charge_description_2,"Other"                 AS flow_reason,"Export" AS accessorial_reason
UNION ALL
SELECT "TOP"  AS fcc,"Take or Pay"                                                   AS fcc_desc,"Take or Pay"                                           AS freight_charge_description_2,"Quarter End"           AS flow_reason,"Quarter End" AS accessorial_reason
UNION ALL
SELECT "TAD"  AS fcc,"Green Tax Code"                                                AS fcc_desc,"Green Tax Code"                                        AS freight_charge_description_2,"Tax Code"              AS flow_reason,"Tax Code" AS accessorial_reason
UNION ALL
SELECT "PDY"  AS fcc,"Remote Area Code"                                              AS fcc_desc,"Remote Area Code"                                      AS freight_charge_description_2,"Area Code"             AS flow_reason,"Area Code" AS accessorial_reason
UNION ALL
SELECT "CVYI" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "HJBT" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "KNIG" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "L2D" AS fcc,"Live to Drop Flip"  AS fcc_desc,  "Live to Drop Flip" AS freight_charge_description_2, " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "SCNN" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "UFLB" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "USXI" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason
UNION ALL
SELECT "PGLI" AS fcc,"Dynamic Pricing"  AS fcc_desc,  "Dynamic Pricing" AS freight_charge_description_2,  " " AS flow_reason, "Linehaul" AS accessorial_reason

;


-- Step 
DROP TABLE IF EXISTS tfs_distinct_matl_doc_num;
CREATE TEMPORARY VIEW `tfs_distinct_matl_doc_num` AS
SELECT DISTINCT matl_doc_num FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star;

-- Step
DROP TABLE IF EXISTS tfs_distinct_tdc_val;
CREATE TEMPORARY VIEW `tfs_distinct_tdc_val` AS
SELECT DISTINCT tdc_val_code FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star;

-- Step
-- Prepare lkp table where key=decomposed_material
DROP TABLE IF EXISTS bop_by_material;
CREATE TEMPORARY VIEW `bop_by_material` AS
SELECT * FROM (
  SELECT 
    decomposed_material, 
    bu, 
    is_bu_pgp, 
    product_category, 
    tdc_val, 
    ROW_NUMBER() OVER (PARTITION BY decomposed_material ORDER BY `is_bu_pgp` ASC) AS `ranked_no` 
  FROM (
    SELECT DISTINCT 
      material_doc_num AS decomposed_material, 
      business_unit_name AS bu, 
      IF(business_unit_name LIKE '%PGP%', 1, 0) AS `is_bu_pgp`, 
      prod_categ_name AS product_category, 
      tdcval_id AS tdc_val 
    FROM prod_categ_lkp 
  ) a 
) b
WHERE ranked_no=1;

-- Step
-- Prepare lkp table where key=tdc_val
DROP TABLE IF EXISTS bop_by_tdc;
CREATE TEMPORARY VIEW `bop_by_tdc` AS
SELECT * FROM (
  SELECT 
    decomposed_material, 
    bu, 
    is_bu_pgp, 
    product_category, 
    tdc_val, 
    ROW_NUMBER() OVER (PARTITION BY tdc_val ORDER BY `is_bu_pgp` ASC) AS `ranked_no` 
  FROM (
    SELECT DISTINCT 
      material_doc_num AS decomposed_material, 
      business_unit_name AS bu, 
      IF(business_unit_name LIKE '%PGP%', 1, 0) AS `is_bu_pgp`, 
      prod_categ_name AS product_category, 
      tdcval_id AS tdc_val 
    FROM prod_categ_lkp 
  ) a 
) b
WHERE `ranked_no`=1;

-- Step
-- Join material number lookup
DROP TABLE IF EXISTS matl_doc_num_and_bop;
CREATE TEMPORARY VIEW `matl_doc_num_and_bop` AS
SELECT tfs_distinct_matl_doc_num.matl_doc_num, bop_by_material.bu AS `bu_material`, bop_by_material.product_category AS `product_category_material`
FROM tfs_distinct_matl_doc_num
LEFT OUTER JOIN bop_by_material
ON SUBSTR(tfs_distinct_matl_doc_num.matl_doc_num, 3)=bop_by_material.decomposed_material;

-- Step 
-- Join tdc value lookup
DROP TABLE IF EXISTS tdc_val_and_bop;
CREATE TEMPORARY VIEW `tdc_val_and_bop` AS
SELECT tfs_distinct_tdc_val.tdc_val_code , bop_by_tdc.bu AS `bu_tdc`, bop_by_tdc.product_category AS `product_category_tdc`
FROM tfs_distinct_tdc_val
LEFT OUTER JOIN bop_by_tdc
ON tfs_distinct_tdc_val.tdc_val_code=bop_by_tdc.tdc_val;

-- Step 
--TEMP TABLE WITH DESTINATION LOCATION ID FROM TAC
DROP TABLE IF EXISTS dest_loc_id_from_tac;
CREATE TEMPORARY VIEW `dest_loc_id_from_tac` AS
select load_id ,freight_type_code, ship_to_party_id from tac_technical_name_star
group by load_id, freight_type_code, ship_to_party_id
;

-------------------------------------
-- step 1

--INSERT OVERWRITE TABLE tfs_technical_names_merged_2
DROP TABLE IF EXISTS tfs_technical_names_merged_2;
--CREATE TEMPORARY VIEW tfs_technical_names_merged_2 AS
CREATE TABLE tfs_technical_names_merged_2 AS
SELECT first_phase_table.`shpmt_id` AS `shpmt_id`,
first_phase_table.`orign_zone_for_tfs_name` AS `tfs_origin_zone_name`,
first_phase_table.`destination_zone` AS `dest_zone_val`,
first_phase_table.`ship_to_party_description` AS `ship_to_party_desc`,
first_phase_table.`customer_description`  AS `customer_desc` ,
first_phase_table.`carrier_description` AS `carr_desc`,
first_phase_table.`voucher_type` AS `voucher_type_code`,
first_phase_table.`tdc_val_code` AS `tdcval_code`,
first_phase_table.`freight_cost_charge` AS `freight_cost_charge_code`,
first_phase_table.`tms_freight_charge_description`  AS `tms_freight_charge_desc` ,
first_phase_table.`freight_charge_description_2` AS `freight_charge2_desc`,
first_phase_table.`flow_reason` AS `flow_reason_val`,
first_phase_table.`accessorial_reason` AS `acsrl_reason_name`,
first_phase_table.`total_transportation_cost_usd` AS `total_trans_cost_usd_amt`,
first_phase_table.`adjustment_cost_usd` AS `adjmt_cost_usd_amt`,
first_phase_table.`contracted_cost_usd` AS `contract_cost_usd_amt`,
first_phase_table.`post_charge_cost_usd` AS `post_charge_cost_usd_amt`,
first_phase_table.`spot_cost_usd` AS `spot_cost_usd_amt`,
first_phase_table.`miscelaneous_cost_usd` AS `misc_cost_usd_amt`,
first_phase_table.`wight_per_load` AS `weight_per_load_qty`,
first_phase_table.`volume_per_load` AS `volume_per_load_qty`,
first_phase_table.`floor_positions_as_shipped` AS `floor_position_as_ship_cnt`,
first_phase_table.`theoretical_pallets` AS `theortc_pallet_cnt`,
from_unixtime(unix_timestamp(first_phase_table.`actual_gi_date`, 'dd/MM/yyyy'), 'yyyy-MM-dd') AS actual_gi_date,
first_phase_table.`charge_code_reason_code` AS `charge_code`,
first_phase_table.`delivery_id_#` AS `dlvry_id`,
first_phase_table.`proft_center_code` AS `profit_center_code`,
first_phase_table.`ctrng_area_code` AS `cntrlng_area_code`,
first_phase_table.`distance` AS `distance_qty`,
first_phase_table.`distance_unit_of_measure` AS `distance_uom`,
first_phase_table.`total_weight_as_shipped` AS `total_weight_ship_qty`,
first_phase_table.`weight_unit_of_measure` AS `weight_uom`,
first_phase_table.`total_volume_as_shipped` AS `total_volume_ship_qty`,
first_phase_table.`volume_unit_of_measure` AS `volume_uom`,
first_phase_table.`currency` AS `currency_code`,
first_phase_table.`#su_per_load` AS `su_per_load_qty`,
first_phase_table.`region_from_code` AS `ship_from_region_code`,
first_phase_table.`region_to_description` AS `ship_to_region_desc`,
first_phase_table.`country_to` AS `country_to_code`,
first_phase_table.`category_code` AS `categ_code`,
first_phase_table.`sector_description` AS `sector_desc`,
first_phase_table.`subsector_description` AS `subsector_desc`,
first_phase_table.`voucher_status` AS `voucher_status_code`,
first_phase_table.`voucher_reference_#` AS `voucher_ref_num`,
first_phase_table.`country_from_description` AS `country_from_desc_name`,
first_phase_table.`country_to_description` AS `country_to_desc_name`,
first_phase_table.`equipment_mode_description` AS `equip_mode_desc`,
first_phase_table.`region_from_description` AS `ship_from_region_desc`,
first_phase_table.`table_uom` AS `table_uom`,
first_phase_table.`origin_description` AS `origin_desc`,
first_phase_table.`cost_center` AS `cost_center_code`,
first_phase_table.`voucher_id` AS `voucher_id`,
first_phase_table.`multi_stop_flag` AS `multi_stop_flag`,
first_phase_table.`temp_protect` AS `temp_protect_code`,
first_phase_table.`spot_flag_val` AS `spot_flag_val`,
first_phase_table.`gbu_code` AS `gbu_code`,
from_unixtime(unix_timestamp(first_phase_table.`goods_receipt_posting_date`, 'dd/MM/yyyy'), 'yyyy-MM-dd') AS `goods_receipt_post_date`,
first_phase_table.`created_time` AS `create_tmstp`,
from_unixtime(unix_timestamp(first_phase_table.`created_date`, 'dd/MM/yyyy'), 'yyyy-MM-dd') AS `create_date`, 
first_phase_table.`na_target_country_code` AS `na_target_country_code`,
first_phase_table.`ship_to_#` AS `ship_to_party_num`,
first_phase_table.`shipping_point_code` AS `ship_point_code`,
first_phase_table.`transportation_planning_point` AS `trans_plan_point_code`,
first_phase_table.`equipment_mode_code` AS `equip_mode_code`,
first_phase_table.`transportation_equipment_type_code` AS `trans_equip_type_code`,
first_phase_table.`freight_type_customer_interplant_indicator` AS `freight_type_customer_interplant_ind_code`,
first_phase_table.`country_from_code` AS `country_from_code`,
first_phase_table.`charge_level_description` AS `tms_charge_lvl_desc`,
first_phase_table.`charge_kind_description` AS `tms_charge_kind_desc`,
first_phase_table.`reason_code_cost_tms_interface` AS `tms_interface_reason_cost_code`,
first_phase_table.`chart_acct_num` AS `chart_account_num`,
first_phase_table.`steps` AS `step_factor`,
first_phase_table.`company_code` AS `company_code`,
first_phase_table.`tdc_val_description` AS `tdcval_desc`,
from_unixtime(unix_timestamp(first_phase_table.`freight_bill_created_date`, 'dd/MM/yyyy'), 'yyyy-MM-dd') AS `freight_bill_create_date`,
first_phase_table.`dlvry_item_count` AS `dlvry_item_cnt`,
  first_phase_table.`carr_country_name` AS `carr_country_name`,
  first_phase_table.`carr_country_code` AS `carr_country_code`,
  first_phase_table.`carr_postal_code` AS `carr_postal_code`,
  first_phase_table.`ship_to_postal_code` AS `ship_to_postal_code`,
  first_phase_table.`ship_to_state_code` AS `ship_to_state_code`,
  first_phase_table.`ship_to_state_name` AS `ship_to_state_name`,
  first_phase_table.`minority_ind_val` AS `minority_ind_val`,
  first_phase_table.`dest_zone_go_name` AS `dest_zone_go_name`,
  first_phase_table.`origin_zone_name` AS `origin_zone_name`,
  case 
when first_phase_table.`freight_type` = 'Customer' then  first_phase_table.`ship_to_#`
when first_phase_table.`freight_type` = 'Interplant' then 
NVL(NVL(temp_tfs.ship_to_party_id,first_phase_table.`dest_loc_code`),first_phase_table.`ship_to_#`)
else first_phase_table.`ship_to_#`
end as `dest_loc_code`,
first_phase_table.`charge_detail_id` AS `charge_detail_id`,
first_phase_table.`material_document_#` AS `material_doc_num`,
first_phase_table.`purchasing_document_#` AS `purchase_doc_num`,
first_phase_table.`charge_kind_code` AS `charge_kind_code`,
first_phase_table.`charge_level_code` AS `charge_lvl`,
first_phase_table.`billing_proposal_no` AS `billing_proposal_num`,
first_phase_table.`gl_account_#` AS `gl_account_num`,
first_phase_table.`underlying_data_for_pallets_calc2` AS `step_per_load2_rate`,
first_phase_table.`underlying_data_for_pallets_calc` AS `step_per_load_rate`,
first_phase_table.`total_transportation_cost_local_currency` AS `total_trans_cost_local_currency_amt`,
first_phase_table.`cost_on_step_local_currency` AS `cost_on_step_local_currency_amt`,
first_phase_table.`adjustment_cost_local_currency` AS `adjmt_cost_local_currency_amt`,
first_phase_table.`contracted_cost_local_currency` AS `contract_cost_local_currency_amt`,
first_phase_table.`post_charge_cost_local_currency` AS `post_charge_cost_local_currency_amt`,
first_phase_table.`spot_cost_local_currency` AS `spot_cost_local_currency_amt`,
first_phase_table.`miscelaneous_cost_local_currency` AS `misc_cost_local_currency_amt`,
first_phase_table.`tfts_load_tmstp` AS `tfts_load_date`,
first_phase_table.`load_from_file` AS `load_from_file_url`,
first_phase_table.`carrier_id` AS `carr_id`,
first_phase_table.`bd_mod_tmstp` AS `row_modify_tmstp`,
first_phase_table.`freight_auction` AS `freight_auction_val`,
first_phase_table.`historical_data_structure_flag` AS `hist_data_structure_flag`,
first_phase_table.`origin_longitude` AS `origin_longitude_val`,
first_phase_table.`origin_latitude` AS `origin_latitude_val`,
first_phase_table.`destination_longitude` AS `dest_longitude_val`,
first_phase_table.`destination_latitude` AS `dest_latitude_val`,
first_phase_table.`destination_postal` AS `dest_postal_code`,
first_phase_table.`5_digit_lane_name` AS `five_digit_lane_name`,
first_phase_table.`regional_2_3_digit_lane_name` AS `region_2_3_digit_lane_name`,
first_phase_table.`customer_specific_lane_name` AS `customer_specific_lane_name`,
first_phase_table.`customer_l1` AS `customer1_lvl`,
first_phase_table.`customer_l2` AS `customer2_lvl`,
first_phase_table.`customer_l3` AS `customer3_lvl`,
first_phase_table.`customer_l4` AS `customer4_lvl`,
first_phase_table.`customer_l5` AS `customer5_lvl`,
first_phase_table.`customer_l6` AS `customer6_lvl`,
first_phase_table.`customer_l7` AS `customer7_lvl`,
first_phase_table.`customer_l8` AS `customer8_lvl`,
first_phase_table.`customer_l9` AS `customer9_lvl`,
first_phase_table.`customer_l10` AS `customer10_lvl`,
first_phase_table.`customer_l11` AS `customer11_lvl`,
first_phase_table.`customer_l12` AS `customer12_lvl`,
first_phase_table.`carrier_&_sourced_service` AS `carr_and_source_service_val`,
first_phase_table.`accrual_cost` AS `accrual_cost_amt`,
first_phase_table.`line_haul_cost` AS `line_haul_cost_amt`,
first_phase_table.`fuel_cost` AS `fuel_cost_amt`,
IF(first_phase_table.`contracted_cost` IS NULL, 0, first_phase_table.`contracted_cost`)-IF(first_phase_table.`line_haul_cost` IS NULL, 0, first_phase_table.`line_haul_cost`)-IF(first_phase_table.`fuel_cost` IS NULL, 0, first_phase_table.`fuel_cost`) AS `other_contract_cost_amt`,
IF(first_phase_table.`line_haul_cost` IS NULL, 0, first_phase_table.`line_haul_cost`)+IF(first_phase_table.`fuel_cost` IS NULL, 0, first_phase_table.`fuel_cost`) AS `frcst_cost_amt`,
first_phase_table.`ave_total_transportation_costs_per_paller` AS `ave_total_trans_cost_per_pallet_amt`,
first_phase_table.`total_transportation_costs_per_lb` AS `total_trans_cost_per_lb_amt`,
first_phase_table.`total_transportation_costs_per_f3_volume` AS `total_trans_cost_per_cubic_feet_volume_amt`,
first_phase_table.`total_transportation_costs_per_mile` AS `total_trans_cost_per_mile_amt`,
first_phase_table.`total_transportation_costs_per_su` AS `total_trans_cost_per_su_amt`,
first_phase_table.`accrued_contracted_cost` AS `accrued_contract_cost_amt`,
first_phase_table.`accrued_line_haul_cost` AS `accrued_line_haul_cost_amt`,
first_phase_table.`accrued_fuel_cost` AS `accrued_fuel_cost_amt`,
first_phase_table.`accrued_other_contract_costs` AS `accrued_other_contract_cost_amt`,
first_phase_table.`accrued_spot_rate_charges` AS `accrued_spot_charge_rate`,
first_phase_table.`post_charges_cost` AS `post_charge_cost_amt`,
first_phase_table.`misc_cost` AS `misc_cost_amt`,
first_phase_table.`contracted_cost` AS `contract_cost_amt`,
first_phase_table.`multi_tdc_val` AS `multi_tdcval_code`,
first_phase_table.`minimum_charge` AS `min_charge_amt`,
first_phase_table.`charge_reason_freight_concentration` AS `charge_reason_freight_concat_name`,
first_phase_table.`avoidable_touch` AS `avoidbl_touch_val`,
first_phase_table.`operational_freight_type` AS `opertn_freight_type_code`,
first_phase_table.`truckload_vs_intermodal` AS `truckload_vs_intermodal_val`,
first_phase_table.`pgp_flag` AS `pgp_flag`,
first_phase_table.`freight_type` AS `freight_type_val`,
first_phase_table.`1st_tendered_carrier` AS `first_tender_carr_name`,
first_phase_table.`serv_tms_code` AS `service_tms_code`,
first_phase_table.distance_per_load AS `distance_per_load_qty`,
first_phase_table.`historical_data` AS `hist_data`
FROM (
SELECT 
joined_table.`shpmt_id` AS `shpmt_id`,
joined_table.`orign_zone_for_tfs_name` AS `orign_zone_for_tfs_name`,
joined_table.`destination_zone` AS `destination_zone`,
joined_table.`ship_to_party_desc` AS `ship_to_party_description`,
joined_table.`customer_description` AS `customer_description`, 
joined_table.`carr_desc` AS `carrier_description`,
joined_table.`voucher_type_code` AS `voucher_type`,
joined_table.`tdc_val_code` AS `tdc_val_code`,
joined_table.`frt_cost_chrg_code` AS `freight_cost_charge`,
joined_table.`fcc_desc` AS `tms_freight_charge_description`, 
joined_table.`freight_charge_description_2` AS `freight_charge_description_2`,
joined_table.`flow_reason` AS `flow_reason`,
joined_table.`accessorial_reason` AS `accessorial_reason`,
joined_table.`total_transportation_cost_usd` AS `total_transportation_cost_usd`,
IF( joined_table.`crncy_code`='USD', joined_table.`adjmt_cost_amt`, IF(joined_table.`exchg_rate` IS NOT NULL, joined_table.`adjmt_cost_amt`*joined_table.`exchg_rate`, NULL )) AS `adjustment_cost_usd`,
IF( joined_table.`crncy_code`='USD', joined_table.`cntrctd_cost_amt`, IF(joined_table.`exchg_rate` IS NOT NULL, joined_table.`cntrctd_cost_amt`*joined_table.`exchg_rate`, NULL )) AS `contracted_cost_usd`,
IF( joined_table.`crncy_code`='USD', joined_table.`post_chrgs_cost_amt`, IF(joined_table.`exchg_rate` IS NOT NULL, joined_table.`post_chrgs_cost_amt`*joined_table.`exchg_rate`, NULL )) AS `post_charge_cost_usd`,
IF( joined_table.`crncy_code`='USD', joined_table.`spot_rate_chrgs_amt`, IF(joined_table.`exchg_rate` IS NOT NULL, joined_table.`spot_rate_chrgs_amt`*joined_table.`exchg_rate`, NULL )) AS `spot_cost_usd`,
IF( joined_table.`crncy_code`='USD', joined_table.`misc_cost_amt`, IF(joined_table.`exchg_rate` IS NOT NULL, joined_table.`misc_cost_amt`*joined_table.`exchg_rate`, NULL )) AS `miscelaneous_cost_usd`,
joined_table.`wght_per_shpmt_per_dlvry_qty` AS `wight_per_load`,
joined_table.`vol_per_shpmt_per_dlvry_qty` AS `volume_per_load`,
joined_table.`floor_postn_qty` AS `floor_positions_as_shipped`,
joined_table.`pllts_thrcl_qty` AS `theoretical_pallets`,
joined_table.`gi_goods_issue_shpmt_date` AS `actual_gi_date`,
joined_table.`reasn_code` AS `charge_code_reason_code`,
joined_table.`dlvry_id` AS `delivery_id_#`,
joined_table.`proft_center_code` AS `proft_center_code`,
joined_table.`ctrng_area_code` AS `ctrng_area_code`,
joined_table.`dstnc_qty` AS `distance`,
joined_table.`dstnc_uom` AS `distance_unit_of_measure`,
joined_table.`wght_tot_qty` AS `total_weight_as_shipped`,
joined_table.`wght_uom` AS `weight_unit_of_measure`,
joined_table.`vol_qty` AS `total_volume_as_shipped`,
joined_table.`vol_uom` AS `volume_unit_of_measure`,
joined_table.`crncy_code` AS `currency`,
joined_table.`su_per_shpmt_qty` AS `#su_per_load`,
joined_table.`regn_from_code` AS `region_from_code`,
joined_table.`regn_to_code` AS `region_to_description`,
joined_table.`cntry_to_code` AS `country_to`,
joined_table.`categ_code` AS `category_code`,
joined_table.`sectr_name` AS `sector_description`,
joined_table.`sbstr_name` AS `subsector_description`,
joined_table.`voucher_sttus_name` AS `voucher_status`,
joined_table.`voucher_ref_num` AS `voucher_reference_#`,
joined_table.`cntry_from_desc` AS `country_from_description`,
joined_table.`cntry_to_desc` AS `country_to_description`,
joined_table.`equip_mode_desc` AS `equipment_mode_description`,
joined_table.`regn_from_desc` AS `region_from_description`,
joined_table.`table_uom` AS `table_uom`,
joined_table.`ship_point_desc` AS `origin_description`,
joined_table.`cost_center_id` AS `cost_center`,
joined_table.`voucher_id` AS `voucher_id`,
joined_table.`multi_stop_ind` AS `multi_stop_flag`,
joined_table.`temp_protect_ind` AS `temp_protect`,
joined_table.`spot_flag_val` AS `spot_flag_val`,
joined_table.`gbu_code` AS `gbu_code`,
joined_table.`gr_posting_date` AS `goods_receipt_posting_date`,
joined_table.`created_datetm` AS `created_time`,
joined_table.`created_date` AS `created_date`,
joined_table.`na_trgt_cntry_code` AS `na_target_country_code`,
joined_table.`ship_to_num` AS `ship_to_#`,
joined_table.`ship_point_code` AS `shipping_point_code`,
joined_table.`trans_plan_point_code` AS `transportation_planning_point`,
joined_table.`equip_mode_code` AS `equipment_mode_code`,
joined_table.`trnsp_type_code` AS `transportation_equipment_type_code`,
joined_table.`frt_type_code` AS `freight_type_customer_interplant_indicator`,
joined_table.`cntry_from_code` AS `country_from_code`,
joined_table.`chrg_level_for_tms_intfc_name` AS `charge_level_description`,
joined_table.`chrg_kind_for_tms_intfc_name` AS `charge_kind_description`,
joined_table.`reasn_code_tms_intfc` AS `reason_code_cost_tms_interface`,
joined_table.`chart_acct_num` AS `chart_acct_num`,
joined_table.`steps_num` AS `steps`,
joined_table.`company_code` AS `company_code`,
joined_table.`tdc_val_desc` AS `tdc_val_description`,
joined_table.`bill_frt_created_date` AS `freight_bill_created_date`,
joined_table.`dlvry_item_count` AS `dlvry_item_count`,
  joined_table.`carr_country_name` AS `carr_country_name`,
  joined_table.`carr_country_code` AS `carr_country_code`,
  joined_table.`carr_postal_code` AS `carr_postal_code`,
  joined_table.`ship_to_postal_code` AS `ship_to_postal_code`,
  joined_table.`ship_to_state_code` AS `ship_to_state_code`,
  joined_table.`ship_to_state_name` AS `ship_to_state_name`,
  joined_table.`minority_ind_val` AS `minority_ind_val`,
  joined_table.`dest_zone_go_name` AS `dest_zone_go_name`,
  joined_table.`origin_zone_name` AS `origin_zone_name`,
  joined_table.`dest_loc_code` AS `dest_loc_code`,
joined_table.`chrg_detail_id` AS `charge_detail_id`,
joined_table.`matl_doc_num` AS `material_document_#`,
joined_table.`purch_doc_num` AS `purchasing_document_#`,
joined_table.`chrg_kindc_num` AS `charge_kind_code`,
joined_table.`chrg_level_num` AS `charge_level_code`,
joined_table.`billg_prpsl_num` AS `billing_proposal_no`,
joined_table.`gl_acct_num` AS `gl_account_#`,
joined_table.`shpmt_dlvry_pllts_qty` AS `underlying_data_for_pallets_calc2`,
joined_table.`pllt_qty` AS `underlying_data_for_pallets_calc`,
joined_table.`tot_trans_costs_amt` AS `total_transportation_cost_local_currency`,
joined_table.`step_cost_intfc_tms_amt` AS `cost_on_step_local_currency`,
joined_table.`adjmt_cost_amt` AS `adjustment_cost_local_currency`,
joined_table.`cntrctd_cost_amt` AS `contracted_cost_local_currency`,
joined_table.`post_chrgs_cost_amt` AS `post_charge_cost_local_currency`,
joined_table.`spot_rate_chrgs_amt` AS `spot_cost_local_currency`,
joined_table.`misc_cost_amt` AS `miscelaneous_cost_local_currency`,
joined_table.`tfts_load_tmstp` AS `tfts_load_tmstp`,
joined_table.`load_from_file` AS `load_from_file`,
joined_table.`carr_id` AS `carrier_id`,
joined_table.`bd_mod_tmstp` AS `bd_mod_tmstp`,
joined_table.`frt_auction_ind` AS `freight_auction`,
'-' AS `historical_data_structure_flag`,
'-' AS `origin_longitude`,
'-' AS `origin_latitude`,
'-' AS `destination_longitude`,
'-' AS `destination_latitude`,
joined_table.`destination_postal` AS `destination_postal`,
IF(joined_table.`frt_type_name` LIKE '%Interplant%', CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', joined_table.`destination_zone`), IF(joined_table.`cntry_to_code` LIKE '%CA%', CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', REGEXP_REPLACE(joined_table.`destination_zone`,' ','')), CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', joined_table.`destination_zone`))) AS `5_digit_lane_name`,
IF(joined_table.`frt_type_name` LIKE '%Interplant%', CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', joined_table.`destination_zone`), IF(joined_table.`cntry_to_code` LIKE '%US%', CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', SUBSTR(joined_table.`destination_zone`, 1,2)), IF(joined_table.`cntry_to_code` LIKE '%CA%', CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', SUBSTR(joined_table.`destination_zone`, 1,3)), CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', joined_table.`destination_zone`)))) AS `regional_2_3_digit_lane_name`,
CONCAT_WS(' ', joined_table.`orign_zone_code`, 'To', joined_table.`ship_to_num`) AS `customer_specific_lane_name`,
joined_table.`customer_l1` AS `customer_l1`,
joined_table.`customer_l2` AS `customer_l2`,
joined_table.`customer_l3` AS `customer_l3`,
joined_table.`customer_l4` AS `customer_l4`,
joined_table.`customer_l5` AS `customer_l5`,
joined_table.`customer_l6` AS `customer_l6`,
joined_table.`customer_l7` AS `customer_l7`,
joined_table.`customer_l8` AS `customer_l8`,
joined_table.`customer_l9` AS `customer_l9`,
joined_table.`customer_l10` AS `customer_l10`,
joined_table.`customer_l11` AS `customer_l11`,
joined_table.`customer_l12` AS `customer_l12`,
'-' AS `carrier_&_sourced_service`,
IF(joined_table.`billg_prpsl_num`='ACCRUAL' AND CAST(joined_table.`matl_doc_num` AS INT)=0 AND CAST(CONCAT(SUBSTR(joined_table.`gr_posting_date`,7,4),SUBSTR(joined_table.`gr_posting_date`,4,2)) as INT)<CONCAT(SUBSTR(CURRENT_DATE,1,4),SUBSTR(CURRENT_DATE,6,2)), joined_table.`step_cost_intfc_tms_amt`, NULL) AS `accrual_cost`,
IF(joined_table.`frt_cost_chrg_code` IN('DST','DIST','FLAT','FLT','LFLT','CVY1','HJBT','KNIG','L2D','SCNN','UFLB','USX1','PGL1'), joined_table.`total_transportation_cost_usd`, NULL) AS `line_haul_cost`,
IF(joined_table.`frt_cost_chrg_code` IN('BTF','FU_S','FSUR','FFLT','FUSUR','FUSU','FUSUR1','FU_SUR'), joined_table.`total_transportation_cost_usd`, NULL) AS `fuel_cost`,
'-' AS `other_contract_costs`,
'-' AS `forecast_cost`,
'-' AS `ave_total_transportation_costs_per_paller`,
joined_table.`tot_trans_costs_amt`/joined_table.`wght_tot_qty` AS `total_transportation_costs_per_lb`,
joined_table.`tot_trans_costs_amt`/joined_table.`vol_qty` AS `total_transportation_costs_per_f3_volume`,
joined_table.`tot_trans_costs_amt`/joined_table.`dstnc_qty` AS `total_transportation_costs_per_mile`,
joined_table.`tot_trans_costs_amt`/joined_table.`su_per_shpmt_qty` AS `total_transportation_costs_per_su`,
'-' AS `accrued_contracted_cost`,
'-' AS `accrued_line_haul_cost`,
'-' AS `accrued_fuel_cost`,
'-' AS `accrued_other_contract_costs`,
'-' AS `accrued_spot_rate_charges`,
IF(joined_table.`chrg_kind_for_tms_intfc_name`='POST CHARGE', joined_table.`tot_trans_costs_amt`, NULL) AS `post_charges_cost`,
IF(joined_table.`chrg_kind_for_tms_intfc_name`='MISCELLANEOUS CHARGE', joined_table.`tot_trans_costs_amt`, NULL) AS `misc_cost`,
joined_table.`contracted_cost` AS `contracted_cost`,
IF(joined_table.`tdc_val_code_count`>1,1,0) AS `multi_tdc_val`,
'-' AS `minimum_charge`,
CONCAT(joined_table.`chrg_kind_for_tms_intfc_name`,' ',joined_table.`reasn_code`,' ',joined_table.frt_cost_chrg_code) AS `charge_reason_freight_concentration`,
'-' AS `avoidable_touch`,
joined_table.`operational_freight_type` AS `operational_freight_type`,
IF(joined_table.`tdc_val_desc` LIKE '%PGP%', 1,0) as `pgp_flag`,
joined_table.`freight_type` AS `freight_type`,
joined_table.`truckload_vs_intermodal` AS `truckload_vs_intermodal`,
joined_table.`1st_tendered_carrier` AS `1st_tendered_carrier`,
joined_table.`serv_tms_code` AS `serv_tms_code`,
joined_table.first_tendered_carrier_id AS first_tendered_carrier_id,
joined_table.first_tendered_carrier_description AS first_tendered_carrier_description,
joined_table.first_tendered_carrier_tms_service AS `first_tendered_carrier_tms_service`,
joined_table.first_tendered_carrier_award AS `first_tendered_carrier_award`,
joined_table.first_tendered_carrier_rate AS `first_tendered_carrier_rate`,
joined_table.weighted_average_rate AS `weighted_average_rate`,
joined_table.distance_per_load AS `distance_per_load`,
joined_table.`historical_data` AS `historical_data`
FROM (
SELECT
freight_stats_na_merged_star.`shpmt_id` AS `shpmt_id`,
freight_stats_na_merged_star.`purch_doc_num` AS `purch_doc_num`,
freight_stats_na_merged_star.`voucher_type_code` AS `voucher_type_code`,
freight_stats_na_merged_star.`billg_prpsl_num` AS `billg_prpsl_num`,
freight_stats_na_merged_star.`frt_cost_chrg_code` AS `frt_cost_chrg_code`,
freight_stats_na_merged_star.`chrg_kindc_num` AS `chrg_kindc_num`,
freight_stats_na_merged_star.`chrg_level_num` AS `chrg_level_num`,
freight_stats_na_merged_star.`reasn_code` AS `reasn_code`,
freight_stats_na_merged_star.`chrg_detail_id` AS `chrg_detail_id`,
freight_stats_na_merged_star.`matl_doc_num` AS `matl_doc_num`,
freight_stats_na_merged_star.`dlvry_id` AS `dlvry_id`,
freight_stats_na_merged_star.`tdc_val_code` AS `tdc_val_code`,
freight_stats_na_merged_star.`proft_center_code` AS `proft_center_code`,
freight_stats_na_merged_star.`ctrng_area_code` AS `ctrng_area_code`,
freight_stats_na_merged_star.`carr_id` AS `carr_id`,
freight_stats_na_merged_star.`cost_center_id` AS `cost_center_id`,
freight_stats_na_merged_star.`dstnc_qty` AS `dstnc_qty`,
freight_stats_na_merged_star.`dstnc_uom` AS `dstnc_uom`,
freight_stats_na_merged_star.`wght_tot_qty` AS `wght_tot_qty`,
freight_stats_na_merged_star.`wght_uom` AS `wght_uom`,
freight_stats_na_merged_star.`vol_qty` AS `vol_qty`,
freight_stats_na_merged_star.`vol_uom` AS `vol_uom`,
freight_stats_na_merged_star.`crncy_code` AS `crncy_code`,
freight_stats_na_merged_star.`shpmt_dlvry_pllts_qty` AS `shpmt_dlvry_pllts_qty`,
freight_stats_na_merged_star.`pllt_qty` AS `pllt_qty`,
freight_stats_na_merged_star.`adjmt_cost_amt` AS `adjmt_cost_amt`,
freight_stats_na_merged_star.`cntrctd_cost_amt` AS `cntrctd_cost_amt`,
freight_stats_na_merged_star.`post_chrgs_cost_amt` AS `post_chrgs_cost_amt`,
freight_stats_na_merged_star.`spot_rate_chrgs_amt` AS `spot_rate_chrgs_amt`,
freight_stats_na_merged_star.`misc_cost_amt` AS `misc_cost_amt`,
freight_stats_na_merged_star.`wght_per_shpmt_per_dlvry_qty` AS `wght_per_shpmt_per_dlvry_qty`,
freight_stats_na_merged_star.`vol_per_shpmt_per_dlvry_qty` AS `vol_per_shpmt_per_dlvry_qty`,
freight_stats_na_merged_star.`tot_trans_costs_amt` AS `tot_trans_costs_amt`,
freight_stats_na_merged_star.`step_cost_intfc_tms_amt` AS `step_cost_intfc_tms_amt`,
freight_stats_na_merged_star.`floor_postn_qty` AS `floor_postn_qty`,
freight_stats_na_merged_star.`pllts_thrcl_qty` AS `pllts_thrcl_qty`,
freight_stats_na_merged_star.`regn_from_code` AS `regn_from_code`,
freight_stats_na_merged_star.`regn_to_code` AS `regn_to_code`,
freight_stats_na_merged_star.`cntry_to_code` AS `cntry_to_code`,
IF(matl_doc_num_and_bop.product_category_material IS NOT NULL, matl_doc_num_and_bop.product_category_material, tdc_val_and_bop.product_category_tdc) AS `categ_code`,
freight_stats_na_merged_star.`sectr_name` AS `sectr_name`,
IF(matl_doc_num_and_bop.bu_material IS NOT NULL, matl_doc_num_and_bop.bu_material, tdc_val_and_bop.bu_tdc) AS `sbstr_name`,
freight_stats_na_merged_star.`voucher_sttus_name` AS `voucher_sttus_name`,
freight_stats_na_merged_star.`voucher_ref_num` AS `voucher_ref_num`,
freight_stats_na_merged_star.`cntry_from_desc` AS `cntry_from_desc`,
freight_stats_na_merged_star.`cntry_to_desc` AS `cntry_to_desc`,
freight_stats_na_merged_star.`carr_desc` AS `carr_desc`,
freight_stats_na_merged_star.`equip_mode_desc` AS `equip_mode_desc`,
freight_stats_na_merged_star.`regn_from_desc` AS `regn_from_desc`,
freight_stats_na_merged_star.`table_uom` AS `table_uom`,
freight_stats_na_merged_star.`ship_to_party_desc` AS `ship_to_party_desc`,
freight_stats_na_merged_star.`ship_point_desc` AS `ship_point_desc`,
freight_stats_na_merged_star.`voucher_id` AS `voucher_id`,
freight_stats_na_merged_star.`multi_stop_ind` AS `multi_stop_ind`,
freight_stats_na_merged_star.`temp_protect_ind` AS `temp_protect_ind`,
freight_stats_na_merged_star.`spot_flag_val` AS `spot_flag_val`,
freight_stats_na_merged_star.`gbu_code` AS `gbu_code`,
freight_stats_na_merged_star.`gr_posting_date` AS `gr_posting_date`,
freight_stats_na_merged_star.`created_datetm` AS `created_datetm`,
freight_stats_na_merged_star.`created_date` AS `created_date`,
freight_stats_na_merged_star.`na_trgt_cntry_code` AS `na_trgt_cntry_code`,
freight_stats_na_merged_star.`ship_to_num` AS `ship_to_num`,
freight_stats_na_merged_star.`gi_goods_issue_shpmt_date` AS `gi_goods_issue_shpmt_date`,
freight_stats_na_merged_star.`ship_point_code` AS `ship_point_code`,
freight_stats_na_merged_star.`trans_plan_point_code` AS `trans_plan_point_code`,
freight_stats_na_merged_star.`equip_mode_code` AS `equip_mode_code`,
freight_stats_na_merged_star.`trnsp_type_code` AS `trnsp_type_code`,
freight_stats_na_merged_star.`frt_type_code` AS `frt_type_code`,
  CASE
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 AND INSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, '-') = 56 THEN SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 51, 5) -- US Zip+4 - We want only five digit US zip code 
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 51, 8) = '00000000' THEN SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, -2) -- two digit zip code
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 51, 7) = '0000000' THEN SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, -3) -- three digit zip code
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 51, 5) = '00000' THEN SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, -5) -- US Zip Code
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 51, 4) = '0000' THEN SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, -6) -- Canada Zip Code with no space in it
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 51, 3) = '000' THEN CONCAT(SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 54, 3), SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 58, 3)) -- Canada Zip Code with space in it
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 60 THEN SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, -10) -- Ship To Party or Customer ID
              WHEN LENGTH(freight_stats_na_merged_star.`orign_zone_for_tfs_name`) = 7 AND LOCATE(' ', freight_stats_na_merged_star.`orign_zone_for_tfs_name`) > 0 THEN CONCAT(SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 1, 3), SUBSTR(freight_stats_na_merged_star.`orign_zone_for_tfs_name`, 5, 3))  -- Canada Zip Code with space in it
			  when freight_stats_na_merged_star.`orign_zone_for_tfs_name` is null then shipping_location.`corporate id 2`
			  ELSE freight_stats_na_merged_star.`orign_zone_for_tfs_name`
	END AS `orign_zone_for_tfs_name`,
freight_stats_na_merged_star.`cntry_from_code` AS `cntry_from_code`,
freight_stats_na_merged_star.`chrg_level_for_tms_intfc_name` AS `chrg_level_for_tms_intfc_name`,
freight_stats_na_merged_star.`chrg_kind_for_tms_intfc_name` AS `chrg_kind_for_tms_intfc_name`,
freight_stats_na_merged_star.`reasn_code_tms_intfc` AS `reasn_code_tms_intfc`,
freight_stats_na_merged_star.`su_per_shpmt_qty` AS `su_per_shpmt_qty`,
freight_stats_na_merged_star.`gl_acct_num` AS `gl_acct_num`,
freight_stats_na_merged_star.`chart_acct_num` AS `chart_acct_num`,
freight_stats_na_merged_star.`steps_num` AS `steps_num`,
freight_stats_na_merged_star.`company_code` AS `company_code`,
freight_stats_na_merged_star.`tdc_val_desc` AS `tdc_val_desc`,
freight_stats_na_merged_star.`bill_frt_created_date` AS `bill_frt_created_date`,
freight_stats_na_merged_star.`dlvry_item_count` AS `dlvry_item_count`,
  freight_stats_na_merged_star.`carr_country_name` AS `carr_country_name`,
  freight_stats_na_merged_star.`carr_country_code` AS `carr_country_code`,
  freight_stats_na_merged_star.`carr_postal_code` AS `carr_postal_code`,
  freight_stats_na_merged_star.`ship_to_postal_code` AS `ship_to_postal_code`,
  freight_stats_na_merged_star.`ship_to_state_code` AS `ship_to_state_code`,
  freight_stats_na_merged_star.`ship_to_state_name` AS `ship_to_state_name`,
  freight_stats_na_merged_star.`minority_ind_val` AS `minority_ind_val`,
CASE
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 AND INSTR(freight_stats_na_merged_star.`dest_zone_go_val`, '-') = 56 THEN SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 51, 5) -- US Zip+4 - We want only five digit US zip code 
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 AND SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 51, 8) = '00000000' THEN SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, -2) -- two digit zip code
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 AND SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 51, 7) = '0000000' THEN SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, -3) -- three digit zip code
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 AND SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 51, 5) = '00000' THEN SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, -5) -- US Zip Code
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 AND SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 51, 4) = '0000' THEN SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, -6) -- Canada Zip Code with no space in it
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 AND SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 51, 3) = '000' THEN CONCAT(SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 54, 3), SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 58, 3)) -- Canada Zip Code with space in it
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 60 THEN SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, -10) -- Ship To Party or Customer ID
              WHEN LENGTH(freight_stats_na_merged_star.`dest_zone_go_val`) = 7 AND LOCATE(' ', freight_stats_na_merged_star.`dest_zone_go_val`) > 0 THEN CONCAT(SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 1, 3), SUBSTR(freight_stats_na_merged_star.`dest_zone_go_val`, 5, 3))  -- Canada Zip Code with space in it
              ELSE freight_stats_na_merged_star.`dest_zone_go_val`
END AS `dest_zone_go_name`,
CASE
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 AND INSTR(freight_stats_na_merged_star.`origin_zone_name`, '-') = 56 THEN SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 51, 5) -- US Zip+4 - We want only five digit US zip code 
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 51, 8) = '00000000' THEN SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, -2) -- two digit zip code
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 51, 7) = '0000000' THEN SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, -3) -- three digit zip code
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 51, 5) = '00000' THEN SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, -5) -- US Zip Code
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 51, 4) = '0000' THEN SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, -6) -- Canada Zip Code with no space in it
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 AND SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 51, 3) = '000' THEN CONCAT(SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 54, 3), SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 58, 3)) -- Canada Zip Code with space in it
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 60 THEN SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, -10) -- Ship To Party or Customer ID
              WHEN LENGTH(freight_stats_na_merged_star.`origin_zone_name`) = 7 AND LOCATE(' ', freight_stats_na_merged_star.`origin_zone_name`) > 0 THEN CONCAT(SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 1, 3), SUBSTR(freight_stats_na_merged_star.`origin_zone_name`, 5, 3))  -- Canada Zip Code with space in it
              ELSE freight_stats_na_merged_star.`origin_zone_name`
	END  AS `origin_zone_name`, 
  CASE
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 AND INSTR(joined_id_tfs_cad_tac.`dest_loc_code`, '-') = 56 THEN SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 51, 5) -- US Zip+4 - We want only five digit US zip code 
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 51, 8) = '00000000' THEN SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, -2) -- two digit zip code
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 51, 7) = '0000000' THEN SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, -3) -- three digit zip code
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 51, 5) = '00000' THEN SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, -5) -- US Zip Code
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 51, 4) = '0000' THEN SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, -6) -- Canada Zip Code with no space in it
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 51, 3) = '000' THEN CONCAT(SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 54, 3), SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 58, 3)) -- Canada Zip Code with space in it
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 60 THEN SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, -10) -- Ship To Party or Customer ID
              WHEN LENGTH(joined_id_tfs_cad_tac.`dest_loc_code`) = 7 AND LOCATE(' ', joined_id_tfs_cad_tac.`dest_loc_code`) > 0 THEN CONCAT(SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 1, 3), SUBSTR(joined_id_tfs_cad_tac.`dest_loc_code`, 5, 3))  -- Canada Zip Code with space in it
              ELSE joined_id_tfs_cad_tac.`dest_loc_code`
	END       AS `dest_loc_code`,
freight_stats_na_merged_star.`tfts_load_tmstp` AS `tfts_load_tmstp`,
freight_stats_na_merged_star.`load_from_file` AS `load_from_file`,
freight_stats_na_merged_star.`bd_mod_tmstp` AS `bd_mod_tmstp`,
  CASE
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 AND INSTR(joined_id_tfs_cad_tac.`destination_zone`, '-') = 56 THEN SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 51, 5) -- US Zip+4 - We want only five digit US zip code 
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 51, 8) = '00000000' THEN SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, -2) -- two digit zip code
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 51, 7) = '0000000' THEN SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, -3) -- three digit zip code
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 51, 5) = '00000' THEN SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, -5) -- US Zip Code
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 51, 4) = '0000' THEN SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, -6) -- Canada Zip Code with no space in it
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 AND SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 51, 3) = '000' THEN CONCAT(SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 54, 3), SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 58, 3)) -- Canada Zip Code with space in it
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 60 THEN SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, -10) -- Ship To Party or Customer ID
              WHEN LENGTH(joined_id_tfs_cad_tac.`destination_zone`) = 7 AND LOCATE(' ', joined_id_tfs_cad_tac.`destination_zone`) > 0 THEN CONCAT(SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 1, 3), SUBSTR(joined_id_tfs_cad_tac.`destination_zone`, 5, 3))  -- Canada Zip Code with space in it
              ELSE joined_id_tfs_cad_tac.`destination_zone`
	END AS `destination_zone`,
joined_id_tfs_cad_tac.`frt_auction_ind` AS `frt_auction_ind`,
cust_656_postal_codes.`cust_1_name` AS `customer_l1`,
cust_656_postal_codes.`cust_2_name` AS `customer_l2`,
cust_656_postal_codes.`cust_3_name` AS `customer_l3`,
cust_656_postal_codes.`cust_4_name` AS `customer_l4`,
cust_656_postal_codes.`cust_5_name` AS `customer_l5`,
cust_656_postal_codes.`cust_6_name` AS `customer_l6`,
cust_656_postal_codes.`cust_7_name` AS `customer_l7`,
cust_656_postal_codes.`cust_8_name` AS `customer_l8`,
cust_656_postal_codes.`cust_9_name` AS `customer_l9`,
cust_656_postal_codes.`cust_10_name` AS `customer_l10`,
cust_656_postal_codes.`cust_11_name` AS `customer_l11`,
cust_656_postal_codes.`cust_12_name` AS `customer_l12`,
cust_656_postal_codes.`postal_code` AS `destination_postal`,  
month_exchng_rate_rds.`exchg_rate`,
IF(freight_stats_na_merged_star.`chrg_kind_for_tms_intfc_name`='ORIGINAL', freight_stats_na_merged_star.`tot_trans_costs_amt`, NULL) as `contracted_cost`,
joined_id_tfs_cad_tac.`tdc_val_code_count` as `tdc_val_code_count`,
joined_id_tfs_cad_tac.orign_zone_code,
joined_id_tfs_cad_tac.frt_type_name,
CASE
WHEN freight_stats_na_merged_star.`frt_type_code` LIKE '%C%' THEN 'Customer'
WHEN freight_stats_na_merged_star.`frt_type_code` LIKE '%I%' THEN 'Interplant'
WHEN freight_stats_na_merged_star.`frt_type_code` LIKE '%E%' AND SUBSTR(freight_stats_na_merged_star.`ship_to_num`, 1, 1)='P' THEN 'Interplant'
ELSE 'Customer'
END AS `freight_type`,
IF (joined_id_tfs_cad_tac.truckload_vs_intermodal IS NULL, 'Service code not found', joined_id_tfs_cad_tac.truckload_vs_intermodal) AS `truckload_vs_intermodal`,
tms_charge_id.`fcc_desc` AS `fcc_desc`,
tms_charge_id.`freight_charge_description_2` AS `freight_charge_description_2`,
tms_charge_id.`flow_reason` AS `flow_reason`,
tms_charge_id.`accessorial_reason` AS `accessorial_reason`,
IF(SUBSTR(freight_stats_na_merged_star.`ship_to_num`,1,1)='P' AND LENGTH(freight_stats_na_merged_star.`ship_to_num`)<7,'I','C') as `operational_freight_type`,
CASE
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%AB ACQUISITION, LLC%' THEN 'ALBERTSONS SAFEWAY'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%COSTCO COMPANIES, INC. (WW)%' AND cust_656_postal_codes.`cust_5_name` LIKE '%COSTCO CORP US%' THEN 'COSTCO'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%COSTCO COMPANIES, INC. (WW)%' AND cust_656_postal_codes.`cust_5_name` LIKE '%COSTCO WHSL (CA)%' THEN 'COSTCO CANADA'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%BJ%S WHOLESALE (WW)%' THEN 'BJS'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%CVS CORPORATION US%' THEN 'CVS'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%TARGET CORP US%' THEN 'TARGET'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%WAL-MART CORP. (WW)%' AND cust_656_postal_codes.`cust_5_name` LIKE 'SAM%' THEN 'SAMS'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%WAL-MART CORP. (WW)%' AND cust_656_postal_codes.`cust_12_name` LIKE 'SAM%' THEN 'SAMS'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%WAL-MART CORP. (WW)%' AND cust_656_postal_codes.`cust_5_name` NOT LIKE 'SAM%' THEN 'WALMART'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%Null%' THEN freight_stats_na_merged_star.`ship_to_party_desc`
WHEN cust_656_postal_codes.`cust_3_name` IS NULL THEN freight_stats_na_merged_star.`ship_to_party_desc`
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%THE KROGER CO. US%' THEN 'KROGER'
WHEN cust_656_postal_codes.`cust_3_name` LIKE '%UNKN LVL 3 UNKNOWN CUSTOMER%' THEN freight_stats_na_merged_star.`ship_to_party_desc`
ELSE cust_656_postal_codes.`cust_3_name`
END AS `customer_description`,
IF( freight_stats_na_merged_star.`crncy_code`='USD', freight_stats_na_merged_star.`tot_trans_costs_amt`, IF(month_exchng_rate_rds.`exchg_rate` IS NOT NULL, freight_stats_na_merged_star.`tot_trans_costs_amt`*month_exchng_rate_rds.`exchg_rate`, NULL )) AS `total_transportation_cost_usd`,
joined_id_tfs_cad_tac.carrier_desc AS `1st_tendered_carrier`,
joined_id_tfs_cad_tac.`serv_tms_code` AS `serv_tms_code`,
'first_tendered_carrier_id' AS first_tendered_carrier_id,
'first_tendered_carrier_description' AS first_tendered_carrier_description,
'first_tendered_carrier_tms_service' AS `first_tendered_carrier_tms_service`,
'first_tendered_carrier_award' AS `first_tendered_carrier_award`,
'first_tendered_carrier_rate' AS `first_tendered_carrier_rate`,
'weighted_average_rate' AS `weighted_average_rate`,
matl_doc_num_and_bop.bu_material AS `bu_matched_by_material`,
tdc_val_and_bop.bu_tdc AS `bu_matched_by_tdc`,
matl_doc_num_and_bop.product_category_material AS `category_matched_by_material`,
tdc_val_and_bop.product_category_tdc AS `category_matched_by_tdc`,
joined_id_tfs_cad_tac.dstnc_qty AS `distance_per_load`,
IF(freight_stats_na_merged_star.tfts_load_tmstp LIKE '%Historical load%', 1, 0) AS `historical_data`
FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star as freight_stats_na_merged_star
LEFT OUTER JOIN joined_id_tfs_cad_tac
ON freight_stats_na_merged_star.`shpmt_id`=joined_id_tfs_cad_tac.`shpmt_id`
LEFT OUTER JOIN  month_exchng_rate_rds
ON concat_ws('-', SUBSTR(`gr_posting_date`,7,4), SUBSTR(`gr_posting_date`,4,2)) = month_exchng_rate_rds.`year_month`
LEFT OUTER JOIN tms_charge_id
ON IF(freight_stats_na_merged_star.`frt_cost_chrg_code` RLIKE '0*510', "510", freight_stats_na_merged_star.`frt_cost_chrg_code`)=tms_charge_id.`fcc`
LEFT OUTER JOIN cust_656_postal_codes
ON freight_stats_na_merged_star.`ship_to_num`=cust_656_postal_codes.`ship_to_num`
LEFT OUTER JOIN matl_doc_num_and_bop
ON freight_stats_na_merged_star.matl_doc_num=matl_doc_num_and_bop.matl_doc_num
LEFT OUTER JOIN tdc_val_and_bop
ON freight_stats_na_merged_star.tdc_val_code=tdc_val_and_bop.tdc_val_code
left outer join shipping_location shipping_location
on freight_stats_na_merged_star.`ship_point_code`=shipping_location.`location id`
) joined_table ) first_phase_table
left outer join dest_loc_id_from_tac temp_tfs
on first_phase_table.`shpmt_id`= temp_tfs.load_id
and upper(first_phase_table.`freight_type`) = upper(temp_tfs.freight_type_code)
;

---- Step 24
---- for fieds: Primary Carrier Flag
DROP TABLE IF EXISTS awards_rate_operational_tariff_filtered;
CREATE TABLE awards_rate_operational_tariff_filtered 
STORED AS PARQUET
AS
SELECT * FROM (
SELECT award_rate AS awards_rate, carrier_id AS carr_id, lane_origin_zone_code, lane_dstn_zone_id AS lane_dest_zone_id, service_code AS serv_id, report_date,
ROW_NUMBER() OVER (PARTITION BY carrier_id, lane_origin_zone_code, lane_dstn_zone_id, service_code, year(report_date), weekofyear(report_date) ORDER BY report_date DESC) AS `row_num` 
FROM operational_tariff_star
WHERE unix_timestamp(rate_exp_date, 'yyyy-MM-dd')>unix_timestamp()
) a
WHERE row_num = 1;

---- Step 25
---- For First Tender Carrier Details - Obtaining: FT Carrier ID, FT Description, FT TMS, FT Award.
DROP TABLE IF EXISTS first_tender_carrier1;
CREATE TEMPORARY VIEW first_tender_carrier1 AS
SELECT load_id, first_tendered_carrier_id, first_tendered_carrier_description, first_tendered_carrier_tms_service, act_goods_issue_date, orign_zone_code, destination_zone, postl_code, first_tendered_carrier_award 
FROM (
SELECT load_id, first_tendered_carrier_id, vd.carrier_desc as first_tendered_carrier_description, first_tendered_carrier_tms_service, act_goods_issue_date, orign_zone_code, destination_zone, postl_code, first_tendered_carrier_award, row_num 
from (
SELECT tac.load_id                  AS load_id, 
       tac.forward_agent_id         AS first_tendered_carrier_id, 
       tac.service_tms_code         AS first_tendered_carrier_tms_service,
       tac.actual_goods_issue_date  AS act_goods_issue_date,
       tac.origin_zone_code         AS orign_zone_code,
       tac.dest_zone_code           AS destination_zone,
       tac.postal_code              AS postl_code,
       tac.avg_award_weekly_vol_qty AS first_tendered_carrier_award,
       ROW_NUMBER() OVER (PARTITION BY load_id ORDER BY tac.tender_date, tac.tender_datetm DESC) as `row_num`
FROM tac_technical_name_star tac
WHERE tender_event_type_code = 'TENDFRST') tac_first
  LEFT OUTER JOIN vendor_masterdata_carrier_description vd
  ON tac_first.first_tendered_carrier_id = vd.carrier_id ) tac_first_2
WHERE tac_first_2.row_num=1;


----- Step 26
---- For First Tender Carrier Details - Obtaining: FT Carrier Rate.
DROP TABLE IF EXISTS first_tender_carrier2;
CREATE TEMPORARY VIEW first_tender_carrier2 as
SELECT DISTINCT
                     tacf.load_id, 
                     tacf.first_tendered_carrier_id, 
                     tacf.first_tendered_carrier_description, 
                     tacf.first_tendered_carrier_tms_service, 
                     tacf.act_goods_issue_date, 
                     tacf.orign_zone_code, 
                     tacf.destination_zone, 
                     tacf.postl_code, 
                     tacf.first_tendered_carrier_award,
                     optf.`RATE` AS rate,
                     optf.`report date` as report_date
from first_tender_carrier1 tacf
LEFT OUTER JOIN operational_tariff_filter optf
  ON tacf.first_tendered_carrier_id = optf.`CARRIER ID`
  AND tacf.first_tendered_carrier_tms_service = optf.`SERVICE`
  AND tacf.orign_zone_code = optf.`LANE ORIGIN ZONE`
  AND tacf.destination_zone = optf.`LANE DESTINATION ZONE`
  AND unix_timestamp(optf.`report date`, 'yyyy-MM-dd') between unix_timestamp(optf.`RATE EFFECTIVE DATE`, 'yyyy-MM-dd') and unix_timestamp(optf.`RATE EXP DATE`, 'yyyy-MM-dd')
WHERE unix_timestamp(tacf.act_goods_issue_date, 'dd/MM/yyyy') between unix_timestamp(optf.`report date`, 'yyyy-MM-dd') and unix_timestamp(date_add(optf.`report date`,6), 'yyyy-MM-dd')
  AND first_tendered_carrier_description is not null;

-- Step 27
-- For Calculation on Weighted Awards for Lanes - Step 1. 
DROP TABLE IF EXISTS weighted_awards_for_lanes1;
--CREATE TEMPORARY VIEW weighted_awards_for_lanes1 as
CREATE TABLE weighted_awards_for_lanes1 as
SELECT *, coalesce(date_add(LEAD(report_date_from,1) over (PARTITION BY lane_origin_zone_code, lane_dstn_zone_id ORDER BY report_date_from), -1), report_date_to) AS next_report_date_from FROM (
SELECT lane_origin_zone_code, lane_dstn_zone_id, grp_key, weighted_average_rate, min(report_date) as report_date_from, max(report_date) as report_date_to 
from (
select *, LAST_VALUE(val_changed, true) OVER (PARTITION BY lane_origin_zone_code, lane_dstn_zone_id ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grp_key
from (
select *, 
  case 
    when weighted_average_rate <> prev_rate then rownum 
    when prev_rate is null then rownum 
    else null 
  end as val_changed 
from (
select *,
    row_number() OVER(partition by lane_origin_zone_code, lane_dstn_zone_id ORDER BY report_date) as rownum
    , LAG(weighted_average_rate,1) over (PARTITION BY lane_origin_zone_code, lane_dstn_zone_id ORDER BY report_date) as prev_rate
from (
SELECT war.lane_origin_zone_code, war.lane_dstn_zone_id, war.report_date, sum(war.pct_by_rate) as weighted_average_rate from (
SELECT `LANE ORIGIN ZONE`      AS lane_origin_zone_code, 
       `LANE DESTINATION ZONE` AS lane_dstn_zone_id,
       `CARRIER ID`            AS carrier_id,
       `SERVICE`               AS service_code,
       `report date`           AS report_date,
       `RATE`                  AS mile_contract_rate,
       `AWARDS`                AS awards_for_record,
       CAST(round(`RATE` * round((`AWARDS` / round(sum(`AWARDS`) OVER (PARTITION BY `LANE ORIGIN ZONE`, `LANE DESTINATION ZONE`, `report date` ORDER BY `report date` desc), 4)),4),4)
           AS decimal(38,4)) as pct_by_rate

FROM  (

SELECT `LANE ORIGIN ZONE`,
       `LANE DESTINATION ZONE`,
       `CARRIER ID`,
       `SERVICE`,
       `report date`,
       cast(COALESCE(`AWARDS`, 0) as double) AS `AWARDS`,
       cast(COALESCE(`RATE`, 0) as double) AS `RATE`
FROM  operational_tariff_filter
WHERE 1=1
  and unix_timestamp(`report date`, 'yyyy-MM-dd') between unix_timestamp(`RATE EFFECTIVE DATE`, 'yyyy-MM-dd') and unix_timestamp(`RATE EXP DATE`, 'yyyy-MM-dd')

) x
) war 
group by war.lane_origin_zone_code, war.lane_dstn_zone_id, war.report_date
) a
) b
) c
) d
group by lane_origin_zone_code, lane_dstn_zone_id, grp_key, weighted_average_rate
) e
;
--Step 28 For Calculation on Weighted Awards for Lanes with total SF volume 
--requested by matthew 
INSERT OVERWRITE TABLE Tariff_WAR_By_Report_Date
(
select 
`SF LANE` as SF_Lane,
grp_key as Grp_key,
`SF WAR` as SF_War,
report_date_from as Report_date_from,
report_date_to as Report_date_to,
next_report_date_from as Next_report_date_from
from 
(SELECT *, coalesce(date_add(LEAD(report_date_from,1) over (PARTITION BY `SF LANE` ORDER BY report_date_from), -1), report_date_to) AS next_report_date_from FROM (
SELECT `SF LANE`, grp_key, `SF WAR`, min(report_date) as report_date_from, max(report_date) as report_date_to 
from (
select *, LAST_VALUE(val_changed, true) OVER (PARTITION BY `SF LANE` ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grp_key
from (
select *, 
  case 
    when `SF WAR` <> prev_rate then rownum 
    when prev_rate is null then rownum 
    else null 
  end as val_changed 
from (
select *,
    row_number() OVER(partition by `SF LANE` ORDER BY report_date) as rownum
    , LAG(`SF WAR`,1) over (PARTITION BY `SF LANE` ORDER BY report_date) as prev_rate
from ( SELECT war.`SF LANE`, war.report_date, sum(war.`SF Rate Times SF PCT Award`) as `SF WAR`  
from ( SELECT new.`SF LANE`, new.report_date, new.mile_contract_rate * (new.`SF PCT Award`) as `SF Rate Times SF PCT Award` from 
(SELECT `SF LANE`,
       `CARRIER ID`            AS carrier_id,
       `SERVICE`               AS service_code,
       `report date`           AS report_date,
       `RATE`                  AS mile_contract_rate,
       `AWARDS`                AS awards_for_record,
       --`RATE` * round((`AWARDS` / round(sum(`TOTAL SF Award Volume`)))) AS `SF Rate Times SF PCT Award`
       (`AWARDS`/ `TOTAL SF Award Volume`) AS `SF PCT Award`
FROM  (

SELECT distinct `SF LANE`,
       `CARRIER ID`,
       `SERVICE`,
       `report date`,
       cast(COALESCE(`AWARDS`, 0) as double) AS `AWARDS`,
       cast(COALESCE(`RATE`, 0) as double) AS `RATE`,
       `TOTAL SF Award Volume`
FROM  operational_tariff_filter
WHERE 1=1
  and unix_timestamp(`report date`, 'yyyy-MM-dd') between unix_timestamp(`RATE EFFECTIVE DATE`, 'yyyy-MM-dd') and unix_timestamp(`RATE EXP DATE`, 'yyyy-MM-dd')
) x)new
) war 
group by war.`SF LANE`, war.report_date
) a
) b
) c
) d
group by `SF LANE`, grp_key, `SF WAR`
) e)
);
--step 2
----------------
--
--INSERT OVERWRITE TABLE tfs_technical_names_merged_3
DROP TABLE IF EXISTS tfs_technical_names_merged_3;
--CREATE TEMPORARY VIEW tfs_technical_names_merged_3 AS
CREATE TABLE tfs_technical_names_merged_3 AS
SELECT tfs_technical_names_merged_2.*, 
first_tender_carrier1.`first_tendered_carrier_id`          AS `first_tender_carr_id`,
first_tender_carrier1.`first_tendered_carrier_description` AS `first_tender_carr_desc`,
first_tender_carrier1.`first_tendered_carrier_tms_service` AS `first_tender_carr_tms_service_code`,
first_tender_carrier1.`first_tendered_carrier_award`       AS `first_tender_carr_award_qty`,
first_tender_carrier2.`rate`                               AS `first_tender_carr_rate`,
IF(awards_rate_operational_tariff_filtered.`awards_rate`>0.02, 1, 0) AS  `primary_carr_flag`,
IF(tfs_technical_names_merged_2.`billing_proposal_num`='ACCRUAL', 
IF(tfs_technical_names_merged_2.`currency_code`='CAD', 
IF(month_exchng_rate_rds.`exchg_rate` IS NOT NULL, tfs_technical_names_merged_2.`accrual_cost_amt`*month_exchng_rate_rds.`exchg_rate`, NULL)
,tfs_technical_names_merged_2.`accrual_cost_amt`), NULL) AS `accrual_cost_usd_amt`,
NVL(
IF(tfs_technical_names_merged_2.`billing_proposal_num`='ACCRUAL', 
IF(tfs_technical_names_merged_2.`currency_code`='CAD', 
IF(month_exchng_rate_rds.`exchg_rate` IS NOT NULL, tfs_technical_names_merged_2.`accrual_cost_amt`*month_exchng_rate_rds.`exchg_rate`, NULL)
,tfs_technical_names_merged_2.`accrual_cost_amt`), NULL)+ tfs_technical_names_merged_2.`total_trans_cost_usd_amt`, tfs_technical_names_merged_2.`total_trans_cost_usd_amt`) as `total_trans_cost_usd_include_accrual_amt`
,IF (tfs_technical_names_merged_2.actual_gi_date<'2018-05-26',
IF (tfs_technical_names_merged_2.`freight_type_val` = 'Interplant',
If(shipping_location.`location id`=tfs_technical_names_merged_2.dest_loc_code, shipping_location.`corporate id 2`, NVL(tfs_technical_names_merged_2.dest_postal_code,tfs_technical_names_merged_2.dest_loc_code))
, NVL(tfs_technical_names_merged_2.dest_postal_code,tfs_technical_names_merged_2.dest_loc_code)),
IF (tfs_technical_names_merged_2.`freight_type_val` = 'Interplant',
If(shipping_location.`location id`=tfs_technical_names_merged_2.dest_loc_code, shipping_location.`corporate id 2`, tfs_technical_names_merged_2.dest_loc_code)
, tfs_technical_names_merged_2.dest_loc_code))
AS `dest_ship_from_code`
FROM tfs_technical_names_merged_2
LEFT OUTER JOIN awards_rate_operational_tariff_filtered
   ON tfs_technical_names_merged_2.carr_id=awards_rate_operational_tariff_filtered.carr_id 
  AND tfs_technical_names_merged_2.tfs_origin_zone_name=awards_rate_operational_tariff_filtered.lane_origin_zone_code 
  AND tfs_technical_names_merged_2.dest_zone_val=awards_rate_operational_tariff_filtered.lane_dest_zone_id 
  AND tfs_technical_names_merged_2.service_tms_code=awards_rate_operational_tariff_filtered.serv_id
  AND year(tfs_technical_names_merged_2.actual_gi_date)=year(awards_rate_operational_tariff_filtered.report_date)
  AND weekofyear(tfs_technical_names_merged_2.actual_gi_date)=weekofyear(awards_rate_operational_tariff_filtered.report_date)
LEFT OUTER JOIN first_tender_carrier1
   ON tfs_technical_names_merged_2.`shpmt_id` = first_tender_carrier1.`load_id`
LEFT OUTER JOIN first_tender_carrier2
   ON tfs_technical_names_merged_2.`shpmt_id` = first_tender_carrier2.`load_id`
LEFT OUTER JOIN  month_exchng_rate_rds
ON concat_ws('-', SUBSTR(tfs_technical_names_merged_2.`actual_gi_date`,1,4), SUBSTR(tfs_technical_names_merged_2.`actual_gi_date`,6,2)) = month_exchng_rate_rds.`year_month`
left outer join shipping_location shipping_location
on tfs_technical_names_merged_2.`dest_loc_code`=shipping_location.`location id`
;

---------------------------------
--step 3 

DROP TABLE IF EXISTS tfs_shpmt_list;
CREATE TEMPORARY VIEW tfs_shpmt_list AS
SELECT DISTINCT
    shpmt_id
  , Concat(Left(tfs_origin_zone_name,20),'-',Left(dest_ship_from_code,20)) `SF Lane`
  , tfs_origin_zone_name
  , freight_type_val
  , dest_ship_from_code
  , dest_postal_code
  , dest_zone_val
  , actual_gi_date
  , ship_point_code
  , dest_loc_code
FROM
    tfs_technical_names_merged_3
;

DROP TABLE IF EXISTS weighted_awards_for_lanes2_new_data;
--CREATE TEMPORARY VIEW weighted_awards_for_lanes2_new_data AS
CREATE TABLE weighted_awards_for_lanes2_new_data AS
SELECT *
FROM
    (
        SELECT
            tfs.shpmt_id             AS `shpmt_id`
          , tfs.tfs_origin_zone_name AS `tfs_origin_zone_name`
          , tfs.freight_type_val     AS `freight_type_val`
          , tfs.dest_ship_from_code  AS `dest_ship_from_code`
          , tfs.dest_postal_code     AS `dest_postal_code`
          , tfs.actual_gi_date       AS `actual_gi_date`
          , tfs.dest_zone_val
          , tfs.ship_point_code
          , tfs.dest_loc_code

          , wal1_new.report_date_from1_new
          , wal1_new.next_report_date_from1_new
          , wal1_1.report_date_from1
          , wal1_1.next_report_date_from1
          , wal1_2.report_date_from2
          , wal1_2.next_report_date_from2
          , wal1_3.report_date_from3
          , wal1_3.next_report_date_from3
          , wal1_4.report_date_from4
          , wal1_4.next_report_date_from4
          , wal1_new.weighted_average_rate1_new
          , wal1_1.weighted_average_rate1
          , wal1_2.weighted_average_rate2
          , wal1_3.weighted_average_rate3
          , wal1_4.weighted_average_rate4
          , COALESCE(wal1_new.weighted_average_rate1_new,wal1_4.weighted_average_rate4, wal1_3.weighted_average_rate3, wal1_2.weighted_average_rate2, wal1_1.weighted_average_rate1) as weighted_average_rate

        FROM
            tfs_shpmt_list AS tfs
            LEFT OUTER JOIN (
            
            --newly added logic and requested by matthew
                SELECT 
                      tfs.`SF LANE`
                    , tfs.shpmt_id
                    , tfs.tfs_origin_zone_name
                    , tfs.dest_ship_from_code
                    , tfs.dest_postal_code
                    , tfs.actual_gi_date
                    , wal1.report_date_from      AS report_date_from1_new
                    , wal1.next_report_date_from AS next_report_date_from1_new
                    , wal1.`SF_WAR` AS weighted_average_rate1_new
                FROM tfs_shpmt_list AS tfs
                    JOIN Tariff_WAR_By_Report_Date AS wal1
                        ON COALESCE(tfs.`SF LANE`, 'XXX') = COALESCE(wal1.`SF_LANE`, 'XXX')
                WHERE tfs.actual_gi_date   >= wal1.report_date_from
                    AND tfs.actual_gi_date <= wal1.next_report_date_from
            ) AS wal1_new
                ON
                    tfs.shpmt_id                 = wal1_new.shpmt_id
                    AND COALESCE(tfs.`SF LANE`, 'XXX') = COALESCE(wal1_new.`SF LANE`, 'XXX')
                    AND tfs.actual_gi_date       = wal1_new.actual_gi_date     
                    
            LEFT OUTER JOIN (
                SELECT 
                      tfs.shpmt_id
                    , tfs.tfs_origin_zone_name
                    , tfs.dest_ship_from_code
                    , tfs.dest_postal_code
                    , tfs.actual_gi_date
                    , wal1.report_date_from      AS report_date_from1
                    , wal1.next_report_date_from AS next_report_date_from1
                    , wal1.weighted_average_rate AS weighted_average_rate1
                FROM tfs_shpmt_list AS tfs
                    JOIN weighted_awards_for_lanes1 AS wal1
                        ON COALESCE(tfs.tfs_origin_zone_name, 'XXX') = COALESCE(wal1.lane_origin_zone_code, 'XXX')
                           AND COALESCE(tfs.dest_zone_val, 'XXX')    = COALESCE(wal1.lane_dstn_zone_id, 'XXX')
                WHERE tfs.actual_gi_date   >= wal1.report_date_from
                    AND tfs.actual_gi_date <= wal1.next_report_date_from
            ) AS wal1_1
                ON
                    tfs.shpmt_id                 = wal1_1.shpmt_id
                    AND COALESCE(tfs.tfs_origin_zone_name, 'XXX') = COALESCE(wal1_1.tfs_origin_zone_name, 'XXX')
                    AND tfs.dest_ship_from_code  = wal1_1.dest_ship_from_code
                    AND COALESCE(tfs.dest_postal_code, 'XXX')     = COALESCE(wal1_1.dest_postal_code, 'XXX')
                    AND tfs.actual_gi_date       = wal1_1.actual_gi_date
            LEFT OUTER JOIN (
                SELECT 
                      tfs.shpmt_id
                    , tfs.tfs_origin_zone_name
                    , tfs.dest_ship_from_code
                    , tfs.dest_postal_code
                    , tfs.actual_gi_date
                    , wal2.report_date_from      AS report_date_from2
                    , wal2.next_report_date_from AS next_report_date_from2
                    , wal2.weighted_average_rate AS weighted_average_rate2
                FROM tfs_shpmt_list AS tfs
                    JOIN weighted_awards_for_lanes1 AS wal2
                        ON COALESCE(tfs.tfs_origin_zone_name, 'XXX') = COALESCE(wal2.lane_origin_zone_code, 'XXX')
                           AND IF(tfs.freight_type_val='Interplant', tfs.dest_ship_from_code, COALESCE(tfs.dest_postal_code, 'XXX')) = wal2.lane_dstn_zone_id
                WHERE tfs.actual_gi_date   >= wal2.report_date_from
                    AND tfs.actual_gi_date <= wal2.next_report_date_from
            ) AS wal1_2
                ON
                    tfs.shpmt_id                 = wal1_2.shpmt_id
                    AND COALESCE(tfs.tfs_origin_zone_name, 'XXX') = COALESCE(wal1_2.tfs_origin_zone_name, 'XXX')
                    AND tfs.dest_ship_from_code  = wal1_2.dest_ship_from_code
                    AND COALESCE(tfs.dest_postal_code, 'XXX')     = COALESCE(wal1_2.dest_postal_code, 'XXX')
                    AND tfs.actual_gi_date       = wal1_2.actual_gi_date
            LEFT OUTER JOIN (
                SELECT 
                      tfs.shpmt_id
                    , tfs.tfs_origin_zone_name
                    , tfs.dest_ship_from_code
                    , tfs.dest_postal_code
                    , tfs.actual_gi_date
                    , wal3.report_date_from      AS report_date_from3
                    , wal3.next_report_date_from AS next_report_date_from3
                    , wal3.weighted_average_rate AS weighted_average_rate3
                FROM tfs_shpmt_list AS tfs
                    JOIN weighted_awards_for_lanes1 AS wal3
                        ON tfs.ship_point_code   = wal3.lane_origin_zone_code
                           AND tfs.dest_loc_code = wal3.lane_dstn_zone_id
                WHERE tfs.actual_gi_date   >= wal3.report_date_from
                    AND tfs.actual_gi_date <= wal3.next_report_date_from
            ) AS wal1_3
                ON
                    tfs.shpmt_id                 = wal1_3.shpmt_id
                    AND COALESCE(tfs.tfs_origin_zone_name, 'XXX') = COALESCE(wal1_3.tfs_origin_zone_name, 'XXX')
                    AND tfs.dest_ship_from_code  = wal1_3.dest_ship_from_code
                    AND COALESCE(tfs.dest_postal_code, 'XXX')     = COALESCE(wal1_3.dest_postal_code, 'XXX')
                    AND tfs.actual_gi_date       = wal1_3.actual_gi_date
             LEFT OUTER JOIN (
                SELECT 
                      tfs.shpmt_id
                    , tfs.tfs_origin_zone_name
                    , tfs.dest_ship_from_code
                    , tfs.dest_postal_code
                    , tfs.actual_gi_date
                    , wal4.report_date_from      AS report_date_from4
                    , wal4.next_report_date_from AS next_report_date_from4
                    , wal4.weighted_average_rate AS weighted_average_rate4
                FROM tfs_shpmt_list AS tfs
                    JOIN weighted_awards_for_lanes1 AS wal4
                        ON tfs.ship_point_code   = wal4.lane_origin_zone_code
                           AND tfs.dest_zone_val = wal4.lane_dstn_zone_id
                WHERE tfs.actual_gi_date   >= wal4.report_date_from
                    AND tfs.actual_gi_date <= wal4.next_report_date_from
            ) AS wal1_4
                ON
                    tfs.shpmt_id                 = wal1_4.shpmt_id
                    AND COALESCE(tfs.tfs_origin_zone_name, 'XXX') = COALESCE(wal1_4.tfs_origin_zone_name, 'XXX')
                    AND tfs.dest_ship_from_code  = wal1_4.dest_ship_from_code
                    AND COALESCE(tfs.dest_postal_code, 'XXX')     = COALESCE(wal1_4.dest_postal_code, 'XXX')
                    AND tfs.actual_gi_date       = wal1_4.actual_gi_date       
              

    ) a
WHERE
    weighted_average_rate IS NOT NULL
;

-------------------
--step 4
INSERT OVERWRITE TABLE tfs_technical_name_star
SELECT
tfs_technical_names_merged_3.`shpmt_id`,
tfs_technical_names_merged_3.`tfs_origin_zone_name`,
tfs_technical_names_merged_3.`dest_zone_val`,
tfs_technical_names_merged_3.`ship_to_party_desc`,
tfs_technical_names_merged_3.`customer_desc`,
tfs_technical_names_merged_3.`carr_desc`,
tfs_technical_names_merged_3.`voucher_type_code`,
tfs_technical_names_merged_3.`tdcval_code`,
tfs_technical_names_merged_3.`freight_cost_charge_code`,
tfs_technical_names_merged_3.`tms_freight_charge_desc`,
tfs_technical_names_merged_3.`freight_charge2_desc`,
tfs_technical_names_merged_3.`flow_reason_val`,
tfs_technical_names_merged_3.`acsrl_reason_name`,
tfs_technical_names_merged_3.`total_trans_cost_usd_amt`, 
tfs_technical_names_merged_3.`adjmt_cost_usd_amt`, 
tfs_technical_names_merged_3.`contract_cost_usd_amt`, 
tfs_technical_names_merged_3.`post_charge_cost_usd_amt`, 
tfs_technical_names_merged_3.`spot_cost_usd_amt`, 
tfs_technical_names_merged_3.`misc_cost_usd_amt`, 
tfs_technical_names_merged_3.`weight_per_load_qty`, 
tfs_technical_names_merged_3.`volume_per_load_qty`, 
tfs_technical_names_merged_3.`floor_position_as_ship_cnt`, 
tfs_technical_names_merged_3.`theortc_pallet_cnt`, 
tfs_technical_names_merged_3.`actual_gi_date`, 
tfs_technical_names_merged_3.`charge_code`, 
tfs_technical_names_merged_3.`dlvry_id`, 
tfs_technical_names_merged_3.`profit_center_code`, 
tfs_technical_names_merged_3.`cntrlng_area_code`, 
tfs_technical_names_merged_3.`distance_qty`, 
tfs_technical_names_merged_3.`distance_uom`, 
tfs_technical_names_merged_3.`total_weight_ship_qty`, 
tfs_technical_names_merged_3.`weight_uom`, 
tfs_technical_names_merged_3.`total_volume_ship_qty`, 
tfs_technical_names_merged_3.`volume_uom`,
tfs_technical_names_merged_3.`currency_code`,
tfs_technical_names_merged_3.`su_per_load_qty`,
tfs_technical_names_merged_3.`ship_from_region_code`,
tfs_technical_names_merged_3.`ship_to_region_desc`,
tfs_technical_names_merged_3.`country_to_code`,
680_tdcval_hierarchy.`categ_name` AS `categ_code`,
680_tdcval_hierarchy.`sector_name` AS `sector_desc`,
680_tdcval_hierarchy.`subsector_name` AS `subsector_desc`,
tfs_technical_names_merged_3.`voucher_status_code`, 
tfs_technical_names_merged_3.`voucher_ref_num`, 
tfs_technical_names_merged_3.`country_from_desc_name`, 
tfs_technical_names_merged_3.`country_to_desc_name`, 
tfs_technical_names_merged_3.`equip_mode_desc`, 
tfs_technical_names_merged_3.`ship_from_region_desc`, 
tfs_technical_names_merged_3.`table_uom`, 
tfs_technical_names_merged_3.`origin_desc`, 
tfs_technical_names_merged_3.`cost_center_code`, 
tfs_technical_names_merged_3.`voucher_id`, 
tfs_technical_names_merged_3.`multi_stop_flag`, 
tfs_technical_names_merged_3.`temp_protect_code`, 
tfs_technical_names_merged_3.`spot_flag_val`, 
tfs_technical_names_merged_3.`gbu_code`, 
tfs_technical_names_merged_3.`goods_receipt_post_date`, 
tfs_technical_names_merged_3.`create_tmstp`, 
tfs_technical_names_merged_3.`create_date`, 
tfs_technical_names_merged_3.`na_target_country_code`, 
tfs_technical_names_merged_3.`ship_to_party_num`, 
tfs_technical_names_merged_3.`ship_point_code`, 
tfs_technical_names_merged_3.`trans_plan_point_code`, 
tfs_technical_names_merged_3.`equip_mode_code`, 
tfs_technical_names_merged_3.`trans_equip_type_code`, 
tfs_technical_names_merged_3.`freight_type_customer_interplant_ind_code`, 
tfs_technical_names_merged_3.`country_from_code`,
tfs_technical_names_merged_3.`tms_charge_lvl_desc`, 
tfs_technical_names_merged_3.`tms_charge_kind_desc`, 
tfs_technical_names_merged_3.`tms_interface_reason_cost_code`, 
tfs_technical_names_merged_3.`chart_account_num`, 
tfs_technical_names_merged_3.`step_factor`, 
tfs_technical_names_merged_3.`company_code`, 
680_tdcval_hierarchy.`tdcval_name` AS `tdcval_desc`,
tfs_technical_names_merged_3.`freight_bill_create_date`, 
tfs_technical_names_merged_3.`dlvry_item_cnt`,
tfs_technical_names_merged_3.`carr_country_name` AS `carr_country_name`,
tfs_technical_names_merged_3.`carr_country_code` AS `carr_country_code`,
IF (tfs_technical_names_merged_3.`carr_country_name`='US',
substr(tfs_technical_names_merged_3.`carr_postal_code`,1,5), tfs_technical_names_merged_3.`carr_postal_code`) AS `carr_postal_code`,
tfs_technical_names_merged_3.`ship_to_postal_code` AS `ship_to_postal_code`,
tfs_technical_names_merged_3.`ship_to_state_code` AS `ship_to_state_code`,
tfs_technical_names_merged_3.`ship_to_state_name` AS `ship_to_state_name`,
tfs_technical_names_merged_3.`minority_ind_val` AS `minority_ind_val`,
IF (tfs_technical_names_merged_3.actual_gi_date<'2018-06-01', tfs_technical_names_merged_3.dest_zone_val
,tfs_technical_names_merged_3.dest_zone_go_name
) as dest_zone_go_name,
IF (tfs_technical_names_merged_3.actual_gi_date<'2018-06-01', tfs_technical_names_merged_3.tfs_origin_zone_name
,tfs_technical_names_merged_3.origin_zone_name
) as origin_zone_name,
tfs_technical_names_merged_3.`dest_loc_code` AS `dest_loc_code`,
tfs_technical_names_merged_3.`charge_detail_id`, 
tfs_technical_names_merged_3.`material_doc_num`, 
tfs_technical_names_merged_3.`purchase_doc_num`, 
tfs_technical_names_merged_3.`charge_kind_code`, 
tfs_technical_names_merged_3.`charge_lvl`, 
tfs_technical_names_merged_3.`billing_proposal_num`, 
tfs_technical_names_merged_3.`gl_account_num`, 
tfs_technical_names_merged_3.`step_per_load2_rate`, 
tfs_technical_names_merged_3.`step_per_load_rate`, 
tfs_technical_names_merged_3.`total_trans_cost_local_currency_amt`, 
tfs_technical_names_merged_3.`cost_on_step_local_currency_amt`, 
tfs_technical_names_merged_3.`adjmt_cost_local_currency_amt`, 
tfs_technical_names_merged_3.`contract_cost_local_currency_amt`, 
tfs_technical_names_merged_3.`post_charge_cost_local_currency_amt`, 
tfs_technical_names_merged_3.`spot_cost_local_currency_amt`, 
tfs_technical_names_merged_3.`misc_cost_local_currency_amt`, 
tfs_technical_names_merged_3.`tfts_load_date`, 
tfs_technical_names_merged_3.`load_from_file_url`, 
tfs_technical_names_merged_3.`carr_id`, 
tfs_technical_names_merged_3.`row_modify_tmstp`, 
tfs_technical_names_merged_3.`freight_auction_val`, 
tfs_technical_names_merged_3.`hist_data_structure_flag`, 
tfs_technical_names_merged_3.`origin_longitude_val`, 
tfs_technical_names_merged_3.`origin_latitude_val`, 
tfs_technical_names_merged_3.`dest_longitude_val`, 
tfs_technical_names_merged_3.`dest_latitude_val`, 
tfs_technical_names_merged_3.`dest_postal_code`, 
tfs_technical_names_merged_3.`five_digit_lane_name`,
tfs_technical_names_merged_3.`region_2_3_digit_lane_name`, 
tfs_technical_names_merged_3.`customer_specific_lane_name`, 
tfs_technical_names_merged_3.`customer1_lvl`, 
tfs_technical_names_merged_3.`customer2_lvl`, 
tfs_technical_names_merged_3.`customer3_lvl`, 
tfs_technical_names_merged_3.`customer4_lvl`, 
tfs_technical_names_merged_3.`customer5_lvl`, 
tfs_technical_names_merged_3.`customer6_lvl`, 
tfs_technical_names_merged_3.`customer7_lvl`, 
tfs_technical_names_merged_3.`customer8_lvl`, 
tfs_technical_names_merged_3.`customer9_lvl`, 
tfs_technical_names_merged_3.`customer10_lvl`, 
tfs_technical_names_merged_3.`customer11_lvl`, 
tfs_technical_names_merged_3.`customer12_lvl`, 
tfs_technical_names_merged_3.`carr_and_source_service_val`, 
tfs_technical_names_merged_3.`accrual_cost_amt`, 
tfs_technical_names_merged_3.`line_haul_cost_amt`,
tfs_technical_names_merged_3.`fuel_cost_amt`, 
tfs_technical_names_merged_3.`other_contract_cost_amt`, 
tfs_technical_names_merged_3.`frcst_cost_amt`, 
tfs_technical_names_merged_3.`ave_total_trans_cost_per_pallet_amt`, 
tfs_technical_names_merged_3.`total_trans_cost_per_lb_amt`, 
tfs_technical_names_merged_3.`total_trans_cost_per_cubic_feet_volume_amt`, 
tfs_technical_names_merged_3.`total_trans_cost_per_mile_amt`, 
tfs_technical_names_merged_3.`total_trans_cost_per_su_amt`, 
tfs_technical_names_merged_3.`accrued_contract_cost_amt`, 
tfs_technical_names_merged_3.`accrued_line_haul_cost_amt`, 
tfs_technical_names_merged_3.`accrued_fuel_cost_amt`, 
tfs_technical_names_merged_3.`accrued_other_contract_cost_amt`, 
tfs_technical_names_merged_3.`accrued_spot_charge_rate`, 
tfs_technical_names_merged_3.`post_charge_cost_amt`, 
tfs_technical_names_merged_3.`misc_cost_amt`, 
tfs_technical_names_merged_3.`contract_cost_amt`, 
tfs_technical_names_merged_3.`multi_tdcval_code`, 
tfs_technical_names_merged_3.`min_charge_amt`, 
tfs_technical_names_merged_3.`charge_reason_freight_concat_name`, 
tfs_technical_names_merged_3.`avoidbl_touch_val`, 
tfs_technical_names_merged_3.`opertn_freight_type_code`, 
tfs_technical_names_merged_3.`truckload_vs_intermodal_val`, 
tfs_technical_names_merged_3.`pgp_flag`, 
tfs_technical_names_merged_3.`freight_type_val`, 
tfs_technical_names_merged_3.`first_tender_carr_name`,
tfs_technical_names_merged_3.`service_tms_code`, 
tfs_technical_names_merged_3.`distance_per_load_qty`, 
tfs_technical_names_merged_3.`hist_data`, 
tfs_technical_names_merged_3.`first_tender_carr_id`, 
tfs_technical_names_merged_3.`first_tender_carr_desc`, 
tfs_technical_names_merged_3.`first_tender_carr_tms_service_code`, 
tfs_technical_names_merged_3.`first_tender_carr_award_qty`, 
tfs_technical_names_merged_3.`first_tender_carr_rate`, 
wal.weighted_average_rate AS `weight_avg_rate`,
tfs_technical_names_merged_3.`primary_carr_flag`, 
tfs_technical_names_merged_3.`accrual_cost_usd_amt`, 
tfs_technical_names_merged_3.`total_trans_cost_usd_include_accrual_amt`,
tfs_technical_names_merged_3.`dest_ship_from_code`
FROM tfs_technical_names_merged_3
LEFT OUTER JOIN weighted_awards_for_lanes2_new_data AS wal
 ON tfs_technical_names_merged_3.`shpmt_id` = wal.`shpmt_id`
 AND COALESCE(tfs_technical_names_merged_3.`tfs_origin_zone_name`, 'XXX') = COALESCE(wal.`tfs_origin_zone_name`, 'XXX')
 AND tfs_technical_names_merged_3.`dest_ship_from_code`  = wal.`dest_ship_from_code`
 AND COALESCE(tfs_technical_names_merged_3.`dest_postal_code`, 'XXX') = COALESCE(wal.`dest_postal_code`, 'XXX')
 AND tfs_technical_names_merged_3.`actual_gi_date` = wal.`actual_gi_date`
 AND tfs_technical_names_merged_3.`service_tms_code` not like 'LTL%'  
 AND tfs_technical_names_merged_3.`country_to_code` != 'MX'
LEFT OUTER JOIN tdcval_hierarchy680_lkp_vw AS 680_tdcval_hierarchy
ON tfs_technical_names_merged_3.`tdcval_code`=substr(680_tdcval_hierarchy.`child_id`, 5);
