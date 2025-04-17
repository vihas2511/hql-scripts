
USE ${hivevar:database};

DROP TABLE IF EXISTS tmp_tbl_distinct_load_id;
CREATE TEMPORARY VIEW tmp_tbl_distinct_load_id 
AS
SELECT DISTINCT
	CASE WHEN LENGTH(load_id) = 9 THEN CONCAT('0',load_id) ELSE load_id END AS load_id
FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star
WHERE LENGTH(load_id) > 1
;
--Changed from Cust_898 to cust_656
DROP TABLE IF EXISTS tmp_tbl_distinct_cust_656;
CREATE TEMPORARY VIEW tmp_tbl_distinct_cust_656 
AS
SELECT 
	customer_id, 
	l1_global_lvl  AS cust1_id,   
	l1_global_name AS cust1_name,   
	COALESCE(l2_key_customer_group_lvl, l1_global_lvl) AS cust2_id,   
	COALESCE(l2_key_customer_group_name, l1_global_name) AS cust2_name, 
	COALESCE(l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust3_id,   
	COALESCE(l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust3_name,
	COALESCE(l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust4_id,
	COALESCE(l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust4_name,
	COALESCE(l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust5_id,
	COALESCE(l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust5_name,
	COALESCE(l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust6_id,
	COALESCE(l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust6_name,
	COALESCE(l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust7_id,
	COALESCE(l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust7_name,
	COALESCE(l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust8_id,
	COALESCE(l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust8_name,
	COALESCE(l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust9_id,
	COALESCE(l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust9_name,
	COALESCE(l10_intrmdt4_lvl, l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust10_id,
	COALESCE(l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust10_name,
	COALESCE(l11_intrmdt5_lvl, l10_intrmdt4_lvl, l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust11_id,
	COALESCE(l11_intrmdt5_name, l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust11_name,
	COALESCE(l12_ship_to_lvl, l11_intrmdt5_lvl, l10_intrmdt4_lvl, l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust12_id,
	COALESCE(l12_ship_to_name, l11_intrmdt5_name, l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust12_name
FROM 
(
	SELECT 
		customer_id, 
		l1_global_lvl,   
		l1_global_name,   
		IF(l2_key_customer_group_lvl='', NULL, l2_key_customer_group_lvl) AS l2_key_customer_group_lvl,
		IF(l2_key_customer_group_name='', NULL, l2_key_customer_group_name) AS l2_key_customer_group_name,
		IF(l3_vary1_lvl='', NULL, l3_vary1_lvl) AS l3_vary1_lvl,
		IF(l3_vary1_name='', NULL, l3_vary1_name) AS l3_vary1_name,
		IF(l4_vary2_lvl='', NULL, l4_vary2_lvl) AS l4_vary2_lvl,
		IF(l4_vary2_name='', NULL, l4_vary2_name) AS l4_vary2_name,
		IF(l5_country_top_account_lvl='', NULL, l5_country_top_account_lvl) AS l5_country_top_account_lvl,
		IF(l5_country_top_account_name='', NULL, l5_country_top_account_name) AS l5_country_top_account_name,
		IF(l6_vary6_lvl='', NULL, l6_vary6_lvl) AS l6_vary6_lvl,
		IF(l6_vary6_name='', NULL, l6_vary6_name) AS l6_vary6_name,
		IF(l7_intrmdt_lvl='', NULL, l7_intrmdt_lvl) AS l7_intrmdt_lvl,
		IF(l7_intrmdt_name='', NULL, l7_intrmdt_name) AS l7_intrmdt_name,
		IF(l8_intrmdt2_lvl='', NULL, l8_intrmdt2_lvl) AS l8_intrmdt2_lvl,
		IF(l8_intrmdt2_name='', NULL, l8_intrmdt2_name) AS l8_intrmdt2_name,
		IF(l9_intrmdt3_lvl='', NULL, l9_intrmdt3_lvl) AS l9_intrmdt3_lvl,
		IF(l9_intrmdt3_name='', NULL, l9_intrmdt3_name) AS l9_intrmdt3_name,
		IF(l10_intrmdt4_lvl='', NULL, l10_intrmdt4_lvl) AS l10_intrmdt4_lvl,
		IF(l10_intrmdt4_name='', NULL, l10_intrmdt4_name) AS l10_intrmdt4_name,
		IF(l11_intrmdt5_lvl='', NULL, l11_intrmdt5_lvl) AS l11_intrmdt5_lvl,
		IF(l11_intrmdt5_name='', NULL, l11_intrmdt5_name) AS l11_intrmdt5_name,
		IF(l12_ship_to_lvl='', NULL, l12_ship_to_lvl) AS l12_ship_to_lvl,
		IF(l12_ship_to_name='', NULL, l12_ship_to_name) AS l12_ship_to_name
	FROM cust_hierarchy656_na_lkp
) a;



DROP TABLE IF EXISTS tmp_tbl_postal_codes;
CREATE TEMPORARY VIEW tmp_tbl_postal_codes
AS
SELECT 
	cust_id, 
	IF(cntry_code LIKE '%US%', REGEXP_EXTRACT(`postal_code`,'([0-9]*)(-*[0-9]*)', 1), IF(cntry_code LIKE '%CA%', SUBSTRING(postal_code, 1, 3), `postal_code`) ) AS `postal_code`
FROM (
	SELECT DISTINCT 
		customer_id  AS cust_id, 
		country_code AS cntry_code, 
		postal_code 
	FROM ${hivevar:dbOsiNa}.customer_dim 
	WHERE country_code IN ('US','CA')
) a;


DROP TABLE IF EXISTS tmp_tbl_cust_656_postal_codes;
CREATE TEMPORARY VIEW tmp_tbl_cust_656_postal_codes
AS
SELECT 
	distinct st.ship_to_party_id, 
	c656.customer_id AS `656_cust`, 
	c656.cust1_id       as   cust_1_id,
	c656.cust1_name     as   cust_1_name,
	c656.cust2_id       as   cust_2_id, 
	c656.cust2_name     as   cust_2_name,
	c656.cust3_id       as   cust_3_id, 
	c656.cust3_name     as   cust_3_name,
	c656.cust4_id       as   cust_4_id, 
	c656.cust4_name     as   cust_4_name,
	c656.cust5_id       as   cust_5_id, 
	c656.cust5_name     as   cust_5_name,
	c656.cust6_id       as   cust_6_id, 
	c656.cust6_name     as   cust_6_name,
	c656.cust7_name     as   cust_7_id, 
	c656.cust8_id       as   cust_7_name,
	c656.cust8_name     as   cust_8_id, 
	c656.cust9_id       as   cust_8_name,
	c656.cust7_id       as   cust_9_id, 
	c656.cust9_name     as   cust_9_name,
	c656.cust10_id      as   cust_10_id, 
	c656.cust10_name    as   cust_10_name,
	c656.cust11_id      as   cust_11_id, 
	c656.cust11_name    as   cust_11_name,
	c656.cust12_id      as   cust_12_id, 
	c656.cust12_name    as   cust_12_name,
	pc.cust_id AS `postal_codes_cust`, 
	pc.postal_code
FROM (SELECT DISTINCT ship_to_party_id FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star) AS st
	LEFT OUTER JOIN tmp_tbl_distinct_cust_656 AS c656
		ON st.ship_to_party_id = c656.customer_id
LEFT OUTER JOIN tmp_tbl_postal_codes AS pc
	ON st.ship_to_party_id = pc.cust_id
;

DROP TABLE IF EXISTS tmp_tbl_cust_656;
CREATE TEMPORARY VIEW tmp_tbl_cust_656 
AS
SELECT 
	customer_id, 
	l1_global_lvl  AS cust1_id,   
	l1_global_name AS cust1_name,   
	COALESCE(l2_key_customer_group_lvl, l1_global_lvl) AS cust2_id,   
	COALESCE(l2_key_customer_group_name, l1_global_name) AS cust2_name, 
	COALESCE(l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust3_id,   
	COALESCE(l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust3_name,
	COALESCE(l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust4_id,
	COALESCE(l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust4_name,
	COALESCE(l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust5_id,
	COALESCE(l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust5_name,
	COALESCE(l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust6_id,
	COALESCE(l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust6_name,
	COALESCE(l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust7_id,
	COALESCE(l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust7_name,
	COALESCE(l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust8_id,
	COALESCE(l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust8_name,
	COALESCE(l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust9_id,
	COALESCE(l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust9_name,
	COALESCE(l10_intrmdt4_lvl, l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust10_id,
	COALESCE(l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust10_name,
	COALESCE(l11_intrmdt5_lvl, l10_intrmdt4_lvl, l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust11_id,
	COALESCE(l11_intrmdt5_name, l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust11_name,
	COALESCE(l12_ship_to_lvl, l11_intrmdt5_lvl, l10_intrmdt4_lvl, l9_intrmdt3_lvl, l8_intrmdt2_lvl, l7_intrmdt_lvl, l6_vary6_lvl, l5_country_top_account_lvl, l4_vary2_lvl, l3_vary1_lvl, l2_key_customer_group_lvl, l1_global_lvl) AS cust12_id,
	COALESCE(l12_ship_to_name, l11_intrmdt5_name, l10_intrmdt4_name, l9_intrmdt3_name, l8_intrmdt2_name, l7_intrmdt_name, l6_vary6_name, l5_country_top_account_name, l4_vary2_name, l3_vary1_name, l2_key_customer_group_name, l1_global_name) AS cust12_name
FROM 
(
	SELECT 
		customer_id, 
		l1_global_lvl,   
		l1_global_name,   
		IF(l2_key_customer_group_lvl='', NULL, l2_key_customer_group_lvl) AS l2_key_customer_group_lvl,
		IF(l2_key_customer_group_name='', NULL, l2_key_customer_group_name) AS l2_key_customer_group_name,
		IF(l3_vary1_lvl='', NULL, l3_vary1_lvl) AS l3_vary1_lvl,
		IF(l3_vary1_name='', NULL, l3_vary1_name) AS l3_vary1_name,
		IF(l4_vary2_lvl='', NULL, l4_vary2_lvl) AS l4_vary2_lvl,
		IF(l4_vary2_name='', NULL, l4_vary2_name) AS l4_vary2_name,
		IF(l5_country_top_account_lvl='', NULL, l5_country_top_account_lvl) AS l5_country_top_account_lvl,
		IF(l5_country_top_account_name='', NULL, l5_country_top_account_name) AS l5_country_top_account_name,
		IF(l6_vary6_lvl='', NULL, l6_vary6_lvl) AS l6_vary6_lvl,
		IF(l6_vary6_name='', NULL, l6_vary6_name) AS l6_vary6_name,
		IF(l7_intrmdt_lvl='', NULL, l7_intrmdt_lvl) AS l7_intrmdt_lvl,
		IF(l7_intrmdt_name='', NULL, l7_intrmdt_name) AS l7_intrmdt_name,
		IF(l8_intrmdt2_lvl='', NULL, l8_intrmdt2_lvl) AS l8_intrmdt2_lvl,
		IF(l8_intrmdt2_name='', NULL, l8_intrmdt2_name) AS l8_intrmdt2_name,
		IF(l9_intrmdt3_lvl='', NULL, l9_intrmdt3_lvl) AS l9_intrmdt3_lvl,
		IF(l9_intrmdt3_name='', NULL, l9_intrmdt3_name) AS l9_intrmdt3_name,
		IF(l10_intrmdt4_lvl='', NULL, l10_intrmdt4_lvl) AS l10_intrmdt4_lvl,
		IF(l10_intrmdt4_name='', NULL, l10_intrmdt4_name) AS l10_intrmdt4_name,
		IF(l11_intrmdt5_lvl='', NULL, l11_intrmdt5_lvl) AS l11_intrmdt5_lvl,
		IF(l11_intrmdt5_name='', NULL, l11_intrmdt5_name) AS l11_intrmdt5_name,
		IF(l12_ship_to_lvl='', NULL, l12_ship_to_lvl) AS l12_ship_to_lvl,
		IF(l12_ship_to_name='', NULL, l12_ship_to_name) AS l12_ship_to_name
	FROM cust_hierarchy656_na_lkp
) a;


DROP TABLE IF EXISTS tmp_tbl_distinct_otd_vfr;
CREATE TEMPORARY VIEW tmp_tbl_distinct_otd_vfr 
AS
SELECT DISTINCT 
	shpmt_id,
	carr_id,
	cntry_to_code
FROM ${hivevar:dbTransVsbltBw}.otd_vfr_na_star AS vfr
WHERE EXISTS (SELECT 1 FROM tmp_tbl_distinct_load_id AS ld WHERE ld.load_id = vfr.shpmt_id)
;


DROP TABLE IF EXISTS tmp_tbl_distinct_on_time_arriv;
CREATE TEMPORARY VIEW tmp_tbl_distinct_on_time_arriv 
AS
SELECT * FROM 
(
	SELECT 
		event_datetm,
		shpmt_num,
		carr_num,
		ROW_NUMBER() OVER (PARTITION BY shpmt_num ORDER BY event_datetm DESC) as rank_num 
	FROM ${hivevar:dbTransVsbltBw}.on_time_arriv_shpmt_custshpmt_na_merged_star AS ont
	WHERE EXISTS (SELECT 1 FROM tmp_tbl_distinct_load_id AS ld WHERE ld.load_id = ont.shpmt_num)
) t1
WHERE rank_num = 1
;


DROP TABLE IF EXISTS tmp_tbl_distinct_cust_md;
CREATE TEMPORARY VIEW tmp_tbl_distinct_cust_md 
AS
SELECT DISTINCT 
	customer1_name,
	customer_id
FROM cust_na_dim AS cust
WHERE EXISTS (SELECT 1 FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star tac WHERE tac.ship_to_party_id = cust.customer_id)
;

DROP TABLE IF EXISTS tmp_tbl_distinct_vendor_1;
CREATE TEMPORARY VIEW tmp_tbl_distinct_vendor_1 
AS
SELECT DISTINCT 
	CASE WHEN LENGTH(vmd.vendor_account_num)=8 THEN CONCAT('00',vmd.vendor_account_num) ELSE vmd.vendor_account_num END AS vendor_account_num,
	carr1_name
FROM vendor_na_dim AS vmd
WHERE EXISTS (SELECT 1 FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star tac WHERE (CASE WHEN LENGTH(tac.frwrd_agent_id)=8 THEN CONCAT('00',tac.frwrd_agent_id) ELSE tac.frwrd_agent_id END) = (CASE WHEN LENGTH(vmd.vendor_account_num)=8 THEN CONCAT('00',vmd.vendor_account_num) ELSE vmd.vendor_account_num END))
;

DROP TABLE IF EXISTS tmp_tbl_distinct_vendor_2;
CREATE TEMPORARY VIEW tmp_tbl_distinct_vendor_2 
AS
SELECT DISTINCT 
	REGEXP_REPLACE(vendor_account_num, "^0+", '') AS vendor_account_num,
	carr1_name
FROM vendor_na_dim AS vmd
--WHERE EXISTS (SELECT 1 FROM tmp_distinct_otd_vfr AS vfr WHERE vfr.carr_id = vmd.vendor_account_num)
;

DROP TABLE IF EXISTS tmp_tbl_count_freight_cost_charge;
CREATE TEMPORARY VIEW tmp_tbl_count_freight_cost_charge 
AS
SELECT 
    shpmt_id, 
    SUM(
        CASE
            WHEN freight_cost_charge_code = 'FA_A' THEN 1 ELSE 0
        END
    ) AS is_fa_a,
    SUM(
        CASE
            WHEN freight_cost_charge_code <> 'FA_A' AND LENGTH(freight_cost_charge_code) > 1 THEN 1 ELSE 0
        END
    ) AS is_other
FROM tfs_technical_name_star AS tfs
--WHERE EXISTS (SELECT 1 FROM tmp_tbl_distinct_load_id AS ld WHERE ld.load_id = tfs.shpmt_id)
GROUP BY shpmt_id
;


DROP TABLE IF EXISTS tmp_tbl_tfs_carr_id;
CREATE TEMPORARY VIEW tmp_tbl_tfs_carr_id 
AS
SELECT * FROM 
(
	SELECT 
		shpmt_id,
		carr_id,
		ROW_NUMBER() OVER (PARTITION BY shpmt_id ORDER BY row_modify_tmstp DESC) as rank_num 
	FROM tfs_technical_name_star AS tfs
	WHERE EXISTS (SELECT 1 FROM tmp_tbl_distinct_load_id AS ld WHERE ld.load_id = tfs.shpmt_id)
) t1
WHERE rank_num = 1
;

DROP TABLE IF EXISTS tmp_tbl_distinct_shipping_location_1;
CREATE TEMPORARY VIEW tmp_tbl_distinct_shipping_location_1
AS
SELECT DISTINCT
    corp2_id, 
    country_name
FROM ${hivevar:dbTransVsbltBw}.shipping_location_na_dim AS sl
WHERE EXISTS (SELECT 1 FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star AS tac WHERE tac.orign_zone_code = sl.corp2_id )
;

DROP TABLE IF EXISTS tmp_tbl_distinct_shipping_location_2;
CREATE TEMPORARY VIEW tmp_tbl_distinct_shipping_location_2
AS
SELECT DISTINCT
    loc_id,
    corp2_id
FROM ${hivevar:dbTransVsbltBw}.shipping_location_na_dim AS sl
WHERE EXISTS (SELECT 1 FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star tac WHERE tac.ship_to_party_id = sl.loc_id)
;

DROP TABLE IF EXISTS tmp_tbl_tac_technical_names_1_1;
CREATE TEMPORARY VIEW tmp_tbl_tac_technical_names_1_1
AS
SELECT /*+ MAPJOIN(tmp_tbl_distinct_shipping_location_1, tmp_tbl_distinct_shipping_location_1, tmp_tbl_distinct_vendor_2) */ 
	CASE WHEN LENGTH(tac.load_id) = 9 THEN CONCAT('0',tac.load_id) ELSE tac.load_id END AS load_id,
	tac.cal_month_code 					AS cal_month_code,					-- Calendar Year/Month
	tac.ship_to_party_id,                               					-- Customer ID & Destination Location ID !!!
	tac.frt_type_name 					AS operational_freight_type_code,	-- Operational Freight Type
	tac.ship_condtns_val 				AS ship_cond_val,					-- Shipping conditions
	tac.sold_to_party_id 				AS sold_to_party_id,				-- Sold To #
	CASE 
		WHEN LENGTH(tac.schduld_date) > 1 THEN CONCAT(SUBSTR(tac.schduld_date,7,4),'-',SUBSTR(tac.schduld_date,4,2),'-',SUBSTR(tac.schduld_date,1,2)) 
		ELSE NULL
	END 								AS schedule_date,					-- Scheduled Date,
	CONCAT(SUBSTR(tac.schduld_datetm, 1, 2), ':', SUBSTR(tac.schduld_datetm, 3, 2), ':', SUBSTR(tac.schduld_datetm, 5, 2))	AS schedule_datetm,	-- Scheduled Time
	tac.serv_code_tms_code 				AS service_tms_code,				-- TMS Service Code
	tac.trans_plan_point_code, 												-- Transportation planning point
	tac.gbu_per_ship_site_name 			AS gbu_per_ship_site_name, 			-- GBU per Shipping Site
	CASE WHEN LENGTH(frwrd_agent_id)=8 THEN CONCAT('00',frwrd_agent_id) ELSE frwrd_agent_id END AS forward_agent_id,				-- Carrier ID
	tac.tender_event_type_code 			AS tender_event_type_code,			-- Tender Event Type Code
	tac.tender_reasn_code 				AS tender_reason_code,				-- Tender Reason Code
	CONCAT(SUBSTR(tac.tender_date,7,4),'-',SUBSTR(tac.tender_date,4,2),'-',SUBSTR(tac.tender_date,1,2)) AS tender_date,	-- Tender Event Date
	CASE
		WHEN INSTR(tac.tender_datetm, ':') > 0 THEN tac.tender_datetm
		ELSE CONCAT(SUBSTR(tac.tender_datetm, 1, 2), ':', SUBSTR(tac.tender_datetm, 3, 2), ':', SUBSTR(tac.tender_datetm, 5, 2))
	END 								AS tender_datetm,					-- Tender Event Time
	tac.carr_mode_code 					AS carr_mode_code,					-- Mode
    tac.orign_zone_code 				AS origin_zone_ship_from_code,		-- Origin SF
	tac.deprtre_cntry_code, 												-- Departure Country Code
	CONCAT(SUBSTR(tac.act_goods_issue_date,7,4),'-',SUBSTR(tac.act_goods_issue_date,4,2),'-',SUBSTR(tac.act_goods_issue_date,1,2)) AS act_goods_issue_date,  -- Actual Ship Date
	tac.act_goods_issue_datetm 			AS actual_goods_issue_datetm,
	tac.days_in_wk_qty 					AS day_in_week_qty,					-- No.of Days in week
	tac.days_in_month_qty 				AS day_in_month_qty,				-- No.of Days in Month
	tac.scac_id_id 						AS scac_id,							-- SCAC
	tac.carr_mode_desc 					AS carr_mode_desc,					-- Carrier Mode Description
	tac.tariff_id 						AS tariff_id,						-- Tariff ID
	tac.sched_code 						AS schedule_code,					-- Schedule ID
	tac.tender_first_carr_desc 			AS tender_first_carr_desc,			-- Tender Event Type Description
	tac.tender_acptn_id 				AS tender_acptn_id,					-- Tender Acceptance Key
	tac.tender_reasn_code_desc 			AS tender_reason_code_desc,			-- Tender Reason Code Description
	tac.cal_wk_code 					AS cal_week_code,					-- Calendar Year/Week
	tac.postl_code 						AS postal_code,						-- Postal Code Raw TMS
	tac.frt_auction_ind 				AS freight_auction_ind,				-- Freight Auction Flag
	tac.ship_to_party_desc 				AS ship_to_party_desc,
    tac.origin_location_code 			AS origin_location_code,			-- Origin Location ID
    tac.origin_zone_2_code 				AS origin_zone_code,				-- Origin Zone
	CASE
		WHEN LENGTH(tac.dest_zone_go_code) = 60 AND INSTR(tac.dest_zone_go_code, '-') = 56 THEN SUBSTR(tac.dest_zone_go_code, 51, 5) -- US Zip+4 - We want only five digit US zip code 
		WHEN LENGTH(tac.dest_zone_go_code) = 60 AND SUBSTR(tac.dest_zone_go_code, 51, 8) = '00000000' THEN SUBSTR(tac.dest_zone_go_code, -2) -- two digit zip code
		WHEN LENGTH(tac.dest_zone_go_code) = 60 AND SUBSTR(tac.dest_zone_go_code, 51, 7) = '0000000' THEN SUBSTR(tac.dest_zone_go_code, -3) -- three digit zip code
		WHEN LENGTH(tac.dest_zone_go_code) = 60 AND SUBSTR(tac.dest_zone_go_code, 51, 5) = '00000' THEN SUBSTR(tac.dest_zone_go_code, -5) -- US Zip Code
		WHEN LENGTH(tac.dest_zone_go_code) = 60 AND SUBSTR(tac.dest_zone_go_code, 51, 4) = '0000' THEN SUBSTR(tac.dest_zone_go_code, -6) -- Canada Zip Code with no space in it
		WHEN LENGTH(tac.dest_zone_go_code) = 60 AND SUBSTR(tac.dest_zone_go_code, 51, 3) = '000' THEN CONCAT(SUBSTR(tac.dest_zone_go_code, 54, 3), SUBSTR(tac.dest_zone_go_code, 58, 3)) -- Canada Zip Code with space in it
		WHEN LENGTH(tac.dest_zone_go_code) = 60 THEN SUBSTR(tac.dest_zone_go_code, -10) -- Ship To Party or Customer ID
        WHEN LENGTH(tac.dest_zone_go_code) = 7 AND LOCATE(' ', tac.dest_zone_go_code) > 0 THEN CONCAT(SUBSTR(tac.dest_zone_go_code, 1, 3), SUBSTR(tac.dest_zone_go_code, 5, 3))  -- Canada Zip Code with space in it
		ELSE tac.dest_zone_go_code
	END 								AS dest_zone_code,					-- Destination Zone
	CASE
		WHEN INSTR(tac.tender_datetm, ':') > 0 
			THEN CONCAT(CONCAT(SUBSTR(tac.tender_date,7,4),'-',SUBSTR(tac.tender_date,4,2),'-',SUBSTR(tac.tender_date,1,2)), ' ', tac.tender_datetm)
		ELSE CONCAT(CONCAT(SUBSTR(tac.tender_date,7,4),'-',SUBSTR(tac.tender_date,4,2),'-',SUBSTR(tac.tender_date,1,2)), ' ', SUBSTR(tac.tender_datetm, 1, 2), ':', SUBSTR(tac.tender_datetm, 3, 2), ':', SUBSTR(tac.tender_datetm, 5, 2))
	END 								AS tender_event_datetm,				-- Tender Event Date & Time
	CONCAT(SUBSTR(tac.sap_orig_ship_date,1,4),'-',SUBSTR(tac.sap_orig_ship_date,5,2),'-',SUBSTR(tac.sap_orig_ship_date,7,2)) AS sap_orig_ship_date,	-- SAP Original Shipdate
	CONCAT(SUBSTR(tac.orig_tender_ship_date,7,4),'-',SUBSTR(tac.orig_tender_ship_date,4,2),'-',SUBSTR(tac.orig_tender_ship_date,1,2)) AS tender_orig_ship_date, -- Original Tendered Ship Date
	CASE
		WHEN tac.actual_ship_datetm = '000000' THEN tac.act_goods_issue_datetm
		ELSE COALESCE(tac.actual_ship_datetm, tac.act_goods_issue_datetm)
	END									AS actual_ship_datetm, 				-- Actual Ship Time
	tac.act_goods_issue_datetm			AS actual_ship_datetm2, 			-- Actual Ship Time 2 - to check
	tac.avg_awarded_wk_val 				AS avg_award_weekly_vol_qty,		-- Average Awarded Weekly Volume
	tac.alloc_type_code 				AS alloc_type_code,					-- ALLOCATION TYPE
	tac.sun_max_load_qty 				AS sun_max_load_qty,				-- SUN MAX (LOADS)
	tac.mon_max_load_qty 				AS mon_max_load_qty,				-- MON MAX (LOADS)
	tac.tue_max_load_qty 				AS tue_max_load_qty,				-- TUE MAX (LOADS)
	tac.wed_max_load_qty 				AS wed_max_load_qty,				-- WED MAX (LOADS)
	tac.thu_max_load_qty 				AS thu_max_load_qty,				-- THU MAX (LOADS)
	tac.fri_max_load_qty 				AS fri_max_load_qty,				-- FRI MAX (LOADS)
	tac.sat_max_load_qty 				AS sat_max_load_qty,				-- SAT MAX (LOADS)
	CASE
		WHEN LENGTH(tac.postl_code) = 6 THEN SUBSTR(tac.postl_code,1,3)		-- Canada Postal Code
		WHEN LENGTH(tac.postl_code) > 6 THEN SUBSTR(tac.postl_code,1,5)		-- US Postal Code Zip+4 Format
		ELSE tac.postl_code 												-- Normal US Postal Code (use AS is)
	END 								AS final_stop_postal_code,			-- Postal Code Final Stop
	CASE
        WHEN LENGTH(sl.country_name) > 1 THEN sl.country_name
        ELSE ''
    END 								AS country_from_code,				-- Country From
	CASE
		WHEN LENGTH(tfs.shpmt_id) > 1 AND tfs.is_fa_a  > 0 THEN 'Y'
		WHEN LENGTH(tfs.shpmt_id) > 1 AND tfs.is_other > 0 THEN 'N'
		ELSE ''
	END 								AS freight_auction_flag,			-- True FA Flag
    CASE 
		WHEN REGEXP_EXTRACT(ship_to_party_id, '^[0-9]', 0) >= 0 THEN 'CUSTOMER'
		ELSE 'INTERPLANT'
	END 								AS freight_type_code, 				-- Freight Type
    CASE
        WHEN unix_timestamp(tender_date,'dd/MM/yyyy') < unix_timestamp('26/05/2018','dd/MM/yyyy') THEN 0
        ELSE 1
    END 								AS pre_tms_update_flag, 			-- Pre TMS Upgrade Flag 
	CASE 
		WHEN tac.avg_awarded_wk_val > 0.01 THEN 1 
		ELSE 0
	END 								AS primary_carr_flag, 				-- Primary Carrier Flag
	CASE
		WHEN tac.tender_event_type_code IN ('TENDREJ', 'TENDCNCL') AND LENGTH(tac.load_id) = 9 THEN CONCAT('0',tac.load_id)
		WHEN tac.tender_event_type_code IN ('TENDREJ', 'TENDCNCL') THEN tac.load_id
		ELSE ''
	END 								AS tender_reject_load_id, 			-- Tender Rejected Loads
	CASE
		WHEN LENGTH(vfr.carr_id)  > 1 THEN vfr.carr_id
		WHEN ota.carr_num > 1 THEN CAST(ota.carr_num AS string)
		WHEN tca.carr_id > 1 THEN CAST(tca.carr_id AS string)
		ELSE ''
	END 								AS actual_carr_id, 					-- Actual Carrier ID
	tac.tfts_load_tmstp 				AS tfts_load_tmstp,
	tac.load_from_file 					AS load_from_file_name,
	tac.bd_mod_tmstp,
	CASE 
		WHEN INSTR(tac.tfts_load_tmstp, 'Historical load') > 0 THEN 1
		ELSE 0
	END AS historical_data_structure_flag

FROM ${hivevar:dbTransVsbltBw}.tender_acceptance_na_merged_star AS tac

    LEFT JOIN tmp_tbl_distinct_shipping_location_1 AS sl
        ON tac.orign_zone_code = sl.corp2_id

	LEFT JOIN tmp_tbl_distinct_otd_vfr AS vfr
		ON vfr.shpmt_id = (CASE WHEN LENGTH(tac.load_id) = 9 THEN CONCAT('0',tac.load_id) ELSE tac.load_id END)

	LEFT JOIN tmp_tbl_distinct_on_time_arriv AS ota
		ON ota.shpmt_num = (CASE WHEN LENGTH(tac.load_id) = 9 THEN CONCAT('0',tac.load_id) ELSE tac.load_id END)

	LEFT JOIN tmp_tbl_tfs_carr_id AS tca
		ON tca.shpmt_id = (CASE WHEN LENGTH(tac.load_id) = 9 THEN CONCAT('0',tac.load_id) ELSE tac.load_id END)

	LEFT JOIN tmp_tbl_count_freight_cost_charge AS tfs
		ON tfs.shpmt_id = (CASE WHEN LENGTH(tac.load_id) = 9 THEN CONCAT('0',tac.load_id) ELSE tac.load_id END)
;

DROP TABLE IF EXISTS tmp_tbl_prev_tender_datetm_1;
CREATE TEMPORARY VIEW tmp_tbl_prev_tender_datetm_1 AS
SELECT
	a.load_id,
	a.tender_event_datetm,
	LAG(a.tender_event_datetm, 1, NULL) OVER (PARTITION BY a.load_id ORDER BY a.tender_event_datetm) AS prev_tender_datetm
FROM (
    SELECT DISTINCT
        load_id,
        tender_event_datetm
    FROM tmp_tbl_tac_technical_names_1_1
) AS a
;

DROP TABLE IF EXISTS tmp_tbl_prev_tender_datetm;
CREATE TEMPORARY VIEW tmp_tbl_prev_tender_datetm AS
SELECT
    load_id,
    tender_event_datetm,
    prev_tender_datetm,
    calc_time_between_tender_val,
    datediff(calc_time_between_tender_val, '1970-01-01') * 24 AS no_of_hrs,
    CONCAT(LPAD((datediff(calc_time_between_tender_val, '1970-01-01') * 24) + CAST(substring(calc_time_between_tender_val,12,2) AS INT), 2, '0'), SUBSTRING(calc_time_between_tender_val,14)) AS time_between_tender_val
    --CONCAT(LPAD((datediff(calc_time_between_tender_val, '1969-12-31') * 24) + CAST(substring(calc_time_between_tender_val,12,2) AS INT) - 19, 2, '0'), SUBSTRING(calc_time_between_tender_val,14)) AS time_between_tender_val,
    --substring(calc_time_between_tender_val,12) AS time_between_tender_val
FROM (
    SELECT
        load_id,
        tender_event_datetm,
        COALESCE(prev_tender_datetm, tender_event_datetm) AS prev_tender_datetm,
        from_unixtime(unix_timestamp(tender_event_datetm) - unix_timestamp(COALESCE(prev_tender_datetm, tender_event_datetm)), 'yyyy-MM-dd HH:mm:ss') AS calc_time_between_tender_val
    FROM tmp_tbl_prev_tender_datetm_1
)b
;

-- Calculate subsectors

DROP TABLE IF EXISTS tmp_tbl_subsector_sum_step;
CREATE TEMPORARY VIEW tmp_tbl_subsector_sum_step 
AS
WITH t AS (
SELECT  shpmt_id,
        SUM( CASE WHEN locate('APPLIANCES', subsector_desc) > 0            THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS appliance_subsector_step_cnt,		
        SUM( CASE WHEN locate('BABY CARE', subsector_desc) > 0             THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS baby_care_subsector_step_cnt,		
        SUM( CASE WHEN locate('CHEMICALS', subsector_desc) > 0             THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS chemical_subsector_step_cnt,		
        SUM( CASE WHEN locate('FABRIC CARE', subsector_desc) > 0           THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS fabric_subsector_step_cnt,			
        SUM( CASE WHEN locate('FAMILY CARE', subsector_desc) > 0           THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS family_subsector_step_cnt,			
        SUM( CASE WHEN locate('FEM CARE', subsector_desc) > 0              THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS fem_subsector_step_cnt,			
        SUM( CASE WHEN locate('HAIR CARE', subsector_desc) > 0             THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS hair_subsector_step_cnt,			
        SUM( CASE WHEN locate('HOME CARE', subsector_desc) > 0             THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS home_subsector_step_cnt,			
        SUM( CASE WHEN locate('ORAL CARE', subsector_desc) > 0             THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS oral_subsector_step_cnt,			
        SUM( CASE WHEN locate('PHC', subsector_desc) > 0                   THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS phc_subsector_step_cnt,			
        SUM( CASE WHEN locate('SHAVE CARE', subsector_desc) > 0            THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS shave_subsector_step_cnt,			
        SUM( CASE WHEN locate('SKIN & PERSONAL CARE', subsector_desc) > 0  THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) ELSE 0 END) AS skin_subsector_cnt,				
        SUM(
				CASE 
					WHEN locate('APPLIANCES', subsector_desc) = 0 
						AND locate('BABY CARE', subsector_desc) = 0 
						AND locate('CHEMICALS', subsector_desc) = 0 
						AND locate('FABRIC CARE', subsector_desc) = 0 
						AND locate('FAMILY CARE', subsector_desc) = 0 
						AND locate('FEM CARE', subsector_desc) = 0 
						AND locate('HAIR CARE', subsector_desc) = 0 
						AND locate('HOME CARE', subsector_desc) = 0 
						AND locate('ORAL CARE', subsector_desc) = 0 
						AND locate('PHC', subsector_desc) = 0 
						AND locate('SHAVE CARE', subsector_desc) = 0 
						AND locate('SKIN & PERSONAL CARE', subsector_desc) = 0 
							THEN CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) 
					ELSE 0 
				END
		) AS other_subsector_cnt,				
        SUM( CAST(COALESCE(step_per_load_rate, 0) AS FLOAT) )                                                                              AS sum_steps_cnt
FROM tfs_technical_name_star AS tfs --ap_transfix_tv_na.tfs_technical_names
GROUP BY shpmt_id
HAVING sum_steps_cnt > 0
) SELECT 
	shpmt_id,
	(appliance_subsector_step_cnt / sum_steps_cnt) AS appliance_subsector_step_cnt,
	(baby_care_subsector_step_cnt / sum_steps_cnt) AS baby_care_subsector_step_cnt,
	(chemical_subsector_step_cnt / sum_steps_cnt)  AS chemical_subsector_step_cnt,
	(fabric_subsector_step_cnt / sum_steps_cnt)    AS fabric_subsector_step_cnt,
	(family_subsector_step_cnt / sum_steps_cnt)    AS family_subsector_step_cnt,
	(fem_subsector_step_cnt / sum_steps_cnt)       AS fem_subsector_step_cnt,
	(hair_subsector_step_cnt / sum_steps_cnt)      AS hair_subsector_step_cnt,
	(home_subsector_step_cnt / sum_steps_cnt)      AS home_subsector_step_cnt,
	(oral_subsector_step_cnt / sum_steps_cnt)      AS oral_subsector_step_cnt,
	(phc_subsector_step_cnt / sum_steps_cnt)       AS phc_subsector_step_cnt,
	(shave_subsector_step_cnt / sum_steps_cnt)     AS shave_subsector_step_cnt,
	(skin_subsector_cnt / sum_steps_cnt)           AS skin_subsector_cnt,
	sum_steps_cnt,
    (other_subsector_cnt / sum_steps_cnt)          AS other_subsector_cnt
FROM t;

-- Exchange rate from CAD to USD
DROP TABLE IF EXISTS tmp_tbl_crncy_code;
CREATE TEMPORARY VIEW tmp_tbl_crncy_code
AS
SELECT DISTINCT
	tfs_c.shpmt_id,
	tfs_c.crncy_code,
	tfs_c.frt_cost_chrg_code,
	tfs_c.gr_posting_date,
	exr.year_month,
	exr.exchg_rate
FROM (
		SELECT
			shpmt_id,
			crncy_code,
			frt_cost_chrg_code,
			MIN(gr_posting_date) AS gr_posting_date
		FROM ${hivevar:dbTransVsbltBw}.freight_stats_na_merged_star 
		WHERE crncy_code = 'CAD'
		GROUP BY
			shpmt_id,
			crncy_code,
			frt_cost_chrg_code
	) AS tfs_c
	JOIN (
		SELECT 
			DATE_FORMAT(exchg_rate_eff_date,'y-MM') AS year_month, 
			exchg_rate  
		FROM ${hivevar:dbRds}.exchg_rate_fct 
		WHERE srce_iso_crncy_code   = 'CAD' 
			AND trgt_iso_crncy_code = 'USD' 
			AND exchg_rate_type_id  = '11' 
			AND YEAR(exchg_rate_eff_date) > '2013'
	) AS exr
		ON CONCAT_WS('-', SUBSTR(tfs_c.gr_posting_date,7,4), SUBSTR(tfs_c.gr_posting_date,4,2)) = exr.year_month
;

-- Calculate costs 
DROP TABLE IF EXISTS tmp_tbl_costs;
CREATE TEMPORARY VIEW tmp_tbl_costs 
AS
WITH t AS (
    SELECT  
		tfs.shpmt_id,
		--cuc.exchg_rate,
        MIN( CAST(tfs.weight_avg_rate AS DECIMAL(38,16))) * MIN(COALESCE(cuc.exchg_rate, 1)) AS min_weighted_average_rate,
        MIN( CAST(tfs.distance_per_load_qty AS DECIMAL(38,16)) )       AS min_distance_per_load,
        SUM( CAST(COALESCE(tfs.total_trans_cost_usd_amt, 0) AS DECIMAL(38,16) ) )   AS actual_carr_trans_cost_amt,
        SUM( CASE 
				WHEN freight_cost_charge_code IN ('DST', 'EXM', 'SCS',' FTP', 'TSC', 'DIST', 'DLTL', 'FLAT', 'SPOT', 'FA_A') 
					THEN CAST(COALESCE(tfs.total_trans_cost_usd_amt, 0) AS DECIMAL(38,16)) 
				ELSE 0 
			END )                                        AS linehaul_or_unsourced,
        SUM( CASE 
				WHEN freight_cost_charge_code IN ('FSUR', 'FU_S', 'FLTL', 'FFLT') 
					THEN CAST(COALESCE(tfs.total_trans_cost_usd_amt, 0) AS DECIMAL(38,16))
				ELSE 0 
			END )                                        AS fuel
    FROM tfs_technical_name_star AS tfs --ap_transfix_tv_na.tfs_technical_names
		LEFT JOIN tmp_tbl_crncy_code AS cuc
			ON tfs.shpmt_id = cuc.shpmt_id
				AND tfs.freight_cost_charge_code = cuc.frt_cost_chrg_code
    WHERE EXISTS (SELECT 1 FROM tmp_tbl_distinct_load_id AS ld WHERE ld.load_id = tfs.shpmt_id)
	GROUP by tfs.shpmt_id
), 
t_linehaul AS (
SELECT 
    t.*,
    CASE WHEN min_weighted_average_rate > 0.0001 THEN min_weighted_average_rate * t.min_distance_per_load
         WHEN min_weighted_average_rate = 0      THEN linehaul_or_unsourced
         ELSE 0
    END AS linehaul,
    CASE
       WHEN COALESCE(min_weighted_average_rate,0) < 0.0001 THEN linehaul_or_unsourced
       ELSE 0
    END AS unsourced
FROM t
),
tac_1 AS (
    SELECT DISTINCT load_id, freight_auction_flag
    FROM tmp_tbl_tac_technical_names_1_1 AS tac_1
)
SELECT t_linehaul.*,
       tac_1.freight_auction_flag,
       CASE
          WHEN min_weighted_average_rate > 0.0001 AND tac_1.freight_auction_flag = 'Y'
			THEN linehaul_or_unsourced - linehaul 
          ELSE NULL
       END AS incrmtl_freight_auction_cost_amt,
       CASE
          WHEN min_weighted_average_rate > 0.0001 AND tac_1.freight_auction_flag = 'N'
			THEN linehaul_or_unsourced - linehaul 
          ELSE NULL
       END AS cnc_carr_mix_cost_amt
FROM tac_1, t_linehaul
WHERE tac_1.load_id = t_linehaul.shpmt_id
;


DROP TABLE IF EXISTS tmp_tbl_dest;
CREATE TEMPORARY VIEW tmp_tbl_dest 
AS
SELECT 
	tac_1.load_id, 
	tac_1.ship_to_party_id,                 								-- Customer ID & Destination Location ID !!!
	tac_1.freight_type_code,
	CASE
        WHEN tac_1.freight_type_code = 'INTERPLANT' AND LENGTH(dsf.corp2_id) > 1 THEN dsf.corp2_id
        ELSE tac_1.ship_to_party_id
	END 								AS dest_ship_from_code,  			-- Destination SF
	CASE
		WHEN REGEXP_EXTRACT(ship_to_party_id, '^[0-9]', 0) >= 0 THEN cmd.customer1_name
		WHEN LENGTH(dsf.corp2_id) > 1 THEN dsf.corp2_id
		ELSE tac_1.ship_to_party_desc
	END 								AS customer_desc					-- Customer ID Description

FROM (SELECT DISTINCT load_id, ship_to_party_id, freight_type_code, ship_to_party_desc FROM tmp_tbl_tac_technical_names_1_1) AS tac_1

    LEFT JOIN tmp_tbl_distinct_shipping_location_2 AS dsf
        ON tac_1.ship_to_party_id = dsf.loc_id

    LEFT JOIN tmp_tbl_distinct_cust_md AS cmd
        ON tac_1.ship_to_party_id = cmd.customer_id

;

DROP TABLE IF EXISTS tmp_tbl_tac_technical_names_2_1;
CREATE TEMPORARY VIEW tmp_tbl_tac_technical_names_2_1 AS
SELECT 
    tac_1.load_id,
	tac_1.cal_month_code,									-- Calendar Year/Month
	tac_1.ship_to_party_id,                 				-- Customer ID & Destination Location ID !!!
	tac_1.operational_freight_type_code,					-- Operational Freight Type
	tac_1.ship_cond_val,									-- Shipping conditions
	tac_1.sold_to_party_id,									-- Sold To #
	tac_1.schedule_date,									-- Scheduled Date
	tac_1.schedule_datetm,									-- Scheduled Time
	tac_1.service_tms_code,									-- TMS Service Code
	tac_1.trans_plan_point_code, 							-- Transportation planning point
	tac_1.gbu_per_ship_site_name, 							-- GBU per Shipping Site
	tac_1.forward_agent_id,									-- Carrier ID
	tac_1.tender_event_type_code,							-- Tender Event Type Code
	tac_1.tender_reason_code,								-- Tender Reason Code
	tac_1.tender_date,										-- Tender Event Date
	tac_1.tender_datetm,									-- Tender Event Time
	tac_1.carr_mode_code,									-- Mode
    tac_1.origin_zone_ship_from_code,						-- Origin SF
	tac_1.deprtre_cntry_code, 								-- Departure Country Code
	tac_1.act_goods_issue_date,  							-- Actual Ship Date
	tac_1.actual_goods_issue_datetm,
	tac_1.day_in_week_qty,									-- No.of Days in week
	tac_1.day_in_month_qty,									-- No.of Days in Month
	tac_1.scac_id,											-- SCAC
	tac_1.carr_mode_desc,									-- Carrier Mode Description
	tac_1.tariff_id,										-- Tariff ID
	tac_1.schedule_code,									-- Schedule ID
	tac_1.tender_first_carr_desc,							-- Tender Event Type Description
	tac_1.tender_acptn_id,									-- Tender Acceptance Key
	tac_1.tender_reason_code_desc,							-- Tender Reason Code Description
	tac_1.cal_week_code,									-- Calendar Year/Week
	tac_1.postal_code,										-- Postal Code Raw TMS
	tac_1.freight_auction_ind,								-- Freight Auction Flag
	tac_1.ship_to_party_desc,
    tac_1.origin_location_code,     						-- Origin Location ID
    tac_1.origin_zone_code,           						-- Origin Zone
	dst.dest_ship_from_code,  								-- Destination SF
	tac_1.dest_zone_code,									-- Destination Zone
	vmd2.carr1_name 					AS carr_desc,		-- Carrier Description
	tac_1.tender_event_datetm,								-- Tender Event Date & Time
	tac_1.sap_orig_ship_date,								-- SAP Original Shipdate
	tac_1.tender_orig_ship_date, 							-- Original Tendered Ship Date
    CASE 
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 1 THEN 'Monday'
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 2 THEN 'Tuesday'
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 3 THEN 'Wednesday'
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 4 THEN 'Thursday'
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 5 THEN 'Friday'
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 6 THEN 'Saturday'
        WHEN DATE_FORMAT(tac_1.tender_orig_ship_date ,'u') = 7 THEN 'Sunday'
        ELSE ''
    END 								AS tender_orig_ship_week_day_name,	-- Day of the Week of Original Tendered Shipdate
	tac_1.actual_ship_datetm, 								-- Actual Ship Time
	tac_1.actual_ship_datetm2, 								-- Actual Ship Time - to check
	tac_1.avg_award_weekly_vol_qty,							-- Average Awarded Weekly Volume
	CASE
        WHEN tac_1.alloc_type_code = 'Weekly'
            THEN (CAST(tac_1.avg_award_weekly_vol_qty AS DECIMAL(30,4)) / 7)
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 7 --gi_ship_week_day_name = 'Sunday'
            THEN tac_1.sun_max_load_qty
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 1 --gi_ship_week_day_name = 'Monday'
            THEN tac_1.mon_max_load_qty
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 2 --gi_ship_week_day_name = 'Tuesday'
            THEN tac_1.tue_max_load_qty
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 3 --gi_ship_week_day_name = 'Wdnesday'
            THEN tac_1.wed_max_load_qty
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 4 --gi_ship_week_day_name = 'Thursday'
            THEN tac_1.thu_max_load_qty
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 5 --gi_ship_week_day_name = 'Friday'
            THEN tac_1.fri_max_load_qty
        WHEN tac_1.alloc_type_code = 'Daily' AND DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 6 --gi_ship_week_day_name = 'Saturday'
            THEN tac_1.sat_max_load_qty
        ELSE NULL
    END 								AS daily_award_qty, 				-- Daily Award
    CASE 
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 1 THEN 'Monday'
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 2 THEN 'Tuesday'
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 3 THEN 'Wednesday'
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 4 THEN 'Thursday'
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 5 THEN 'Friday'
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 6 THEN 'Saturday'
        WHEN DATE_FORMAT(tac_1.act_goods_issue_date ,'u') = 7 THEN 'Sunday'
        ELSE ''
    END 								AS actual_ship_week_day_name,		-- Day of the Week
	tac_1.alloc_type_code,									-- ALLOCATION TYPE
	tac_1.sun_max_load_qty,									-- SUN MAX (LOADS)
	tac_1.mon_max_load_qty,									-- MON MAX (LOADS)
	tac_1.tue_max_load_qty,									-- TUE MAX (LOADS)
	tac_1.wed_max_load_qty,									-- WED MAX (LOADS)
	tac_1.thu_max_load_qty,									-- THU MAX (LOADS)
	tac_1.fri_max_load_qty,									-- FRI MAX (LOADS)
	tac_1.sat_max_load_qty,									-- SAT MAX (LOADS)
	tac_1.final_stop_postal_code,							-- Postal Code Final Stop
	tac_1.country_from_code,								-- Country From
	CASE
		WHEN LENGTH(vfr.cntry_to_code) > 1 THEN vfr.cntry_to_code
		WHEN LENGTH(final_stop_postal_code) IN (5,9,10) THEN 'US'
		ELSE 'CA'
	END AS country_to_code,									-- Country To
	tac_1.freight_auction_flag,								-- True FA Flag
	tac_1.freight_type_code, 								-- Freight Type
    tac_1.pre_tms_update_flag, 								-- Pre TMS Upgrade Flag 
	CASE
		WHEN historical_data_structure_flag = 0 AND pre_tms_update_flag = 0 THEN 0
		WHEN historical_data_structure_flag = 1 AND pre_tms_update_flag = 0 THEN 1
		ELSE 2
	END AS data_structure_version_num, 						-- Data Structure Version
	tac_1.primary_carr_flag, 								-- Primary Carrier Flag
	CASE
		WHEN tac_1.freight_auction_ind = 'YES' 
			AND tac_1.freight_auction_flag ='N' 
			AND tac_1.primary_carr_flag = '1' 
			AND tac_1.forward_agent_id = (CASE WHEN LENGTH(tac_1.actual_carr_id) = 8 THEN CONCAT('00',tac_1.actual_carr_id) ELSE tac_1.actual_carr_id END)
				THEN tac_1.load_id
		WHEN tac_1.freight_auction_flag = 'Y' THEN null
		ELSE null
	END AS tbpc_no_freight_auction_adjmt_id, 				-- Tendered Back to Primary Carrier with no FA adjustment
	CASE
		WHEN tac_1.freight_auction_flag IS NULL THEN NULL
		WHEN tac_1.freight_auction_flag = '' THEN NULL
		WHEN tac_1.freight_auction_ind = 'YES' 
			AND tac_1.freight_auction_flag ='Y' 
			AND tac_1.primary_carr_flag = '1' 
			AND tac_1.forward_agent_id = (CASE WHEN LENGTH(tac_1.actual_carr_id) = 8 THEN CONCAT('00',tac_1.actual_carr_id) ELSE tac_1.actual_carr_id END)
				THEN tac_1.load_id
		ELSE NULL
	END AS tbpc_freight_auction_adjmt_id, 					-- Tendered Back to Primary Carrier with FA adjustment
	REGEXP_REPLACE(tac_1.tender_reject_load_id, "^0+", '') AS tender_reject_load_id,	-- Tender Rejected Loads
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		WHEN pc656.cust_3_name LIKE '%AB ACQUISITION, LLC%' THEN 'ALBERTSON\'S SAFEWAY'
		WHEN pc656.cust_3_name LIKE '%COSTCO COMPANIES, INC. (WW)%' AND pc656.cust_5_name LIKE '%COSTCO CORP US%' THEN 'COSTCO'
		WHEN pc656.cust_3_name LIKE '%COSTCO COMPANIES, INC. (WW)%' AND pc656.cust_5_name LIKE '%COSTCO WHSL (CA)%' THEN 'COSTCO CANADA'
		WHEN pc656.cust_3_name LIKE '%BJ%S WHOLESALE (WW)%' THEN 'BJ\'S'
		WHEN pc656.cust_3_name LIKE '%CVS CORPORATION US%' THEN 'CVS'
		WHEN pc656.cust_3_name LIKE '%TARGET CORP US%' THEN 'TARGET'
		WHEN pc656.cust_3_name LIKE '%WAL-MART CORP. (WW)%' AND pc656.cust_5_name LIKE 'SAM%' THEN 'SAM\'S'
		WHEN pc656.cust_3_name LIKE '%WAL-MART CORP. (WW)%' AND pc656.cust_12_name LIKE 'SAM%' THEN 'SAM\'S'
		WHEN pc656.cust_3_name LIKE '%WAL-MART CORP. (WW)%' AND pc656.cust_5_name NOT LIKE 'SAM%' THEN 'WALMART'
		WHEN pc656.cust_3_name LIKE '%Null%' THEN tac_1.ship_to_party_desc  -- freight_stats_na_merged_star.ship_to_party_desc
		WHEN pc656.cust_3_name IS NULL THEN tac_1.ship_to_party_desc  -- freight_stats_na_merged_star.ship_to_party_desc
		WHEN pc656.cust_3_name LIKE '%THE KROGER CO. US%' THEN 'KROGER'
		WHEN pc656.cust_3_name LIKE '%UNKN LVL 3 UNKNOWN CUSTOMER%' THEN tac_1.ship_to_party_desc  -- freight_stats_na_merged_star.ship_to_party_desc
		ELSE REGEXP_REPLACE(pc656.cust_3_name,'\(WW\)','')
	END AS customer_code, 								     -- Customer
	
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_1_id, c656.cust1_id)
	END AS customer_lvl1_code, 								-- Customer Level 1 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_1_name, c656.cust1_name)
	END AS customer_lvl1_desc, 								-- Customer Level 1 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_2_id, c656.cust2_id)
	END AS customer_lvl2_code, 								-- Customer Level 2 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_2_name, c656.cust2_name)
	END AS customer_lvl2_desc, 								-- Customer Level 2 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_3_id, c656.cust3_id)
	END AS customer_lvl3_code, 								-- Customer Level 3 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_3_name, c656.cust3_name)
	END AS customer_lvl3_desc, 								-- Customer Level 3 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_4_id, c656.cust4_id)
	END AS customer_lvl4_code, 								-- Customer Level 4 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_4_name, c656.cust4_name)
	END AS customer_lvl4_desc, 								-- Customer Level 4 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_5_id, c656.cust5_id)
	END AS customer_lvl5_code, 								-- Customer Level 5 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_5_name, c656.cust5_name)
	END AS customer_lvl5_desc, 								-- Customer Level 5 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_6_id, c656.cust6_id)
	END AS customer_lvl6_code, 								-- Customer Level 6 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_6_name, c656.cust6_name)
	END AS customer_lvl6_desc, 								-- Customer Level 6 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_7_id, c656.cust7_id)
	END AS customer_lvl7_code, 								-- Customer Level 7 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_7_name, c656.cust7_name)
	END AS customer_lvl7_desc, 								-- Customer Level 7 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_8_id, c656.cust8_id)
	END AS customer_lvl8_code, 								-- Customer Level 8 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_8_name, c656.cust8_name)
	END AS customer_lvl8_desc, 								-- Customer Level 8 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_9_id, c656.cust9_id)
	END AS customer_lvl9_code, 								-- Customer Level 9 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_9_name, c656.cust9_name)
	END AS customer_lvl9_desc, 								-- Customer Level 9 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_10_id, c656.cust10_id)
	END AS customer_lvl10_code, 							-- Customer Level 10 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_10_name, c656.cust10_name)
	END AS customer_lvl10_desc, 							-- Customer Level 10 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_11_id, c656.cust11_id)
	END AS customer_lvl11_code, 							-- Customer Level 11 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_11_name, c656.cust11_name)
	END AS customer_lvl11_desc, 							-- Customer Level 11 Description
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_12_id, c656.cust12_id)
	END AS customer_lvl12_code, 							-- Customer Level 12 ID
	CASE
		WHEN tac_1.freight_type_code = 'INTERPLANT' THEN dst.dest_ship_from_code
		ELSE COALESCE(pc656.cust_12_name, c656.cust12_name)
	END AS customer_lvl12_desc, 							-- Customer Level 12 Description
	REGEXP_REPLACE(tac_1.actual_carr_id, "^0+", '') AS actual_carr_id,	-- Actual Carrier ID
	vmd.carr1_name 						AS actual_carr_desc,				-- Actual Carrier Description
	CAST(NVL(tmp_tbl_costs.actual_carr_trans_cost_amt, 0) AS DECIMAL(38,16))       AS actual_carr_trans_cost_amt,		    -- Actual Carrier Total Transportation Cost USD
	CAST(NVL(tmp_tbl_costs.linehaul, 0)     AS DECIMAL(38,16))                     AS linehaul_cost_amt,					-- Linehaul
	CAST(NVL(tmp_tbl_costs.incrmtl_freight_auction_cost_amt, 0) AS DECIMAL(38,16)) AS incrmtl_freight_auction_cost_amt,	    -- Incremental FA
	CAST(NVL(tmp_tbl_costs.cnc_carr_mix_cost_amt, 0) AS DECIMAL(38,16))            AS cnc_carr_mix_cost_amt,				-- CNC_Carrier Mix
	CAST(NVL(tmp_tbl_costs.unsourced, 0) AS DECIMAL(38,16))                        AS unsource_cost_amt,					-- Unsourced
	CAST(NVL(tmp_tbl_costs.fuel, 0) AS DECIMAL(38,16))                             AS fuel_cost_amt,						-- Fuel
	CAST(NVL(tmp_tbl_costs.actual_carr_trans_cost_amt, 0)
       - NVL(tmp_tbl_costs.linehaul, 0)
       - NVL(tmp_tbl_costs.incrmtl_freight_auction_cost_amt, 0)
       - NVL(tmp_tbl_costs.cnc_carr_mix_cost_amt, 0)
       - NVL(tmp_tbl_costs.unsourced, 0)
       - NVL(tmp_tbl_costs.fuel, 0) AS DECIMAL(38,16))  AS accessorial_cost_amt,		 -- Accessorial
    CAST(sss.appliance_subsector_step_cnt AS FLOAT) AS appliance_subsector_step_cnt, -- APPLIANCES
    CAST(sss.baby_care_subsector_step_cnt AS FLOAT) AS baby_care_subsector_step_cnt, -- BABY CARE
    CAST(sss.chemical_subsector_step_cnt  AS FLOAT) AS chemical_subsector_step_cnt,  -- CHEMICALS
    CAST(sss.fabric_subsector_step_cnt    AS FLOAT) AS fabric_subsector_step_cnt,    -- FABRIC CARE
    CAST(sss.family_subsector_step_cnt    AS FLOAT) AS family_subsector_step_cnt,    -- FAMILY CARE
    CAST(sss.fem_subsector_step_cnt       AS FLOAT) AS fem_subsector_step_cnt,       -- FEM CARE
    CAST(sss.hair_subsector_step_cnt      AS FLOAT) AS hair_subsector_step_cnt,      -- HAIR CARE
    CAST(sss.home_subsector_step_cnt      AS FLOAT) AS home_subsector_step_cnt,      -- HOME CARE
    CAST(sss.oral_subsector_step_cnt      AS FLOAT) AS oral_subsector_step_cnt,      -- ORAL CARE
    CAST(sss.phc_subsector_step_cnt       AS FLOAT) AS phc_subsector_step_cnt,       -- PHC
    CAST(sss.shave_subsector_step_cnt     AS FLOAT) AS shave_subsector_step_cnt,     -- SHAVE CARE
    CAST(sss.skin_subsector_cnt           AS FLOAT) AS skin_subsector_cnt,			  -- SKIN & PERSONAL CARE
	CAST(sss.other_subsector_cnt          AS FLOAT) AS other_subsector_cnt,		  -- OTHER
	dst.customer_desc,										-- Customer ID Description
	CONCAT(tac_1.act_goods_issue_date, ' ', SUBSTR(tac_1.actual_ship_datetm, 1, 2), ':', SUBSTR(tac_1.actual_ship_datetm, 3, 2), ':', SUBSTR(tac_1.actual_ship_datetm, 5, 2)) AS actual_ship_tmstp,	-- Actual Ship Date and Time
	CONCAT(tac_1.act_goods_issue_date, ' ', SUBSTR(tac_1.actual_ship_datetm2, 1, 2), ':', SUBSTR(tac_1.actual_ship_datetm2, 3, 2), ':', SUBSTR(tac_1.actual_ship_datetm2, 5, 2)) AS actual_ship_tmstp2,	-- Actual Ship Date and Time - to check
	CASE
		WHEN tac_1.tender_event_type_code = 'TENDACC' AND tac_1.freight_auction_flag = 'N' THEN REGEXP_REPLACE(tac_1.load_id, "^0+", '')
		ELSE ''
	END AS tender_accept_load_with_no_fa, 					-- Tender Accepted Loads with no FA
	CASE
		WHEN tac_1.tender_event_type_code = 'TENDACC' AND tac_1.freight_auction_flag = 'Y' THEN REGEXP_REPLACE(tac_1.load_id, "^0+", '')
		ELSE ''
	END AS tender_accept_load_with_fa, 						-- Tender Accepted Loads with FA
	tac_1.tfts_load_tmstp AS tfts_load_tmstp,
	tac_1.load_from_file_name,
	tac_1.bd_mod_tmstp,
	tac_1.historical_data_structure_flag,
	ROW_NUMBER() OVER ( PARTITION BY 	 tac_1.load_id
										,tac_1.ship_to_party_id      
										,tac_1.forward_agent_id        
										,tac_1.tender_event_type_code
										,tac_1.tender_reason_code     
										,tac_1.tender_date
										,tac_1.tender_datetm         
										,tac_1.carr_mode_code        
										,tac_1.origin_zone_ship_from_code --origin_zone_code       
										,tac_1.service_tms_code
						ORDER BY tac_1.historical_data_structure_flag
	) AS row_num

FROM tmp_tbl_tac_technical_names_1_1 AS tac_1

	LEFT JOIN tmp_tbl_cust_656_postal_codes   AS pc656
		ON tac_1.ship_to_party_id = pc656.ship_to_party_id

	LEFT JOIN tmp_tbl_cust_656 AS c656
		ON c656.customer_id = pc656.ship_to_party_id

	LEFT JOIN tmp_tbl_distinct_otd_vfr AS vfr
		ON vfr.shpmt_id = tac_1.load_id

    LEFT JOIN tmp_tbl_subsector_sum_step AS sss
        ON tac_1.load_id = sss.shpmt_id

    LEFT JOIN tmp_tbl_costs 
        ON tac_1.load_id = tmp_tbl_costs.shpmt_id 
			AND tac_1.freight_auction_flag = tmp_tbl_costs.freight_auction_flag

	LEFT JOIN tmp_tbl_distinct_vendor_1 AS vmd2
		ON tac_1.forward_agent_id = vmd2.vendor_account_num

	LEFT JOIN tmp_tbl_dest AS dst
		ON tac_1.ship_to_party_id       = dst.ship_to_party_id
			AND tac_1.load_id           = dst.load_id
			AND tac_1.freight_type_code = dst.freight_type_code

	LEFT JOIN tmp_tbl_distinct_vendor_2 AS vmd
		ON REGEXP_REPLACE(tac_1.actual_carr_id, "^0+", '') = vmd.vendor_account_num

;

INSERT OVERWRITE TABLE tac_technical_name_star
SELECT 
    tac_1.load_id,
	tac_1.cal_month_code,									-- Calendar Year/Month
	tac_1.ship_to_party_id,                 				-- Customer ID & Destination Location ID !!!
	tac_1.operational_freight_type_code,					-- Operational Freight Type
	tac_1.ship_cond_val,									-- Shipping conditions
	tac_1.sold_to_party_id,									-- Sold To #
	tac_1.schedule_date,									-- Scheduled Date
	tac_1.schedule_datetm,									-- Scheduled Time
	tac_1.service_tms_code,									-- TMS Service Code
	tac_1.trans_plan_point_code, 							-- Transportation planning point
	tac_1.gbu_per_ship_site_name, 							-- GBU per Shipping Site
	REGEXP_REPLACE(tac_1.forward_agent_id, "^0+", '') AS forward_agent_id,		-- Carrier ID
	tac_1.tender_event_type_code,							-- Tender Event Type Code
	tac_1.tender_reason_code,								-- Tender Reason Code
	tac_1.tender_date,										-- Tender Event Date
	tac_1.tender_datetm,									-- Tender Event Time
	tac_1.carr_mode_code,									-- Mode
    tac_1.origin_zone_ship_from_code,						-- Origin SF
	tac_1.deprtre_cntry_code, 								-- Departure Country Code
	tac_1.act_goods_issue_date,  							-- Actual Ship Date
	tac_1.actual_goods_issue_datetm,
	tac_1.day_in_week_qty,									-- No.of Days in week
	tac_1.day_in_month_qty,									-- No.of Days in Month
	tac_1.scac_id,											-- SCAC
	tac_1.carr_mode_desc,									-- Carrier Mode Description
	tac_1.tariff_id,										-- Tariff ID
	tac_1.schedule_code,									-- Schedule ID
	tac_1.tender_first_carr_desc,							-- Tender Event Type Description
	tac_1.tender_acptn_id,									-- Tender Acceptance Key
	tac_1.tender_reason_code_desc,							-- Tender Reason Code Description
	tac_1.cal_week_code,									-- Calendar Year/Week
	tac_1.postal_code,										-- Postal Code Raw TMS
	tac_1.freight_auction_ind,								-- Freight Auction Flag
	tac_1.ship_to_party_desc,
    tac_1.origin_location_code,     						-- Origin Location ID
    tac_1.origin_zone_code,           						-- Origin Zone
	tac_1.dest_ship_from_code,  							-- Destination SF
    tac_1.dest_zone_code,									-- Destination Zone
	tac_1.carr_desc,										-- Carrier Description
	tac_1.tender_event_datetm,								-- Tender Event Date & Time
	tac_1.sap_orig_ship_date,								-- SAP Original Shipdate
	tac_1.tender_orig_ship_date, 							-- Original Tendered Ship Date
    tac_1.tender_orig_ship_week_day_name,					-- Day of the Week of Original Tendered Shipdate
	tac_1.actual_ship_datetm, 								-- Actual Ship Time
	tac_1.avg_award_weekly_vol_qty,							-- Average Awarded Weekly Volume
	tac_1.daily_award_qty, 									-- Daily Award
    tac_1.actual_ship_week_day_name,						-- Day of the Week
	tac_1.alloc_type_code,									-- ALLOCATION TYPE
	tac_1.sun_max_load_qty,									-- SUN MAX (LOADS)
	tac_1.mon_max_load_qty,									-- MON MAX (LOADS)
	tac_1.tue_max_load_qty,									-- TUE MAX (LOADS)
	tac_1.wed_max_load_qty,									-- WED MAX (LOADS)
	tac_1.thu_max_load_qty,									-- THU MAX (LOADS)
	tac_1.fri_max_load_qty,									-- FRI MAX (LOADS)
	tac_1.sat_max_load_qty,									-- SAT MAX (LOADS)
	tac_1.final_stop_postal_code,							-- Postal Code Final Stop
	tac_1.country_from_code,								-- Country From
	tac_1.country_to_code,									-- Country To
	tac_1.freight_auction_flag,								-- True FA Flag
	tac_1.freight_type_code, 								-- Freight Type
    tac_1.pre_tms_update_flag, 								-- Pre TMS Upgrade Flag 
	tac_1.data_structure_version_num, 						-- Data Structure Version
	tac_1.primary_carr_flag, 								-- Primary Carrier Flag
	tac_1.tbpc_no_freight_auction_adjmt_id, 				-- Tendered Back to Primary Carrier with no FA adjustment
	tac_1.tbpc_freight_auction_adjmt_id, 					-- Tendered Back to Primary Carrier with FA adjustment
	tac_1.tender_reject_load_id,							-- Tender Rejected Loads
	CASE
		WHEN tac_1.tender_event_type_code = 'TENDFRST' THEN tac_1.tender_event_datetm
		ELSE prev_tend.prev_tender_datetm 
	END AS prev_tender_datetm,		                        -- Previous Tender Date & Time
	prev_tend.time_between_tender_val, 						-- Time Between Tender Events
	CASE
		WHEN tac_1.tender_event_type_code = 'TENDCNCL' AND CAST(SUBSTR(prev_tend.time_between_tender_val,1,2) AS INT) >= 2 THEN tac_1.load_id
		ELSE NULL
	END AS cancel_no_response_id, 							-- Canceled Due To No Response
	tac_1.customer_code, 									-- Customer
	tac_1.customer_lvl1_code, 								-- Customer Level 1 ID
	tac_1.customer_lvl1_desc, 								-- Customer Level 1 Description
	tac_1.customer_lvl2_code, 								-- Customer Level 2 ID
	tac_1.customer_lvl2_desc, 								-- Customer Level 2 Description
	tac_1.customer_lvl3_code, 								-- Customer Level 3 ID
	tac_1.customer_lvl3_desc, 								-- Customer Level 3 Description
	tac_1.customer_lvl4_code, 								-- Customer Level 4 ID
	tac_1.customer_lvl4_desc, 								-- Customer Level 4 Description
	tac_1.customer_lvl5_code, 								-- Customer Level 5 ID
	tac_1.customer_lvl5_desc, 								-- Customer Level 5 Description
	tac_1.customer_lvl6_code, 								-- Customer Level 6 ID
	tac_1.customer_lvl6_desc, 								-- Customer Level 6 Description
	tac_1.customer_lvl7_code, 								-- Customer Level 7 ID
	tac_1.customer_lvl7_desc, 								-- Customer Level 7 Description
	tac_1.customer_lvl8_code, 								-- Customer Level 8 ID
	tac_1.customer_lvl8_desc, 								-- Customer Level 8 Description
	tac_1.customer_lvl9_code, 								-- Customer Level 9 ID
	tac_1.customer_lvl9_desc, 								-- Customer Level 9 Description
	tac_1.customer_lvl10_code, 								-- Customer Level 10 ID
	tac_1.customer_lvl10_desc, 								-- Customer Level 10 Description
	tac_1.customer_lvl11_code, 								-- Customer Level 11 ID
	tac_1.customer_lvl11_desc, 								-- Customer Level 11 Description
	tac_1.customer_lvl12_code, 								-- Customer Level 12 ID
	tac_1.customer_lvl12_desc, 								-- Customer Level 12 Description
	tac_1.actual_carr_id, 									-- Actual Carrier ID
	tac_1.actual_carr_desc,									-- Actual Carrier Description
	tac_1.actual_carr_trans_cost_amt,		    			-- Actual Carrier Total Transportation Cost USD
	tac_1.linehaul_cost_amt,								-- Linehaul
	tac_1.incrmtl_freight_auction_cost_amt,	    			-- Incremental FA
	tac_1.cnc_carr_mix_cost_amt,							-- CNC_Carrier Mix
	tac_1.unsource_cost_amt,								-- Unsourced
	tac_1.fuel_cost_amt,									-- Fuel
	tac_1.accessorial_cost_amt,		 						-- Accessorial
    tac_1.appliance_subsector_step_cnt, 					-- APPLIANCES
    tac_1.baby_care_subsector_step_cnt, 					-- BABY CARE
    tac_1.chemical_subsector_step_cnt,  					-- CHEMICALS
    tac_1.fabric_subsector_step_cnt,    					-- FABRIC CARE
    tac_1.family_subsector_step_cnt,    					-- FAMILY CARE
    tac_1.fem_subsector_step_cnt,       					-- FEM CARE
    tac_1.hair_subsector_step_cnt,      					-- HAIR CARE
    tac_1.home_subsector_step_cnt,      					-- HOME CARE
    tac_1.oral_subsector_step_cnt,      					-- ORAL CARE
    tac_1.phc_subsector_step_cnt,       					-- PHC
    tac_1.shave_subsector_step_cnt,     					-- SHAVE CARE
    tac_1.skin_subsector_cnt,			  					-- SKIN & PERSONAL CARE
	tac_1.other_subsector_cnt,		  						-- OTHER
	tac_1.customer_desc,									-- Customer ID Description
	tac_1.actual_ship_tmstp,								-- Actual Ship Date and Time
	tac_1.tender_accept_load_with_no_fa, 					-- Tender Accepted Loads with no FA
	tac_1.tender_accept_load_with_fa, 						-- Tender Accepted Loads with FA
	tac_1.tfts_load_tmstp,
	tac_1.load_from_file_name,
	tac_1.bd_mod_tmstp,
	tac_1.historical_data_structure_flag

FROM tmp_tbl_tac_technical_names_2_1 AS tac_1

	LEFT JOIN tmp_tbl_prev_tender_datetm AS prev_tend
		ON prev_tend.load_id                         = tac_1.load_id
			AND prev_tend.tender_event_datetm        = tac_1.tender_event_datetm
--			AND prev_tend.tender_event_type_code     = tac_1.tender_event_type_code
--			AND prev_tend.ship_to_party_id           = tac_1.ship_to_party_id
--			AND prev_tend.forward_agent_id           = tac_1.forward_agent_id
--			AND prev_tend.tender_reason_code         = tac_1.tender_reason_code
--			AND prev_tend.carr_mode_code             = tac_1.carr_mode_code
--			AND prev_tend.origin_zone_ship_from_code = tac_1.origin_zone_ship_from_code
--			AND prev_tend.service_tms_code           = tac_1.service_tms_code

WHERE tac_1.row_num = 1		
;
