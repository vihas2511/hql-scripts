USE ${hivevar:database};

INSERT OVERWRITE TABLE tfs_acsrl_star
SELECT DISTINCT
       `load id`                                              AS load_id
     , `freight cost charge`                                  AS freight_cost_charge
     , `tdc val code`                                         AS tdc_val_code
     , `tdc val description`                                  AS tdc_val_description
     , `tms freight charge description`                       AS tms_freight_charge_description
     , `freight type`                                         AS freight_type
     , `accessorial reason`                                   AS accessorial_reason
     , CAST(`total transportation cost usd` as DECIMAL(30,4)) AS total_accessorial_cost_usd
     , `origin zone`                                          AS origin_zone
     , `destination zone`                                     AS destination_zone
     , `customer description`                                 AS customer_description
     , `carrier description`                                  AS carrier_description
     , `actual gi date`                                       AS actual_gi_date
     , `country to`                                           AS country_to
     , `subsector description`                                AS subsector
     , `country to description`                               AS country_to_description
     , `goods receipt posting date`                           AS goods_receipt_posting_date
     , `created date`                                         AS created_date
     , `ship to description`                                  AS ship_to_description
     , `freight auction`                                      AS freight_auction
     , `historical data structure flag`                       AS historical_data_structure_flag
     , `destination postal`                                   AS destination_postal
     , `customer l8`                                          AS customer_l8
     , `truckload_vs_intermodal_val`                          AS truckload_vs_intermodal
     , `origin sf`                                            AS origin_sf
     , `destination sf`                                       AS destination_sf
     , REGEXP_REPLACE(REGEXP_REPLACE(`subsector description`,' SUB-SECTOR',''),' SUBSECTOR','') AS subsector_update
     , CASE
              WHEN MONTH(TO_DATE(`actual gi date`)) < 7
                     THEN CONCAT('FY',SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`))-1 AS string),LENGTH(CAST(YEAR(TO_DATE(`actual gi date`))-1 AS string))-1),SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`)) AS string),LENGTH(CAST(YEAR(TO_DATE(`actual gi date`)) AS string))-1))
                     ELSE CONCAT('FY',SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`)) AS string),LENGTH(CAST(YEAR(TO_DATE(`actual gi date`)) AS string))-1),SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`))+1 AS string),LENGTH(CAST(YEAR(TO_DATE(`actual gi date`))+1 AS string))-1))
       END AS FY
FROM
     tfs
WHERE
       TO_DATE(`actual gi date`) <= DATE_SUB(CURRENT_TIMESTAMP(), 14)
       AND `freight cost charge` NOT IN ('FA_A'
                                   ,'FSUR'
                                   , 'FU_S'
                                   , 'FLTL'
                                   , 'FFLT'
                                   , 'FCHG'
                                   , 'FUSU'
                                   , 'DST'
                                   , 'SCS'
                                   , 'FTP'
                                   , 'TSC'
                                   , 'DIST'
                                   , 'DLTL'
                                   , 'FLAT'
                                   , 'SPOT'
								   ,'CVYI'
								   ,'HJBT'
								   ,'KNIG'
								   ,'L2D'
								   ,'SCNN'
								   ,'UFLB'
								   ,'USXI'
								   ,'PGLI')
;
