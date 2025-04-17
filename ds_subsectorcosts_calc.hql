
USE ${hivevar:database};

Drop TABLE IF EXISTS ds_subsectorcosts_max
;

CREATE TEMPORARY VIEW ds_subsectorcosts_max as
SELECT
         subsector
       , voucher_id
       , MAX(mx.origin_zone)                    AS origin_zone
       , MAX(mx.destination_zone)               AS destination_zone
       , MAX(mx.customer_description)           AS customer_description
       , MAX(mx.carrier_description)            AS carrier_description
       , MAX(mx.actual_gi_date)                 AS actual_gi_date
       , MAX(mx.goods_receipt_posting_date)     AS goods_receipt_posting_date
       , MAX(mx.created_date)                   AS created_date
       , MAX(mx.ship_to_id)                     AS ship_to_id
       , MAX(mx.freight_auction)                AS freight_auction
       , MAX(mx.historical_data_structure_flag) AS historical_data_structure_flag
       , MAX(mx.destination_postal)             AS destination_postal
       , MAX(mx.service_tms_code)               AS service_tms_code
       , MAX(mx.customer_l8)                    AS customer_l8
       , MAX(mx.ship_to_description)            AS ship_to_description
       , MAX(mx.distance_per_load)              AS distance_per_load
       , MAX(mx.origin_sf)                      AS origin_sf
       , MAX(mx.destination_sf)                 AS destination_sf
       , MAX(mx.truckload_vs_intermodal_val)    AS truckload_vs_intermodal_val
FROM
         (
                  SELECT
                           `load id` AS load_id
                         , case
                                    when `origin zone` = "SF_EDWARDSVILLE"
                                             then "SF_ST_LOUIS"
                                             else 'origin zone'
                           end as origin_zone
                         , case
                                    when `destination zone` = "SF_EDWARDSVILLE"
                                             then "SF_ST_LOUIS"
                                             else 'destination zone'
                           end                              as destination_zone
                         , `customer description`           as customer_description
                         , `carrier description`            as carrier_description
                         , max(`actual gi date`)            as actual_gi_date
                         , `country to`                     as country_to
                         , `subsector description`          as subsector
                         , `country to description`         as country_to_description
                         , `goods receipt posting date`     as goods_receipt_posting_date
                         , max(`created date`)              as created_date
                         , max(`ship to #`)                 as ship_to_id
                         , max(`ship to description`)       as ship_to_description
                         , `freight auction`                as freight_auction
                         , `historical data structure flag` as historical_data_structure_flag
                         , `destination postal`             as destination_postal
                         , `customer l8`                    as customer_l8
                         , truckload_vs_intermodal_val
                         , `distance per load`   as distance_per_load
                         , `service tms code`    as service_tms_code
                         , `voucher id`          as voucher_id
                         , min(`origin sf`)      as origin_sf
                         , min(`destination sf`) as destination_sf

                  FROM tfs

                  GROUP BY
                           `load id`
                         , `origin zone`
                         , `destination zone`
                         , `customer description`
                         , `carrier description`
                         , `country to`
                         , `subsector description`
                         , `country to description`
                         , `goods receipt posting date`
                         , `freight auction`
                         , `historical data structure flag`
                         , `destination postal`
                         , `customer l8`
                         , truckload_vs_intermodal_val
                         , `distance per load`
                         , `freight type`
                         , `service tms code`
                         , `voucher id`
         )
         mx
GROUP BY
         subsector
       , voucher_id
;


Drop TABLE IF EXISTS ds_subsectorcosts_noloadid_v13
;

CREATE TEMPORARY VIEW ds_subsectorcosts_noloadid_v13 as
SELECT
       load_id
     , voucher_id
     , service_tms_code
     , freight_type
     , country_to
     , dltl
     , dst
     , fuel
     , exm
     , lh_rules
     , weighted_average_rate_adjusted_long
     , accrual_cost_adjusted
     , accessorial_costs
     , weighted_average_rate_adjusted
     , distance_per_load_dec
     , unsourced_costs
     , line_haul_final
     , total_cost
     , origin_zone
     , customer_description
     , carrier_description
     , actual_gi_date
     , goods_receipt_posting_date
     , created_date
     , ship_to_id
     , freight_auction
     , historical_data_structure_flag
     , destination_postal
     , origin_sf
     , destination_sf
     , truckload_vs_intermodal_val
     , subsector_update AS subsector
     , fy
     , su_per_load
     , scs
     , ftp
     , tsc
     , flat
     , fa_a
     , steps_ratio_dec
     , cnc_costs
     , freight_auction_costs
     , country_to_description
     , customer_l8
     , distance_per_load
     , ship_to_description
     , destination_zone_new
FROM
       (
                 SELECT
                           null_final.load_id                                                                    AS load_id
                         , null_final.voucher_id                                                                 AS voucher_id
                         , null_final.subsector                                                                  AS subsector
                         , null_final.service_tms_code                                                           AS service_tms_code
                         , null_final.freight_type                                                               AS freight_type
                         , null_final.country_to                                                                 AS country_to
                         , null_final.dltl                                                                       AS dltl
                         , null_final.dst                                                                        AS dst
                         , null_final.fuel                                                                       AS fuel
                         , null_final.exm                                                                        AS exm
                         , null_final.lh_rules                                                                   AS lh_rules
                         , null_final.weighted_average_rate_adjusted_long                                        AS weighted_average_rate_adjusted_long
                         , null_final.accrual_cost_adjusted                                                      AS accrual_cost_adjusted
                         , null_final.accessorial_costs                                                          AS accessorial_costs
                         , null_final.weighted_average_rate_adjusted                                             AS weighted_average_rate_adjusted
                         , null_final.distance_per_load_dec                                                      AS distance_per_load_dec
                         , null_final.unsourced_costs                                                            AS unsourced_costs
                         , null_final.line_haul_final                                                            AS line_haul_final
                         , null_final.total_cost                                                                 AS total_cost
                         , tfs_max.origin_zone                                                                   AS origin_zone
                         , tfs_max.destination_zone                                                              AS destination_zone
                         , tfs_max.customer_description                                                          AS customer_description
                         , tfs_max.carrier_description                                                           AS carrier_description
                         , tfs_max.actual_gi_date                                                                AS actual_gi_date
                         , tfs_max.goods_receipt_posting_date                                                    AS goods_receipt_posting_date
                         , tfs_max.created_date                                                                  AS created_date
                         , tfs_max.ship_to_id                                                                    AS ship_to_id
                         , tfs_max.freight_auction                                                               AS freight_auction
                         , tfs_max.historical_data_structure_flag                                                AS historical_data_structure_flag
                         , tfs_max.destination_postal                                                            AS destination_postal
                         , tfs_max.origin_sf                                                                     AS origin_sf
                         , tfs_max.destination_sf                                                                AS destination_sf
                         , tfs_max.truckload_vs_intermodal_val                                                   AS truckload_vs_intermodal_val
                         , regexp_replace(regexp_replace(null_final.subsector,' SUB-SECTOR',''),' SUBSECTOR','') AS subsector_update
                         , case
                                     WHEN MONTH(TO_DATE(actual_gi_date)) < 7
                                               THEN CONCAT('FY',SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date))-1 AS STRING),-2,2),SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date)) AS STRING),-2,2))
                                               ELSE CONCAT('FY',SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date)) AS STRING),-2,2),SUBSTR(CAST(YEAR(TO_DATE(actual_gi_date))+1 AS STRING),-2,2))
                           END AS FY
                         , 0   AS su_per_load
                         , 0   AS scs
                         , 0   AS ftp
                         , 0   AS tsc
                         , 0   AS flat
                         , 0   AS fa_a
                         , 0   AS steps_ratio_dec
                         , 0   AS cnc_costs
                         , 0   AS freight_auction_costs
                         , ""  AS country_to_description
                         , ""  AS customer_l8
                         , 0   AS distance_per_load
                         , ""  AS ship_to_description
                         , ""  AS destination_zone_new
                 FROM
                           (
                                  SELECT *
                                       , (COALESCE(Line_Haul_Final,0) + COALESCE(accessorials,0)+ COALESCE(fuel,0) + COALESCE(Unsourced_Costs,0)) AS Total_Cost
                                  FROM
                                         (
                                                SELECT *
                                                     , CASE
                                                              WHEN LH_Rules = 0
                                                                     THEN COALESCE(dst,0)+COALESCE(exm,0)+COALESCE(dltl,0)
                                                                     ELSE 0
                                                       END AS Unsourced_Costs
                                                     , CASE
                                                              WHEN LH_Rules = 1
                                                                     THEN COALESCE(dst,0)+COALESCE(exm,0)+COALESCE(dltl,0)
                                                                     ELSE 0
                                                       END AS Line_Haul_Final
                                                FROM
                                                       (
                                                              SELECT *
                                                                   , CASE
                                                                            WHEN load_id = "MISSING"
                                                                                   THEN 1
                                                                            WHEN freight_type           = "Customer"
                                                                                   AND service_tms_code = "DSD"
                                                                                   THEN 1
                                                                            WHEN service_tms_code IS NULL
                                                                                   OR service_tms_code IN ("Null"
                                                                                                         , ""
                                                                                                         , "HCPU"
                                                                                                         , "LTL"
                                                                                                         , "LTL7"
                                                                                                         , "MES6"
                                                                                                         , "METL"
                                                                                                         , "TLOC")
                                                                                   THEN 1
                                                                            WHEN country_to = "MX"
                                                                                   THEN 1
                                                                            WHEN weighted_average_rate IS NULL
                                                                                   THEN 0
                                                                            WHEN weighted_average_rate = 0
                                                                                   THEN 1
                                                                            WHEN weighted_average_rate > 0
                                                                                   THEN 2
                                                                                   ELSE 3
                                                                     END                                                       AS LH_Rules
                                                                   , (weighted_average_rate/cost_ratio)                        AS weighted_average_rate_adjusted_long
                                                                   , CAST((weighted_average_rate/cost_ratio) AS DECIMAL(30,8)) AS weighted_average_rate_adjusted
                                                                   , (accrual_cost /cost_ratio)                                AS accrual_cost_adjusted
                                                                   , COALESCE(accessorials, 0.0000)                            AS accessorial_costs
                                                                   , CAST(distance_per_load AS DECIMAL(30,8))                  AS distance_per_load_dec
                                                              FROM
                                                                     (
                                                                               SELECT
                                                                                         null_orig_dest_tfs.load_id                                 AS load_id
                                                                                       , null_orig_dest_tfs.voucher_id                              AS voucher_id
                                                                                       , null_orig_dest_tfs.subsector                               AS subsector
                                                                                       , null_orig_dest_tfs.cost_ratio                              AS cost_ratio
                                                                                       , null_orig_dest_tfs.accrual_cost                            AS accrual_cost
                                                                                       , null_orig_dest_tfs.service_tms_code                        AS service_tms_code
                                                                                       , null_orig_dest_tfs.freight_type                            AS freight_type
                                                                                       , null_orig_dest_tfs.country_to                              AS country_to
                                                                                       , null_orig_dest_tfs.distance_per_load                       AS distance_per_load

                                                                                       --, null_orig_dest_tfs.destination_postal                      AS destination_postal
                                                                                       --, null_orig_dest_tfs.actual_gi_date                          AS actual_gi_date
                                                                                       --, null_orig_dest_tfs.delivery_id                             AS delivery_id
                                                                                       --, null_orig_dest_tfs.tdc_val_code                            AS tdc_val_code
                                                                                       --, null_orig_dest_tfs.freight_cost_charge                     AS freight_cost_charge
                                                                                       --, null_orig_dest_tfs.total_cost_usd                          AS total_cost_usd
                                                                                       --, null_orig_dest_tfs.total_cost_local                        AS total_cost_local
                                                                                       --, null_orig_dest_tfs.su                                      AS su
                                                                                       --, null_orig_dest_tfs.steps                                   AS steps
                                                                                       --, null_orig_dest_tfs.cost_grouping                           AS cost_grouping
                                                                                       --, null_orig_dest_tfs.weighted_average_rate_old               AS weighted_average_rate_old
                                                                                       --, null_orig_dest_tfs.shipping_point_code                     AS shipping_point_code
                                                                                       --, null_orig_dest_tfs.destination_zone_new                    AS destination_zone_new
                                                                                       --, null_orig_dest_tfs.origin_zone                             AS origin_zone
                                                                                       --, tfs_hist.weighted_average_rate_hist                        AS weighted_average_rate_hist
                                                                                       --, CAST(tfs_hist.weighted_average_rate_hist AS DECIMAL(30,8)) AS war_dec
                                                                                       -- ??? ADDED MAX(...
                                                                                       , MAX(CASE
                                                                                                   WHEN TO_DATE(actual_gi_date)                    >= '2016-11-01'
                                                                                                             or weighted_average_rate_old IS NOT NULL
                                                                                                             THEN weighted_average_rate_old
                                                                                                   WHEN CAST(tfs_hist.weighted_average_rate_hist AS DECIMAL(30,8)) IS NULL
                                                                                                             THEN weighted_average_rate_old
                                                                                                             ELSE CAST(tfs_hist.weighted_average_rate_hist AS DECIMAL(30,8))
                                                                                         END) AS weighted_average_rate
                                                                                       , SUM
                                                                                                   (
                                                                                                             CASE
                                                                                                                       WHEN cost_grouping='DLTL'
                                                                                                                                 THEN total_cost_usd
                                                                                                             END
                                                                                                   )
                                                                                         AS dltl
                                                                                       , SUM
                                                                                                   (
                                                                                                             CASE
                                                                                                                       WHEN cost_grouping='DST'
                                                                                                                                 THEN total_cost_usd
                                                                                                             END
                                                                                                   )
                                                                                         AS dst
                                                                                       , SUM
                                                                                                   (
                                                                                                             CASE
                                                                                                                       WHEN cost_grouping='FUEL'
                                                                                                                                 THEN total_cost_usd
                                                                                                             END
                                                                                                   )
                                                                                         AS fuel
                                                                                       , SUM
                                                                                                   (
                                                                                                             CASE
                                                                                                                       WHEN cost_grouping='EXM'
                                                                                                                                 THEN total_cost_usd
                                                                                                             END
                                                                                                   )
                                                                                         AS exm
                                                                                       , SUM
                                                                                                   (
                                                                                                             CASE
                                                                                                                       WHEN cost_grouping='Accessorials'
                                                                                                                                 THEN total_cost_usd
                                                                                                             END
                                                                                                   )
                                                                                         AS accessorials
                                                                               FROM
                                                                                         (
                                                                                                    SELECT
                                                                                                               null_orig.origin_zone_null              AS origin_zone_null
                                                                                                             , null_orig_tfs.load_id                   AS load_id
                                                                                                             , null_orig_tfs.destination_postal        AS destination_postal
                                                                                                             , null_orig_tfs.voucher_id                AS voucher_id
                                                                                                             , null_orig_tfs.actual_gi_date            AS actual_gi_date
                                                                                                             , null_orig_tfs.origin_zone               AS origin_zone
                                                                                                             , null_orig_tfs.delivery_id               AS delivery_id
                                                                                                             , null_orig_tfs.tdc_val_code              AS tdc_val_code
                                                                                                             , null_orig_tfs.freight_cost_charge       AS freight_cost_charge
                                                                                                             , null_orig_tfs.total_cost_usd            AS total_cost_usd
                                                                                                             , null_orig_tfs.total_cost_local          AS total_cost_local
                                                                                                             , null_orig_tfs.subsector                 AS subsector
                                                                                                             , null_orig_tfs.su                        AS su
                                                                                                             , null_orig_tfs.steps                     AS steps
                                                                                                             , null_orig_tfs.cost_ratio                AS cost_ratio
                                                                                                             , null_orig_tfs.cost_grouping             AS cost_grouping
                                                                                                             , null_orig_tfs.accrual_cost              AS accrual_cost
                                                                                                             , null_orig_tfs.service_tms_code          AS service_tms_code
                                                                                                             , null_orig_tfs.freight_type              AS freight_type
                                                                                                             , null_orig_tfs.country_to                AS country_to
                                                                                                             , null_orig_tfs.distance_per_load         AS distance_per_load
                                                                                                             , null_orig_tfs.weighted_average_rate_old AS weighted_average_rate_old
                                                                                                             , null_orig_tfs.shipping_point_code       AS shipping_point_code
                                                                                                             , null_orig_tfs.destination_zone_new      AS destination_zone_new
                                                                                                             , CASE
                                                                                                                          WHEN null_orig_tfs.origin_zone IS NULL
                                                                                                                                     THEN null_orig.origin_zone_null
                                                                                                                                     ELSE null_orig_tfs.origin_zone
                                                                                                               END AS origin_zone_new
                                                                                                    FROM
                                                                                                               (
                                                                                                                        select
                                                                                                                                 ship_point_code       AS shipping_point_code
                                                                                                                               , origin_zone_null_name AS origin_zone_null
                                                                                                                        FROM
                                                                                                                                 tfs_null_origin_lkp --ds_null_origin3
                                                                                                                        GROUP BY
                                                                                                                                 ship_point_code
                                                                                                                               , origin_zone_null_name
                                                                                                               )
                                                                                                               AS null_orig
                                                                                                               RIGHT JOIN
                                                                                                                          (
                                                                                                                                 SELECT
                                                                                                                                        null_dest.destination_postal_update AS destination_postal_update
                                                                                                                                      , null_dest.destination_zone_update   AS destination_zone_update
                                                                                                                                      , tfs.load_id                         AS load_id
                                                                                                                                      , tfs.destination_postal              AS destination_postal
                                                                                                                                      , tfs.voucher_id                      AS voucher_id
                                                                                                                                      , tfs.actual_gi_date                  AS actual_gi_date
                                                                                                                                      , tfs.origin_zone                     AS origin_zone
                                                                                                                                      , tfs.destination_zone                AS destination_zone
                                                                                                                                      , tfs.delivery_id                     AS delivery_id
                                                                                                                                      , tfs.tdc_val_code                    AS tdc_val_code
                                                                                                                                      , tfs.freight_cost_charge             AS freight_cost_charge
                                                                                                                                      , tfs.total_cost_usd                  AS total_cost_usd
                                                                                                                                      , tfs.total_cost_local                AS total_cost_local
                                                                                                                                      , tfs.subsector                       AS subsector
                                                                                                                                      , tfs.su                              AS su
                                                                                                                                      , tfs.steps                           AS steps
                                                                                                                                      , tfs.cost_ratio                      AS cost_ratio
                                                                                                                                      , tfs.cost_grouping                   AS cost_grouping
                                                                                                                                      , tfs.accrual_cost                    AS accrual_cost
                                                                                                                                      , tfs.service_tms_code                AS service_tms_code
                                                                                                                                      , tfs.freight_type                    AS freight_type
                                                                                                                                      , tfs.country_to                      AS country_to
                                                                                                                                      , tfs.distance_per_load               AS distance_per_load
                                                                                                                                      , tfs.weighted_average_rate_old       AS weighted_average_rate_old
                                                                                                                                      , tfs.shipping_point_code             AS shipping_point_code
                                                                                                                                      , case
                                                                                                                                               WHEN TO_DATE(tfs.actual_gi_date) >= '2016-11-01'
                                                                                                                                                      THEN tfs.destination_zone
                                                                                                                                               WHEN tfs.destination_zone                  IS NULL
                                                                                                                                                      and null_dest.destination_zone_update IS NOT NULL
                                                                                                                                                      THEN null_dest.destination_zone_update
                                                                                                                                               WHEN tfs.destination_zone              IS NULL
                                                                                                                                                      and null_dest.destination_zone_update IS NULL
                                                                                                                                                      THEN tfs.destination_postal
                                                                                                                                                      ELSE tfs.destination_zone
                                                                                                                                        END AS destination_zone_new
                                                                                                                                 FROM
                                                                                                                                        (
                                                                                                                                                 SELECT
                                                                                                                                                          dest_postal_code AS destination_postal_update
                                                                                                                                                        , dest_zone_val    AS destination_zone_update
                                                                                                                                                 FROM
                                                                                                                                                          tfs_null_dest_lkp -- ds_null_destination_v2
                                                                                                                                                 GROUP BY
                                                                                                                                                          dest_postal_code
                                                                                                                                                        , dest_zone_val
                                                                                                                                        )
                                                                                                                                        null_dest
                                                                                                                                        RIGHT JOIN 
                                                                                                                                               (
                                                                                                                                                      SELECT --DISTINCT
                                                                                                                                                             CASE
                                                                                                                                                                    WHEN `load id` is NULL
                                                                                                                                                                           THEN "No Load ID"
                                                                                                                                                                    WHEN `load id` = ""
                                                                                                                                                                           THEN "No Load ID"
                                                                                                                                                                           ELSE `load id`
                                                                                                                                                             END                  AS load_id
                                                                                                                                                           , `destination postal` AS destination_postal
                                                                                                                                                           , `voucher id`         AS voucher_id
                                                                                                                                                           , `actual gi date`     AS actual_gi_date
                                                                                                                                                           , case
                                                                                                                                                                    WHEN `origin zone` = "SF_EDWARDSVILLE"
                                                                                                                                                                           THEN "SF_ST_LOUIS"
                                                                                                                                                                           ELSE `origin zone`
                                                                                                                                                             END AS origin_zone
                                                                                                                                                           , case
                                                                                                                                                                    WHEN `destination zone` = "SF_EDWARDSVILLE"
                                                                                                                                                                           THEN "SF_ST_LOUIS"
                                                                                                                                                                           ELSE `destination zone`
                                                                                                                                                             END                                                    AS destination_zone
                                                                                                                                                           , `delivery id #`                                        AS delivery_id
                                                                                                                                                           , `tdc val code`                                         AS tdc_val_code
                                                                                                                                                           , `freight cost charge`                                  AS freight_cost_charge
                                                                                                                                                           , CAST(`total transportation cost usd` AS            DECIMAL(30,8)) AS `total_cost_usd`
                                                                                                                                                           , CAST(`total transportation cost local currency` AS DECIMAL(30,8)) AS total_cost_local
                                                                                                                                                           , case
                                                                                                                                                                    WHEN `subsector description` is NULL
                                                                                                                                                                           THEN "MISSING"
                                                                                                                                                                           ELSE `subsector description`
                                                                                                                                                             END                                                                                                                      AS subsector
                                                                                                                                                           , CAST(`#su per load` AS                             DECIMAL(30,8))                                                        AS `su`
                                                                                                                                                           , CAST(`steps` AS                                    DECIMAL(30,8))                                                        AS steps
                                                                                                                                                           , CAST(`total transportation cost local currency` AS DECIMAL(30,8))/CAST(`total transportation cost usd` AS DECIMAL(30,8)) AS cost_ratio
                                                                                                                                                           , CASE
                                                                                                                                                                    WHEN `freight cost charge` IN ('FSUR'
                                                                                                                                                                                                 , 'FU_S'
                                                                                                                                                                                                 , 'FLTL'
                                                                                                                                                                                                 , 'FFLT'
                                                                                                                                                                                                 , 'FCHG'
                                                                                                                                                                                                 , 'FUSU')
                                                                                                                                                                           THEN 'FUEL'
                                                                                                                                                                    WHEN `freight cost charge` IN ('DST','CVYI','HJBT','KNIG','L2D','SCNN','UFLB','USXI','PGLI')
                                                                                                                                                                           THEN 'DST'
                                                                                                                                                                    WHEN `freight cost charge` = 'EXM'
                                                                                                                                                                           THEN 'EXM'
                                                                                                                                                                    WHEN `freight cost charge` = 'SCS'
                                                                                                                                                                           THEN 'SCS'
                                                                                                                                                                    WHEN `freight cost charge` = 'FTP'
                                                                                                                                                                           THEN 'FTP'
                                                                                                                                                                    WHEN `freight cost charge` = 'TSC'
                                                                                                                                                                           THEN 'TSC'
                                                                                                                                                                    WHEN `freight cost charge` = 'DIST'
                                                                                                                                                                           THEN 'DIST'
                                                                                                                                                                    WHEN `freight cost charge` = 'DLTL'
                                                                                                                                                                           THEN 'DLTL'
                                                                                                                                                                    WHEN `freight cost charge` = 'FLAT'
                                                                                                                                                                           THEN 'FLAT'
                                                                                                                                                                    WHEN `freight cost charge` = 'SPOT'
                                                                                                                                                                           THEN 'SPOT'
                                                                                                                                                                    WHEN `freight cost charge` = 'FA_A'
                                                                                                                                                                           THEN 'FA_A'
                                                                                                                                                                           ELSE 'Accessorials'
                                                                                                                                                             END AS cost_grouping
                                                                                                                                                           , CASE
                                                                                                                                                                    WHEN CAST(`accrual cost` AS DECIMAL(30,8)) IS NULL
                                                                                                                                                                           THEN 0.000
                                                                                                                                                                           ELSE CAST(`accrual cost` AS DECIMAL(30,8))
                                                                                                                                                             END                                        AS accrual_cost
                                                                                                                                                           , `service tms code`                         AS service_tms_code
                                                                                                                                                           , `freight type`                             AS freight_type
                                                                                                                                                           , `country to`                               AS country_to
                                                                                                                                                           , CAST(`distance per load` AS     DECIMAL(30,8)) AS distance_per_load
                                                                                                                                                           , CAST(`weighted average rate` AS DECIMAL(30,8)) AS weighted_average_rate_old
                                                                                                                                                           , `origin location id`                           AS shipping_point_code
                                                                                                                                                      FROM
                                                                                                                                                             tfs
                                                                                                                                               )
                                                                                                                                               tfs
                                                                                                                                               ON
                                                                                                                                                      null_dest.destination_postal_update=tfs.destination_postal
                                                                                                                                 WHERE
                                                                                                                                        tfs.load_id = "No Load ID"
                                                                                                                          )
                                                                                                                          null_orig_tfs
                                                                                                                          ON
                                                                                                                                     null_orig.shipping_point_code=null_orig_tfs.shipping_point_code
                                                                                         )
                                                                                         null_orig_dest_tfs
                                                                                         LEFT JOIN
                                                                                                   (
                                                                                                          select
                                                                                                                 origin_zone_name AS origin_zone
                                                                                                               , dest_zone_val    AS destination_zone
                                                                                                               , weight_avg_rate  AS weighted_average_rate_hist
                                                                                                          FROM
                                                                                                                 tfs_hist_weight_avg_rate_lkp -- ds_historical_test11
                                                                                                   )
                                                                                                   tfs_hist
                                                                                                   ON
                                                                                                             null_orig_dest_tfs.origin_zone              = tfs_hist.origin_zone
                                                                                                             AND null_orig_dest_tfs.destination_zone_new = tfs_hist.destination_zone
                                                                                         GROUP BY 
                                                                                                null_orig_dest_tfs.load_id
                                                                                              , null_orig_dest_tfs.voucher_id
                                                                                              , null_orig_dest_tfs.subsector
                                                                                              , null_orig_dest_tfs.cost_ratio
                                                                                              , null_orig_dest_tfs.accrual_cost
                                                                                              , null_orig_dest_tfs.service_tms_code
                                                                                              , null_orig_dest_tfs.freight_type
                                                                                              , null_orig_dest_tfs.country_to
                                                                                              , null_orig_dest_tfs.distance_per_load
                                                                     )
                                                                     conditions1
                                                       )
                                                       conditions2
                                         )
                                         totescosts
                           )
                           null_final
                           LEFT JOIN
                                     ds_subsectorcosts_max AS tfs_max
                                     ON
                                               null_final.voucher_id=tfs_max.voucher_id
       )
       tb
;


DROP TABLE IF EXISTS ds_subc_v14_max
;

CREATE TEMPORARY VIEW ds_subc_v14_max as
SELECT
         `load id`                     AS load_id
       , CASE
                  WHEN `origin zone` = "SF_EDWARDSVILLE"
                           THEN "SF_ST_LOUIS"
                           ELSE `origin zone`
         END AS origin_zone_new
       , CASE
                  WHEN `destination zone` = "SF_EDWARDSVILLE"
                           THEN "SF_ST_LOUIS"
                           ELSE `destination zone`
         END                           AS destination_zone_new
       , `origin location id`          AS shipping_point_code
       , `customer description`        AS customer_description
       , `carrier description`         AS carrier_description
       , `country to`                  AS country_to
       , `subsector description`       AS subsector
       , `country to description`      AS country_to_description
       , `goods receipt posting date`  AS goods_receipt_posting_date
       , `freight auction`             AS freight_auction
       , `destination postal`          AS destination_postal
       , `customer l8`                 AS customer_l8
       , `truckload_vs_intermodal_val` AS truckload_vs_intermodal_val
       , `service tms code`            AS service_tms_code
       , MAX(`actual gi date`)         AS actual_gi_date
       , MAX(`created date`)           AS created_date
       , MAX
                  (
                           CASE
                                    WHEN `ship to #` = ""
                                             THEN 0
                                             ELSE CAST(TRIM(`ship to #`) AS INT)
                           END
                  )
         AS max_ship_to
       , MAX
                  (
                           CASE
                                    WHEN `historical data structure flag` = "-"
                                             THEN 1
                                             ELSE 0
                           END
                  )
                                  AS max_hist_num
       , MIN(`distance per load`) AS distance_per_load
       , MIN(`origin sf`)         AS origin_sf
       , MIN(`destination sf`)    AS destination_sf
FROM
         tfs
GROUP BY
         `load id`
       , `origin zone`
       , `destination zone`
       , `origin location id`
       , `customer description`
       , `carrier description`
       , `country to`
       , `subsector description`
       , `country to description`
       , `goods receipt posting date`
       , `freight auction`
       , `destination postal`
       , `customer l8`
       , `truckload_vs_intermodal_val`
       , `distance`
       , `freight type`
       , `service tms code`
;

DROP TABLE IF EXISTS ds_subc_v14_ship_to
;

CREATE TEMPORARY VIEW ds_subc_v14_ship_to AS
SELECT
                  (
                           CASE
                                    WHEN `ship to #` = ""
                                             THEN 0
                                             ELSE CAST(TRIM(`ship to #`) AS INT)
                           END
                  )
                               AS ship_to
       , `ship to description` AS ship_to_description
FROM
         tfs
GROUP BY
         `ship to #`
       , `ship to description`
;

DROP TABLE IF EXISTS ds_subc_v14_dest_id
;

CREATE TEMPORARY VIEW ds_subc_v14_dest_id AS
SELECT
         CONCAT('0',load_id) AS new_load_id
       , destination_location_id
FROM
         tac_tender_pg
GROUP BY
         load_id
       , destination_location_id
;

DROP TABLE IF EXISTS ds_subc_v14_dest_upd
;

CREATE TEMPORARY VIEW ds_subc_v14_dest_upd AS
SELECT
         dest_postal_code AS destination_postal_update
       , dest_zone_val    AS destination_zone_update
FROM
         tfs_null_dest_lkp -- ds_null_destination_v2
GROUP BY
         dest_postal_code
       , dest_zone_val
;

DROP TABLE IF EXISTS ds_subc_v14_orig_null
;

CREATE TEMPORARY VIEW ds_subc_v14_orig_null AS
SELECT
         ship_point_code       AS shipping_point_code
       , origin_zone_null_name AS origin_zone_null
FROM
         tfs_null_origin_lkp -- ds_null_origin3
GROUP BY
         ship_point_code
       , origin_zone_null_name
;

DROP TABLE IF EXISTS ds_subc_v14_tfs
;

CREATE TEMPORARY VIEW ds_subc_v14_tfs AS
SELECT
          tfs_dest_orig.load_id                                 AS load_id
        , tfs_dest_orig.subsector                               AS subsector
        , tfs_dest_orig.delivery_id                             AS delivery_id
        , tfs_dest_orig.tdc_val_code                            AS tdc_val_code
        , tfs_dest_orig.cost_grouping                           AS cost_grouping
        --, tfs_dest_orig.destination_postal                      AS destination_postal
        --, tfs_dest_orig.voucher_id                              AS voucher_id
        , tfs_dest_orig.actual_gi_date                          AS actual_gi_date
        --, tfs_dest_orig.freight_cost_charge                     AS freight_cost_charge
        , tfs_dest_orig.total_cost_usd                          AS total_cost_usd
        --, tfs_dest_orig.total_cost_local                        AS total_cost_local
        , tfs_dest_orig.su                                      AS su
        , tfs_dest_orig.steps                                   AS steps
        , tfs_dest_orig.cost_ratio                              AS cost_ratio
        , tfs_dest_orig.accrual_cost                            AS accrual_cost
        , tfs_dest_orig.service_tms_code                        AS service_tms_code
        , tfs_dest_orig.freight_type                            AS freight_type
        , tfs_dest_orig.country_to                              AS country_to
        , tfs_dest_orig.distance_per_load                       AS distance_per_load
        , tfs_dest_orig.weighted_average_rate_old               AS weighted_average_rate_old
        --, tfs_dest_orig.shipping_point_code                     AS shipping_point_code
        --, tfs_dest_orig.destination_zone_new                    AS destination_zone_new
        --, tfs_dest_orig.origin_zone                             AS origin_zone
        , war_hist.weight_avg_rate                              AS weighted_average_rate_hist
        , cast(trim(war_hist.weight_avg_rate) as DECIMAL(30,8)) AS war_dec
        , case
                    when to_date(tfs_dest_orig.actual_gi_date)                    >= '2016-11-01'
                              or tfs_dest_orig.weighted_average_rate_old is not null
                              then tfs_dest_orig.weighted_average_rate_old
                    when cast(trim(war_hist.weight_avg_rate) as DECIMAL(30,8)) is null
                              then tfs_dest_orig.weighted_average_rate_old
                              else cast(trim(war_hist.weight_avg_rate) as DECIMAL(30,8))
          end as weighted_average_rate
FROM
          (
                     SELECT
                              --  orig_null.origin_zone_null         AS origin_zone_null
                                tfs_dest.load_id                   AS load_id
                              --, tfs_dest.destination_postal        AS destination_postal
                              --, tfs_dest.voucher_id                AS voucher_id
                              , tfs_dest.actual_gi_date            AS actual_gi_date
                              , tfs_dest.origin_zone               AS origin_zone		--_1
                              , tfs_dest.delivery_id               AS delivery_id
                              , tfs_dest.tdc_val_code              AS tdc_val_code
                              --, tfs_dest.freight_cost_charge       AS freight_cost_charge
                              , tfs_dest.total_cost_usd            AS total_cost_usd
                              --, tfs_dest.total_cost_local          AS total_cost_local
                              , tfs_dest.subsector                 AS subsector
                              , tfs_dest.su                        AS su
                              , tfs_dest.steps                     AS steps
                              , tfs_dest.cost_ratio                AS cost_ratio
                              , tfs_dest.cost_grouping             AS cost_grouping
                              , tfs_dest.accrual_cost              AS accrual_cost
                              , tfs_dest.service_tms_code          AS service_tms_code
                              , tfs_dest.freight_type              AS freight_type
                              , tfs_dest.country_to                AS country_to
                              , tfs_dest.distance_per_load         AS distance_per_load
                              , tfs_dest.weighted_average_rate_old AS weighted_average_rate_old
                              --, tfs_dest.shipping_point_code       AS shipping_point_code
                              , tfs_dest.destination_zone_new      AS destination_zone_new
                              --, case
                              --             when tfs_dest.origin_zone is null
                              --                        then orig_null.origin_zone_null
                              --                        else tfs_dest.origin_zone
                              --  end as origin_zone
                     FROM
                                --ds_subc_v14_orig_null AS orig_null
                                --RIGHT JOIN
                                           (
                                                      SELECT
                                                                 dest_upd.destination_postal_update AS destination_postal_update
                                                               , dest_upd.destination_zone_update   AS destination_zone_update
                                                               , tfs.load_id                        AS load_id
                                                               --, tfs.destination_postal             AS destination_postal
                                                               --, tfs.voucher_id                     AS voucher_id
                                                               , tfs.actual_gi_date                 AS actual_gi_date
                                                               , tfs.origin_zone                    AS origin_zone
                                                               , tfs.destination_zone               AS destination_zone
                                                               , tfs.delivery_id                    AS delivery_id
                                                               , tfs.tdc_val_code                   AS tdc_val_code
                                                               --, tfs.freight_cost_charge            AS freight_cost_charge
                                                               , tfs.total_cost_usd                 AS total_cost_usd
                                                               --, tfs.total_cost_local               AS total_cost_local
                                                               , tfs.subsector                      AS subsector
                                                               , tfs.su                             AS su
                                                               , tfs.steps                          AS steps
                                                               , tfs.cost_ratio                     AS cost_ratio
                                                               , tfs.cost_grouping                  AS cost_grouping
                                                               , tfs.accrual_cost                   AS accrual_cost
                                                               , tfs.service_tms_code               AS service_tms_code
                                                               , tfs.freight_type                   AS freight_type
                                                               , tfs.country_to                     AS country_to
                                                               , tfs.distance_per_load              AS distance_per_load
                                                               , tfs.weighted_average_rate_old      AS weighted_average_rate_old
                                                               --, tfs.shipping_point_code            AS shipping_point_code
                                                               , case
                                                                            when to_date(tfs.actual_gi_date) >= '2016-11-01'
                                                                                       then tfs.destination_zone
                                                                            when tfs.destination_zone                           is null
                                                                                       and dest_upd.destination_zone_update is not null
                                                                                       then dest_upd.destination_zone_update
                                                                            when tfs.destination_zone                       is null
                                                                                       and dest_upd.destination_zone_update is null
                                                                                       then tfs.destination_postal
                                                                                       else tfs.destination_zone
                                                                 end as destination_zone_new
                                                      FROM
                                                                 ds_subc_v14_dest_upd AS dest_upd
                                                                 RIGHT JOIN
                                                                            (
                                                                                   SELECT 
                                                                                          case
                                                                                                 when `load id` is NULL
                                                                                                        then "No Load ID"
                                                                                                 when `load id` = ""
                                                                                                        then "No Load ID"
                                                                                                        else `load id`
                                                                                          end                  as load_id
                                                                                        , `destination postal` as destination_postal
                                                                                        --, `voucher id`         as voucher_id
                                                                                        , `actual gi date`     as actual_gi_date
                                                                                        , case
                                                                                                 when `origin zone` = "SF_EDWARDSVILLE"
                                                                                                        then "SF_ST_LOUIS"
                                                                                                        else `origin zone`
                                                                                          end as origin_zone
                                                                                        , case
                                                                                                 when `destination zone` = "SF_EDWARDSVILLE"
                                                                                                        then "SF_ST_LOUIS"
                                                                                                        else `destination zone`
                                                                                          end                                                          as destination_zone
                                                                                        , `delivery id #`                                              as delivery_id
                                                                                        , `tdc val code`                                               as tdc_val_code
                                                                                        --, `freight cost charge`                                        as freight_cost_charge
                                                                                        , cast(trim(`total transportation cost usd`) as            DECIMAL(30,8)) as `total_cost_usd`
                                                                                        --, cast(trim(`total transportation cost local currency`) as DECIMAL(30,8)) as total_cost_local
                                                                                        , case
                                                                                                 when `subsector description` is NULL
                                                                                                        then "MISSING"
                                                                                                        else `subsector description`
                                                                                          end                                                                                                                                  as subsector
                                                                                        , cast(trim(`#su per load`) as                             DECIMAL(30,8))                                                              as `su`
                                                                                        , cast(trim(`steps`) as                                    DECIMAL(30,8))                                                              as steps
                                                                                        , cast(trim(`total transportation cost local currency`) as DECIMAL(30,8))/cast(trim(`total transportation cost usd`) as DECIMAL(30,8)) as cost_ratio
                                                                                        , case
                                                                                                 when `freight cost charge` in('FSUR'
                                                                                                                             , 'FU_S'
                                                                                                                             , 'FLTL'
                                                                                                                             , 'FFLT'
                                                                                                                             , 'FCHG'
                                                                                                                             , 'FUSU')
                                                                                                        then 'FUEL'
                                                                                                 when `freight cost charge` IN ('DST','CVYI','HJBT','KNIG','L2D','SCNN','UFLB','USXI','PGLI')
                                                                                                        then 'DST'
                                                                                                 when `freight cost charge` = 'EXM'
                                                                                                        then 'EXM'
                                                                                                 when `freight cost charge` = 'SCS'
                                                                                                        then 'SCS'
                                                                                                 when `freight cost charge` = 'FTP'
                                                                                                        then 'FTP'
                                                                                                 when `freight cost charge` = 'TSC'
                                                                                                        then 'TSC'
                                                                                                 when `freight cost charge` = 'DIST'
                                                                                                        then 'DIST'
                                                                                                 when `freight cost charge` = 'DLTL'
                                                                                                        then 'DLTL'
                                                                                                 when `freight cost charge` = 'FLAT'
                                                                                                        then 'FLAT'
                                                                                                 when `freight cost charge` = 'SPOT'
                                                                                                        then 'SPOT'
                                                                                                 when `freight cost charge` = 'FA_A'
                                                                                                        then 'FA_A'
                                                                                                        else 'Accessorials'
                                                                                          end as cost_grouping
                                                                                        , case
                                                                                                 when cast(trim(`accrual cost`) as DECIMAL(30,8)) IS NULL
                                                                                                        then 0.000
                                                                                                        else cast(trim(`accrual cost`) as DECIMAL(30,8))
                                                                                          end                                              AS accrual_cost
                                                                                        , `service tms code`                               as service_tms_code
                                                                                        , `freight type`                                   as freight_type
                                                                                        , `country to`                                     as country_to
                                                                                        , cast(trim(`distance per load`) as     DECIMAL(30,8)) as distance_per_load
                                                                                        , cast(trim(`weighted average rate`) as DECIMAL(30,8)) as weighted_average_rate_old
                                                                                        --, `origin location id`                                 as shipping_point_code
                                                                                   FROM
                                                                                          tfs
                                                                                   WHERE
                                                                                          `load id` <> "MISSING"
                                                                            )
                                                                            tfs
                                                                            ON
                                                                                       COALESCE(dest_upd.destination_postal_update, 'XX')=COALESCE(tfs.destination_postal, 'XX')
                                           )
                                           tfs_dest
                                           --ON
                                           --           orig_null.shipping_point_code=tfs_dest.shipping_point_code
          )
          tfs_dest_orig
          LEFT JOIN
                    tfs_hist_weight_avg_rate_lkp AS war_hist -- ds_historical_test11
                    ON
                              COALESCE(tfs_dest_orig.origin_zone, 'XX')             =COALESCE(war_hist.origin_zone_name, 'XX')
                              AND COALESCE(tfs_dest_orig.destination_zone_new, 'XX')=COALESCE(war_hist.dest_zone_val, 'XX')
;


Drop table IF EXISTS ds_subc_v14_su
;

Create TEMPORARY VIEW ds_subc_v14_su as
SELECT
             load_id
           , subsector
           , SUM(su) AS su
FROM
(
                          SELECT
                                       load_id
                                     , delivery_id
                                     , tdc_val_code
                                     , subsector
                                     , MAX(su) AS su
                          FROM
                                       ds_subc_v14_tfs
                          GROUP BY load_id
                                     , delivery_id
                                     , tdc_val_code
                                     , subsector
) as a
GROUP BY
         load_id
       , subsector
;


Drop table IF EXISTS ds_subc_v14_calcs
;

Create TEMPORARY VIEW ds_subc_v14_calcs as
SELECT
             load_id
           , subsector
           , cost_grouping
           --, steps_load
           --, steps_subsector
           , SUM(steps) over(
                partition by load_id) as steps_load
           , SUM(steps) over(
                partition by load_id
                           , subsector) as steps_subsector
           , total_cost_usd
           , cost_ratio
           , accrual_cost
           , service_tms_code
           , country_to
           , distance_per_load
           , weighted_average_rate
           , freight_type
           --, su
           --, SUM(su) AS su
FROM
             (
                          SELECT
                                       load_id
                                     , subsector
                                     , cost_grouping
                                     --, SUM(steps) over(
                                     --     partition by load_id) as steps_load
                                     --, SUM(steps) over(
                                     --     partition by load_id
                                     --                , subsector) as steps_subsector
                                     --, MAX(su) over(
                                     --  partition by load_id
                                     --             , delivery_id
                                     --             , tdc_val_code
                                     --             , subsector) AS su
                                     , SUM(steps) AS steps
                                     , SUM(total_cost_usd)        AS total_cost_usd
                                     , MAX(cost_ratio)            AS cost_ratio
                                     , SUM(accrual_cost)          AS accrual_cost
                                     , MAX(service_tms_code)      AS service_tms_code
                                     , MAX(country_to)            AS country_to
                                     , MIN(distance_per_load)     AS distance_per_load
                                     , MAX(weighted_average_rate) AS weighted_average_rate
                                     , MAX(freight_type)          AS freight_type
                          FROM
                                       ds_subc_v14_tfs
                          GROUP BY load_id
                                 , subsector
                                 , cost_grouping

) as a
--GROUP BY
--         load_id
--       , subsector
--       , cost_grouping
--       , steps_load
--       , steps_subsector
--       , total_cost_usd
--       , cost_ratio
--       , accrual_cost
--       , service_tms_code
--       , country_to
--       , distance_per_load
--       , weighted_average_rate
--       , freight_type
;

Drop table IF EXISTS ds_subc_v14_calcs_2
;

Create TEMPORARY VIEW ds_subc_v14_calcs_2 as
SELECT
         load_id
       , subsector
       , SUM(su)                         AS su
       , MAX(steps_load)                 AS steps_load
       , MAX(steps_subsector)            AS steps_subsector
       , MAX(steps_subsector/steps_load) AS steps_ratio
       , AVG(su)                         AS AVG_su_
       , MAX(cost_ratio)                 AS MAX_cost_ratio_
       , SUM(accrual_cost)               AS SUM_accrual_cost_
       , MAX(service_tms_code)           AS MAX_service_tms_code_
       , MAX(country_to)                 AS MAX_country_to_
       , MIN(distance_per_load)          AS MIN_distance_per_load_
       , MAX(weighted_average_rate)      AS MAX_weighted_average_rate_
       , MAX(steps_ratio)                AS MAX_steps_ratio_
       , MAX(freight_type)               AS MAX_freight_type_
FROM (
       SELECT DISTINCT
              cc.load_id
            , cc.subsector
            , su
            , steps_load
            , steps_subsector
            , (steps_subsector/steps_load) AS steps_ratio
            , cost_ratio
            , accrual_cost
            , service_tms_code
            , country_to
            , distance_per_load
            , weighted_average_rate
            --, steps_ratio AS steps_ratio_
            , freight_type
       FROM ds_subc_v14_calcs AS cc
       JOIN ds_subc_v14_su as c_su
           ON cc.load_id = c_su.load_id
             AND cc.subsector = c_su.subsector
) a
GROUP BY
         load_id
       , subsector
;

Drop table IF EXISTS ds_subsectorcosts_v14
;

Create TEMPORARY VIEW ds_subsectorcosts_v14 as
SELECT
       load_id
     , su_per_load
     , service_tms_code
     , country_to
     , freight_type
     , dltl
     , scs
     , ftp
     , dst
     , tsc
     , fuel
     , flat
     , exm
     , fa_a
     , lh_rules
     , weighted_average_rate_adjusted_long
     , accrual_cost_adjusted
     , accessorial_costs
     , weighted_average_rate_adjusted
     , distance_per_load_dec
     , steps_ratio_dec
     , unsourced_costs
     , line_haul_final
     , cnc_costs
     , freight_auction_costs
     , total_cost
     , customer_description
     , carrier_description
     , actual_gi_date
     , country_to_description
     , goods_receipt_posting_date
     , created_date
     , freight_auction
     , destination_postal
     , customer_l8
     , distance_per_load
     , ship_to_description
     , origin_sf
     , destination_sf
     , truckload_vs_intermodal_val
     , historical_data_structure_flag
     , ship_to_id
     , destination_zone_new
     , origin_zone_new AS origin_zone
     , fy
     , subsector_update AS subsector
     , ""               as voucher_id
FROM
       (
                  SELECT
                             table_179224308_1.origin_zone_null                    AS origin_zone_null
                           , table_179224308_2.destination_postal_update           AS destination_postal_update
                           , table_179224308_2.destination_zone_update             AS destination_zone_update
                           , table_179224308_2.load_id                             AS load_id
                           , table_179224308_2.subsector                           AS subsector
                           , table_179224308_2.su_per_load                         AS su_per_load
                           , table_179224308_2.service_tms_code                    AS service_tms_code
                           , table_179224308_2.country_to                          AS country_to
                           , table_179224308_2.freight_type                        AS freight_type
                           , table_179224308_2.dltl                                AS dltl
                           , table_179224308_2.scs                                 AS scs
                           , table_179224308_2.ftp                                 AS ftp
                           , table_179224308_2.dst                                 AS dst
                           , table_179224308_2.tsc                                 AS tsc
                           , table_179224308_2.fuel                                AS fuel
                           , table_179224308_2.flat                                AS flat
                           , table_179224308_2.exm                                 AS exm
                           , table_179224308_2.fa_a                                AS fa_a
                           , table_179224308_2.lh_rules                            AS lh_rules
                           , table_179224308_2.weighted_average_rate_adjusted_long AS weighted_average_rate_adjusted_long
                           , table_179224308_2.accrual_cost_adjusted               AS accrual_cost_adjusted
                           , table_179224308_2.accessorial_costs                   AS accessorial_costs
                           , table_179224308_2.weighted_average_rate_adjusted      AS weighted_average_rate_adjusted
                           , table_179224308_2.distance_per_load_dec               AS distance_per_load_dec
                           , table_179224308_2.steps_ratio_dec                     AS steps_ratio_dec
                           , table_179224308_2.unsourced_costs                     AS unsourced_costs
                           , table_179224308_2.line_haul_final                     AS line_haul_final
                           , table_179224308_2.cnc_costs                           AS cnc_costs
                           , table_179224308_2.freight_auction_costs               AS freight_auction_costs
                           , table_179224308_2.total_cost                          AS total_cost
                           , table_179224308_2.origin_zone                         AS origin_zone
                           , table_179224308_2.destination_zone                    AS destination_zone
                           , table_179224308_2.customer_description                AS customer_description
                           , table_179224308_2.carrier_description                 AS carrier_description
                           , table_179224308_2.actual_gi_date                      AS actual_gi_date
                           , table_179224308_2.country_to_description              AS country_to_description
                           , table_179224308_2.goods_receipt_posting_date          AS goods_receipt_posting_date
                           , table_179224308_2.created_date                        AS created_date
                           , table_179224308_2.freight_auction                     AS freight_auction
                           , table_179224308_2.destination_postal                  AS destination_postal
                           , table_179224308_2.customer_l8                         AS customer_l8
                           , table_179224308_2.distance_per_load                   AS distance_per_load
                           , table_179224308_2.ship_to_description                 AS ship_to_description
                           , table_179224308_2.shipping_point_code                 AS shipping_point_code
                           , table_179224308_2.origin_sf                           AS origin_sf
                           , table_179224308_2.destination_sf                      AS destination_sf
                           , table_179224308_2.truckload_vs_intermodal_val         AS truckload_vs_intermodal_val
                           , table_179224308_2.historical_data_structure_flag      AS historical_data_structure_flag
                           , table_179224308_2.ship_to_id                          AS ship_to_id
                           , table_179224308_2.destination_zone_new                AS destination_zone_new
                           , case
                                        when table_179224308_2.origin_zone is null
                                                   then table_179224308_1.origin_zone_null
                                        when table_179224308_2.origin_zone = ""
                                                   then table_179224308_1.origin_zone_null
                                                   else table_179224308_2.origin_zone
                             end as origin_zone_new
                           , case
                                        when month(to_date(table_179224308_2.actual_gi_date)) < 7
                                                   then concat('FY',substr(cast(year(to_date(table_179224308_2.actual_gi_date))-1 as string),-2,2),substr(cast(year(to_date(table_179224308_2.actual_gi_date)) as string),-2,2))
                                                   else concat('FY',substr(cast(year(to_date(table_179224308_2.actual_gi_date)) as string),-2,2),substr(cast(year(to_date(table_179224308_2.actual_gi_date))+1 as string),-2,2))
                             end                                                                                          as FY
                           , regexp_replace(regexp_replace(table_179224308_2.subsector,' SUB-SECTOR',''),' SUBSECTOR','') AS subsector_update
                  FROM
                             ds_subc_v14_orig_null AS table_179224308_1
                             RIGHT JOIN
                                        (
                                                   SELECT
                                                              table_370155390_1.destination_postal_update           AS destination_postal_update
                                                            , table_370155390_1.destination_zone_update             AS destination_zone_update
                                                            , table_370155390_2.load_id                             AS load_id
                                                            , table_370155390_2.subsector                           AS subsector
                                                            , table_370155390_2.su_per_load                         AS su_per_load
                                                            , table_370155390_2.service_tms_code                    AS service_tms_code
                                                            , table_370155390_2.country_to                          AS country_to
                                                            , table_370155390_2.freight_type                        AS freight_type
                                                            , table_370155390_2.dltl                                AS dltl
                                                            , table_370155390_2.scs                                 AS scs
                                                            , table_370155390_2.ftp                                 AS ftp
                                                            , table_370155390_2.dst                                 AS dst
                                                            , table_370155390_2.tsc                                 AS tsc
                                                            , table_370155390_2.fuel                                AS fuel
                                                            , table_370155390_2.flat                                AS flat
                                                            , table_370155390_2.exm                                 AS exm
                                                            , table_370155390_2.fa_a                                AS fa_a
                                                            , table_370155390_2.lh_rules                            AS lh_rules
                                                            , table_370155390_2.weighted_average_rate_adjusted_long AS weighted_average_rate_adjusted_long
                                                            , table_370155390_2.accrual_cost_adjusted               AS accrual_cost_adjusted
                                                            , table_370155390_2.accessorial_costs                   AS accessorial_costs
                                                            , table_370155390_2.weighted_average_rate_adjusted      AS weighted_average_rate_adjusted
                                                            , table_370155390_2.distance_per_load_dec               AS distance_per_load_dec
                                                            , table_370155390_2.steps_ratio_dec                     AS steps_ratio_dec
                                                            , table_370155390_2.unsourced_costs                     AS unsourced_costs
                                                            , table_370155390_2.line_haul_final                     AS line_haul_final
                                                            , table_370155390_2.cnc_costs                           AS cnc_costs
                                                            , table_370155390_2.freight_auction_costs               AS freight_auction_costs
                                                            , table_370155390_2.total_cost                          AS total_cost
                                                            , table_370155390_2.origin_zone                         AS origin_zone
                                                            , table_370155390_2.destination_zone                    AS destination_zone
                                                            , table_370155390_2.customer_description                AS customer_description
                                                            , table_370155390_2.carrier_description                 AS carrier_description
                                                            , table_370155390_2.actual_gi_date                      AS actual_gi_date
                                                            , table_370155390_2.country_to_description              AS country_to_description
                                                            , table_370155390_2.goods_receipt_posting_date          AS goods_receipt_posting_date
                                                            , table_370155390_2.created_date                        AS created_date
                                                            , table_370155390_2.freight_auction                     AS freight_auction
                                                            , table_370155390_2.destination_postal                  AS destination_postal
                                                            , table_370155390_2.customer_l8                         AS customer_l8
                                                            , table_370155390_2.distance_per_load                   AS distance_per_load
                                                            , table_370155390_2.ship_to_description                 AS ship_to_description
                                                            , table_370155390_2.shipping_point_code                 AS shipping_point_code
                                                            , table_370155390_2.origin_sf                           AS origin_sf
                                                            , table_370155390_2.destination_sf                      AS destination_sf
                                                            , table_370155390_2.truckload_vs_intermodal_val         AS truckload_vs_intermodal_val
                                                            , table_370155390_2.historical_data_structure_flag      AS historical_data_structure_flag
                                                            , table_370155390_2.ship_to_id                          AS ship_to_id
                                                            , case
                                                                         when to_date(table_370155390_2.actual_gi_date) >= '2017-07-01'
                                                                                    then table_370155390_2.destination_zone
                                                                         when table_370155390_2.destination_zone                      is null
                                                                                    and table_370155390_1.destination_zone_update is not null
                                                                                    then table_370155390_1.destination_zone_update
                                                                         when table_370155390_2.destination_zone                  is null
                                                                                    and table_370155390_1.destination_zone_update is null
                                                                                    then table_370155390_2.destination_postal
                                                                                    else table_370155390_2.destination_zone
                                                              end as destination_zone_new
                                                   FROM
                                                              ds_subc_v14_dest_upd AS table_370155390_1
                                                              RIGHT JOIN
                                                                         (
                                                                                   SELECT
                                                                                             table_662518308_1.load_id                             AS load_id
                                                                                           , table_662518308_1.subsector                           AS subsector
                                                                                           , table_662518308_1.su_per_load                         AS su_per_load
                                                                                           , table_662518308_1.service_tms_code                    AS service_tms_code
                                                                                           , table_662518308_1.country_to                          AS country_to
                                                                                           , table_662518308_1.freight_type                        AS freight_type
                                                                                           , table_662518308_1.dltl                                AS dltl
                                                                                           , table_662518308_1.scs                                 AS scs
                                                                                           , table_662518308_1.ftp                                 AS ftp
                                                                                           , table_662518308_1.dst                                 AS dst
                                                                                           , table_662518308_1.tsc                                 AS tsc
                                                                                           , table_662518308_1.fuel                                AS fuel
                                                                                           , table_662518308_1.flat                                AS flat
                                                                                           , table_662518308_1.exm                                 AS exm
                                                                                           , table_662518308_1.fa_a                                AS fa_a
                                                                                           , table_662518308_1.lh_rules                            AS lh_rules
                                                                                           , table_662518308_1.weighted_average_rate_adjusted_long AS weighted_average_rate_adjusted_long
                                                                                           , table_662518308_1.accrual_cost_adjusted               AS accrual_cost_adjusted
                                                                                           , table_662518308_1.accessorial_costs                   AS accessorial_costs
                                                                                           , table_662518308_1.weighted_average_rate_adjusted      AS weighted_average_rate_adjusted
                                                                                           , table_662518308_1.distance_per_load_dec               AS distance_per_load_dec
                                                                                           , table_662518308_1.steps_ratio_dec                     AS steps_ratio_dec
                                                                                           , table_662518308_1.unsourced_costs                     AS unsourced_costs
                                                                                           , table_662518308_1.line_haul_final                     AS line_haul_final
                                                                                           , table_662518308_1.cnc_costs                           AS cnc_costs
                                                                                           , table_662518308_1.freight_auction_costs               AS freight_auction_costs
                                                                                           , table_662518308_1.total_cost                          AS total_cost
                                                                                           , table_662518308_2.origin_zone                         AS origin_zone
                                                                                           , table_662518308_2.destination_zone                    AS destination_zone
                                                                                           , table_662518308_2.customer_description                AS customer_description
                                                                                           , table_662518308_2.carrier_description                 AS carrier_description
                                                                                           , table_662518308_2.actual_gi_date                      AS actual_gi_date
                                                                                           , table_662518308_2.country_to_description              AS country_to_description
                                                                                           , table_662518308_2.goods_receipt_posting_date          AS goods_receipt_posting_date
                                                                                           , table_662518308_2.created_date                        AS created_date
                                                                                           , table_662518308_2.freight_auction                     AS freight_auction
                                                                                           , table_662518308_2.destination_postal                  AS destination_postal
                                                                                           , table_662518308_2.customer_l8                         AS customer_l8
                                                                                           , table_662518308_2.distance_per_load                   AS distance_per_load
                                                                                           , table_662518308_2.ship_to_description                 AS ship_to_description
                                                                                           , table_662518308_2.shipping_point_code                 AS shipping_point_code
                                                                                           , table_662518308_2.origin_sf                           AS origin_sf
                                                                                           , table_662518308_2.destination_sf                      AS destination_sf
                                                                                           , table_662518308_2.truckload_vs_intermodal_val         AS truckload_vs_intermodal_val
                                                                                           , table_662518308_2.historical_data_structure_flag      AS historical_data_structure_flag
                                                                                           , table_662518308_2.ship_to_id                          AS ship_to_id
                                                                                   FROM
                                                                                             (
                                                                                                    SELECT *
                                                                                                         , (coalesce(Freight_Auction_Costs,0) + coalesce(CNC_Costs,0) + coalesce(Line_Haul_Final,0) + coalesce(accessorials,0)+ coalesce(fuel,0) + coalesce(Unsourced_Costs,0)) AS Total_Cost
                                                                                                    FROM
                                                                                                           (
                                                                                                                  SELECT *
                                                                                                                       , CASE
                                                                                                                                WHEN (
                                                                                                                                              lh_rules = 2
                                                                                                                                              AND (
                                                                                                                                                     fa_a          = 0
                                                                                                                                                     OR fa_a IS NULL
                                                                                                                                              )
                                                                                                                                       )
                                                                                                                                       THEN (coalesce(dst,0)+coalesce(exm,0)+coalesce(dltl,0)+coalesce(fa_a,0)+coalesce(scs,0)+coalesce(ftp,0)+coalesce(tsc,0)+coalesce(flat,0)-Line_Haul_Final)
                                                                                                                                       ELSE 0
                                                                                                                         END AS CNC_Costs
                                                                                                                       , CASE
                                                                                                                                WHEN (
                                                                                                                                              lh_rules = 2
                                                                                                                                              AND (
                                                                                                                                                     fa_a    > 0
                                                                                                                                                     or fa_a < 0
                                                                                                                                              )
                                                                                                                                       )
                                                                                                                                       THEN (coalesce(fa_a,0) + coalesce(dst,0)+coalesce(exm,0)+coalesce(dltl,0)+coalesce(scs,0)+coalesce(ftp,0)+coalesce(tsc,0)+coalesce(flat,0)-Line_Haul_Final)
                                                                                                                                       ELSE 0
                                                                                                                         END AS freight_auction_costs
                                                                                                                  FROM
                                                                                                                         (
                                                                                                                                SELECT *
                                                                                                                                     , CASE
                                                                                                                                              WHEN LH_Rules = 0
                                                                                                                                                     THEN coalesce(dst,0)+coalesce(exm,0)+coalesce(dltl,0)+coalesce(fa_a,0)+coalesce(scs,0)+coalesce(ftp,0)+coalesce(tsc,0)+coalesce(flat,0)
                                                                                                                                                     ELSE 0
                                                                                                                                       END AS Unsourced_Costs
                                                                                                                                     , CASE
                                                                                                                                              WHEN LH_Rules = 1
                                                                                                                                                     THEN coalesce(dst,0)+coalesce(exm,0)+coalesce(dltl,0)+coalesce(fa_a,0)+coalesce(scs,0)+coalesce(ftp,0)+coalesce(tsc,0)+coalesce(flat,0)
                                                                                                                                              WHEN LH_Rules = 2
                                                                                                                                                     THEN weighted_average_rate_adjusted*distance_per_load_dec*steps_ratio_dec
                                                                                                                                                     ELSE 0
                                                                                                                                       END AS Line_Haul_Final
                                                                                                                                FROM
                                                                                                                                       (
                                                                                                                                              SELECT *
                                                                                                                                                   , CASE
                                                                                                                                                            WHEN load_id = "MISSING"
                                                                                                                                                                   THEN 1
                                                                                                                                                            WHEN freight_type           = "Customer"
                                                                                                                                                                   AND service_tms_code = "DSD"
                                                                                                                                                                   THEN 1
                                                                                                                                                            WHEN service_tms_code IS NULL
                                                                                                                                                                   OR service_tms_code IN ("Null"
                                                                                                                                                                                         , ""
                                                                                                                                                                                         , "HCPU"
                                                                                                                                                                                         , "LTL"
                                                                                                                                                                                         , "LTL7"
                                                                                                                                                                                         , "MES6"
                                                                                                                                                                                         , "METL"
                                                                                                                                                                                         , "TLOC")
                                                                                                                                                                   THEN 1
                                                                                                                                                            WHEN country_to = "MX"
                                                                                                                                                                   THEN 1
                                                                                                                                                            WHEN weighted_average_rate IS NULL
                                                                                                                                                                   THEN 0
                                                                                                                                                            WHEN weighted_average_rate = 0
                                                                                                                                                                   THEN 1
                                                                                                                                                            WHEN weighted_average_rate > 0
                                                                                                                                                                   THEN 2
                                                                                                                                                                   ELSE 3
                                                                                                                                                     END                                                       AS LH_Rules
                                                                                                                                                   , (weighted_average_rate/cost_ratio)                        AS weighted_average_rate_adjusted_long
                                                                                                                                                   , (accrual_cost         /cost_ratio)                        AS accrual_cost_adjusted
                                                                                                                                                   , coalesce(accessorials, 0.00000000)                        as accessorial_costs
                                                                                                                                                   , cast((weighted_average_rate/cost_ratio) as DECIMAL(30,8)) AS weighted_average_rate_adjusted
                                                                                                                                                   , cast(distance_per_load as                  DECIMAL(30,8)) AS distance_per_load_dec
                                                                                                                                                   , cast(steps_ratio as                        DECIMAL(30,8)) AS steps_ratio_dec
                                                                                                                                              FROM
                                                                                                                                                     (
                                                                                                                                                              SELECT
                                                                                                                                                                       T.load_id
                                                                                                                                                                     , T.subsector
                                                                                                                                                                     , T.su_per_load
                                                                                                                                                                     , T.cost_ratio
                                                                                                                                                                     , T.accrual_cost
                                                                                                                                                                     , T.service_tms_code
                                                                                                                                                                     , T.country_to
                                                                                                                                                                     , T.distance_per_load
                                                                                                                                                                     , T.weighted_average_rate
                                                                                                                                                                     , T.steps_ratio
                                                                                                                                                                     , T.freight_type
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='DLTL'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS dltl
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='SCS'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS scs
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='FTP'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS ftp
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='DST'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS dst
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='TSC'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS tsc
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='FUEL'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS fuel
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='FLAT'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS flat
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='EXM'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS exm
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='FA_A'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS fa_a
                                                                                                                                                                     , SUM
                                                                                                                                                                                (
                                                                                                                                                                                         CASE
                                                                                                                                                                                                  WHEN cost_grouping='Accessorials'
                                                                                                                                                                                                           THEN total_cost_usd
                                                                                                                                                                                         END
                                                                                                                                                                                )
                                                                                                                                                                       AS accessorials
                                                                                                                                                              FROM
                                                                                                                                                                       (
                                                                                                                                                                                 SELECT
                                                                                                                                                                                           table_2036977015_1.load_id                    AS load_id
                                                                                                                                                                                         , table_2036977015_1.subsector                  AS subsector
                                                                                                                                                                                         , table_2036977015_1.cost_grouping              AS cost_grouping
                                                                                                                                                                                         , table_2036977015_1.total_cost_usd             AS total_cost_usd
                                                                                                                                                                                         , table_2036977015_2.avg_su_                    AS su_per_load
                                                                                                                                                                                         , table_2036977015_2.max_cost_ratio_            AS cost_ratio
                                                                                                                                                                                         , table_2036977015_2.sum_accrual_cost_          AS accrual_cost
                                                                                                                                                                                         , table_2036977015_2.max_service_tms_code_      AS service_tms_code
                                                                                                                                                                                         , table_2036977015_2.max_country_to_            AS country_to
                                                                                                                                                                                         , table_2036977015_2.min_distance_per_load_     AS distance_per_load
                                                                                                                                                                                         , table_2036977015_2.max_weighted_average_rate_ AS weighted_average_rate
                                                                                                                                                                                         , table_2036977015_2.steps_ratio                AS steps_ratio
                                                                                                                                                                                         , table_2036977015_2.max_freight_type_          AS freight_type
                                                                                                                                                                                 FROM
                                                                                                                                                                                           ds_subc_v14_calcs AS table_2036977015_1
                                                                                                                                                                                           LEFT JOIN
                                                                                                                                                                                                     ds_subc_v14_calcs_2 AS table_2036977015_2
                                                                                                                                                                                                     ON
                                                                                                                                                                                                               table_2036977015_1.load_id      =table_2036977015_2.load_id
                                                                                                                                                                                                               AND table_2036977015_1.subsector=table_2036977015_2.subsector
                                                                                                                                                                       )
                                                                                                                                                                       T
                                                                                                                                                              GROUP BY
                                                                                                                                                                       T.load_id
                                                                                                                                                                     , T.subsector
                                                                                                                                                                     , T.su_per_load
                                                                                                                                                                     , T.cost_ratio
                                                                                                                                                                     , T.accrual_cost
                                                                                                                                                                     , T.service_tms_code
                                                                                                                                                                     , T.country_to
                                                                                                                                                                     , T.distance_per_load
                                                                                                                                                                     , T.weighted_average_rate
                                                                                                                                                                     , T.steps_ratio
                                                                                                                                                                     , T.freight_type
                                                                                                                                                     )
                                                                                                                                                     conditions1
                                                                                                                                       )
                                                                                                                                       conditions2
                                                                                                                         )
                                                                                                                         conditional3
                                                                                                           )
                                                                                                           totescosts
                                                                                             )
                                                                                             table_662518308_1
                                                                                             LEFT JOIN
                                                                                                       (
                                                                                                                SELECT
                                                                                                                         load_id
                                                                                                                       , subsector
                                                                                                                       , MAX(table_1258021560.origin_zone)                 AS origin_zone
                                                                                                                       , MAX(table_1258021560.destination_zone)            AS destination_zone
                                                                                                                       , MAX(table_1258021560.customer_description)        AS customer_description
                                                                                                                       , MAX(table_1258021560.carrier_description)         AS carrier_description
                                                                                                                       , MAX(table_1258021560.actual_gi_date)              AS actual_gi_date
                                                                                                                       , MAX(table_1258021560.country_to)                  AS country_to
                                                                                                                       , MAX(table_1258021560.country_to_description)      AS country_to_description
                                                                                                                       , MAX(table_1258021560.goods_receipt_posting_date)  AS goods_receipt_posting_date
                                                                                                                       , MAX(table_1258021560.created_date)                AS created_date
                                                                                                                       , MAX(table_1258021560.freight_auction)             AS freight_auction
                                                                                                                       , MAX(table_1258021560.destination_postal)          AS destination_postal
                                                                                                                       , MAX(table_1258021560.customer_l8)                 AS customer_l8
                                                                                                                       , MAX(table_1258021560.distance_per_load)           AS distance_per_load
                                                                                                                       , MAX(table_1258021560.service_tms_code)            AS service_tms_code
                                                                                                                       , MAX(table_1258021560.ship_to_description)         AS ship_to_description
                                                                                                                       , MAX(table_1258021560.max_hist_num)                AS max_hist_num
                                                                                                                       , MAX(table_1258021560.shipping_point_code)         AS shipping_point_code
                                                                                                                       , MAX(table_1258021560.final_ship_to)               AS final_ship_to
                                                                                                                       , MAX(table_1258021560.origin_sf)                   AS origin_sf
                                                                                                                       , MAX(table_1258021560.destination_sf)              AS destination_sf
                                                                                                                       , MAX(table_1258021560.truckload_vs_intermodal_val) AS truckload_vs_intermodal_val
                                                                                                                       , MAX
                                                                                                                                  (
                                                                                                                                           case
                                                                                                                                                    when max_hist_num = 1
                                                                                                                                                             then "-"
                                                                                                                                                             else ""
                                                                                                                                           end
                                                                                                                                  )
                                                                                                                         as historical_data_structure_flag
                                                                                                                       , MAX
                                                                                                                                  (
                                                                                                                                           case
                                                                                                                                                    when final_ship_to = '0'
                                                                                                                                                             then ""
                                                                                                                                                             else cast(final_ship_to as string)
                                                                                                                                           end
                                                                                                                                  )
                                                                                                                         as ship_to_id
                                                                                                                FROM
                                                                                                                         (
                                                                                                                                   SELECT
                                                                                                                                             table_1030309172_1.load_id                                                                           AS load_id
                                                                                                                                           , table_1030309172_1.origin_zone                                                                       AS origin_zone
                                                                                                                                           , table_1030309172_1.destination_zone                                                                  AS destination_zone
                                                                                                                                           , table_1030309172_1.shipping_point_code                                                               AS shipping_point_code
                                                                                                                                           , table_1030309172_1.customer_description                                                              AS customer_description
                                                                                                                                           , table_1030309172_1.carrier_description                                                               AS carrier_description
                                                                                                                                           , table_1030309172_1.actual_gi_date                                                                    AS actual_gi_date
                                                                                                                                           , table_1030309172_1.country_to                                                                        AS country_to
                                                                                                                                           , table_1030309172_1.subsector                                                                         AS subsector
                                                                                                                                           , table_1030309172_1.country_to_description                                                            AS country_to_description
                                                                                                                                           , table_1030309172_1.goods_receipt_posting_date                                                        AS goods_receipt_posting_date
                                                                                                                                           , table_1030309172_1.created_date                                                                      AS created_date
                                                                                                                                           , table_1030309172_1.max_ship_to                                                                       AS max_ship_to
                                                                                                                                           , table_1030309172_1.freight_auction                                                                   AS freight_auction
                                                                                                                                           , table_1030309172_1.max_hist_num                                                                      AS max_hist_num
                                                                                                                                           , table_1030309172_1.destination_postal                                                                AS destination_postal
                                                                                                                                           , table_1030309172_1.customer_l8                                                                       AS customer_l8
                                                                                                                                           , table_1030309172_1.truckload_vs_intermodal_val                                                       AS truckload_vs_intermodal_val
                                                                                                                                           , table_1030309172_1.distance_per_load                                                                 AS distance_per_load
                                                                                                                                           , table_1030309172_1.service_tms_code                                                                  AS service_tms_code
                                                                                                                                           , table_1030309172_1.origin_sf                                                                         AS origin_sf
                                                                                                                                           , table_1030309172_1.destination_sf                                                                    AS destination_sf
                                                                                                                                           , table_1030309172_1.ship_to_description                                                               AS ship_to_description
                                                                                                                                           , table_1030309172_2.destination_location_id                                                           AS destination_location_id
                                                                                                                                           , coalesce(table_1030309172_2.destination_location_id, cast(table_1030309172_1.max_ship_to as STRING)) as final_ship_to
                                                                                                                                   FROM
                                                                                                                                             (
                                                                                                                                                       SELECT
                                                                                                                                                                 table_1243140996_1.load_id                     AS load_id
                                                                                                                                                               , table_1243140996_1.origin_zone_new             AS origin_zone
                                                                                                                                                               , table_1243140996_1.destination_zone_new        AS destination_zone
                                                                                                                                                               , table_1243140996_1.shipping_point_code         AS shipping_point_code
                                                                                                                                                               , table_1243140996_1.customer_description        AS customer_description
                                                                                                                                                               , table_1243140996_1.carrier_description         AS carrier_description
                                                                                                                                                               , table_1243140996_1.actual_gi_date              AS actual_gi_date
                                                                                                                                                               , table_1243140996_1.country_to                  AS country_to
                                                                                                                                                               , table_1243140996_1.subsector                   AS subsector
                                                                                                                                                               , table_1243140996_1.country_to_description      AS country_to_description
                                                                                                                                                               , table_1243140996_1.goods_receipt_posting_date  AS goods_receipt_posting_date
                                                                                                                                                               , table_1243140996_1.created_date                AS created_date
                                                                                                                                                               , table_1243140996_1.max_ship_to                 AS max_ship_to
                                                                                                                                                               , table_1243140996_1.freight_auction             AS freight_auction
                                                                                                                                                               , table_1243140996_1.max_hist_num                AS max_hist_num
                                                                                                                                                               , table_1243140996_1.destination_postal          AS destination_postal
                                                                                                                                                               , table_1243140996_1.customer_l8                 AS customer_l8
                                                                                                                                                               , table_1243140996_1.truckload_vs_intermodal_val AS truckload_vs_intermodal_val
                                                                                                                                                               , table_1243140996_1.distance_per_load           AS distance_per_load
                                                                                                                                                               , table_1243140996_1.service_tms_code            AS service_tms_code
                                                                                                                                                               , table_1243140996_1.origin_sf                   AS origin_sf
                                                                                                                                                               , table_1243140996_1.destination_sf              AS destination_sf
                                                                                                                                                               , table_1243140996_2.ship_to_description         AS ship_to_description
                                                                                                                                                       FROM
                                                                                                                                                                 ds_subc_v14_max AS table_1243140996_1
                                                                                                                                                                 LEFT JOIN
                                                                                                                                                                           ds_subc_v14_ship_to AS table_1243140996_2
                                                                                                                                                                           ON
                                                                                                                                                                                     table_1243140996_1.max_ship_to=table_1243140996_2.ship_to
                                                                                                                                             )
                                                                                                                                             table_1030309172_1
                                                                                                                                             LEFT JOIN
                                                                                                                                                       ds_subc_v14_dest_id AS table_1030309172_2
                                                                                                                                                       ON
                                                                                                                                                                 table_1030309172_1.load_id=table_1030309172_2.new_load_id
                                                                                                                         )
                                                                                                                         table_1258021560
                                                                                                                GROUP BY
                                                                                                                         load_id
                                                                                                                       , subsector
                                                                                                       )
                                                                                                       table_662518308_2
                                                                                                       ON
                                                                                                                 table_662518308_1.load_id      =table_662518308_2.load_id
                                                                                                                 AND table_662518308_1.subsector=table_662518308_2.subsector
                                                                         )
                                                                         table_370155390_2
                                                                         ON
                                                                                    COALESCE(table_370155390_1.destination_postal_update, 'XX')=COALESCE(table_370155390_2.destination_postal, 'XX')
                                        )
                                        table_179224308_2
                                        ON
                                                   table_179224308_1.shipping_point_code=table_179224308_2.shipping_point_code
       )
       table_377828182
;


Drop table IF EXISTS ds_subsectorcosts_v14_u
;

Create TEMPORARY VIEW ds_subsectorcosts_v14_u as
select * from ds_subsectorcosts_v14
UNION ALL
select
       load_id
     , su_per_load
     , service_tms_code
     , country_to
     , freight_type
     , dltl
     , scs
     , ftp
     , dst
     , tsc
     , fuel
     , flat
     , exm
     , fa_a
     , lh_rules
     , weighted_average_rate_adjusted_long
     , accrual_cost_adjusted
     , accessorial_costs
     , weighted_average_rate_adjusted
     , distance_per_load_dec
     , steps_ratio_dec
     , unsourced_costs
     , line_haul_final
     , cnc_costs
     , freight_auction_costs
     , total_cost
     , customer_description
     , carrier_description
     , actual_gi_date
     , country_to_description
     , goods_receipt_posting_date
     , created_date
     , freight_auction
     , destination_postal
     , customer_l8
     , cast(distance_per_load as STRING) as distance_per_load
     , ship_to_description
     , origin_sf
     , destination_sf
     , truckload_vs_intermodal_val
     , historical_data_structure_flag
     , ship_to_id
     , destination_zone_new
     , origin_zone
     , fy
     , subsector
     , cast(voucher_id as STRING) as voucher_id
from
       ds_subsectorcosts_noloadid_v13
;


INSERT OVERWRITE table tfs_subsector_cost_star
select *
     , case
              when origin_zone = 'origin zone'
                     then 'Undefined'
                     else origin_zone
       end as origin_zone_v1
     , case
              when destination_zone_new = ''
                     then 'Undefined'
              when destination_zone_new is NULL
                     then 'Undefined'
              when destination_zone_new = 'NULL'
                     then 'Undefined'
                     else destination_zone_new
       end as destination_zone_v1
from
       ds_subsectorcosts_v14_u
;
