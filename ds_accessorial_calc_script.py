import logging
import sys

from pg_composite_pipelines_configuration.configuration import Configuration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType

from pg_tw_fa_artemis.common import get_dbutils, get_spark

def insertTfsAcsrlStarTask(spark, logger, source_db_name, target_db_name, target_table):
    query = f"""
    SELECT DISTINCT
        `load id`                                              AS load_id,
        `freight cost charge`                                  AS freight_cost_charge,
        `tdc val code`                                         AS tdc_val_code,
        `tdc val description`                                  AS tdc_val_description,
        `tms freight charge description`                       AS tms_freight_charge_description,
        `freight type`                                         AS freight_type,
        `accessorial reason`                                   AS accessorial_reason,
        CAST(`total transportation cost usd` AS DECIMAL(30,4)) AS total_accessorial_cost_usd,
        `origin zone`                                          AS origin_zone,
        `destination zone`                                     AS destination_zone,
        `customer description`                                 AS customer_description,
        `carrier description`                                  AS carrier_description,
        `actual gi date`                                       AS actual_gi_date,
        `country to`                                           AS country_to,
        `subsector description`                                AS subsector,
        `country to description`                               AS country_to_description,
        `goods receipt posting date`                           AS goods_receipt_posting_date,
        `created date`                                         AS created_date,
        `ship to description`                                  AS ship_to_description,
        `freight auction`                                      AS freight_auction,
        `historical data structure flag`                       AS historical_data_structure_flag,
        `destination postal`                                   AS destination_postal,
        `customer l8`                                          AS customer_l8,
        `truckload_vs_intermodal_val`                          AS truckload_vs_intermodal,
        `origin sf`                                            AS origin_sf,
        `destination sf`                                       AS destination_sf,
        REGEXP_REPLACE(REGEXP_REPLACE(`subsector description`, ' SUB-SECTOR', ''), ' SUBSECTOR', '') AS subsector_update,
        CASE
            WHEN MONTH(TO_DATE(`actual gi date`)) < 7 THEN
                CONCAT('FY',
                       SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`)) - 1 AS STRING),
                              LENGTH(CAST(YEAR(TO_DATE(`actual gi date`)) - 1 AS STRING)) - 1),
                       SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`)) AS STRING),
                              LENGTH(CAST(YEAR(TO_DATE(`actual gi date`)) AS STRING)) - 1))
            ELSE
                CONCAT('FY',
                       SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`)) AS STRING),
                              LENGTH(CAST(YEAR(TO_DATE(`actual gi date`)) AS STRING)) - 1),
                       SUBSTR(CAST(YEAR(TO_DATE(`actual gi date`)) + 1 AS STRING),
                              LENGTH(CAST(YEAR(TO_DATE(`actual gi date`)) + 1 AS STRING)) - 1))
        END AS FY
    FROM {source_db_name}.tfs
    WHERE TO_DATE(`actual gi date`) <= DATE_SUB(CURRENT_TIMESTAMP(), 14)
      AND `freight cost charge` NOT IN (
        'FA_A', 'FSUR', 'FU_S', 'FLTL', 'FFLT', 'FCHG', 'FUSU', 'DST', 'SCS', 'FTP', 'TSC',
        'DIST', 'DLTL', 'FLAT', 'SPOT', 'CVYI', 'HJBT', 'KNIG', 'L2D', 'SCNN', 'UFLB', 'USXI', 'PGLI')
    """

    result_df = spark.sql(query)

    result_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{target_db_name}.{target_table}"
    )

    logger.info(
        "Data has been successfully loaded into {}.{}".format(
            target_db_name, target_table
        )
    )

    return 0

def main():
    spark = get_spark()
    dbutils = get_dbutils()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    config = Configuration.load_for_default_environment(__file__, dbutils)

    source_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"
    target_db_name = f"{config['catalog-name']}.{config['schema-name']}"
    target_table = config['tables']['tfs_acsrl_star']

    insertTfsAcsrlStarTask(
        spark=spark,
        logger=logger,
        source_db_name=source_db_name,
        target_db_name=target_db_name,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()
