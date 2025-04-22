import logging
from typing import Dict, List, Optional, Union

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pg_composite_pipelines_configuration.configuration import Configuration

from pg_tw_fa_marm_reporting.common import get_dbutils, get_spark


def load_prod_uom_star(spark, logger, g11_db_name, target_db_name, target_table):

    g11_prod_dim = (
        spark.read.table("{}.mara".format(g11_db_name))
        .select("matnr", "meins", "mtart")
        .withColumnRenamed("matnr", "prod_id")
        .withColumnRenamed("meins", "base_uom")
        .withColumnRenamed("mtart", "prod_type_code")
        .where("simp_chng_type_code != 'D'")
    )

    prod_uom_lkp_sel_cols = [
        "matnr",
        "meinh",
        "umrez",
        "umren",
        "eannr",
        "ean11",
        "numtp",
        "laeng",
        "breit",
        "hoehe",
        "meabm",
        "volum",
        "voleh",
        "brgew",
        "gewei",
        "mesub",
        "atinn",
        "mesrt",
        "xfhdw",
        "xbeww",
        "kzwso",
        "msehi",
        "bflme_marm",
        "gtin_variant",
        "nest_ftr",
        "max_stack",
        "capause",
        "ty2tq",
        "zzdeactdat",
    ]

    g11_prod_uom_lkp = (
        spark.table("{}.marm".format(g11_db_name))
        .select(prod_uom_lkp_sel_cols)
        .withColumnRenamed("matnr", "prod_id")
        .withColumnRenamed("meinh", "stock_keep_alt_uom")
        .withColumnRenamed("umrez", "numerator_alt_uom_buom_factor")
        .withColumnRenamed("umren", "denominator_alt_uom_buom_factor")
        .withColumnRenamed("eannr", "ean_id")
        .withColumnRenamed("ean11", "ean_upc_id")
        .withColumnRenamed("numtp", "ean_categ_code")
        .withColumnRenamed("laeng", "length_val")
        .withColumnRenamed("breit", "width_val")
        .withColumnRenamed("hoehe", "height_val")
        .withColumnRenamed("meabm", "dimension_uom")
        .withColumnRenamed("volum", "vol_qty")
        .withColumnRenamed("voleh", "vol_uom")
        .withColumnRenamed("brgew", "gross_weight_val")
        .withColumnRenamed("gewei", "weight_uom")
        .withColumnRenamed("mesub", "lower_lvl_pkg_uom")
        .withColumnRenamed("atinn", "internal_chartrc_num")
        .withColumnRenamed("mesrt", "uom_sort_num")
        .withColumnRenamed("xfhdw", "lead_batch_uom_flag")
        .withColumnRenamed("xbeww", "vltn_flag")
        .withColumnRenamed("kzwso", "uom_usage_code")
        .withColumnRenamed("msehi", "chartrc_uom")
        .withColumnRenamed("bflme_marm", "generic_prod_code")
        .withColumnRenamed("gtin_variant", "gtin_variant_code")
        .withColumnRenamed("nest_ftr", "nesting_vol_pct")
        .withColumnRenamed("max_stack", "max_stack_factor")
        .withColumnRenamed("capause", "capacity_usage_val")
        .withColumnRenamed("ty2tq", "ewm_cw_uom_categ_code")
        .withColumnRenamed("zzdeactdat", "inactvtn_date")
        .where("simp_chng_type_code != 'D'")
    )

    g11_ztxxpt0104_tdcval = (
        spark.table("{}.ZTXXPT0104".format(g11_db_name))
        .where("zattrtypid= 'TDC_VAL'")
        .select("matnr", "zattrtypid", "zattrvalid")
        .withColumnRenamed("matnr", "prod_id")
        .withColumnRenamed("zattrtypid", "attr_type_id")
        .withColumnRenamed("zattrvalid", "attr_value_id")
    )

    logger.info("Getting G11 prod_attr_value_lkp (ZTXXPT0103) data...\n")

    # Get G11 prod_attr_value_lkp (ZTXXPT0103) data. It contains product attribute code & description
    g11_ztxxpt0103_lkp = (
        spark.read.table("{}.ZTXXPT0103".format(g11_db_name))
        .select("ZATTRTYPID", "ZATTRVALID", "ZATTRVALAB", "ZATTRVALDC")
        .withColumnRenamed("ZATTRTYPID", "attr_type_id")
        .withColumnRenamed("ZATTRVALID", "attr_value_id")
        .withColumnRenamed("ZATTRVALAB", "attr_value_abbr_code")
        .withColumnRenamed("ZATTRVALDC", "attr_value_desc")
    )

    logger.info("Joining product attribute ID with code & description...\n")

    # Join product attribute ID with code & description
    g11_zttxpt_tdcval = (
        g11_ztxxpt0104_tdcval.join(
            g11_ztxxpt0103_lkp, ["attr_type_id", "attr_value_id"], "inner"
        )
        .drop("attr_type_id", "attr_value_id")
        .withColumn("tdcval_code", f.col("attr_value_abbr_code").substr(1, 5))
        .withColumn(
            "tdcval_short_desc",
            f.col("attr_value_abbr_code").substr(
                f.lit(6), f.length("attr_value_abbr_code") - 5
            ),
        )
        .drop("attr_value_abbr_code")
        .withColumnRenamed("attr_value_desc", "tdcval_name")
    )
    logger.info("Selecting columns in prod_uom_star in proper order for insertion...\n")

    # Select columns in prod_uom_star in proper order for insertion
    target_table_name = "{}.{}".format(target_db_name, target_table)
    target_columns = spark.read.table("{}".format(target_table_name)).columns

    logger.info(
        "Getting final DF - join prod_dim with UoM & TDCVal code & description...\n"
    )

    # Get final DF - join prod_dim with UoM & TDCVal code & description
    time_zone = spark.conf.get("spark.sql.session.timeZone")
    prod_uom_star_df = (
        g11_prod_dim.join(g11_prod_uom_lkp, "prod_id", "full_outer")
        .join(g11_zttxpt_tdcval, "prod_id", "full_outer")
        .withColumn(
            "last_update_utc_tmstp",
            f.to_utc_timestamp(f.current_timestamp(), time_zone),
        )
        .select(target_columns)
    )

    prod_uom_star_filter_df = prod_uom_star_df.distinct()

    # Insert into final table
    prod_uom_star_filter_df.write.format("delta").insertInto(
        tableName=target_table_name, overwrite=True
    )
    logger.info(
        " Final UOM Star table data has been successfully loaded into %s....\n",
        f"{target_table_name}",
    )

    return 0


# to pass params in from the task, add them here
def main():
    spark = get_spark()
    dbutils = get_dbutils()

    config = Configuration.load_for_default_environment(__file__, dbutils)

    # catalog = config["catalog-name"]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # table_config = config["tables"][0]
    target_table = f"{config['tables']['prod_uom_star']}"

    spark = get_spark()
    g11_db_name = f"{config['src-catalog-name']}.{config['g11_db_name']}"
    schema = f"{config['catalog-name']}.{config['schema-name']}"

    logger.info("g11 source db name %s", g11_db_name)
    logger.info("target_table %s", target_table)
    logger.info("target DB Name %s", schema)

    load_prod_uom_star(
        spark=spark,
        logger=logger,
        g11_db_name=g11_db_name,
        target_db_name=schema,
        target_table=target_table,
    )


if __name__ == "__main__":
    # if you need to read params from your task/workflow, use sys.argv[] to retrieve them and pass them to main here
    # eg sys.argv[0] for first positional param
    main()