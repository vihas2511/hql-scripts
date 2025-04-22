'''Xxxxx
'''

import pyspark.sql.functions as f
import utils

def load_prod_uom_star(
        logging, g11_db_name, target_db_name, target_table, debug_mode_ind, debug_postfix):
    ''' '''
    logging.info("Started loading prod_uom_star table")
    spark_session = utils.get_spark_session(
        logging, 'prod_uom_star_{}', 'yarn', []
        )
    #spark_session.conf.set("parquet.compression", "SNAPPY")

    #Remove debug tables (if they are)
    utils.removeDebugTables(
        logging, spark_session, target_db_name,
        debug_mode_ind, debug_postfix)
    
    #Get G11 prod_dim (MARA) data
    ### TODO :  co gdy w 1 tabelce sa poprzedzajace 0 a w drugiej nie ma? Do decyzji biznesu
    g11_prod_dim = spark_session.read.table("{}.mara".format(g11_db_name)).select("matnr", "meins", "mtart").withColumnRenamed("matnr", "prod_id").withColumnRenamed("meins", "base_uom").withColumnRenamed("mtart", "prod_type_code").where("simp_chng_type_code != 'D'")
    
    #Get G11 prod_uom_lkp (MARM) data
    # prod_uom_lkp_sel_cols = [
    #   "prod_id", "stock_keep_alt_uom", "numerator_alt_uom_buom_factor", "denominator_alt_uom_buom_factor", "ean_id", "ean_upc_id", "ean_categ_code", 
    #   "length_val", "width_val", "height_val", "dimension_uom", "vol_qty", "vol_uom", "gross_weight_val", "weight_uom", "lower_lvl_pkg_uom", 
    #   "internal_chartrc_num", "uom_sort_num", "lead_batch_uom_flag", "vltn_flag", "uom_usage_code", "chartrc_uom", "generic_prod_code", 
    #   "gtin_variant_code", "nesting_vol_pct", "max_stack_factor", "capacity_usage_val", "ewm_cw_uom_categ_code", "inactvtn_date"
    # ]

    prod_uom_lkp_sel_cols = [
      "matnr", "meinh", "umrez", "umren", "eannr", "ean11", "numtp", 
      "laeng", "breit", "hoehe", "meabm", "volum", "voleh", "brgew", "gewei", "mesub", 
      "atinn", "mesrt", "xfhdw", "xbeww", "kzwso", "msehi", "bflme_marm", 
      "gtin_variant", "nest_ftr", "max_stack", "capause", "ty2tq", "zzdeactdat"
    ]


    # g11_prod_uom_lkp = spark_session.read.table("{}.prod_uom_lkp".format(g11_db_name))\
    #     .select(prod_uom_lkp_sel_cols).where("curr_ind = 'Y'").where("simp_chng_type_code != 'D'")

    g11_prod_uom_lkp = spark_session.table("{}.marm".format(g11_db_name))\
        .select(prod_uom_lkp_sel_cols).withColumnRenamed("matnr", "prod_id").withColumnRenamed("meinh", "stock_keep_alt_uom").withColumnRenamed("umrez", "numerator_alt_uom_buom_factor")\
            .withColumnRenamed("umren", "denominator_alt_uom_buom_factor").withColumnRenamed("eannr", "ean_id").withColumnRenamed("ean11", "ean_upc_id")\
            .withColumnRenamed("numtp", "ean_categ_code").withColumnRenamed("laeng", "length_val").withColumnRenamed("breit", "width_val").withColumnRenamed("hoehe", "height_val")\
            .withColumnRenamed("meabm", "dimension_uom").withColumnRenamed("volum", "vol_qty").withColumnRenamed("voleh", "vol_uom").withColumnRenamed("brgew", "gross_weight_val")\
            .withColumnRenamed("gewei", "weight_uom").withColumnRenamed("mesub", "lower_lvl_pkg_uom").withColumnRenamed("atinn", "internal_chartrc_num")\
            .withColumnRenamed("mesrt", "uom_sort_num").withColumnRenamed("xfhdw", "lead_batch_uom_flag").withColumnRenamed("xbeww", "vltn_flag")\
            .withColumnRenamed("kzwso", "uom_usage_code").withColumnRenamed("msehi", "chartrc_uom").withColumnRenamed("bflme_marm", "generic_prod_code")\
            .withColumnRenamed("gtin_variant", "gtin_variant_code").withColumnRenamed("nest_ftr", "nesting_vol_pct").withColumnRenamed("max_stack", "max_stack_factor")\
            .withColumnRenamed("capause", "capacity_usage_val").withColumnRenamed("ty2tq", "ewm_cw_uom_categ_code").withColumnRenamed("zzdeactdat", "inactvtn_date")\
            .where("simp_chng_type_code != 'D'")
    
    #Get G11 prod_attr_value_assign_dim (ZTXXPT0104) data, we need only TDCVAL value. It contains only product attribute id
    #g11_ztxxpt0104_tdcval = spark_session.read.table("{}.prod_attr_value_assign_dim".format(g11_db_name)).where("attr_type_id= 'TDC_VAL'").select("prod_id", "attr_type_id", "attr_value_id")
    
    g11_ztxxpt0104_tdcval = spark_session.table("{}.ZTXXPT0104".format(g11_db_name)).where("zattrtypid= 'TDC_VAL'").select("matnr", "zattrtypid", "zattrvalid")\
        .withColumnRenamed("matnr","prod_id").withColumnRenamed("zattrtypid","attr_type_id").withColumnRenamed("zattrvalid" , "attr_value_id")

    #Get G11 prod_attr_value_lkp (ZTXXPT0103) data. It contains product attribute code & description
    g11_ztxxpt0103_lkp = spark_session.read.table("{}.ZTXXPT0103".format(g11_db_name)).select("ZATTRTYPID", "ZATTRVALID", "ZATTRVALAB", "ZATTRVALDC")\
        .withColumnRenamed("ZATTRTYPID","attr_type_id").withColumnRenamed("ZATTRVALID","attr_value_id").withColumnRenamed("ZATTRVALAB", "attr_value_abbr_code")\
        .withColumnRenamed("ZATTRVALDC" , "attr_value_desc")
    
    #Join product attribute ID with code & description
    g11_zttxpt_tdcval = g11_ztxxpt0104_tdcval.join(g11_ztxxpt0103_lkp, ["attr_type_id", "attr_value_id"], 'inner').drop("attr_type_id", "attr_value_id")\
        .withColumn("tdcval_code", f.col("attr_value_abbr_code").substr(1, 5))\
        .withColumn("tdcval_short_desc", f.col("attr_value_abbr_code").substr(f.lit(6), f.length("attr_value_abbr_code") - 5)).drop("attr_value_abbr_code")\
        .withColumnRenamed("attr_value_desc", "tdcval_name")

    #Select columns in prod_uom_star in proper order for insertion
    target_table_name = '{}.{}'.format(target_db_name, target_table)
    target_columns = spark_session.read.table('{}'.format(target_table_name)).columns
    
    #Get final DF - join prod_dim with UoM & TDCVal code & description
    time_zone = spark_session.conf.get("spark.sql.session.timeZone")
    prod_uom_star_df = g11_prod_dim.join(g11_prod_uom_lkp, 'prod_id', 'full_outer')\
        .join(g11_zttxpt_tdcval, 'prod_id', 'full_outer')\
        .withColumn(
            "last_update_utc_tmstp", 
            f.to_utc_timestamp(f.current_timestamp(), time_zone)
        ).select(target_columns)

    prod_uom_star_filter_df = prod_uom_star_df.distinct()

    #Insert into final table
    prod_uom_star_filter_df.write.format("delta").insertInto(tableName=target_table_name,  overwrite=True)
    logging.info("Loading sortable MARM has finished")
    return 0