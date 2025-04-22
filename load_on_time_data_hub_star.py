'''Function loading the on_time_data_hub_star table
'''

import pyspark.sql.functions as f
from pyspark.sql import Window
import utils
from get_src_data import get_rds as rds, get_transfix as tvb
from load_otd import expr_on_time_data_hub as expr


def get_on_time_data_hub_star(logging, spark_session, rds_db_name, trans_vsblt_db_name, target_db_name,
                              staging_location, debug_mode_ind, debug_postfix):
    cust_dim_df = \
        rds.get_cust_dim(logging, spark_session, rds_db_name, target_db_name, staging_location, debug_mode_ind,
                         debug_postfix)

    trade_chanl_hier_dim_df = \
        rds.get_trade_chanl_hier_dim(logging, spark_session, rds_db_name, target_db_name, staging_location,
                                     debug_mode_ind, debug_postfix)

    ship_loc_df = \
        tvb.get_shipping_location_na_dim(logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location,
                                         debug_mode_ind, debug_postfix)\
        .drop("loc_name").drop("state_province_code").drop("postal_code")\
        .withColumnRenamed("loc_id", "ship_point_code")\
        .withColumnRenamed("origin_zone_ship_from_code", "sl_origin_zone_ship_from_code")

    utils.manageOutput(logging, spark_session, ship_loc_df, 1, "ship_loc_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    sambc_df = \
        tvb.get_sambc_master(logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location,
                             debug_mode_ind, debug_postfix) \
        .select("customer_lvl3_desc", "sambc_flag")

    utils.manageOutput(logging, spark_session, sambc_df, 1, "sambc_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    csot_bucket_final_df = \
        tvb.get_csot_bucket(logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location,
                            debug_mode_ind, debug_postfix).drop("load_id")\
        .withColumn("poloadid_join", f.col("poload_id"))\
        .withColumn("cust_po_num", f.col("poload_id")).withColumn("load_id", f.col("poload_id"))\
        .withColumnRenamed("cust_po_num", "pg_order_num").drop("cust_po_num")\
        .withColumnRenamed("poload_id", "poload_id_new").drop("poload_id")

    utils.manageOutput(logging, spark_session, csot_bucket_final_df, 1, "csot_bucket_final_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    actual_ship_time_df = tvb.get_otd_vfr_na_star(logging, spark_session, trans_vsblt_db_name, target_db_name,
                                                  staging_location, debug_mode_ind, debug_postfix) \
        .select("shpmt_id", "actual_ship_datetm").distinct()\
        .withColumn("load_id", f.expr(expr.load_id_expr)) \
        .withColumn("actual_ship_datetm", f.expr(expr.actual_ship_datetm_expr)).drop("shpmt_id")

    tms_unload_method_df = \
        tvb.get_tms_unload_method_dest_zone_lkp(logging, spark_session, trans_vsblt_db_name, target_db_name,
                                                staging_location, debug_mode_ind, debug_postfix)

    on_time_df = \
        tvb.get_on_time_arriv_shpmt_custshpmt_na_star(logging, spark_session, trans_vsblt_db_name, target_db_name,
                                                      staging_location, debug_mode_ind, debug_postfix) \
        .withColumn("load_id", f.substring(f.col('shpmt_id'), -9, 9))\
        .withColumn("str_carr_num", f.col('carr_num').cast("int"))\
        .withColumn("poloadid_join", f.concat(f.col('pg_order_num'), f.col('load_id'))) \
        .withColumn("poload_id", f.concat(f.col('pg_order_num'), f.col('load_id'))) \
        .withColumn("order_create_tmstp",
                    f.concat(f.col('order_create_date'), f.lit(' '), f.col('order_create_datetm'))) \
        .withColumn("schedule_tmstp",
                    f.concat(f.col('plan_shpmt_start_date'), f.lit(' '), f.col('plan_shpmt_start_datetm'))) \
        .withColumn("tender_tmstp", f.concat(f.col('tender_date'), f.lit(' '), f.col('tender_datetm'))) \
        .withColumn("final_lrdt_tmstp", f.concat(f.col('final_lrdt_date'), f.lit(' '), f.col('final_lrdt_datetm'))) \
        .withColumn("actual_load_end_tmstp",
                    f.concat(f.col('actual_load_end_date'), f.lit(' '), f.col('actual_load_end_datetm'))) \
        .withColumn("orig_request_dlvry_from_tmstp",
                    f.concat(f.col('orig_request_dlvry_from_date'), f.lit(' '), f.col('orig_request_dlvry_from_datetm'))) \
        .withColumn("orig_request_dlvry_to_tmstp",
                    f.concat(f.col('orig_request_dlvry_to_date'), f.lit(' '), f.col('orig_request_dlvry_to_datetm'))) \
        .withColumn("request_dlvry_from_tmstp",
                    f.concat(f.col('request_dlvry_from_date'), f.lit(' '), f.col('request_dlvry_from_datetm'))) \
        .withColumn("request_dlvry_to_tmstp",
                    f.concat(f.col('request_dlvry_to_date'), f.lit(' '), f.col('request_dlvry_to_datetm'))) \
        .withColumn("actual_dlvry_tmstp",
                    f.concat(f.col('actual_shpmt_end_date'), f.lit(' '), f.col('actual_shpmt_end_aot_datetm'))) \
        .withColumn("frt_type_desc", f.expr(expr.ci_code_expr)) \
        .withColumn("distance_bin_val", f.expr(expr.bin_code_expr)) \
        .withColumn("parent_shpmt_flag", f.expr(expr.flag_case_code_expr)) \
        .withColumn("shpmt_cnt", f.expr(expr.no_shpmt_expr)) \
        .withColumnRenamed("ship_to_party_id", "customer_id") \
        .withColumnRenamed("first_tendered_rdd_from", "first_tendered_rdd_from_datetm") \
        .withColumnRenamed("first_tendered_rdd_to", "first_tendered_rdd_to_datetm") \
        .withColumn("plan_shpmt_end_tmstp_calc",
                    f.concat(f.col('plan_shpmt_end_date'), f.lit(' '), f.col('plan_shpmt_end_aot_datetm'))) \
        .withColumn("min_event_datetm_rn",
                    f.row_number().over(
                        Window.partitionBy("load_id", "trnsp_stage_num").orderBy(f.col("event_datetm")))) \
        .withColumn("max_event_datetm_rn",
                    f.row_number().over(
                        Window.partitionBy("load_id", "trnsp_stage_num").orderBy(f.col("event_datetm").desc()))) \
        .withColumn("first_dttm", f.expr(expr.min_rn_code_expr)) \
        .withColumn("last_dttm", f.expr(expr.max_rn_code_expr))

    utils.manageOutput(logging, spark_session, on_time_df, 1, "on_time_df", target_db_name, staging_location,
                        debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    tfs_df = \
        tvb.get_tfs(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                    debug_postfix) \
        .select("shpmt_id", "freight_auction_val").filter('freight_auction_val = "YES"')\
        .withColumn("load_id", f.regexp_replace("shpmt_id", '^0', '')) \
        .drop("shpmt_id").distinct()

    tac_df = \
        tvb.get_tac(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                    debug_postfix) \
        .withColumn("load_id", f.regexp_replace("load_id", '^0', ''))\
        .drop("origin_zone_ship_from_code").drop("origin_loc_id").drop("carr_desc")\
        .drop("carr_mode_code", "tender_event_type_code").drop("tender_reason_code").drop("tender_date")\
        .drop("tender_datetm").drop("actual_goods_issue_date").drop("tariff_id")\
        .drop("schedule_code").drop("tender_first_carr_desc").drop("tender_reason_code_desc")\
        .drop("actual_ship_week_day_name").drop("ship_cond_val").drop("postal_code")\
        .drop("final_stop_postal_code").drop("country_from_code").drop("country_to_code").drop("freight_auction_flag")\
        .drop("freight_type_code").drop("origin_zone_code").drop("daily_award_qty")

    utils.manageOutput(logging, spark_session, tac_df, 1, "tac_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining customer with trade channel hierarchy.")

    trade_chanl_df = cust_dim_df.join(trade_chanl_hier_dim_df, "trade_chanl_id", how='inner')\
        .withColumnRenamed("cust_id", "customer_lvl12_code")

    utils.manageOutput(logging, spark_session, trade_chanl_df, 1, "trade_chanl_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating dates for shipment has finished.")

    logging.info("Calculating dates for shipment.")

    on_time_final_df = \
        on_time_df.groupBy("load_id", "trnsp_stage_num")\
        .agg(f.max("last_dttm").alias("last_appt_dlvry_tmstp"),
             f.max("first_dttm").alias("first_appt_dlvry_tmstp"))

    utils.manageOutput(logging, spark_session, on_time_final_df, 1, "on_time_final_df", target_db_name,
                        staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating dates for shipment has finished.")

    logging.info("Calculating on time data values for shipment.")

    shpmt_vals_otd_df = \
        on_time_df.select("load_id", "event_datetm", "ship_to_party_code", "plan_shpmt_end_date",
                          "plan_shpmt_end_aot_datetm", "ship_cond_code", "otd_cnt", "lot_ontime_status_last_appt_val",
                          "tat_late_counter_val", "service_tms_code", "frt_auction_code", "plan_shpmt_end_tmstp_calc") \
        .withColumn("shpmt_cnt", f.expr(expr.count_expr)) \
        .withColumn("max_event_datetm_rn",
                    f.row_number().over(Window.partitionBy("load_id").orderBy(f.col("event_datetm").desc()))) \
        .withColumn("multi_stop_num",
                    f.dense_rank().over(Window.partitionBy("load_id").orderBy(f.col("ship_to_party_code")))) \
        .groupBy("load_id") \
        .agg(f.max(f.expr(expr.tms_code_expr)).alias("actual_service_tms_code"),
             f.max("multi_stop_num").alias("multi_stop_num"),
             f.max("plan_shpmt_end_tmstp_calc").alias("plan_shpmt_end_tmstp"),
             f.max("tat_late_counter_val").alias("max_tat_late_counter_val"),
             f.max("otd_cnt").alias("max_otd_cnt"),
             f.max("shpmt_cnt").alias("max_shpmt_cnt"),
             f.max("frt_auction_code").alias("max_frt_auction_code")) \
        .withColumn("load_on_time_pct", f.expr(expr.pct_lot_expr)) \
        .withColumn("freight_auction_flag", f.expr(expr.fa_flag_expr))

    utils.manageOutput(logging, spark_session, shpmt_vals_otd_df, 1, "shpmt_vals_otd_df", target_db_name,
                        staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating on time data values for shipment has finished.")

    logging.info("Group by TAC data for costs data.")

    tac_calcs_df = \
        tac_df.groupBy("load_id") \
            .agg(
            f.max("actual_carr_trans_cost_amt").alias("actual_carr_trans_cost_amt"),
            f.max("linehaul_cost_amt").alias("linehaul_cost_amt"),
            f.max("incrmtl_freight_auction_cost_amt").alias("incrmtl_freight_auction_cost_amt"),
            f.max("cnc_carr_mix_cost_amt").alias("cnc_carr_mix_cost_amt"),
            f.max("unsource_cost_amt").alias("unsource_cost_amt"),
            f.max("fuel_cost_amt").alias("fuel_cost_amt"),
            f.max("acsrl_cost_amt").alias("acsrl_cost_amt"),
            f.max("dest_ship_from_code").alias("tac_dest_ship_from_code"),
            f.max("applnc_subsector_step_cnt").alias("applnc_subsector_step_cnt"),
            f.max("baby_care_subsector_step_cnt").alias("baby_care_subsector_step_cnt"),
            f.max("chemical_subsector_step_cnt").alias("chemical_subsector_step_cnt"),
            f.max("fabric_subsector_step_cnt").alias("fabric_subsector_step_cnt"),
            f.max("family_subsector_step_cnt").alias("family_subsector_step_cnt"),
            f.max("fem_subsector_step_cnt").alias("fem_subsector_step_cnt"),
            f.max("hair_subsector_step_cnt").alias("hair_subsector_step_cnt"),
            f.max("home_subsector_step_cnt").alias("home_subsector_step_cnt"),
            f.max("oral_subsector_step_cnt").alias("oral_subsector_step_cnt"),
            f.max("phc_subsector_step_cnt").alias("phc_subsector_step_cnt"),
            f.max("shave_subsector_step_cnt").alias("shave_subsector_step_cnt"),
            f.max("skin_subsector_cnt").alias("skin_subsector_cnt"),
            f.max("other_subsector_cnt").alias("other_subsector_cnt")
        )

    logging.info("Group by TAC data for costs data has finished.")

    logging.info("Group by TAC data for customer hierarchy.")

    tac_cust_hier_df = \
        tac_df.select("ship_to_party_id", "customer_code", "customer_lvl1_code", "customer_lvl1_desc",
                       "customer_lvl2_code", "customer_lvl2_desc", "customer_lvl3_code", "customer_lvl3_desc",
                       "customer_lvl4_code", "customer_lvl4_desc", "customer_lvl5_code", "customer_lvl5_desc",
                       "customer_lvl6_code", "customer_lvl6_desc", "customer_lvl7_code", "customer_lvl7_desc",
                       "customer_lvl8_code", "customer_lvl8_desc", "customer_lvl9_code", "customer_lvl9_desc",
                       "customer_lvl10_code", "customer_lvl10_desc", "customer_lvl11_code", "customer_lvl11_desc",
                       "customer_lvl12_code", "customer_lvl12_desc") \
        .withColumn("customer_desc", f.expr(expr.customer_desc_expr)) \
        .withColumn("customer_desc", f.regexp_replace(f.col("customer_desc"), "\\(.*\\)", ""))\
        .drop("customer_code").distinct() \
        .join(trade_chanl_df, "customer_lvl12_code", how='left')

    utils.manageOutput(logging, spark_session, tac_cust_hier_df, 1, "tac_cust_hier_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Group by TAC data for customer hierarchy has finished.")

    logging.info("Group by TAC data.")
    
    tac_avg_df = \
        tac_df.selectExpr("load_id", "forward_agent_id AS str_carr_num", "service_tms_code AS actual_service_tms_code",
                          "avg_award_weekly_vol_qty").distinct()\
        .withColumn("primary_carr_flag", f.expr(expr.carr_flag_expr)) \
        .toDF("load_id", "str_carr_num", "actual_service_tms_code", "avg_award_weekly_vol_qty", "primary_carr_flag")

    logging.info("Group by TAC data has finished.")

    logging.info("Get TAC destination zones.")

    tac_dest_zone_df = tac_df.select("load_id", "tender_event_datetm", "dest_zone_code")\
        .withColumn("rn", f.row_number().over(Window.partitionBy("load_id").orderBy(f.col("tender_event_datetm")))) \
        .filter("rn = 1").withColumnRenamed("dest_zone_code", "tac_dest_zone_code").drop("tender_event_datetm")

    logging.info("Get TAC destination zones has finished.")

    ship_to_party_final_df = \
        tvb.get_tender_acceptance_na_star(logging, spark_session, trans_vsblt_db_name, target_db_name,
                                          staging_location, debug_mode_ind, debug_postfix)\
        .select("load_id", "ship_to_party_id").withColumn("load_id", f.regexp_replace("load_id", '^0', '')).distinct()\
        .join(tac_cust_hier_df, "ship_to_party_id", how='left').withColumnRenamed("ship_to_party_id", "customer_id")

    utils.manageOutput(logging, spark_session, ship_to_party_final_df, 1, "ship_to_party_final_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining final tables.")
    #otd_pre_joined_df = on_time_df.select("load_id", "trnsp_stage_num", "str_carr_num", "ship_point_code",
    #                                      "poloadid_join", "cust_po_num").distinct() \
    #    .join(shpmt_vals_otd_df, "load_id", how='left')\
    #    .join(ship_to_party_final_df, "load_id", how='left')\
    #    .join(actual_ship_time_df, "load_id", how='left')\
    #    .join(tms_unload_method_df, "load_id", how='left')\
    #    .join(tac_calcs_df, "load_id", how='left')\
    #    .join(tfs_df, "load_id", how='left') \
    #    .join(f.broadcast(csot_bucket_final_df), "load_id", how='left') \
    #    .drop(csot_bucket_final_df.cust_po_num).drop(csot_bucket_final_df.poload_id)\
    #    .drop(csot_bucket_final_df.poloadid_join) \
    #    .withColumnRenamed("csot_update_reason_code", "csot_update_reason_code3") \
    #    .withColumnRenamed("reason_code", "reason_code3").withColumnRenamed("aot_reason_code", "aot_reason_code3") \
    #    .join(on_time_final_df, ["load_id", "trnsp_stage_num"], how='left')\
    #    .join(tac_avg_df, ["load_id", "str_carr_num", "actual_service_tms_code"], how='left') \
    #    .join(f.broadcast(ship_loc_df), "ship_point_code", how='left') \
    #    .join(f.broadcast(csot_bucket_final_df), "poloadid_join", how='left').drop(csot_bucket_final_df.cust_po_num)\
    #    .drop(csot_bucket_final_df.poload_id).drop(csot_bucket_final_df.load_id) \
    #    .withColumnRenamed("csot_update_reason_code", "csot_update_reason_code1") \
    #    .withColumnRenamed("reason_code", "reason_code1") \
    #    .withColumnRenamed("aot_reason_code", "aot_reason_code1") \
    #    .join(f.broadcast(csot_bucket_final_df), "cust_po_num", how='left') \
    #    .drop(csot_bucket_final_df.poload_id).drop(csot_bucket_final_df.poloadid_join).drop(csot_bucket_final_df.load_id)\
    #    .withColumnRenamed("csot_update_reason_code", "csot_update_reason_code2") \
    #    .withColumnRenamed("reason_code", "reason_code2") \
    #    .withColumnRenamed("aot_reason_code", "aot_reason_code2") \
    #    .join(f.broadcast(sambc_df), "customer_lvl3_desc", how='left') \
    #    # .join(trade_chanl_df, "customer_lvl12_code", how='left') \
    #    # .join(tac_cust_hier_df, "ship_to_party_id", how='left').withColumnRenamed("ship_to_party_id", "customer_id") \
    #
    #utils.manageOutput(logging, spark_session, otd_pre_joined_df, 1, "otd_pre_joined_df", target_db_name,
    #                   staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))
    #
    #otd_joined_df = \
    #    on_time_df.join(otd_pre_joined_df, ["load_id", "trnsp_stage_num", "str_carr_num", "ship_point_code",
    #                                        "poloadid_join", "cust_po_num"], how='left')\
    #    .withColumn("true_fa_flag", f.expr(expr.true_fa_flag_expr))\
    #    .withColumn("csot_update_reason_code", f.expr(expr.csot_update_reason_code_expr))\
    #    .withColumn("reason_code", f.expr(expr.reason_code_expr))\
    #    .withColumn("aot_reason_code", f.expr(expr.aot_reason_code_expr))\
    #    .withColumn("csot_scrubs_value", f.col("csot_update_reason_code")) \
    #    .withColumn("aot_measrbl_flag", f.expr(expr.aot_meas_expr)) \
    #    .withColumn("iot_measrbl_flag", f.expr(expr.iot_meas_expr)) \
    #    .withColumn("lot_measrbl_flag", f.expr(expr.lot_meas_expr)) \
    #    .withColumn("csot_measrbl_pos_num", f.expr(expr.csot_pos_expr)) \
    #    .withColumn("aot_load_id", f.expr(expr.aot_loads_expr)) \
    #    .withColumn("aot_on_time_load_id", f.expr(expr.aot_loads_on_time_expr)) \
    #    .withColumn("aot_late_load_id", f.expr(expr.aot_late_loads_expr)) \
    #    .withColumn("iot_load_id", f.expr(expr.iot_loads_expr)) \
    #    .withColumn("iot_on_time_load_id", f.expr(expr.iot_loads_on_time_expr)) \
    #    .withColumn("iot_late_load_id", f.expr(expr.iot_late_loads_expr)) \
    #    .withColumn("lot_load_id", f.expr(expr.lot_loads_expr)) \
    #    .withColumn("lot_on_time_load_id", f.expr(expr.lot_loads_on_time_expr)) \
    #    .withColumn("lot_late_load_id", f.expr(expr.lot_late_loads_expr)) \
    #    .withColumn("csot_intrmdt_failure_reason_bucket_updated_name", f.expr(expr.csot_intermediate_failure_reason_bucket_updated_expr)) \
    #    .withColumn("csot_failure_reason_bucket_updated_name", f.expr(expr.csot_failure_reason_bucket_updated_expr)) \
    #    .withColumn("csot_on_time_num", f.expr(expr.csot_on_time_expr)) \
    #    .withColumn("csot_not_on_time_num", f.expr(expr.csot_not_on_time_expr)) \
    #    .withColumn("in_full_rate", f.lit("")) \
    #    .withColumn("otif_qty", f.lit("")) \
    #    .withColumn("trnsp_stage_num", f.col('trnsp_stage_num').cast("int"))\
    #    .withColumn("actual_ship_tmstp", f.concat(f.col("actual_ship_date"), f.lit(" "), f.col("actual_ship_datetm")))\
    #    .withColumn("actual_load_method_val", f.col("drop_live_ind_desc"))\
    #    .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.from_unixtime(f.unix_timestamp()), 'PRT'))\

    otd_old_joined_df = \
        on_time_df\
        .join(shpmt_vals_otd_df, "load_id", how='left')\
        .join(ship_to_party_final_df, "load_id", how='left')\
        .join(actual_ship_time_df, "load_id", how='left')\
        .join(tms_unload_method_df, "load_id", how='left')\
        .join(tac_calcs_df, "load_id", how='left')\
        .join(tac_dest_zone_df, "load_id", how='left')\
        .join(tfs_df, "load_id", how='left') \
        .join(f.broadcast(csot_bucket_final_df), "load_id", how='left') \
        .drop(csot_bucket_final_df.pg_order_num).drop(csot_bucket_final_df.poload_id_new).drop(csot_bucket_final_df.poloadid_join) \
        .withColumnRenamed("csot_update_reason_code", "csot_update_reason_code3") \
        .withColumnRenamed("reason_code", "reason_code3") \
        .withColumnRenamed("aot_reason_code", "aot_reason_code3") \
        .join(on_time_final_df, ["load_id", "trnsp_stage_num"], how='left') \
        .join(tac_avg_df, ["load_id", "str_carr_num", "actual_service_tms_code"], how='left') \
        .join(f.broadcast(ship_loc_df), "ship_point_code", how='left') \
        .join(f.broadcast(csot_bucket_final_df), "poloadid_join", how='left').drop(csot_bucket_final_df.pg_order_num)\
        .drop(csot_bucket_final_df.poload_id_new).drop(csot_bucket_final_df.load_id) \
        .withColumnRenamed("csot_update_reason_code", "csot_update_reason_code1") \
        .withColumnRenamed("reason_code", "reason_code1") \
        .withColumnRenamed("aot_reason_code", "aot_reason_code1") \
        .join(f.broadcast(csot_bucket_final_df), "pg_order_num", how='left') \
        .drop(csot_bucket_final_df.poload_id_new).drop(csot_bucket_final_df.poloadid_join).drop(csot_bucket_final_df.load_id)\
        .withColumnRenamed("csot_update_reason_code", "csot_update_reason_code2") \
        .withColumnRenamed("reason_code", "reason_code2") \
        .withColumnRenamed("aot_reason_code", "aot_reason_code2") \
        .join(f.broadcast(sambc_df), "customer_lvl3_desc", how='left') \
        .withColumn("true_fa_flag", f.expr(expr.true_fa_flag_expr))\
        .withColumn("csot_update_reason_code", f.expr(expr.csot_update_reason_code_expr))\
        .withColumn("reason_code", f.expr(expr.reason_code_expr))\
        .withColumn("aot_reason_code", f.expr(expr.aot_reason_code_expr))\
        .withColumn("csot_scrubs_value", f.col("csot_update_reason_code")) \
        .withColumn("aot_measrbl_flag", f.expr(expr.aot_meas_expr)) \
        .withColumn("iot_measrbl_flag", f.expr(expr.iot_meas_expr)) \
        .withColumn("lot_measrbl_flag", f.expr(expr.lot_meas_expr)) \
        .withColumn("csot_measrbl_pos_num", f.expr(expr.csot_pos_expr)) \
        .withColumn("aot_load_id", f.expr(expr.aot_loads_expr)) \
        .withColumn("aot_on_time_load_id", f.expr(expr.aot_loads_on_time_expr)) \
        .withColumn("aot_late_load_id", f.expr(expr.aot_late_loads_expr)) \
        .withColumn("iot_load_id", f.expr(expr.iot_loads_expr)) \
        .withColumn("lot_load_id", f.expr(expr.lot_loads_expr)) \
        .withColumn("lot_on_time_load_id", f.expr(expr.lot_loads_on_time_expr)) \
        .withColumn("lot_late_load_id", f.expr(expr.lot_late_loads_expr)) \
        .withColumn("csot_intrmdt_failure_reason_bucket_updated_name", f.expr(expr.csot_intermediate_failure_reason_bucket_updated_expr)) \
        .withColumn("csot_failure_reason_bucket_updated_name", f.expr(expr.csot_failure_reason_bucket_updated_expr)) \
        .withColumn("csot_on_time_num", f.expr(expr.csot_on_time_expr)) \
        .withColumn("csot_not_on_time_num", f.expr(expr.csot_not_on_time_expr)) \
        .withColumn("in_full_rate", f.lit("")) \
        .withColumn("otif_qty", f.lit("")) \
        .withColumn("trnsp_stage_num", f.col('trnsp_stage_num').cast("int")) \
        .withColumn("actual_ship_tmstp", f.concat(f.col("actual_ship_date"), f.lit(" "),
                                                  f.coalesce(f.col("actual_ship_datetm"), f.lit("00:00:00"))))\
        .withColumn("actual_load_method_val", f.col("drop_live_ind_desc")) \
        .withColumn("dest_zone_code", f.coalesce(f.col("dest_zone_code"), f.col("tac_dest_zone_code"))) \
        .withColumn("origin_zone_ship_from_code", f.expr(expr.origin_zone_ship_from_code_expr)) \
        .withColumn("dest_loc_code", f.regexp_replace("dest_loc_code", '^0+', '')) \
        .withColumn("true_frt_type_desc", f.expr(expr.true_frt_type_desc_expr)) \
        .withColumn("dest_ship_from_code", f.expr(expr.dest_ship_from_code_expr)) \
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.from_unixtime(f.unix_timestamp()), 'PRT')) \
        .withColumn("carr_desc", f.expr(expr.carr_desc_expr))

    utils.manageOutput(logging, spark_session, otd_old_joined_df, 0, "otd_old_joined_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    on_time_iot_df = \
        otd_old_joined_df.select("load_id", "event_datetm").filter("iot_measrbl_flag = 1") \
        .withColumn("max_event_datetm_rn_iot",
                    f.row_number().over(Window.partitionBy("load_id").orderBy(f.col("event_datetm").desc())))

    otd_joined_df = \
        otd_old_joined_df\
        .join(on_time_iot_df, ["load_id", "event_datetm"], how='left') \
        .withColumn("iot_on_time_load_id", f.expr(expr.iot_loads_on_time_expr)) \
        .withColumn("iot_late_load_id", f.expr(expr.iot_late_loads_expr))

    logging.info("Joining final tables has finished.")
    utils.manageOutput(logging, spark_session, otd_joined_df, 0, "otd_joined_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    return otd_joined_df


def load_on_time_data_hub_star(logging, config_module, debug_mode_ind, debug_postfix):
    ''' Load the on_time_data_hub_star table'''

    # Create a spark session
    #params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_session = utils.get_spark_session(logging, params.SPARK_MASTER, params.NUM_EXECUTORS, 0, 'otd')

    params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_params = utils.ConfParams.build_from_module(logging, config_spark_module, debug_mode_ind, debug_postfix)
    spark_conf = list(set().union(
        params.SPARK_GLOBAL_PARAMS,
        params.SPARK_PROD_OTD_PARAMS)
        )
    spark_session = utils.get_spark_session(logging, 'tfx_otd', params.SPARK_MASTER, spark_conf)

    #Remove debug tables (if they are)
    utils.removeDebugTables(logging, spark_session, params.TARGET_DB_NAME, debug_mode_ind, debug_postfix)

    # spark_session.conf.set('spark.executor.memory', '50g')  # 30g
    # spark_session.conf.set('spark.executor.memoryOverhead', '3g')  # 3g
    # spark_session.conf.set('spark.driver.memory', '40g')  # 40g
    spark_session.sql('REFRESH TABLE {}.cust_dim'.format(params.RDS_DB_NAME))
    spark_session.sql('REFRESH TABLE {}.trade_chanl_hier_dim'.format(params.RDS_DB_NAME))

    logging.info("Started loading {}.on_time_data_hub_star table.".format(params.TARGET_DB_NAME))

    # Get target table column list
    target_table_cols = spark_session.table('{}.on_time_data_hub_star'.format(params.TARGET_DB_NAME)).schema.fieldNames()

    on_time_data_hub_star_df = \
        get_on_time_data_hub_star(logging, spark_session, params.RDS_DB_NAME, params.TRANS_VB_DB_NAME,
                                  params.TARGET_DB_NAME, params.STAGING_LOCATION, debug_mode_ind, debug_postfix) \
        .select(target_table_cols)

    logging.info("Inserting data into a table (overwriting old data)")

    on_time_data_hub_star_df.write.insertInto(
         tableName='{}.{}'.format(params.TARGET_DB_NAME,
                                  'on_time_data_hub_star'),
         overwrite=True)

    logging.info("Loading {}.on_time_data_hub_star table has finished".format(params.TARGET_DB_NAME))
