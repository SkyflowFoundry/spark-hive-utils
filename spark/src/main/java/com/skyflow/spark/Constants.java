package com.skyflow.spark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;

public class Constants {
    public static final String LOG_PREFIX = "[VaultHelper] ";
    public static final Integer INSERT_BATCH_SIZE = 1000;
    public static final Integer DETOKENIZE_BATCH_SIZE = 1000;
    public static final String COLUMN_NAME = "columnName";
    public static final String TABLE_NAME = "tableName";
    public static final String TOKEN_GROUP_NAME = "tokenGroupName";
    public static final String REDACTION = "redaction";
    public static final String COLUMN_MAPPING = "columnMapping";
    public static final long BASE_MILLI_SECONDS = 100; // base delay
    public static final long MAX_DELAY_MILLI_SECONDS = 10000; // max delay
    // Set of retryable error codes
    public static final HashSet<Integer> RETRYABLE_ERROR_CODES = new HashSet<>(Arrays.asList(
            429,
            500,
            502,
            503,
            504,
            529));

    public static final String STATUS_OK = "200";
    public static final String STATUS_ERROR = "500";
    public static final String SKYFLOW_STATUS_CODE = "skyflow_status_code";
    public static final String ERROR = "error";
    public static final String DETOKENIZE_FAILED = "Detokenization failed";
    public static final String NO_RETRIES_NEEDED_PROCEEDING = "No retryable records found, no retries needed — proceeding";
    public static final String PROCESSED_ALL_BATCHES = "Processed all the batches.";
    public static final String INSERT_FAILED = "Insert failed";

    public static final Map<String, SkyflowColumn> SKYFLOW_COLUMN_MAP = new HashMap<String, SkyflowColumn>() {
        {
            put("first_nm", SkyflowColumn.NAME);
            put("mid_nm", SkyflowColumn.NAME);
            put("last_nm", SkyflowColumn.NAME);
            put("ph_nbr", SkyflowColumn.PHONE_NUMBER);
            put("email_id", SkyflowColumn.EMAIL);
            put("pref_lang_first_nm", SkyflowColumn.NAME);
            put("pref_lang_mid_nm", SkyflowColumn.NAME);
            put("pref_lang_last_nm", SkyflowColumn.NAME);
            put("birth_dt", SkyflowColumn.DATE);
            put("cust_profl.gender_cd", SkyflowColumn.GENDER);
            put("occ_desc", SkyflowColumn.OCCUPATION);
            put("scndry_ph_nbr", SkyflowColumn.PHONE_NUMBER);
            put("addr_line_1_txt", SkyflowColumn.ADDRESS);
            put("addr_line_2_txt", SkyflowColumn.ADDRESS);
            put("addr_line_3_txt", SkyflowColumn.ADDRESS);
            put("city_nm", SkyflowColumn.CITY_NAME);
            put("st_prov_cd", SkyflowColumn.STATE_CODE);
            put("st_prov_nm", SkyflowColumn.STATE_NAME);
            put("zip_cd", SkyflowColumn.ZIPCODE);
            put("addl_ph_nbr", SkyflowColumn.PHONE_NUMBER);
            put("pin_lat_val", SkyflowColumn.GEO_LAT);
            put("pin_long_val", SkyflowColumn.GEO_LONG);
            put("lat_val", SkyflowColumn.GEO_LAT);
            put("long_val", SkyflowColumn.GEO_LONG);
            put("card_disp_1_full_nm", SkyflowColumn.NAME);
            put("card_disp_2_full_nm", SkyflowColumn.NAME);
            put("full_nm", SkyflowColumn.NAME);
            put("card_nbr", SkyflowColumn.CREDIT_CARD_NUMBER);
            put("card_exp_dt", SkyflowColumn.DATE_MM_YYYY);
            put("p2pe_pi_hash_val", SkyflowColumn.P2PE_PI_HASH);
            put("pref_lang_full_nm", SkyflowColumn.NAME);
            put("persn_first_nm", SkyflowColumn.NAME);
            put("persn_mid_nm", SkyflowColumn.NAME);
            put("persn_last_nm", SkyflowColumn.NAME);
            put("addr_line_4_txt", SkyflowColumn.ADDRESS);
            put("addr_line_5_txt", SkyflowColumn.ADDRESS);
            put("addr_line_6_txt", SkyflowColumn.ADDRESS);
            put("st_prov_type_cd", SkyflowColumn.STATE_PROV_TYPE_CODE);
            put("persn_email_id", SkyflowColumn.EMAIL);
            put("persn_alt_email_id", SkyflowColumn.EMAIL);
            put("pref_shpg_addr_txt", SkyflowColumn.ADDRESS);
            put("persn_pref_lang_first_nm", SkyflowColumn.NAME);
            put("persn_pref_lang_mid_nm", SkyflowColumn.NAME);
            put("persn_pref_lang_last_nm", SkyflowColumn.NAME);
            put("exct_age_nbr", SkyflowColumn.AGE);
            put("est_age_nbr", SkyflowColumn.AGE);
            put("cust_shopper_experian_profl.gender_cd", SkyflowColumn.EXPERIAN_GENDER_CODE);
            put("hh_est_income_cd", SkyflowColumn.EXPERIAN_INCOME_CODE);
            put("hh_est_income_desc", SkyflowColumn.EXPERIAN_INCOME_CODE); // check
            put("hh_enh_est_income_amt", SkyflowColumn.INCOME_AMOUNT);
            put("enh_ethncty_grp_cd", SkyflowColumn.EXPERIAN_ETHNICITY_GROUP_CODE);
            put("enh_ethncty_grp_desc", SkyflowColumn.EXPERIAN_ETHNICITY_GROUP_DESC);
            put("ethnic_lang_cd", SkyflowColumn.EXPERIAN_LANGUAGE_CODE);
            put("mrtl_status_cd", SkyflowColumn.EXPERIAN_MARITAL_STATUS_CODE);
            put("mrtl_status_desc", SkyflowColumn.EXPERIAN_MARITAL_STATUS_DESC);
            put("edu_lvl_cd", SkyflowColumn.EXPERIAN_EDU_LVL_CODE);
            put("edu_lvl_desc", SkyflowColumn.EXPERIAN_EDU_LVL_DESC);
            put("ethnic_grp_cd", SkyflowColumn.ETHNIC_GROUP_CODE);
            put("ethnic_grp_desc", SkyflowColumn.ETHNIC_GROUP_DESC);
            put("cust_zip_5_cd", SkyflowColumn.ZIPCODE);
            put("baby_nm", SkyflowColumn.NAME);
        }
    };
}
