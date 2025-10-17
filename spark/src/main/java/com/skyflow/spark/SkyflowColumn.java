package com.skyflow.spark;

public enum SkyflowColumn {
    ADDRESS("address"),
    AGE("age"),
    CITY_NAME("city_name"),
    CREDIT_CARD_NUMBER("credit_card_number"),
    DATE("date"),
    DATE_MM_YYYY("date_mm_yyyy"),
    EMAIL("email"),
    ETHNIC_GROUP_CODE("ethnic_group_code"),
    ETHNIC_GROUP_DESC("ethnic_group_desc"),
    EXPERIAN_EDU_LVL_CODE("experian_edu_lvl_code"),
    EXPERIAN_EDU_LVL_DESC("experian_edu_lvl_desc"),
    EXPERIAN_ETHNICITY_GROUP_CODE("experian_ethnicity_group_code"),
    EXPERIAN_ETHNICITY_GROUP_DESC("experian_ethnicity_group_desc"),
    EXPERIAN_GENDER_CODE("experian_gender_code"),
    EXPERIAN_INCOME_CODE("experian_income_code"),
    EXPERIAN_LANGUAGE_CODE("experian_language_code"),
    EXPERIAN_MARITAL_STATUS_CODE("experian_marital_status_code"),
    EXPERIAN_MARITAL_STATUS_DESC("experian_marital_status_desc"),
    GENDER("gender"),
    GEO_LAT("geo_lat"),
    GEO_LONG("geo_long"),
    INCOME_AMOUNT("income_amount"),
    NAME("name"),
    OCCUPATION("occupation"),
    P2PE_PI_HASH("p2pe_pi_hash"),
    PHONE_NUMBER("phone_number"),
    STATE_CODE("state_code"),
    STATE_NAME("state_name"),
    STATE_PROV_TYPE_CODE("state_prov_type_code"),
    ZIPCODE("zipcode");

    private final String value;

    SkyflowColumn(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
