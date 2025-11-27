package com.skyflow.spark;

import java.util.Arrays;
import java.util.HashSet;

public class Constants {
    // Logger prefix reused across helper classes.
    public static final String LOG_PREFIX = "[VaultHelper] ";
    public static final Integer BATCH_SIZE = 1000;
    public static final String COLUMN_NAME = "columnName";
    public static final String TABLE_NAME = "tableName";
    public static final String UNIQUE = "unique";
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

    // Status markers appended to downstream Spark datasets.
    public static final String STATUS_OK = "200";
    public static final String STATUS_ERROR = "500";
    public static final String SKYFLOW_STATUS_CODE = "skyflow_status_code";
    public static final String ERROR = "error";
    public static final String DETOKENIZE_FAILED = "Detokenization failed";
    public static final String NO_RETRIES_NEEDED_PROCEEDING = "No retryable records found, no retries needed — proceeding";
    public static final String PROCESSED_ALL_BATCHES = "Processed all the batches.";
    public static final String INSERT_FAILED = "Insert failed";

}
