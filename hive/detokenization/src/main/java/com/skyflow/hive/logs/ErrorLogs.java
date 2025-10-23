package com.skyflow.hive.logs;

/**
 * Enum containing internal error log messages for the Skyflow Hive UDF.
 * These messages are used for logging purposes and provide more technical details
 * about errors that occur during UDF execution.
 */
public enum ErrorLogs {
    // Session State Validation Errors
    EITHER_CONFIG_OR_CONFIG_FILE_PATH_REQUIRED("No config provided in hive. Either provide 'skyflow.config' or 'skyflow.config.file'."),
    CONFIG_FILE_NOT_FOUND("Unable to find config file at provided path."),
    INVALID_JSON_FORMAT("The JSON provided in not in proper JSON format."),
    INVALID_TYPE_OF_JSON("Expected JSON to be of type JsonObject but found a different JSON type"),

    // Configuration Validation Errors
    VAULT_ID_IS_REQUIRED("Invalid skyflow config provided in hive. Vault ID is required. Please check your skyflow config json."),
    EMPTY_VAULT_ID("Invalid skyflow config provided in hive. Vault ID can not be empty. Please check your skyflow config json."),
    CLUSTER_ID_IS_REQUIRED("Invalid skyflow config provided in hive. Cluster ID is required. Please check your skyflow config json."),
    EMPTY_CLUSTER_ID("Invalid skyflow config provided in hive. Cluster ID can not be empty. Please check your skyflow config json."),
    INVALID_ENVIRONMENT("Invalid environment specified. Please check your skyflow config json."),
    EITHER_CREDENTIALS_OR_FILE_PATH_REQUIRED("Invalid skyflow config provided in hive. Either file path or credentials json is required. Please check your skyflow config json."),
    BOTH_CREDENTIALS_OR_FILE_PATH_PROVIDED("Invalid skyflow config provided in hive. Both file path or credentials json sre provided. Please check your skyflow config json."),

    UNEXPECTED_ERROR_SESSION_STATE("Unexpected error occurred while validating session state."),
    INVALID_ARGUMENT_COUNT("Invalid number of arguments provided."),
    DETOKENIZE_FAILED("Detokenize operation resulted in failure."),
    FAILED_SDK_INIT("Initialization failed. Failed to initialise skyflow SDK."),
    ;

    /**
     * The error log message text
     */
    private final String log;

    /**
     * Constructs an error log enum value
     *
     * @param log The log message text
     */
    ErrorLogs(String log) {
        this.log = log;
    }

    /**
     * Gets the error log message
     *
     * @return The log message text
     */
    public final String getLog() {
        return log;
    }
}
