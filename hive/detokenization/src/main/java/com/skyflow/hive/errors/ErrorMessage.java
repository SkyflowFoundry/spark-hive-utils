package com.skyflow.hive.errors;

/**
 * Enum containing user-facing error messages for the Skyflow Hive UDF.
 * These messages are formatted and displayed to users when various error conditions occur.
 * Messages can contain placeholders (%s1) that are replaced with context-specific values.
 */
public enum ErrorMessage {
    // Session State Validation Errors
    // These errors occur during validation of the Hive session state and configuration
    EitherConfigOrConfigFilePathRequired("Initialization failed. Either provide 'skyflow.config' or 'skyflow.config.file'."),
    ConfigFileNotFound("Initialization failed. Unable to find config file at path %s1. Please provide a valid path for 'skyflow.config.file'."),
    InvalidJsonFormat("Initialization failed. The JSON provided is not in proper JSON format. Specify valid JSON in %s1."),
    InvalidTypeOfJson("Initialization failed. Expected JSON to be of type JsonObject but found a different JSON type. Specify a JsonObject in %s1."),
    FailedSDKInit("Initialization failed. Failed to initialize SDK client."),

    // Configuration Validation Errors
    // These errors occur when validating the Skyflow configuration parameters
    InvalidVaultId("Initialization failed. Invalid vault ID. Specify a valid vault ID in skyflow config json."),
    EmptyVaultId("Initialization failed. Invalid vault ID. Vault ID must not be empty in skyflow config json."),
    EitherVaultUrlOrClusterIdRequired("Initialization failed. Specify either 'clusterId' or 'vaultURL' in skyflow config json."),
    EmptyClusterId("Initialization failed. Cluster ID is either null or empty. Specify a valid cluster ID in skyflow config json."),
    EmptyVaultUrl("Initialization failed. Vault URL is either null or empty. Specify a valid vault URL in skyflow config json."),
    InvalidEnvironment("Initialization failed. Invalid environment specified. Allowed values are [SANDBOX, PROD]. Please check your skyflow config json."),
    EitherCredentialsOrFilePathRequired("Initialization failed. Either provide credentials json or file path in skyflow config json."),
    BothCredentialsOrFilePathProvided("Initialization failed. Both credentials json or file path in skyflow config json are provided. Specify only one of them."),

    // generic
    UnexpectedError("Initialization failed. Unexpected error occurred."),
    InvalidArgumentCount("Initialization failed. Invalid number of arguments provided. detokenize() takes exactly one argument."),
    DetokenizationFailed("Initialization failed. Error occurred while detokenizing the token. %s1"),
    ;

    /**
     * The formatted error message text
     */
    private final String message;

    /**
     * Constructs an error message enum value
     *
     * @param message The error message text
     */
    ErrorMessage(String message) {
        this.message = message;
    }

    /**
     * Gets the formatted error message
     *
     * @return The error message text
     */
    public String getMessage() {
        return message;
    }
}
