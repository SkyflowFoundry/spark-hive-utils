package com.skyflow.hive.errors;

public enum ErrorMessage {
    // validate session state
    EitherConfigOrConfigFilePathRequired("Initialization failed. Either provide 'skyflow.config' or 'skyflow.config.file'."),
    ConfigFileNotFound("Initialization failed. Unable to find config file at path %s1. Please provide a valid path for 'skyflow.config.file'."),
    InvalidJsonFormat("Initialization failed. The JSON provided is not in proper JSON format. Specify valid JSON in %s1."),
    InvalidTypeOfJson("Initialization failed. Expected JSON to be of type JsonObject but found a different JSON type. Specify a JsonObject in %s1."),
    FailedSDKInit("Initialization failed. Failed to initialize SDK client."),

    // validate config
    InvalidVaultId("Initialization failed. Invalid vault ID. Specify a valid vault ID in skyflow config json."),
    EmptyVaultId("Initialization failed. Invalid vault ID. Vault ID must not be empty in skyflow config json."),
    InvalidClusterId("Initialization failed. Invalid cluster ID. Specify cluster ID in skyflow config json."),
    EmptyClusterId("Initialization failed. Invalid cluster ID. Specify a valid cluster ID in skyflow config json."),
    EitherCredentialsOrFilePathRequired("Initialization failed. Either provide credentials json or file path in skyflow config json."),
    BothCredentialsOrFilePathProvided("Initialization failed. Both credentials json or file path in skyflow config json are provided. Specify only one of them."),

    // generic
    UnexpectedError("Initialization failed. Unexpected error occurred."),
    ;

    private final String message;

    ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
