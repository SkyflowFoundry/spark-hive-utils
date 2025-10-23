package com.skyflow.hive.utils;

/**
 * Configuration class that holds Skyflow SDK initialization parameters.
 * This class stores essential configuration values like vault ID, cluster ID,
 * environment settings, and credential information required for Skyflow operations.
 */
public final class Config {
    /** The ID of the Skyflow vault to be accessed */
    private String vaultId;
    /** The ID of the Skyflow cluster containing the vault */
    private String clusterId;
    /** The environment (PROD/SANDBOX) for Skyflow operations */
    private String env;
    /** Path to the credentials file if using file-based authentication */
    private String filePath;
    /** Raw credentials string if using direct credentials */
    private String credentials;

    /**
     * Gets the Skyflow vault ID
     * @return String The vault ID
     */
    public String getVaultId() {
        return vaultId;
    }

    /**
     * Gets the Skyflow cluster ID
     * @return String The cluster ID
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Gets the environment setting (PROD/SANDBOX)
     * @return String The environment setting
     */
    public String getEnv() {
        return env;
    }

    /**
     * Gets the path to the credentials file
     * @return String Path to the credentials file
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * Gets the raw credentials string
     * @return String The credentials string
     */
    public String getCredentials() {
        return credentials;
    }
}
