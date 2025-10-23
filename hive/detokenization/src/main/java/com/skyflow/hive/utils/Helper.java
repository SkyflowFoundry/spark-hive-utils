package com.skyflow.hive.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.skyflow.Skyflow;
import com.skyflow.config.Credentials;
import com.skyflow.config.VaultConfig;
import com.skyflow.enums.Env;
import com.skyflow.enums.LogLevel;
import com.skyflow.errors.SkyflowException;
import com.skyflow.hive.errors.ErrorMessage;
import com.skyflow.hive.logs.ErrorLogs;
import com.skyflow.utils.logger.LogUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Helper class that provides utility methods for Skyflow SDK initialization and configuration.
 * This class handles the initialization of the Skyflow SDK with appropriate configuration
 * parameters obtained from the Hive session state.
 */
public final class Helper {
    /**
     * Retrieves configuration parameters from the Hive session state and initializes the Skyflow SDK.
     * This method handles the complete initialization process including config validation.
     *
     * @return Skyflow An initialized instance of the Skyflow SDK
     * @throws UDFArgumentException If there are any configuration or initialization errors
     */
    public static synchronized Skyflow getConfigParametersAndInitialiseSDK() throws UDFArgumentException {
        try {
            SessionState sessionState = SessionState.get();
            JsonObject configObject = Validations.validateSessionState(sessionState);
            Gson gson = new Gson();
            Config skyflowConfig = gson.fromJson(configObject, Config.class);
            Validations.validateConfig(skyflowConfig);
            return initializeSkyflowSDK(skyflowConfig);
        } catch (UDFArgumentException e) {
            throw e;
        } catch (Exception e) {
            LogUtil.printErrorLog(ErrorLogs.FAILED_SDK_INIT.getLog());
            throw new UDFArgumentException(ErrorMessage.FailedSDKInit.getMessage() + e);
        }
    }

    /**
     * Initializes the Skyflow SDK with the provided configuration.
     * Sets up credentials, vault configuration, and builds the Skyflow client.
     *
     * @param skyflowConfig The configuration object containing vault and credential settings
     * @return Skyflow An initialized instance of the Skyflow SDK
     */
    private static synchronized Skyflow initializeSkyflowSDK(Config skyflowConfig) {
        try {
            Credentials credentials = new Credentials();
            if (skyflowConfig.getCredentials() != null) {
                credentials.setCredentialsString(skyflowConfig.getCredentials());
            } else {
                credentials.setPath(skyflowConfig.getFilePath());
            }

            VaultConfig vaultConfig = new VaultConfig();
            vaultConfig.setVaultId(skyflowConfig.getVaultId());
            vaultConfig.setClusterId(skyflowConfig.getClusterId());
            vaultConfig.setCredentials(credentials);
            // Only assign Env when explicitly passed in config, else SDK will fall back to default i.e. PROD
            if (skyflowConfig.getEnv() != null) {
                vaultConfig.setEnv(Env.valueOf(skyflowConfig.getEnv().toUpperCase()));
            }

            return Skyflow.builder()
                    .setLogLevel(LogLevel.ERROR)
                    .addVaultConfig(vaultConfig)
                    .build();
        } catch (SkyflowException e) {
            throw new RuntimeException(e);
        }
    }
}
