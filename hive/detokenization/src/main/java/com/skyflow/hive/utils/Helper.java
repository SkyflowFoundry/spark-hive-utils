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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.session.SessionState;

public final class Helper {
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
            throw new UDFArgumentException(ErrorMessage.FailedSDKInit.getMessage() + e);
        }
    }

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
