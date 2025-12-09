package com.skyflow.hive.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.skyflow.enums.Env;
import com.skyflow.hive.errors.ErrorMessage;
import com.skyflow.hive.logs.ErrorLogs;
import com.skyflow.utils.Utils;
import com.skyflow.utils.logger.LogUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Validation utility class that provides methods to validate various aspects of the Skyflow configuration.
 * This includes session state validation, configuration validation, and credential validation.
 */
public final class Validations {

    /**
     * Validates the session state and extracts configuration information.
     * Checks for either direct configuration or configuration file path in the session.
     *
     * @param sessionState The Hive session state to validate
     * @return JsonObject The parsed configuration as a JSON object
     * @throws UDFArgumentException If validation fails or configuration is invalid
     */
    public static JsonObject validateSessionState(SessionState sessionState) throws UDFArgumentException {
        String config = sessionState.getConf().get(Constants.SKYFLOW_CONFIG);
        String configFilePath = sessionState.getConf().get(Constants.SKYFLOW_CONFIG_FILE);

        try {
            if (config != null && !config.trim().isEmpty()) {
                return JsonParser.parseString(config).getAsJsonObject();
            } else if (configFilePath != null && !configFilePath.trim().isEmpty()) {
                FileReader reader = new FileReader(configFilePath);
                return JsonParser.parseReader(reader).getAsJsonObject();
            } else {
                LogUtil.printErrorLog(ErrorLogs.EITHER_CONFIG_OR_CONFIG_FILE_PATH_REQUIRED.getLog());
                throw new UDFArgumentException(ErrorMessage.EitherConfigOrConfigFilePathRequired.getMessage());
            }
        } catch (FileNotFoundException e) {
            LogUtil.printErrorLog(ErrorLogs.CONFIG_FILE_NOT_FOUND.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.ConfigFileNotFound.getMessage(), configFilePath));
        } catch (JsonSyntaxException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_JSON_FORMAT.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidJsonFormat.getMessage(), Constants.SKYFLOW_CONFIG_JSON));
        } catch (IllegalStateException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_TYPE_OF_JSON.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidTypeOfJson.getMessage(), Constants.SKYFLOW_CONFIG_JSON));
        } catch (UDFArgumentException e) {
            throw e;
        } catch (Exception e) {
            LogUtil.printErrorLog(ErrorLogs.UNEXPECTED_ERROR_SESSION_STATE.getLog());
            throw new RuntimeException(ErrorMessage.UnexpectedError.getMessage(), e);
        }
    }

    /**
     * Validates the Config object for required fields and proper values.
     * Checks vault ID, cluster ID, environment settings, and credential configuration.
     *
     * @param config The Config object to validate
     * @throws UDFArgumentException If any validation fails
     */
    public static void validateConfig(Config config) throws UDFArgumentException {
        String vaultId = config.getVaultId();
        String clusterId = config.getClusterId();
        String vaultURL = config.getVaultURL();
        String credentials = config.getCredentials();
        String env = config.getEnv();
        String filePath = config.getFilePath();

        if (vaultId == null) {
            LogUtil.printErrorLog(ErrorLogs.VAULT_ID_IS_REQUIRED.getLog());
            throw new UDFArgumentException(ErrorMessage.InvalidVaultId.getMessage());
        } else if (vaultId.trim().isEmpty()) {
            LogUtil.printErrorLog(ErrorLogs.EMPTY_VAULT_ID.getLog());
            throw new UDFArgumentException(ErrorMessage.EmptyVaultId.getMessage());
        } else if (vaultURL == null && clusterId == null) {
            LogUtil.printErrorLog(ErrorLogs.EITHER_VAULT_URL_OR_CLUSTER_ID_REQUIRED.getLog());
            throw new UDFArgumentException(ErrorMessage.EitherVaultUrlOrClusterIdRequired.getMessage());
        } else if (vaultURL != null && vaultURL.trim().isEmpty()) {
            LogUtil.printErrorLog(ErrorLogs.EMPTY_VAULT_URL.getLog());
            throw new UDFArgumentException(ErrorMessage.EmptyVaultUrl.getMessage());
        } else if (clusterId != null && clusterId.trim().isEmpty()) {
            LogUtil.printErrorLog(ErrorLogs.EMPTY_CLUSTER_ID.getLog());
            throw new UDFArgumentException(ErrorMessage.EmptyClusterId.getMessage());
        } else if (env != null) {
            if (!env.equals(Env.SANDBOX.name()) && !env.equals(Env.PROD.name())) {
                LogUtil.printErrorLog(ErrorLogs.INVALID_ENVIRONMENT.getLog());
                throw new UDFArgumentException(ErrorMessage.InvalidEnvironment.getMessage());
            }
        } else if (filePath == null && credentials == null) {
            LogUtil.printErrorLog(ErrorLogs.EITHER_CREDENTIALS_OR_FILE_PATH_REQUIRED.getLog());
            throw new UDFArgumentException(ErrorMessage.EitherCredentialsOrFilePathRequired.getMessage());
        } else if (filePath != null && credentials != null) {
            LogUtil.printErrorLog(ErrorLogs.BOTH_CREDENTIALS_OR_FILE_PATH_PROVIDED.getLog());
            throw new UDFArgumentException(ErrorMessage.BothCredentialsOrFilePathProvided.getMessage());
        } else if (credentials != null) {
            validateCredentialsString(credentials);
        } else {
            validateFilePath(filePath);
        }
    }

    /**
     * Validates that the provided credentials string is a valid JSON object.
     *
     * @param credentials The credentials string to validate
     * @throws UDFArgumentException If the credentials string is not valid JSON
     */
    public static void validateCredentialsString(String credentials) throws UDFArgumentException {
        try {
            JsonParser.parseString(credentials).getAsJsonObject();
        } catch (JsonSyntaxException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_JSON_FORMAT.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidJsonFormat.getMessage(),
                    Constants.SKYFLOW_CONFIG_JSON_CREDENTIALS));
        } catch (IllegalStateException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_TYPE_OF_JSON.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidTypeOfJson.getMessage(),
                    Constants.SKYFLOW_CONFIG_JSON_CREDENTIALS));
        }
    }

    /**
     * Validates that the file at the given path exists and contains valid JSON.
     *
     * @param filePath Path to the credentials file to validate
     * @throws UDFArgumentException If the file doesn't exist or contains invalid JSON
     */
    public static void validateFilePath(String filePath) throws UDFArgumentException {
        try {
            FileReader reader = new FileReader(filePath);
            JsonParser.parseReader(reader).getAsJsonObject();
        } catch (FileNotFoundException e) {
            LogUtil.printErrorLog(ErrorLogs.CONFIG_FILE_NOT_FOUND.getLog());
            throw new UDFArgumentException(
                    Utils.parameterizedString(ErrorMessage.ConfigFileNotFound.getMessage(), filePath));
        } catch (JsonSyntaxException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_JSON_FORMAT.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidJsonFormat.getMessage(),
                    Constants.SKYFLOW_CONFIG_JSON_FILEPATH));
        } catch (IllegalStateException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_TYPE_OF_JSON.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidTypeOfJson.getMessage(),
                    Constants.SKYFLOW_CONFIG_JSON_FILEPATH));
        }
    }
}