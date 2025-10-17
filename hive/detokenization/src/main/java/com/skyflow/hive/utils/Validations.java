package com.skyflow.hive.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.skyflow.hive.errors.ErrorMessage;
import com.skyflow.hive.logs.ErrorLogs;
import com.skyflow.utils.Utils;
import com.skyflow.utils.logger.LogUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.FileNotFoundException;
import java.io.FileReader;

public final class Validations {

    public static JsonObject validateSessionState(SessionState sessionState) throws UDFArgumentException {
        String config = sessionState.getConf().get("skyflow.config");
        String configFilePath = sessionState.getConf().get("skyflow.config.file");

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
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidJsonFormat.getMessage(), "skyflow config json"));
        } catch (IllegalStateException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_TYPE_OF_JSON.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidTypeOfJson.getMessage(), "skyflow config json"));
        } catch (Exception e) {
            LogUtil.printErrorLog(ErrorLogs.UNEXPECTED_ERROR_SESSION_STATE.getLog());
            throw new RuntimeException(ErrorMessage.UnexpectedError.getMessage(), e);
        }
    }

    public static void validateConfig(Config config) throws UDFArgumentException {
        String vaultId = config.getVaultId();
        String clusterId = config.getClusterId();
        String credentials = config.getCredentials();
        String filePath = config.getFilePath();

        if (vaultId == null) {
            LogUtil.printErrorLog(ErrorLogs.VAULT_ID_IS_REQUIRED.getLog());
            throw new UDFArgumentException(ErrorMessage.InvalidVaultId.getMessage());
        } else if (vaultId.trim().isEmpty()) {
            LogUtil.printErrorLog(ErrorLogs.EMPTY_VAULT_ID.getLog());
            throw new UDFArgumentException(ErrorMessage.EmptyVaultId.getMessage());
        } else if (clusterId == null) {
            LogUtil.printErrorLog(ErrorLogs.CLUSTER_ID_IS_REQUIRED.getLog());
            throw new UDFArgumentException(ErrorMessage.InvalidClusterId.getMessage());
        } else if (clusterId.trim().isEmpty()) {
            LogUtil.printErrorLog(ErrorLogs.EMPTY_CLUSTER_ID.getLog());
            throw new UDFArgumentException(ErrorMessage.EmptyClusterId.getMessage());
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

    public static void validateCredentialsString(String credentials) throws UDFArgumentException {
        try {
            JsonParser.parseString(credentials).getAsJsonObject();
        } catch (JsonSyntaxException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_JSON_FORMAT.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidJsonFormat.getMessage(), "skyflow config json credentials"));
        } catch (IllegalStateException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_TYPE_OF_JSON.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidTypeOfJson.getMessage(), "skyflow config json credentials"));
        }
    }

    public static void validateFilePath(String filePath) throws UDFArgumentException {
        try {
            FileReader reader = new FileReader(filePath);
            JsonParser.parseReader(reader).getAsJsonObject();
        } catch (FileNotFoundException e) {
            LogUtil.printErrorLog(ErrorLogs.CONFIG_FILE_NOT_FOUND.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.ConfigFileNotFound.getMessage(), filePath));
        } catch (JsonSyntaxException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_JSON_FORMAT.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidJsonFormat.getMessage(), "skyflow config json filePath"));
        } catch (IllegalStateException e) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_TYPE_OF_JSON.getLog());
            throw new UDFArgumentException(Utils.parameterizedString(ErrorMessage.InvalidTypeOfJson.getMessage(), "skyflow config json filePath"));
        }
    }
}