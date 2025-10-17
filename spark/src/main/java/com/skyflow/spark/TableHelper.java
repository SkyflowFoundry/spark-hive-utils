package com.skyflow.spark;

import com.skyflow.enums.Env;
import com.skyflow.enums.LogLevel;

import java.util.logging.Level;

public class TableHelper {
    private String vaultUrl;
    private String vaultId;
    private String vaultCredentials;
    private String tableName;
    private String clusterId;
    private Env env;
    private Level logLevel = Level.INFO;
    private LogLevel jvmLogLevel = LogLevel.INFO;

    public TableHelper(String vaultUrl, String vaultId, String vaultCredentials, String tableName) {
        this.vaultUrl = vaultUrl;
        this.vaultId = vaultId;
        this.vaultCredentials = vaultCredentials;
        this.tableName = tableName;
    }

    public TableHelper(String vaultId, String vaultCredentials, String tableName, String clusterId, Env env,
            Level level, LogLevel logLevel) {
        this.vaultId = vaultId;
        this.vaultCredentials = vaultCredentials;
        this.tableName = tableName;
        this.clusterId = clusterId;
        this.env = env;
        this.logLevel = level;
        this.jvmLogLevel = logLevel;
    }

    public TableHelper(String vaultUrl, String vaultId, String vaultCredentials, String tableName, String clusterId,
            Env env, Level level, LogLevel logLevel) {
        this.vaultUrl = vaultUrl;
        this.vaultId = vaultId;
        this.vaultCredentials = vaultCredentials;
        this.tableName = tableName;
        this.clusterId = clusterId;
        this.env = env;
        this.logLevel = level;
        this.jvmLogLevel = logLevel;
    }

    public void setVaultId(String vaultId) {
        this.vaultId = vaultId;
    }

    public void setVaultUrl(String vaultUrl) {
        this.vaultUrl = vaultUrl;
    }

    public void setVaultCredentials(String vaultCredentials) {
        this.vaultCredentials = vaultCredentials;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public void setEnv(Env env) {
        this.env = env;
    }

    public void setLogLevel(Level logLevel) {
        this.logLevel = logLevel;
    }

    public void setJvmLogLevel(LogLevel jvmLogLevel) {
        this.jvmLogLevel = jvmLogLevel;
    }

    public String getVaultUrl() {
        return vaultUrl;
    }

    public String getVaultId() {
        return vaultId;
    }

    public String getVaultCredentials() {
        return vaultCredentials;
    }

    public String getTableName() {
        return tableName;
    }

    public Env getEnv() {
        return env;
    }

    public String getClusterId() {
        return clusterId;
    }

    public Level getLogLevel() {
        return logLevel;
    }

    public LogLevel getJvmLogLevel() {
        return jvmLogLevel;
    }
}
