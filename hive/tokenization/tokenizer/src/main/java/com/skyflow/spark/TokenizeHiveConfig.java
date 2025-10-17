package com.skyflow.spark;

import com.skyflow.enums.Env;
import com.skyflow.enums.LogLevel;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;

class TokenizeHiveConfig {
    private final String sourceTable;
    private final String destinationTable;
    private final String fsUri;
    private final String metastoreUri;
    private final String sparkAppName;
    private final String sparkMaster;
    private final boolean useDatanodeHostname;
    private final Optional<String> partitionSpec;
    private final Optional<String> partitionColumn;
    private final Optional<String> partitionValue;
    private final String columnMapping;
    private final String vaultId;
    private final String credentialJson;
    private final String tokenTable;
    private final String clusterId;
    private final Env env;
    private final Level javaLogLevel;
    private final LogLevel skyflowLogLevel;
    private final int insertBatchSize;
    private final int detokenizeBatchSize;
    private final int retryCount;

    private TokenizeHiveConfig(
            String sourceTable,
            String destinationTable,
            String fsUri,
            String metastoreUri,
            String sparkAppName,
            String sparkMaster,
            boolean useDatanodeHostname,
            Optional<String> partitionSpec,
            Optional<String> partitionColumn,
            Optional<String> partitionValue,
            String columnMapping,
            String vaultId,
            String credentialJson,
            String tokenTable,
            String clusterId,
            Env env,
            Level javaLogLevel,
            LogLevel skyflowLogLevel,
            int insertBatchSize,
            int detokenizeBatchSize,
            int retryCount
    ) {
        this.sourceTable = sourceTable;
        this.destinationTable = destinationTable;
        this.fsUri = fsUri;
        this.metastoreUri = metastoreUri;
        this.sparkAppName = sparkAppName;
        this.sparkMaster = sparkMaster;
        this.useDatanodeHostname = useDatanodeHostname;
        this.partitionSpec = partitionSpec;
        this.partitionColumn = partitionColumn;
        this.partitionValue = partitionValue;
        this.columnMapping = columnMapping;
        this.vaultId = vaultId;
        this.credentialJson = credentialJson;
        this.tokenTable = tokenTable;
        this.clusterId = clusterId;
        this.env = env;
        this.javaLogLevel = javaLogLevel;
        this.skyflowLogLevel = skyflowLogLevel;
        this.insertBatchSize = insertBatchSize;
        this.detokenizeBatchSize = detokenizeBatchSize;
        this.retryCount = retryCount;
    }

    static TokenizeHiveConfig load() {
        return fromEnv(mergedEnvironment(System.getenv(), Paths.get(".env")));
    }

    static Map<String, String> mergedEnvironment(Map<String, String> runtimeEnv, Path dotEnvPath) {
        Map<String, String> merged = new HashMap<>(readDotEnv(dotEnvPath));
        merged.putAll(runtimeEnv);
        return merged;
    }

    static TokenizeHiveConfig fromEnv(Map<String, String> env) {
        String src = require(env, "TOKENIZER_SOURCE_TABLE");
        String dst = require(env, "TOKENIZER_DEST_TABLE");
        String fsUri = require(env, "TOKENIZER_FS_URI");
        String msUri = require(env, "TOKENIZER_METASTORE_URI");
        String appName = require(env, "TOKENIZER_SPARK_APP_NAME");
        String master = require(env, "TOKENIZER_SPARK_MASTER");
        boolean useDatanodeHostname = parseBoolean(require(env, "TOKENIZER_SPARK_USE_DATANODE_HOSTNAME"));

        Optional<String> specOpt = optional(env, "TOKENIZER_PARTITION_SPEC");
        Optional<String> columnOpt = optional(env, "TOKENIZER_PARTITION_COLUMN");
        Optional<String> valueOpt = optional(env, "TOKENIZER_PARTITION_VALUE");

        if (specOpt.isEmpty() && (columnOpt.isPresent() ^ valueOpt.isPresent())) {
            throw new IllegalArgumentException(
                    "Provide both TOKENIZER_PARTITION_COLUMN and TOKENIZER_PARTITION_VALUE, or neither.");
        }

        Optional<String> partitionSpec = specOpt;
        Optional<String> partitionColumn = specOpt.isPresent() ? Optional.empty() : columnOpt;
        Optional<String> partitionValue = specOpt.isPresent() ? Optional.empty() : valueOpt;

        String columnMapping = requireWithFallback(env, "SKYFLOW_COLUMN_MAPPING", "");
        String vaultId = requireWithFallback(env, "SKYFLOW_VAULT_ID", "");
        String credentialJson = requireWithFallback(env, "SKYFLOW_CREDENTIAL_JSON", "");
        String tokenTable = requireWithFallback(env, "SKYFLOW_TOKEN_TABLE", "");
        String connectionId = requireWithFallback(env, "SKYFLOW_CLUSTER_ID", "");

        Env resolvedEnv = parseEnv(requireWithFallback(env, "SKYFLOW_ENV", "SANDBOX"));
        Level javaLevel = parseJavaLevel(requireWithFallback(env, "SKYFLOW_SPARK_LOG_LEVEL", "INFO"));
        LogLevel skyflowLevel = parseSkyflowLogLevel(requireWithFallback(env, "SKYFLOW_SDK_LOG_LEVEL", "INFO"));

        int insertBatchSize = parseInt(requireWithFallback(env, "SKYFLOW_INSERT_BATCH_SIZE", "SKYFLOW_INSERT_BATCH_SIZE"), "1000");
        int detokenizeBatchSize = parseInt(requireWithFallback(env, "SKYFLOW_DETOKENIZE_BATCH_SIZE", "SKYFLOW_DETOKENIZE_BATCH_SIZE"), "1000");
        int retryCount = parseInt(requireWithFallback(env, "SKYFLOW_RETRY_COUNT", "SKYFLOW_RETRY_COUNT"), "3");

        return new TokenizeHiveConfig(
                src,
                dst,
                fsUri,
                msUri,
                appName,
                master,
                useDatanodeHostname,
                partitionSpec,
                partitionColumn,
                partitionValue,
                columnMapping,
                vaultId,
                credentialJson,
                tokenTable,
                connectionId,
                resolvedEnv,
                javaLevel,
                skyflowLevel,
                insertBatchSize,
                detokenizeBatchSize,
                retryCount
        );
    }

    String getSourceTable() {
        return sourceTable;
    }

    String getDestinationTable() {
        return destinationTable;
    }

    String getFsUri() {
        return fsUri;
    }

    String getMetastoreUri() {
        return metastoreUri;
    }

    String getSparkAppName() {
        return sparkAppName;
    }

    String getSparkMaster() {
        return sparkMaster;
    }

    boolean useDatanodeHostname() {
        return useDatanodeHostname;
    }

    Optional<String> getPartitionSpec() {
        return partitionSpec;
    }

    Optional<String> getPartitionColumn() {
        return partitionColumn;
    }

    Optional<String> getPartitionValue() {
        return partitionValue;
    }

    String getColumnMapping() {
        return columnMapping;
    }

    String getVaultId() {
        return vaultId;
    }

    String getCredentialJson() {
        return credentialJson;
    }

    String getTokenTable() {
        return tokenTable;
    }

    String getClusterId() {
        return clusterId;
    }

    Env getEnv() {
        return env;
    }

    Level getJavaLogLevel() {
        return javaLogLevel;
    }

    LogLevel getSkyflowLogLevel() {
        return skyflowLogLevel;
    }

    int getInsertBatchSize() {
        return insertBatchSize;
    }

    int getDetokenizeBatchSize() {
        return detokenizeBatchSize;
    }

    int getRetryCount() {
        return retryCount;
    }

    Properties buildTokenizationProperties() {
        Properties props = new Properties();
        props.put("columnMapping", columnMapping);
        return props;
    }

    private static Map<String, String> readDotEnv(Path dotEnvPath) {
        Map<String, String> values = new HashMap<>();
        if (dotEnvPath == null || !Files.exists(dotEnvPath)) {
            return values;
        }
        try (BufferedReader reader = Files.newBufferedReader(dotEnvPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int separatorIndex = line.indexOf('=');
                if (separatorIndex <= 0) {
                    continue;
                }
                String key = line.substring(0, separatorIndex).trim();
                String value = line.substring(separatorIndex + 1);
                if (!key.isEmpty()) {
                    values.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read .env file at " + dotEnvPath, e);
        }
        return values;
    }

    private static String require(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Environment variable " + key + " is required.");
        }
        return value;
    }

    private static String requireWithFallback(Map<String, String> env, String primary, String fallback) {
        String value = env.get(primary);
        if (value != null && !value.trim().isEmpty()) {
            return value;
        }
        if (fallback != null) {
            return require(env, fallback);
        }
        throw new IllegalArgumentException("Environment variable " + primary + " is required.");
    }

    private static Optional<String> optional(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.trim().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    private static boolean parseBoolean(String raw) {
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException("Boolean value expected but found: " + raw);
    }

    private static int parseInt(String raw, String key) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(key + " must be an integer.");
        }
    }

    private static Env parseEnv(String raw) {
        try {
            return Env.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported Skyflow environment value: " + raw);
        }
    }

    private static LogLevel parseSkyflowLogLevel(String raw) {
        try {
            return LogLevel.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported Skyflow SDK log level value: " + raw);
        }
    }

    private static Level parseJavaLevel(String raw) {
        try {
            return Level.parse(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported Spark log level value: " + raw);
        }
    }
}
