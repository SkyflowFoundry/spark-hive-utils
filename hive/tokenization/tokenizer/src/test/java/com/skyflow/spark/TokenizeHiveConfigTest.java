package com.skyflow.spark;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TokenizeHiveConfigTest {

    @Test
    void fromEnvWithPartitionSpecLoadsConfig() {
        Map<String, String> env = baseEnv();
        env.put("TOKENIZER_PARTITION_SPEC", "ds='2024-01-01'");

        TokenizeHiveConfig config = TokenizeHiveConfig.fromEnv(env);

        assertEquals("mydb.credit_cards_test", config.getSourceTable());
        assertEquals("mydb.credit_cards_test_tok", config.getDestinationTable());
        assertTrue(config.useDatanodeHostname());
        assertTrue(config.getPartitionSpec().isPresent());
        assertFalse(config.getPartitionColumn().isPresent());
    }

    @Test
    void fromEnvWithPartitionColumnAndValueLoadsConfig() {
        Map<String, String> env = baseEnv();
        env.remove("TOKENIZER_PARTITION_SPEC");
        env.put("TOKENIZER_PARTITION_COLUMN", "ds");
        env.put("TOKENIZER_PARTITION_VALUE", "2024-01-01");

        TokenizeHiveConfig config = TokenizeHiveConfig.fromEnv(env);

        assertTrue(config.getPartitionSpec().isEmpty());
        assertEquals("ds", config.getPartitionColumn().orElseThrow());
        assertEquals("2024-01-01", config.getPartitionValue().orElseThrow());
    }

    @Test
    void missingRequiredVariableThrows() {
        Map<String, String> env = baseEnv();
        env.remove("TOKENIZER_SOURCE_TABLE");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> TokenizeHiveConfig.fromEnv(env));

        assertTrue(ex.getMessage().contains("TOKENIZER_SOURCE_TABLE"));
    }

    @Test
    void invalidBooleanThrows() {
        Map<String, String> env = baseEnv();
        env.put("TOKENIZER_SPARK_USE_DATANODE_HOSTNAME", "not-a-bool");

        assertThrows(IllegalArgumentException.class, () -> TokenizeHiveConfig.fromEnv(env));
    }

    @Test
    void missingPartitionFilterIsAllowed() {
        Map<String, String> env = baseEnv();
        env.remove("TOKENIZER_PARTITION_SPEC");
        TokenizeHiveConfig config = TokenizeHiveConfig.fromEnv(env);

        assertTrue(config.getPartitionSpec().isEmpty());
        assertTrue(config.getPartitionColumn().isEmpty());
    }

    @Test
    void partialPartitionColumnConfigurationThrows() {
        Map<String, String> env = baseEnv();
        env.remove("TOKENIZER_PARTITION_SPEC");
        env.put("TOKENIZER_PARTITION_COLUMN", "ds");

        assertThrows(IllegalArgumentException.class, () -> TokenizeHiveConfig.fromEnv(env));
    }

    @Test
    void mergedEnvironmentReadsFromDotEnv() throws IOException {
        Path dotEnv = writeDotEnv(baseEnv());

        Map<String, String> merged = TokenizeHiveConfig.mergedEnvironment(Collections.emptyMap(), dotEnv);
        TokenizeHiveConfig config = TokenizeHiveConfig.fromEnv(merged);

        assertEquals("mydb.credit_cards_test", config.getSourceTable());
        assertEquals("mydb.credit_cards_test_tok", config.getDestinationTable());
    }

    @Test
    void runtimeEnvironmentOverridesDotEnv() throws IOException {
        Path dotEnv = writeDotEnv(baseEnv());
        Map<String, String> runtimeEnv = new HashMap<>();
        runtimeEnv.put("TOKENIZER_SOURCE_TABLE", "override.table");

        Map<String, String> merged = TokenizeHiveConfig.mergedEnvironment(runtimeEnv, dotEnv);
        assertEquals("override.table", merged.get("TOKENIZER_SOURCE_TABLE"));
    }

    private Map<String, String> baseEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("TOKENIZER_SOURCE_TABLE", "mydb.credit_cards_test");
        env.put("TOKENIZER_DEST_TABLE", "mydb.credit_cards_test_tok");
        env.put("TOKENIZER_FS_URI", "hdfs://localhost:8020");
        env.put("TOKENIZER_METASTORE_URI", "thrift://localhost:9083");
        env.put("TOKENIZER_SPARK_APP_NAME", "SparkHiveTokenize");
        env.put("TOKENIZER_SPARK_MASTER", "local[*]");
        env.put("TOKENIZER_SPARK_USE_DATANODE_HOSTNAME", "true");
        env.put("TOKENIZER_PARTITION_SPEC", "ds='2024-01-01'");
        env.put("SKYFLOW_COLUMN_MAPPING", "{\n" +
                "\"card_number\" : { \"tableName\": \"credit_card_hashes\", \"columnName\": \"credit_card_hashes\" }\n" +
                "}");
        env.put("SKYFLOW_VAULT_ID", "ID");
        env.put("SKYFLOW_CREDENTIAL_JSON", "{}");
        env.put("SKYFLOW_TOKEN_TABLE", "credit_card_hashes");
        env.put("SKYFLOW_CLUSTER_ID", "qhdmceurtnlz");
        env.put("SKYFLOW_ENV", "SANDBOX");
        env.put("SKYFLOW_SPARK_LOG_LEVEL", "FINE");
        env.put("SKYFLOW_SDK_LOG_LEVEL", "ERROR");
        env.put("SKYFLOW_INSERT_BATCH_SIZE", "1000");
        env.put("SKYFLOW_DETOKENIZE_BATCH_SIZE", "1000");
        env.put("SKYFLOW_RETRY_COUNT", "2");
        return env;
    }

    private Path writeDotEnv(Map<String, String> values) throws IOException {
        Path tempDir = Files.createTempDirectory("tokenize-config-test");
        Path dotEnv = tempDir.resolve(".env");
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            builder.append(entry.getKey())
                    .append("=")
                    .append(entry.getValue())
                    .append(System.lineSeparator());
        }
        Files.writeString(dotEnv, builder.toString());
        return dotEnv;
    }
}
