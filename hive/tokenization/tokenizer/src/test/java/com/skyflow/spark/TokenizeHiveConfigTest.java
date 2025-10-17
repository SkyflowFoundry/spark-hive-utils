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
        env.put("SKYFLOW_VAULT_ID", "gbadb55ab2274ab3b366ec805705d8be");
        env.put("SKYFLOW_CREDENTIAL_JSON", "{\"clientID\":\"b4cc80649f594b62b0657d704f2bbd0a\",\"clientName\":\"vyshnavi-blitz-sa-jwt\",\"tokenURI\":\"https://manage-blitz.skyflowapis.dev/v1/auth/sa/oauth/token\",\"keyID\":\"ae018aee355f4952bb390e1750b957d6\",\"privateKey\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDP3FQeM2do+YRJ\\nQ1/+aWApuWW7qwqTf274vqzR4iK+HZoyi4HfGbcambFEqYGXkpTzJwVXMpw+WJOQ\\nE8W/ebtSTT6NVAWEOnDlBZTeVcEaH6SemXh0pUqAaLKaVu5F2CeoVl/e49DjSCzX\\nqcOyIE/Zm5u/nD38PxQqmieGW0bHGfsAQc+dg23KxsQUlLNGnkrvJ914Y+L/Sxjv\\nh42DuNEmXEms6x9guFKbxpfl+8f8z0G5tMcLoeYGShmpuAMwAjn0w1S18vzoXGHY\\naF9fz8GTioWWVUONCP9F3oTnU2Pbu+IWgQ3khaBYv9EecxsEbbYeWOsn085EAYOm\\nQIlEZzmHAgMBAAECggEAKfyBaVBM1SGBnSLk4wj6CbOPXycfjcex7MDnO8YH6osk\\nOI2BgEU7fT4KVqo+qRVMzQjWxVRNxzIxLGsyCIlYUzmTJw0PyDPd1gUYmwiLZvF5\\nWSf68uTodaOwYAirPKR67j/0QZuW+DH+DNoX04U/W99YyI2Q38EGG8zjqvoJA/JY\\nVUCaRUuZroBhyHBDkqc/lPgjSwdMH8+QuDqoAoiqYv6CkMg1fe+62b2Ls+Bt85xj\\n81PTsSE0V2xqkURHfZhPt9Ea/G35OqAIarJdE9hwK1QUpp88hZ/EQ5kq7JtD9gHc\\nvc6gK8sW/qOy4uGpYFxbUzgeD+dzERxjekjWPJW+QQKBgQDk2tZBe5xjoo/kv5+a\\nn5UErl/LQ4Bhz4wvZRCum361tFmX8F2u+uNxmyBkJFX5p+nAzJJMA6VckNgsIlIx\\nSQWz6ELx3qZou+MN/OY7A4yiJXs3F0r6WXEprsHmFhYt6XvwNX5y+RoBr9e94qtu\\nrgZYMJrOFr0+D+eOU2LWXbmpEQKBgQDohAE+HNafNukC0jDtvhJR98QBSEr7EtJb\\nNxb8qkENGlomg8KnFWY/sWSx5EyfB4lsWxis+5e3qkM07pIDN6F+ClHXuwIOQ5P+\\nhd0JSGROlMUqArxob39asEM1nxxTH1mckJ0oMmf4C+eykdMd0GUNaY6YDsbo1tzg\\nbOOlGtB5FwKBgFn8NizPVuHSZLP0KOSahPSvP9ljtFJBUoS8P3/4gi0eOfBFQgDw\\ng4gDEAxwQSIGRSJbDdc3w8iO54ELCbh5VvWziMcj3djmr9OrMjfYIG8NP9KcpQ2l\\nJ5rVLUa0PopU+TetQQgiLHinVLREMVY2tSXuDqCkTkrd5BWRchaKCduRAoGBAKQw\\n60u4Q5qExQBX+3M3AGrfgorBSSmJPgBpo5LiizM8g6Kk8qTYSK77jrbMF4tJYDKj\\nFYa514Y79l9/hJ4+/4wor2iexzHZaL2YO+EdPK/9AEnNSsyYx577z0ojRHYyMi6M\\nNF45Ug/oIEKKFUH9cg6Jlscr0CyF5c2ZNxvVFpgbAoGAPEOnZ+idd0rrKTqJKbHr\\nAbUtlYZkY3nn5WBceliIwEgbhWPnUjUdOJZ7tAiqMp2x+VZeDlK4Dj9CWtwarS9V\\nnycUO8T7brzIlBabwO5vv9MbVDt9yiZqwCCMb0P7I64an2xq04/0CHy9UnOj0W1U\\nhBZ6Obz11opcFpfLLdYD5lU=\\n-----END PRIVATE KEY-----\\n\",\"keyValidAfterTime\":\"2025-09-02T10:52:54.000Z\",\"keyValidBeforeTime\":\"2026-09-02T10:52:54.000Z\",\"keyAlgorithm\":\"KEY_ALG_RSA_2048\"}");
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
