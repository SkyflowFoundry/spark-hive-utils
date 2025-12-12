package com.skyflow.hive.utils;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ConfigTest {

    @Test
    void testConfigDeserializationAllFields() {
        String json = "{" +
                "\"vaultId\":\"test-vault\"," +
                "\"clusterId\":\"test-cluster\"," +
                "\"env\":\"PROD\"," +
                "\"filePath\":\"/path/to/creds\"," +
                "\"credentials\":\"raw-creds-string\"," +
                "\"vaultURL\":\"https://test.vault.url\"" +
                "}";

        Config config = new Gson().fromJson(json, Config.class);

        assertEquals("test-vault", config.getVaultId());
        assertEquals("test-cluster", config.getClusterId());
        assertEquals("PROD", config.getEnv());
        assertEquals("/path/to/creds", config.getFilePath());
        assertEquals("raw-creds-string", config.getCredentials());
        assertEquals("https://test.vault.url", config.getVaultURL());
    }

    @Test
    void testConfigDeserializationPartialFields() {
        String json = "{\"vaultId\":\"test-vault\"}";
        Config config = new Gson().fromJson(json, Config.class);

        assertEquals("test-vault", config.getVaultId());
        assertNull(config.getClusterId());
        assertNull(config.getVaultURL());
    }

    @Test
    void testNullValues() {
        String json = "{}";
        Config config = new Gson().fromJson(json, Config.class);

        assertNull(config.getVaultId());
        assertNull(config.getClusterId());
        assertNull(config.getEnv());
        assertNull(config.getFilePath());
        assertNull(config.getCredentials());
        assertNull(config.getVaultURL());
    }

    @Test
    void testConfigGettersDefaultState() {
        Config config = new Config();
        assertNull(config.getVaultId(), "VaultId should be null by default");
        assertNull(config.getClusterId(), "ClusterId should be null by default");
        assertNull(config.getEnv(), "Env should be null by default");
        assertNull(config.getFilePath(), "FilePath should be null by default");
        assertNull(config.getCredentials(), "Credentials should be null by default");
        assertNull(config.getVaultURL(), "VaultURL should be null by default");
    }
}