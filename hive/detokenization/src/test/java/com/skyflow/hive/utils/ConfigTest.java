package com.skyflow.hive.utils;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;


public class ConfigTest {

    @Test
    public void testConfigDeserialization() {
        String json = "{\"vaultId\":\"test-vault\",\"clusterId\":\"test-cluster\",\"env\":\"PROD\",\"filePath\":\"/path/to/creds\"}";
        Config config = new Gson().fromJson(json, Config.class);

        Assert.assertEquals("test-vault", config.getVaultId());
        Assert.assertEquals("test-cluster", config.getClusterId());
        Assert.assertEquals("PROD", config.getEnv());
        Assert.assertEquals("/path/to/creds", config.getFilePath());
    }

    @Test
    public void testNullValues() {
        String json = "{}";
        Config config = new Gson().fromJson(json, Config.class);

        Assert.assertNull(config.getVaultId());
        Assert.assertNull(config.getClusterId());
        Assert.assertNull(config.getEnv());
        Assert.assertNull(config.getFilePath());
    }

    @Test
    public void testConfigGetters() {
        Config config = new Config();
        Assert.assertNull("VaultId should be null by default", config.getVaultId());
        Assert.assertNull("ClusterId should be null by default", config.getClusterId());
        Assert.assertNull("Env should be null by default", config.getEnv());
        Assert.assertNull("FilePath should be null by default", config.getFilePath());
        Assert.assertNull("Credentials should be null by default", config.getCredentials());
    }
}