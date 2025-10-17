package com.skyflow.hive.utils;

import com.skyflow.Skyflow;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HelperTest extends BaseSkyflowTest {

    @Test
    public void testGetConfigFromJsonString() throws UDFArgumentException {
        String config = "{\"vaultId\":\"test-vault\",\"clusterId\":\"test-cluster\",\"env\":\"PROD\",\"credentials\":\"{\\\"accessToken\\\":\\\"test-token\\\"}\"}";
        when(mockConf.get("skyflow.config")).thenReturn(config);
        when(mockConf.get("skyflow.config.file")).thenReturn(null);

        Skyflow client = Helper.getConfigParametersAndInitialiseSDK();
        assertNotNull(client);
    }

    @Test
    public void testGetConfigFromJsonStringWithoutEnv() throws UDFArgumentException {
        String config = "{\"vaultId\":\"test-vault\",\"clusterId\":\"test-cluster\",\"credentials\":\"{\\\"accessToken\\\":\\\"test-token\\\"}\"}";
        when(mockConf.get("skyflow.config")).thenReturn(config);
        when(mockConf.get("skyflow.config.file")).thenReturn(null);

        Skyflow client = Helper.getConfigParametersAndInitialiseSDK();
        assertNotNull(client);
    }

    @Test
    public void testMissingConfig() {
        when(mockConf.get("skyflow.config")).thenReturn(null);
        when(mockConf.get("skyflow.config.file")).thenReturn(null);

        assertThrows(UDFArgumentException.class, () -> Helper.getConfigParametersAndInitialiseSDK());
    }

    @Test
    public void testInvalidJsonConfig() {
        when(mockConf.get("skyflow.config")).thenReturn("invalid json");
        when(mockConf.get("skyflow.config.file")).thenReturn(null);

        assertThrows(UDFArgumentException.class, () -> Helper.getConfigParametersAndInitialiseSDK());
    }

    @Test
    public void testInvalidConfigFile() {
        when(mockConf.get("skyflow.config")).thenReturn(null);
        when(mockConf.get("skyflow.config.file")).thenReturn("/nonexistent/path.json");

        assertThrows(UDFArgumentException.class, () -> Helper.getConfigParametersAndInitialiseSDK());
    }
}