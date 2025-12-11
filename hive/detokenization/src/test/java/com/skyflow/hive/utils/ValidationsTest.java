package com.skyflow.hive.utils;

import com.google.gson.JsonObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ValidationsTest {

    @Mock
    private SessionState mockSessionState;

    @Mock
    private HiveConf mockConf;

    @Test
    public void testValidateSessionStateWithDirectConfig() throws UDFArgumentException {
        when(mockSessionState.getConf()).thenReturn(mockConf);

        String validJson = "{\"vaultId\":\"123\"}";
        when(mockConf.get(Constants.SKYFLOW_CONFIG)).thenReturn(validJson);
        when(mockConf.get(Constants.SKYFLOW_CONFIG_FILE)).thenReturn(null);

        JsonObject result = Validations.validateSessionState(mockSessionState);

        assertNotNull(result);
        assertEquals("123", result.get("vaultId").getAsString());
    }

    @Test
    public void testValidateSessionStateWithConfigFile(@TempDir Path tempDir) throws Exception {
        when(mockSessionState.getConf()).thenReturn(mockConf);

        File configFile = tempDir.resolve("config.json").toFile();
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("{\"vaultId\":\"file-vault\"}");
        }

        when(mockConf.get(Constants.SKYFLOW_CONFIG)).thenReturn(null);
        when(mockConf.get(Constants.SKYFLOW_CONFIG_FILE)).thenReturn(configFile.getAbsolutePath());

        JsonObject result = Validations.validateSessionState(mockSessionState);

        assertNotNull(result);
        assertEquals("file-vault", result.get("vaultId").getAsString());
    }

    @Test
    public void testValidateSessionStateMissingBoth() {
        when(mockSessionState.getConf()).thenReturn(mockConf);

        when(mockConf.get(Constants.SKYFLOW_CONFIG)).thenReturn(null);
        when(mockConf.get(Constants.SKYFLOW_CONFIG_FILE)).thenReturn(null);

        Exception ex = assertThrows(RuntimeException.class, () ->
                Validations.validateSessionState(mockSessionState)
        );

        assertNotNull(ex.getCause());
        String causeMessage = ex.getCause().getMessage();

        assertTrue(
                causeMessage.contains("Either provide") ||
                        causeMessage.contains("skyflow.config")
        );
    }

    @Test
    public void testValidateSessionStateInvalidJson() {
        when(mockSessionState.getConf()).thenReturn(mockConf);

        when(mockConf.get(Constants.SKYFLOW_CONFIG)).thenReturn("{bad-json");
        when(mockConf.get(Constants.SKYFLOW_CONFIG_FILE)).thenReturn(null);

        assertThrows(UDFArgumentException.class, () ->
                Validations.validateSessionState(mockSessionState)
        );
    }

    @Test
    public void testValidateSessionStateFileNotFound() {
        when(mockSessionState.getConf()).thenReturn(mockConf);

        when(mockConf.get(Constants.SKYFLOW_CONFIG)).thenReturn(null);
        String badPath = "/non/existent/path.json";
        when(mockConf.get(Constants.SKYFLOW_CONFIG_FILE)).thenReturn(badPath);

        UDFArgumentException ex = assertThrows(UDFArgumentException.class, () ->
                Validations.validateSessionState(mockSessionState)
        );

        assertTrue(ex.getMessage().contains(badPath) || ex.getMessage().contains("Unable to find"));
    }

    @Test
    public void testValidateConfigSuccess() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("v123");
        when(config.getClusterId()).thenReturn("c123");
        when(config.getEnv()).thenReturn("PROD");
        when(config.getCredentials()).thenReturn("{}"); // Valid JSON string
        when(config.getFilePath()).thenReturn(null);

        assertDoesNotThrow(() -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigMissingVaultId() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn(null);

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigEmptyVaultId() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("   ");

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigMissingClusterIdAndVaultURL() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("v123");
        when(config.getClusterId()).thenReturn(null);
        when(config.getVaultURL()).thenReturn(null);

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigEmptyVaultURL() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("v123");
        when(config.getVaultURL()).thenReturn("   "); // Present but empty

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigInvalidEnv() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("v123");
        when(config.getClusterId()).thenReturn("c123");
        when(config.getEnv()).thenReturn("INVALID_ENV_NAME");

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigMissingCredsAndFile() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("v123");
        when(config.getClusterId()).thenReturn("c123");
        when(config.getCredentials()).thenReturn(null);
        when(config.getFilePath()).thenReturn(null);

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateConfigBothCredsAndFileProvided() {
        Config config = mock(Config.class);
        when(config.getVaultId()).thenReturn("v123");
        when(config.getClusterId()).thenReturn("c123");
        when(config.getCredentials()).thenReturn("{}");
        when(config.getFilePath()).thenReturn("/some/path");

        assertThrows(UDFArgumentException.class, () -> Validations.validateConfig(config));
    }

    @Test
    public void testValidateCredentialsStringInvalidJson() {
        assertThrows(UDFArgumentException.class, () ->
                Validations.validateCredentialsString("{bad-json")
        );
    }

    @Test
    public void testValidateCredentialsStringNotJsonObject() {
        assertThrows(UDFArgumentException.class, () ->
                Validations.validateCredentialsString("[1, 2]")
        );
    }

    @Test
    public void testValidateFilePathSuccess(@TempDir Path tempDir) throws Exception {
        File credsFile = tempDir.resolve("creds.json").toFile();
        try (FileWriter writer = new FileWriter(credsFile)) {
            writer.write("{\"token\":\"abc\"}");
        }

        assertDoesNotThrow(() -> Validations.validateFilePath(credsFile.getAbsolutePath()));
    }

    @Test
    public void testValidateFilePathNotFound() {
        assertThrows(UDFArgumentException.class, () ->
                Validations.validateFilePath("/non/existent/creds.json")
        );
    }

    @Test
    public void testValidateFilePathInvalidContent(@TempDir Path tempDir) throws Exception {
        File badFile = tempDir.resolve("bad.json").toFile();
        try (FileWriter writer = new FileWriter(badFile)) {
            writer.write("not json");
        }

        assertThrows(UDFArgumentException.class, () ->
                Validations.validateFilePath(badFile.getAbsolutePath())
        );
    }
}