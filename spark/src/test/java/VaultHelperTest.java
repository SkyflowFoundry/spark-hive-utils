import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;

import com.skyflow.spark.VaultHelper;
import com.skyflow.spark.SkyflowColumn;
import com.skyflow.spark.Constants;
import com.skyflow.spark.TableHelper;

import com.skyflow.Skyflow;
import com.skyflow.config.VaultConfig;
import com.skyflow.enums.Env;
import com.skyflow.enums.LogLevel;
import com.skyflow.errors.SkyflowException;
import com.skyflow.vault.controller.VaultController;
import com.skyflow.vault.data.ErrorRecord;
import com.skyflow.vault.data.InsertRequest;
import com.skyflow.vault.data.InsertRecord;
import com.skyflow.vault.data.InsertResponse;
import com.skyflow.vault.data.DetokenizeRequest;
import com.skyflow.vault.data.DetokenizeResponse;
import com.skyflow.vault.data.DetokenizeResponseObject;
import com.skyflow.vault.data.Summary;
import com.skyflow.vault.data.DetokenizeSummary;
import com.skyflow.vault.data.Success;
import com.skyflow.vault.data.Token;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class VaultHelperTest {

    @Mock
    private Skyflow skyflowMock;

    @Mock
    private VaultController vaultMock;

    @Mock
    private InsertResponse insertResponseMock;

    @Mock
    private DetokenizeResponse detokenizeResponseMock;

    @Mock
    private TableHelper tableHelperMock;

    private VaultHelper vaultHelper;

    private SparkSession spark;

    private StructType sampleSchema;

    // Simulate COLUMN_MAPPINGS for this test
    private static final Map<String, SkyflowColumn> COLUMN_MAPPINGS = new HashMap<>();

    @BeforeEach
    void setup() throws SkyflowException {
        MockitoAnnotations.openMocks(this);

        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        COLUMN_MAPPINGS.put("ph_nbr", SkyflowColumn.PHONE_NUMBER);
        COLUMN_MAPPINGS.put("email_id", SkyflowColumn.EMAIL);

        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);

        when(tableHelperMock.getVaultId()).thenReturn("vault-id");
        when(tableHelperMock.getVaultUrl()).thenReturn("https://cluster-id.vault.skyflow.com");
        when(tableHelperMock.getVaultCredentials()).thenReturn("cred-string");
        when(tableHelperMock.getTableName()).thenReturn("test_table");

        spark = SparkSession.builder()
                .appName("VaultHelperTest")
                .master("local[*]")
                .getOrCreate();

        sampleSchema = new StructType(new StructField[] {
                new StructField("first_nm", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ph_nbr", DataTypes.StringType, true, Metadata.empty())
        });

        // Create VaultHelper instance BUT spy it to override initializeSkyflowClient to
        // inject skyflowMock
        vaultHelper = spy(new VaultHelper(spark, 5, 5));
        vaultHelper = spy(new VaultHelper(spark, 5, 5, 2));

        doAnswer(invocation -> {
            vaultHelper.skyflowClient = skyflowMock;
            when(skyflowMock.vault()).thenReturn(vaultMock);
            return null;
        }).when(vaultHelper).initializeSkyflowClient((tableHelperMock));

        tableHelperMock = mock(TableHelper.class);
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.close();
        }
        // Remove all handlers from logger to prevent handler duplication across tests
        Logger logger = Logger.getLogger(VaultHelper.class.getName());
        for (Handler handler : logger.getHandlers()) {
            logger.removeHandler(handler);
        }
    }

    // Helper to mock static Skyflow.builder() method to return the passed
    // builderMock
    private void mockStaticSkyflowBuilder(Skyflow.SkyflowClientBuilder builderMock) {
        // Since Skyflow.builder() is static, mockito-inline or PowerMockito is needed
        // to mock static methods.
        // If not available, consider injecting Skyflow.Builder or factory for easier
        // testing.
        // Here, assume vaultHelper has a protected method to get builder which can be
        // spied/mocked.
        // Otherwise, this is a placeholder for static mocking.

        // Example placeholder: spy vaultHelper and override method that calls
        // Skyflow.builder()
        doReturn(builderMock).when(vaultHelper).getSkyflowBuilder();
    }

    /**
     * Utility to mock initializeSkyflowClient to inject skyflowMock and vaultMock.
     * Use only in tests that require it.
     */
    private void mockInitializeSkyflowClientForTest() throws SkyflowException {
        doAnswer(invocation -> {
            vaultHelper.skyflowClient = skyflowMock;
            when(skyflowMock.vault()).thenReturn(vaultMock);
            return null;
        }).when(vaultHelper).initializeSkyflowClient(any(TableHelper.class));
    }

    private Dataset<Row> createSampleInputDataset() {
        List<Row> data = Arrays.asList(
                RowFactory.create("John", "1234567890"),
                RowFactory.create("Alice", "0987654321"),
                RowFactory.create("Bob", "0187654321"));
        return spark.createDataFrame(data, sampleSchema);
    }

    private Dataset<Row> createSampleTokenizedDataset() {
        List<Row> data = Arrays.asList(
                RowFactory.create("token0", "token1"),
                RowFactory.create("token2", "token3"),
                RowFactory.create("token4", "token5"));
        return spark.createDataFrame(data, sampleSchema);
    }
    // Builder and initialization tests

    @Test
    void get_skyflow_builder_returns_non_null_builder() {
        VaultHelper vaultHelper = new VaultHelper(spark);

        Skyflow.SkyflowClientBuilder builder = vaultHelper.getSkyflowBuilder();

        assertNotNull(builder, "getSkyflowBuilder should return a non-null builder instance");
        // Optionally verify the returned object is of the expected class (not a mock)
        assertEquals(Skyflow.SkyflowClientBuilder.class, builder.getClass());
    }

    @Test
    void get_skyflow_builder_returns_new_instance_each_call() {
        VaultHelper vaultHelper = new VaultHelper(spark);

        Skyflow.SkyflowClientBuilder builder1 = vaultHelper.getSkyflowBuilder();
        Skyflow.SkyflowClientBuilder builder2 = vaultHelper.getSkyflowBuilder();

        assertNotNull(builder1);
        assertNotNull(builder2);
        assertNotSame(builder1, builder2, "getSkyflowBuilder should return a new builder instance on each call");
    }

    @Test
    void initialize_skyflow_client_with_cluster_id() throws SkyflowException {
        // Setup mock TableHelper
        when(tableHelperMock.getVaultCredentials()).thenReturn("cred-string");
        when(tableHelperMock.getVaultId()).thenReturn("vault-id");
        when(tableHelperMock.getClusterId()).thenReturn("cluster-id");
        when(tableHelperMock.getEnv()).thenReturn(Env.DEV);
        when(tableHelperMock.getJvmLogLevel()).thenReturn(LogLevel.INFO);
        when(tableHelperMock.getLogLevel()).thenReturn(Level.INFO);

        // Spy Skyflow.builder() to avoid real client creation
        Skyflow.SkyflowClientBuilder builderMock = mock(Skyflow.SkyflowClientBuilder.class);
        when(builderMock.setLogLevel(any())).thenReturn(builderMock);
        when(builderMock.addVaultConfig(any())).thenReturn(builderMock);
        Skyflow skyflowMock = mock(Skyflow.class);
        when(builderMock.build()).thenReturn(skyflowMock);

        // Inject the mocked builder via spy on Skyflow static builder method
        mockStaticSkyflowBuilder(builderMock);

        vaultHelper.initializeSkyflowClient(tableHelperMock);

        // Verify Credentials string set correctly
        ArgumentCaptor<VaultConfig> vaultConfigCaptor = ArgumentCaptor.forClass(VaultConfig.class);

        verify(builderMock).setLogLevel(LogLevel.INFO);
        verify(builderMock).addVaultConfig(vaultConfigCaptor.capture());
        verify(builderMock).build();

        VaultConfig configUsed = vaultConfigCaptor.getValue();

        assertEquals("vault-id", configUsed.getVaultId());
        assertEquals("cluster-id", configUsed.getClusterId());
        assertEquals(Env.DEV, configUsed.getEnv());
        assertNotNull(configUsed.getCredentials());
        assertEquals("cred-string", configUsed.getCredentials().getCredentialsString());

        // Verify logger level set and handler added
        Logger logger = Logger.getLogger(VaultHelper.class.getName());
        boolean foundConsoleHandler = false;
        for (Handler handler : logger.getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                foundConsoleHandler = true;
                break;
            }
        }
        assertTrue(foundConsoleHandler, "ConsoleHandler should be added to logger");
        assertEquals(Level.INFO, logger.getLevel());
    }

    @Test
    void initialize_skyflow_client_throws_exception_logs_and_rethrows() throws SkyflowException {
        when(tableHelperMock.getVaultCredentials()).thenReturn("cred-string");
        when(tableHelperMock.getVaultId()).thenReturn("vault-id");
        when(tableHelperMock.getClusterId()).thenReturn("cluster-id");
        when(tableHelperMock.getEnv()).thenReturn(Env.DEV);
        when(tableHelperMock.getJvmLogLevel()).thenReturn(LogLevel.INFO);
        when(tableHelperMock.getLogLevel()).thenReturn(Level.INFO);

        // Spy Skyflow.builder() to throw exception on build()
        Skyflow.SkyflowClientBuilder builderMock = mock(Skyflow.SkyflowClientBuilder.class);
        when(builderMock.setLogLevel(any())).thenReturn(builderMock);
        when(builderMock.addVaultConfig(any())).thenReturn(builderMock);
        when(builderMock.build()).thenThrow(new RuntimeException("Build failed"));

        mockStaticSkyflowBuilder(builderMock);

        SkyflowException thrown = assertThrows(SkyflowException.class,
                () -> vaultHelper.initializeSkyflowClient(tableHelperMock));
        assertTrue(thrown.getMessage().contains("Failed to initialize Skyflow client"));

        // Check if logger logged severe message (cannot easily verify logs here without
        // additional setup)
    }

    // End builder and initialization tests

    // Tokenization tests

    @Test
    void tokenize_success_no_retry() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);
        Dataset<Row> data = createSampleInputDataset();

        List<Success> successes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Success success = mock(Success.class);
            when(success.getIndex()).thenReturn(i);
            Token token = mock(Token.class);
            when(token.getToken()).thenReturn("token" + i);
            when(token.getTokenGroupName()).thenReturn("name");
            Map<String, List<Token>> tokensMap = new HashMap<>();
            tokensMap.put("name", Collections.singletonList(token));
            when(success.getTokens()).thenReturn(tokensMap);
            successes.add(success);
        }

        Summary summary = mock(Summary.class);
        when(summary.getTotalFailed()).thenReturn(0);

        when(insertResponseMock.getSuccess()).thenReturn(successes);
        when(insertResponseMock.getErrors()).thenReturn(Collections.emptyList());
        when(insertResponseMock.getSummary()).thenReturn(summary);

        when(vaultMock.bulkInsert(any())).thenReturn(insertResponseMock);
        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("token" + i, rows.get(i).getAs("first_nm"));
            assertEquals("200", rows.get(i).getAs("skyflow_status_code"));
            assertNull(rows.get(i).getAs("error"));
        }

        verify(vaultMock, times(1)).bulkInsert(any());
    }

    @Test
    void tokenize_respects_property_based_column_mappings() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        StructType schema = new StructType(new StructField[] {
                new StructField("cust_id", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> data = spark.createDataFrame(Collections.singletonList(RowFactory.create("12345")), schema);

        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{" +
                        "\"cust_id\": {" +
                        "\"tableName\": \"customer_table_override\", " +
                        "\"columnName\": \"customer_column_override\", " +
                        "\"tokenGroupName\": \"override_group\", " +
                        "\"redaction\": \"override_redaction\"}" +
                        "}");

        Summary summary = mock(Summary.class);
        when(summary.getTotalFailed()).thenReturn(0);

        Success success = mock(Success.class);
        when(success.getIndex()).thenReturn(0);
        Token token = mock(Token.class);
        when(token.getToken()).thenReturn("token-12345");
        when(token.getTokenGroupName()).thenReturn("override_group");
        Map<String, List<Token>> tokens = new HashMap<>();
        tokens.put("customer_column_override", Collections.singletonList(token));
        when(success.getTokens()).thenReturn(tokens);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.singletonList(success));
        when(insertResponseMock.getErrors()).thenReturn(Collections.emptyList());
        when(insertResponseMock.getSummary()).thenReturn(summary);

        ArgumentCaptor<InsertRequest> insertRequestCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        when(vaultMock.bulkInsert(any())).thenReturn(insertResponseMock);

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, properties);

        verify(vaultMock).bulkInsert(insertRequestCaptor.capture());
        InsertRequest capturedRequest = insertRequestCaptor.getValue();
        assertEquals(1, capturedRequest.getRecords().size());
        InsertRecord record = capturedRequest.getRecords().get(0);
        assertEquals("customer_table_override", record.getTable());
        assertTrue(record.getData().containsKey("customer_column_override"));
        assertEquals("12345", record.getData().get("customer_column_override"));

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals("token-12345", row.getAs("cust_id"));
        assertEquals(Constants.STATUS_OK, row.getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(row.getAs(Constants.ERROR));
    }

    @Test
    void tokenize_returns_passthrough_when_no_tokenizable_columns() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        StructType schema = new StructType(new StructField[] {
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("phone", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> data = spark.createDataFrame(Arrays.asList(
                RowFactory.create("Alice", "1111"),
                RowFactory.create("Bob", "2222")), schema);

        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"cust_id\": {\"tableName\": \"customer_table\", \"columnName\": \"customer_column\"}}");

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, properties);

        verify(vaultMock, times(0)).bulkInsert(any());

        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals("Alice", rows.get(0).getAs("name"));
        assertEquals("1111", rows.get(0).getAs("phone"));
        assertEquals(Constants.STATUS_OK, rows.get(0).getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(rows.get(0).getAs(Constants.ERROR));
        assertEquals("Bob", rows.get(1).getAs("name"));
        assertEquals("2222", rows.get(1).getAs("phone"));
        assertEquals(Constants.STATUS_OK, rows.get(1).getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(rows.get(1).getAs(Constants.ERROR));
    }

    @Test
    void tokenize_handles_duplicate_values_across_tables() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);

        Dataset<Row> data = spark.createDataFrame(Collections.singletonList(RowFactory.create("shared", "shared")),
                sampleSchema);

        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{" +
                        "\"first_nm\": {\"tableName\": \"name\", \"columnName\": \"name\"}," +
                        "\"ph_nbr\": {\"tableName\": \"phone_number\", \"columnName\": \"phone_number\"}" +
                        "}");

        Success successPrimary = mock(Success.class);
        when(successPrimary.getIndex()).thenReturn(0);
        Token tokenPrimary = mock(Token.class);
        when(tokenPrimary.getToken()).thenReturn("token-primary");
        Map<String, List<Token>> tokensPrimary = new HashMap<>();
        tokensPrimary.put("name", Collections.singletonList(tokenPrimary));
        when(successPrimary.getTokens()).thenReturn(tokensPrimary);
        when(successPrimary.getTable()).thenReturn("name");

        Success successAlias = mock(Success.class);
        when(successAlias.getIndex()).thenReturn(1);
        Token tokenAlias = mock(Token.class);
        when(tokenAlias.getToken()).thenReturn("token-alias");
        Map<String, List<Token>> tokensAlias = new HashMap<>();
        tokensAlias.put("phone_number", Collections.singletonList(tokenAlias));
        when(successAlias.getTokens()).thenReturn(tokensAlias);
        when(successAlias.getTable()).thenReturn("phone_number");

        Summary summary = mock(Summary.class);
        when(summary.getTotalFailed()).thenReturn(0);

        InsertResponse response = mock(InsertResponse.class);
        when(response.getSuccess())
                .thenReturn(Arrays.asList(successPrimary, successAlias));
        when(response.getErrors()).thenReturn(Collections.emptyList());
        when(response.getSummary()).thenReturn(summary);

        ArgumentCaptor<InsertRequest> requestCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        when(vaultMock.bulkInsert(any())).thenReturn(response);

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, properties);

        verify(vaultMock).bulkInsert(requestCaptor.capture());
        InsertRequest captured = requestCaptor.getValue();
        assertEquals(2, captured.getRecords().size());

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals("token-primary", row.getAs("first_nm"));
        assertEquals("token-alias", row.getAs("ph_nbr"));
        assertEquals(Constants.STATUS_OK, row.getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(row.getAs(Constants.ERROR));
    }

    @Test
    void tokenize_multiple_retries_partial_success() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);
        Dataset<Row> data = createSampleInputDataset();

        // Batch 1 initial: 1 success (index 0), 2 errors (1,2)
        Success success0 = mock(Success.class);
        when(success0.getIndex()).thenReturn(0);
        Token token0 = mock(Token.class);
        when(token0.getToken()).thenReturn("token0");
        when(token0.getTokenGroupName()).thenReturn("name");
        HashMap<String, List<Token>> tokensMap = new HashMap<>();
        tokensMap.put("name", Collections.singletonList(token0));
        when(success0.getTokens()).thenReturn(tokensMap);

        ErrorRecord err1 = mock(ErrorRecord.class);
        when(err1.getIndex()).thenReturn(1);
        when(err1.getCode()).thenReturn(503); // retryable error

        ErrorRecord err2 = mock(ErrorRecord.class);
        when(err2.getIndex()).thenReturn(2);
        when(err2.getCode()).thenReturn(503); // retryable error

        Summary summaryInitial = mock(Summary.class);
        when(summaryInitial.getTotalFailed()).thenReturn(2);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.singletonList(success0));
        when(insertResponseMock.getErrors()).thenReturn(Arrays.asList(err1, err2));
        when(insertResponseMock.getSummary()).thenReturn(summaryInitial);

        // Retry 1: success for index 1, error for index 2
        Success retrySuccess1 = mock(Success.class);
        when(retrySuccess1.getIndex()).thenReturn(0); // retry request indices remapped to 0 and 1
        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("token1");
        when(token1.getTokenGroupName()).thenReturn("name");
        HashMap<String, List<Token>> tokensMap1 = new HashMap<>();
        tokensMap1.put("name", Collections.singletonList(token1));
        when(retrySuccess1.getTokens()).thenReturn(tokensMap1);

        ErrorRecord retryErr2 = mock(ErrorRecord.class);
        when(retryErr2.getIndex()).thenReturn(1);
        when(retryErr2.getCode()).thenReturn(503);

        Summary summaryRetry1 = mock(Summary.class);
        when(summaryRetry1.getTotalFailed()).thenReturn(1);

        InsertResponse retryResponse1 = mock(InsertResponse.class);
        when(retryResponse1.getSuccess()).thenReturn(Collections.singletonList(retrySuccess1));
        when(retryResponse1.getErrors()).thenReturn(Collections.singletonList(retryErr2));
        when(retryResponse1.getSummary()).thenReturn(summaryRetry1);

        // Retry 2: error for index 2 (final)
        ErrorRecord retryErr3 = mock(ErrorRecord.class);
        when(retryErr3.getIndex()).thenReturn(0);
        when(retryErr3.getCode()).thenReturn(503);

        Summary summaryRetry2 = mock(Summary.class);
        when(summaryRetry2.getTotalFailed()).thenReturn(0);

        InsertResponse retryResponse2 = mock(InsertResponse.class);
        when(retryResponse2.getSuccess()).thenReturn(Collections.emptyList());
        when(retryResponse2.getErrors()).thenReturn(Collections.singletonList(retryErr3));
        when(retryResponse2.getSummary()).thenReturn(summaryRetry2);

        InsertRecord insertRecord = mock(InsertRecord.class);
        when(insertRecord.getTable()).thenReturn("name");
        when(vaultMock.bulkInsert(any()))
                .thenReturn(insertResponseMock) // Initial
                .thenReturn(retryResponse1) // Retry 1
                .thenReturn(retryResponse2); // Retry 2

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);

        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        // Verify each row has expected token after retries
        assertEquals("token0", rows.get(0).getAs("first_nm"));
        assertEquals("200", rows.get(0).getAs("skyflow_status_code"));

        Set<String> statuses = new HashSet<>(Arrays.asList(
                rows.get(1).getAs("skyflow_status_code"),
                rows.get(2).getAs("skyflow_status_code")));

        assertTrue(
                statuses.contains("200") && statuses.contains("500"),
                "Statuses should contain both 200 and 500");

        // Verify bulkInsert called 3 times (initial + 2 retries)
        verify(vaultMock, times(3)).bulkInsert(any());
    }

    @Test
    void tokenize_multiple_retries_no_retryable_errors() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);
        Dataset<Row> data = createSampleInputDataset();

        // Batch 1 initial: 1 success (index 0), 2 errors (1,2)
        Success success0 = mock(Success.class);
        when(success0.getIndex()).thenReturn(0);
        Token token0 = mock(Token.class);
        when(token0.getToken()).thenReturn("token0");
        when(token0.getTokenGroupName()).thenReturn("name");
        HashMap<String, List<Token>> tokensMap = new HashMap<>();
        tokensMap.put("name", Collections.singletonList(token0));
        when(success0.getTokens()).thenReturn(tokensMap);

        ErrorRecord err1 = mock(ErrorRecord.class);
        when(err1.getIndex()).thenReturn(1);
        when(err1.getCode()).thenReturn(400); // non retryable error

        ErrorRecord err2 = mock(ErrorRecord.class);
        when(err2.getIndex()).thenReturn(2);
        when(err2.getCode()).thenReturn(400); // non retryable error

        Summary summaryInitial = mock(Summary.class);
        when(summaryInitial.getTotalFailed()).thenReturn(2);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.singletonList(success0));
        when(insertResponseMock.getErrors()).thenReturn(Arrays.asList(err1, err2));
        when(insertResponseMock.getSummary()).thenReturn(summaryInitial);

        when(vaultMock.bulkInsert(any()))
                .thenReturn(insertResponseMock); // Initial

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);

        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        // Verify each row has expected token after retries
        assertEquals("token0", rows.get(0).getAs("first_nm"));
        assertEquals("200", rows.get(0).getAs("skyflow_status_code"));

        assertEquals("Alice", rows.get(1).getAs("first_nm"));
        assertEquals("500", rows.get(1).getAs("skyflow_status_code"));

        assertEquals("Bob", rows.get(2).getAs("first_nm"));
        assertEquals("500", rows.get(2).getAs("skyflow_status_code"));

        // Verify bulkInsert called 3 times (initial + 2 retries)
        verify(vaultMock, times(1)).bulkInsert(any());
    }

    // Detokenization tests

    @Test
    void detokenize_respects_property_based_column_mappings() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        StructType schema = new StructType(new StructField[] {
                new StructField("cust_id", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> tokenizedData = spark.createDataFrame(Collections.singletonList(RowFactory.create("token-12345")),
                schema);

        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{" +
                        "\"cust_id\": {" +
                        "\"tableName\": \"customer_table_override\", " +
                        "\"columnName\": \"customer_column_override\", " +
                        "\"tokenGroupName\": \"override_group\", " +
                        "\"redaction\": \"override_redaction\"}" +
                        "}");

        DetokenizeResponseObject responseObject = mock(DetokenizeResponseObject.class);
        when(responseObject.getToken()).thenReturn("token-12345");
        when(responseObject.getValue()).thenReturn("decoded-12345");

        DetokenizeSummary summary = mock(DetokenizeSummary.class);
        when(summary.getTotalFailed()).thenReturn(0);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Collections.singletonList(responseObject));
        when(detokenizeResponseMock.getErrors()).thenReturn(Collections.emptyList());
        when(detokenizeResponseMock.getSummary()).thenReturn(summary);

        ArgumentCaptor<DetokenizeRequest> detokenizeRequestCaptor = ArgumentCaptor.forClass(DetokenizeRequest.class);
        when(vaultMock.bulkDetokenize(any())).thenReturn(detokenizeResponseMock);

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, properties);

        verify(vaultMock).bulkDetokenize(detokenizeRequestCaptor.capture());
        DetokenizeRequest request = detokenizeRequestCaptor.getValue();
        assertTrue(request.getTokens().contains("token-12345"));
        assertEquals(1, request.getTokenGroupRedactions().size());
        assertEquals("override_group", request.getTokenGroupRedactions().get(0).getTokenGroupName());
        assertEquals("override_redaction", request.getTokenGroupRedactions().get(0).getRedaction());

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals("decoded-12345", row.getAs("cust_id"));
        assertEquals(Constants.STATUS_OK, row.getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(row.getAs(Constants.ERROR));
    }

    @Test
    void detokenize_returns_passthrough_when_no_detokenizable_columns() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        StructType schema = new StructType(new StructField[] {
                new StructField("first_nm", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ph_nbr", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> data = spark.createDataFrame(Arrays.asList(
                RowFactory.create("token-1", "token-2"),
                RowFactory.create("token-3", "token-4")), schema);

        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"cust_id\": {\"tableName\": \"customer_table\", \"columnName\": \"customer_column\"}}");

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, data, properties);

        verify(vaultMock, times(0)).bulkDetokenize(any());

        List<Row> rows = result.collectAsList();
        assertEquals(2, rows.size());
        assertEquals("token-1", rows.get(0).getAs("first_nm"));
        assertEquals("token-2", rows.get(0).getAs("ph_nbr"));
        assertEquals(Constants.STATUS_OK, rows.get(0).getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(rows.get(0).getAs(Constants.ERROR));
        assertEquals("token-3", rows.get(1).getAs("first_nm"));
        assertEquals("token-4", rows.get(1).getAs("ph_nbr"));
        assertEquals(Constants.STATUS_OK, rows.get(1).getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(rows.get(1).getAs(Constants.ERROR));
    }

    @Test
    void detokenize_handles_duplicate_tokens_across_tables() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        StructType schema = new StructType(new StructField[] {
                new StructField("primary_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("alternate_name", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> data = spark.createDataFrame(
                Collections.singletonList(RowFactory.create("token-primary", "token-primary")), schema);

        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{" +
                        "\"primary_name\": {\"tableName\": \"customers\", \"columnName\": \"customer_name\"}," +
                        "\"alternate_name\": {\"tableName\": \"aliases\", \"columnName\": \"alias_name\"}" +
                        "}");

        DetokenizeResponseObject primary = mock(DetokenizeResponseObject.class);
        when(primary.getToken()).thenReturn("token-primary");
        when(primary.getValue()).thenReturn("Alice");

        DetokenizeSummary summary = mock(DetokenizeSummary.class);
        when(summary.getTotalFailed()).thenReturn(0);

        DetokenizeResponse response = mock(DetokenizeResponse.class);
        when(response.getSuccess()).thenReturn(Collections.singletonList(primary));
        when(response.getErrors()).thenReturn(Collections.emptyList());
        when(response.getSummary()).thenReturn(summary);

        when(vaultMock.bulkDetokenize(any())).thenReturn(response);

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, data, properties);

        verify(vaultMock, times(1)).bulkDetokenize(any());

        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals("Alice", row.getAs("primary_name"));
        assertEquals("Alice", row.getAs("alternate_name"));
        assertEquals(Constants.STATUS_OK, row.getAs(Constants.SKYFLOW_STATUS_CODE));
        assertNull(row.getAs(Constants.ERROR));
    }

    @Test
    void detokenize_multiple_retries_partial_success() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> tokenizedData = createSampleTokenizedDataset();

        DetokenizeResponseObject resp0 = mock(DetokenizeResponseObject.class);
        when(resp0.getToken()).thenReturn("token0");
        when(resp0.getValue()).thenReturn("John");

        DetokenizeResponseObject resp1 = mock(DetokenizeResponseObject.class);
        when(resp1.getToken()).thenReturn("token3");
        when(resp1.getValue()).thenReturn("Elis");

        DetokenizeResponseObject resp2 = mock(DetokenizeResponseObject.class);
        when(resp2.getToken()).thenReturn("token4");
        when(resp2.getValue()).thenReturn("Bob");

        DetokenizeResponseObject resp3 = mock(DetokenizeResponseObject.class);
        when(resp3.getToken()).thenReturn("token5");
        when(resp3.getValue()).thenReturn("Edward");

        ErrorRecord err1 = mock(ErrorRecord.class);
        when(err1.getIndex()).thenReturn(1);
        when(err1.getCode()).thenReturn(503); // retryable

        ErrorRecord err2 = mock(ErrorRecord.class);
        when(err2.getIndex()).thenReturn(2);
        when(err2.getCode()).thenReturn(503); // retryable

        DetokenizeSummary summaryInitial = mock(DetokenizeSummary.class);
        when(summaryInitial.getTotalFailed()).thenReturn(2);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Arrays.asList(resp0, resp1, resp2, resp3));
        when(detokenizeResponseMock.getErrors()).thenReturn(Arrays.asList(err1, err2));
        when(detokenizeResponseMock.getSummary()).thenReturn(summaryInitial);

        // Retry 1: success for token2, error for token3
        DetokenizeResponseObject retryResp1 = mock(DetokenizeResponseObject.class);
        when(retryResp1.getToken()).thenReturn("token1");
        when(retryResp1.getValue()).thenReturn("Gwen");

        ErrorRecord retryErr2 = mock(ErrorRecord.class);
        when(retryErr2.getIndex()).thenReturn(0);
        when(retryErr2.getCode()).thenReturn(503);

        DetokenizeSummary summaryRetry1 = mock(DetokenizeSummary.class);
        when(summaryRetry1.getTotalFailed()).thenReturn(1);

        DetokenizeResponse retryResponse1 = mock(DetokenizeResponse.class);
        when(retryResponse1.getSuccess()).thenReturn(Collections.singletonList(retryResp1));
        when(retryResponse1.getErrors()).thenReturn(Collections.singletonList(retryErr2));
        when(retryResponse1.getSummary()).thenReturn(summaryRetry1);

        // Retry 2: failure for token3
        ErrorRecord retryErr3 = mock(ErrorRecord.class);
        when(retryErr3.getIndex()).thenReturn(0);
        when(retryErr3.getCode()).thenReturn(503);

        DetokenizeSummary summaryRetry2 = mock(DetokenizeSummary.class);
        when(summaryRetry2.getTotalFailed()).thenReturn(1);

        DetokenizeResponse retryResponse2 = mock(DetokenizeResponse.class);
        when(retryResponse2.getSuccess()).thenReturn(Collections.emptyList());
        when(retryResponse2.getErrors()).thenReturn(Collections.singletonList(retryErr3));
        when(retryResponse2.getSummary()).thenReturn(summaryRetry2);

        when(vaultMock.bulkDetokenize(any()))
                .thenReturn(detokenizeResponseMock) // Initial
                .thenReturn(retryResponse1) // Retry 1
                .thenReturn(retryResponse2); // Retry 2

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, null);

        assertNotNull(result);

        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        assertEquals("John", rows.get(0).getAs("first_nm"));
        assertEquals("200", rows.get(0).getAs("skyflow_status_code"));
        assertEquals("token2", rows.get(1).getAs("first_nm"));
        assertEquals("500", rows.get(1).getAs("skyflow_status_code"));
        assertEquals("Bob", rows.get(2).getAs("first_nm"));

        verify(vaultMock, times(3)).bulkDetokenize(any());
    }

    @Test
    void detokenize_multiple_retries_no_retryable_errors() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> tokenizedData = createSampleTokenizedDataset();

        DetokenizeResponseObject resp0 = mock(DetokenizeResponseObject.class);
        when(resp0.getToken()).thenReturn("token2");
        when(resp0.getValue()).thenReturn("John");

        DetokenizeResponseObject resp1 = mock(DetokenizeResponseObject.class);
        when(resp1.getToken()).thenReturn("token3");
        when(resp1.getValue()).thenReturn("Elis");

        DetokenizeResponseObject resp2 = mock(DetokenizeResponseObject.class);
        when(resp2.getToken()).thenReturn("token4");
        when(resp2.getValue()).thenReturn("Bob");

        DetokenizeResponseObject resp3 = mock(DetokenizeResponseObject.class);
        when(resp3.getToken()).thenReturn("token5");
        when(resp3.getValue()).thenReturn("Edward");

        ErrorRecord err1 = mock(ErrorRecord.class);
        when(err1.getIndex()).thenReturn(0);
        when(err1.getCode()).thenReturn(404); // retryable

        ErrorRecord err2 = mock(ErrorRecord.class);
        when(err2.getIndex()).thenReturn(1);
        when(err2.getCode()).thenReturn(404); // retryable

        DetokenizeSummary summaryInitial = mock(DetokenizeSummary.class);
        when(summaryInitial.getTotalFailed()).thenReturn(2);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Arrays.asList(resp0, resp1, resp2, resp3));
        when(detokenizeResponseMock.getErrors()).thenReturn(Arrays.asList(err1, err2));
        when(detokenizeResponseMock.getSummary()).thenReturn(summaryInitial);

        when(vaultMock.bulkDetokenize(any()))
                .thenReturn(detokenizeResponseMock); // Initial

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, null);

        assertNotNull(result);

        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        assertEquals("token0", rows.get(0).getAs("first_nm"));
        assertEquals("500", rows.get(0).getAs("skyflow_status_code"));
        assertEquals("John", rows.get(1).getAs("first_nm"));
        assertEquals("200", rows.get(1).getAs("skyflow_status_code"));
        assertEquals("Bob", rows.get(2).getAs("first_nm"));
        assertEquals("200", rows.get(2).getAs("skyflow_status_code"));

        verify(vaultMock, times(1)).bulkDetokenize(any());
    }

    @Test
    void tokenize_empty_dataset() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        Dataset<Row> emptyDataset = spark.emptyDataFrame();

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, emptyDataset, null);

        assertNotNull(result);
        assertEquals(0, result.count());

        verify(vaultMock, times(0)).bulkInsert(any());
    }

    @Test
    void detokenize_empty_dataset() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> emptyDataset = spark.emptyDataFrame();

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, emptyDataset, null);

        assertNotNull(result);
        assertEquals(0, result.count());

        verify(vaultMock, times(0)).bulkDetokenize(any());
    }

    @Test
    void tokenize_initialization_failure() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        doThrow(new RuntimeException("Initialization failed"))
                .when(vaultHelper).initializeSkyflowClient(tableHelperMock);

        Dataset<Row> data = createSampleInputDataset();

        Exception ex = assertThrows(SkyflowException.class,
                () -> vaultHelper.tokenize(tableHelperMock, data, null));
        assertTrue(ex.getMessage().contains("Initialization failed"));
    }

    @Test
    void detokenize_initialization_failure() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        doThrow(new SkyflowException("Initialization failed"))
                .when(vaultHelper).initializeSkyflowClient(tableHelperMock);

        Dataset<Row> tokenizedData = createSampleTokenizedDataset();

        SkyflowException ex = assertThrows(SkyflowException.class,
                () -> vaultHelper.detokenize(tableHelperMock, tokenizedData, null));
        assertTrue(ex.getMessage().contains("Initialization failed"));
    }

    @Test
    void tokenize_multiple_retries_all_success() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);
        Dataset<Row> data = createSampleInputDataset();

        // Initial response: all failed but retryable errors
        List<ErrorRecord> initialErrors = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ErrorRecord err = mock(ErrorRecord.class);
            when(err.getIndex()).thenReturn(i);
            when(err.getCode()).thenReturn(503);
            initialErrors.add(err);
        }
        Summary initialSummary = mock(Summary.class);
        when(initialSummary.getTotalFailed()).thenReturn(3);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.emptyList());
        when(insertResponseMock.getErrors()).thenReturn(initialErrors);
        when(insertResponseMock.getSummary()).thenReturn(initialSummary);

        // Retry 1: success on all
        List<Success> retrySuccesses = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Success success = mock(Success.class);
            when(success.getIndex()).thenReturn(i);
            Token token = mock(Token.class);
            when(token.getToken()).thenReturn("token_r");
            when(token.getTokenGroupName()).thenReturn("name");
            Map<String, List<Token>> tokensMap = new HashMap<>();
            tokensMap.put("name", Collections.singletonList(token));
            when(success.getTokens()).thenReturn(tokensMap);
            retrySuccesses.add(success);
        }
        Summary retrySummary = mock(Summary.class);
        when(retrySummary.getTotalFailed()).thenReturn(0);

        InsertResponse retryResponse = mock(InsertResponse.class);
        when(retryResponse.getSuccess()).thenReturn(retrySuccesses);
        when(retryResponse.getErrors()).thenReturn(Collections.emptyList());
        when(retryResponse.getSummary()).thenReturn(retrySummary);

        when(vaultMock.bulkInsert(any()))
                .thenReturn(insertResponseMock) // initial failed
                .thenReturn(retryResponse); // retry all success

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        for (int i = 0; i < 3; i++) {
            assertEquals("token_r", rows.get(i).getAs("first_nm"));
            assertEquals("200", rows.get(i).getAs("skyflow_status_code"));
            assertNull(rows.get(i).getAs("error"));
        }

        verify(vaultMock, times(2)).bulkInsert(any());
    }

    @Test
    void tokenize_multiple_retries_all_failures() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> data = createSampleInputDataset();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);

        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);

        // Initial response: all failed retryable errors
        List<ErrorRecord> initialErrors = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ErrorRecord err = mock(ErrorRecord.class);
            when(err.getIndex()).thenReturn(i);
            when(err.getCode()).thenReturn(503);
            initialErrors.add(err);
        }
        Summary initialSummary = mock(Summary.class);
        when(initialSummary.getTotalFailed()).thenReturn(3);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.emptyList());
        when(insertResponseMock.getErrors()).thenReturn(initialErrors);
        when(insertResponseMock.getSummary()).thenReturn(initialSummary);

        // Retry 1: all retryable failed again
        List<ErrorRecord> retryErrors = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ErrorRecord err = mock(ErrorRecord.class);
            when(err.getIndex()).thenReturn(i);
            when(err.getCode()).thenReturn(503);
            when(err.getError()).thenReturn("Service unavailable");
            retryErrors.add(err);
        }
        Summary retrySummary = mock(Summary.class);
        when(retrySummary.getTotalFailed()).thenReturn(3);

        InsertResponse retryResponse = mock(InsertResponse.class);
        when(retryResponse.getSuccess()).thenReturn(Collections.emptyList());
        when(retryResponse.getErrors()).thenReturn(retryErrors);
        when(retryResponse.getSummary()).thenReturn(retrySummary);

        when(vaultMock.bulkInsert(any()))
                .thenReturn(insertResponseMock) // initial failed
                .thenReturn(retryResponse); // retry all failed again

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        for (int i = 0; i < 3; i++) {
            // Original data preserved
            assertEquals(data.collectAsList().get(i).getAs("first_nm").toString(), rows.get(i).getAs("first_nm"));
            // Status code 500 and error present for failures
            assertEquals("500", rows.get(i).getAs("skyflow_status_code"));
            assertNotNull(rows.get(i).getAs("error"));
        }

        verify(vaultMock, times(3)).bulkInsert(any());
    }

    @Test
    void detokenize_multiple_retries_all_success() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        Dataset<Row> tokenizedData = createSampleTokenizedDataset();

        // Initial response: all failed retryable errors
        List<ErrorRecord> initialErrors = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ErrorRecord err = mock(ErrorRecord.class);
            when(err.getIndex()).thenReturn(i);
            when(err.getCode()).thenReturn(503);
            initialErrors.add(err);
        }
        DetokenizeSummary initialSummary = mock(DetokenizeSummary.class);
        when(initialSummary.getTotalFailed()).thenReturn(6);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Collections.emptyList());
        when(detokenizeResponseMock.getErrors()).thenReturn(initialErrors);
        when(detokenizeResponseMock.getSummary()).thenReturn(initialSummary);

        // Retry 1: success on all
        List<DetokenizeResponseObject> retrySuccesses = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            DetokenizeResponseObject resp = mock(DetokenizeResponseObject.class);
            when(resp.getToken()).thenReturn("token" + i);
            when(resp.getValue()).thenReturn("value" + i);
            retrySuccesses.add(resp);
        }
        DetokenizeSummary retrySummary = mock(DetokenizeSummary.class);
        when(retrySummary.getTotalFailed()).thenReturn(0);

        DetokenizeResponse retryResponse = mock(DetokenizeResponse.class);
        when(retryResponse.getSuccess()).thenReturn(retrySuccesses);
        when(retryResponse.getErrors()).thenReturn(Collections.emptyList());
        when(retryResponse.getSummary()).thenReturn(retrySummary);

        when(vaultMock.bulkDetokenize(any()))
                .thenReturn(detokenizeResponseMock) // initial failed
                .thenReturn(retryResponse); // retry all success

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        assertEquals("value" + 0, rows.get(0).getAs("first_nm"));
        assertEquals("200", rows.get(0).getAs("skyflow_status_code"));
        assertNull(rows.get(0).getAs("error"));

        // should be invoked twice, as every token is succeeded on the 1st retry
        verify(vaultMock, times(2)).bulkDetokenize(any());
    }

    @Test
    void detokenize_multiple_retries_all_failures() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> tokenizedData = createSampleTokenizedDataset();

        // Initial response: all failed retryable errors
        List<ErrorRecord> initialErrors = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ErrorRecord err = mock(ErrorRecord.class);
            when(err.getIndex()).thenReturn(i);
            when(err.getCode()).thenReturn(503);
            initialErrors.add(err);
        }
        DetokenizeSummary initialSummary = mock(DetokenizeSummary.class);
        when(initialSummary.getTotalFailed()).thenReturn(3);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Collections.emptyList());
        when(detokenizeResponseMock.getErrors()).thenReturn(initialErrors);
        when(detokenizeResponseMock.getSummary()).thenReturn(initialSummary);

        // Retry 1: all retryable failed again
        List<ErrorRecord> retryErrors = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ErrorRecord err = mock(ErrorRecord.class);
            when(err.getIndex()).thenReturn(i);
            when(err.getCode()).thenReturn(503);
            retryErrors.add(err);
        }
        DetokenizeSummary retrySummary = mock(DetokenizeSummary.class);
        when(retrySummary.getTotalFailed()).thenReturn(3);

        DetokenizeResponse retryResponse = mock(DetokenizeResponse.class);
        when(retryResponse.getSuccess()).thenReturn(Collections.emptyList());
        when(retryResponse.getErrors()).thenReturn(retryErrors);
        when(retryResponse.getSummary()).thenReturn(retrySummary);

        when(vaultMock.bulkDetokenize(any()))
                .thenReturn(detokenizeResponseMock) // initial failed
                .thenReturn(retryResponse); // retry all failed again

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();
        assertEquals(3, rows.size());

        for (int i = 0; i < 3; i++) {
            // Original tokens preserved in name column
            assertEquals(tokenizedData.collectAsList().get(i).getAs("first_nm").toString(),
                    rows.get(i).getAs("first_nm"));
            // Status code 500 and error present for failures
            assertEquals("500", rows.get(i).getAs("skyflow_status_code"));
            assertNotNull(rows.get(i).getAs("error"));
        }

        verify(vaultMock, times(3)).bulkDetokenize(any());
    }

    @Test
    void tokenize_all_rows_fail_no_retryable_errors() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> data = createSampleInputDataset();

        // InsertResponse has errors, none retryable
        ErrorRecord err1 = mock(ErrorRecord.class);
        when(err1.getCode()).thenReturn(400); // Non-retryable
        when(err1.getIndex()).thenReturn(0);

        ErrorRecord err2 = mock(ErrorRecord.class);
        when(err2.getCode()).thenReturn(400); // Non-retryable
        when(err2.getIndex()).thenReturn(1);

        ErrorRecord err3 = mock(ErrorRecord.class);
        when(err3.getCode()).thenReturn(400); // Non-retryable
        when(err3.getIndex()).thenReturn(2);

        Summary summary = mock(Summary.class);
        when(summary.getTotalFailed()).thenReturn(2);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.emptyList());
        when(insertResponseMock.getErrors()).thenReturn(Arrays.asList(err1, err2, err3));
        when(insertResponseMock.getSummary()).thenReturn(summary);

        when(vaultMock.bulkInsert(any())).thenReturn(insertResponseMock);

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();

        // All rows should fail
        assertEquals(3, rows.size());
        assertEquals("500", rows.get(0).getAs("skyflow_status_code"));
        assertEquals("500", rows.get(1).getAs("skyflow_status_code"));
        assertEquals("500", rows.get(2).getAs("skyflow_status_code"));

        // Verify no retries occurred
        verify(vaultMock, times(1)).bulkInsert(any());
    }

    @Test
    void detokenize_all_rows_fail_no_retryable_errors() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        Dataset<Row> tokenizedData = createSampleTokenizedDataset();

        // DetokenizeResponse has errors, none retryable
        ErrorRecord err1 = mock(ErrorRecord.class);
        when(err1.getCode()).thenReturn(400); // Non-retryable
        when(err1.getIndex()).thenReturn(0);

        ErrorRecord err2 = mock(ErrorRecord.class);
        when(err2.getCode()).thenReturn(400); // Non-retryable
        when(err2.getIndex()).thenReturn(1);

        ErrorRecord err3 = mock(ErrorRecord.class);
        when(err3.getCode()).thenReturn(400); // Non-retryable
        when(err3.getIndex()).thenReturn(2);

        DetokenizeSummary summary = mock(DetokenizeSummary.class);
        when(summary.getTotalFailed()).thenReturn(3);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Collections.emptyList());
        when(detokenizeResponseMock.getErrors()).thenReturn(Arrays.asList(err1, err2, err3));
        when(detokenizeResponseMock.getSummary()).thenReturn(summary);

        when(vaultMock.bulkDetokenize(any())).thenReturn(detokenizeResponseMock);

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();

        // All rows should fail
        assertEquals(3, rows.size());
        assertEquals("500", rows.get(0).getAs("skyflow_status_code"));
        assertEquals("500", rows.get(1).getAs("skyflow_status_code"));

        // Verify no retries occurred
        verify(vaultMock, times(1)).bulkDetokenize(any());
    }

    @Test
    void tokenize_retry_logic_with_exponential_backoff() throws SkyflowException {
        mockInitializeSkyflowClientForTest();
        Dataset<Row> data = createSampleInputDataset();
        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);

        // Initial response: 1 success, 1 retryable error
        Success success0 = mock(Success.class);
        when(success0.getIndex()).thenReturn(0);
        Token token = mock(Token.class);
        when(token.getToken()).thenReturn("token1");
        when(token.getTokenGroupName()).thenReturn("name");
        HashMap<String, List<Token>> tokensMap = new HashMap<>();
        tokensMap.put("name", Collections.singletonList(token));
        when(success0.getTokens()).thenReturn(tokensMap);
        when(success0.getTable()).thenReturn("name");

        ErrorRecord error1 = mock(ErrorRecord.class);
        when(error1.getIndex()).thenReturn(1);
        when(error1.getCode()).thenReturn(503); // Retryable

        Summary summaryInitial = mock(Summary.class);
        when(summaryInitial.getTotalFailed()).thenReturn(1);

        when(insertResponseMock.getSuccess()).thenReturn(Collections.singletonList(success0));
        when(insertResponseMock.getErrors()).thenReturn(Collections.singletonList(error1));
        when(insertResponseMock.getSummary()).thenReturn(summaryInitial);

        // Retry response: success on retry
        Success retrySuccess = mock(Success.class);
        when(retrySuccess.getIndex()).thenReturn(0);
        Token retryToken = mock(Token.class);
        when(retryToken.getToken()).thenReturn("token2");
        when(retryToken.getTokenGroupName()).thenReturn("name");
        HashMap<String, List<Token>> retryTokensMap = new HashMap<>();
        retryTokensMap.put("name", Collections.singletonList(retryToken));
        when(retrySuccess.getTokens()).thenReturn(retryTokensMap);

        Summary summaryRetry = mock(Summary.class);
        when(summaryRetry.getTotalFailed()).thenReturn(0);

        InsertResponse retryResponse = mock(InsertResponse.class);
        when(retryResponse.getSuccess()).thenReturn(Collections.singletonList(retrySuccess));
        when(retryResponse.getErrors()).thenReturn(Collections.emptyList());
        when(retryResponse.getSummary()).thenReturn(summaryRetry);

        when(vaultMock.bulkInsert(any()))
                .thenReturn(insertResponseMock) // Initial
                .thenReturn(retryResponse); // Retry

        Dataset<Row> result = vaultHelper.tokenize(tableHelperMock, data, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();

        // Verify retry fixed the error
        assertEquals("token1", rows.get(0).getAs("first_nm"));
        assertEquals("token2", rows.get(1).getAs("first_nm"));
        assertEquals("200", rows.get(1).getAs("skyflow_status_code"));

        // Verify retry occurred
        verify(vaultMock, times(2)).bulkInsert(any());
    }

    @Test
    void detokenize_retry_logic_with_exponential_backoff() throws SkyflowException {
        mockInitializeSkyflowClientForTest();

        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("first_nm", SkyflowColumn.NAME);
        // inject this into the class under test if it's static there
        Constants.SKYFLOW_COLUMN_MAP.clear();
        Constants.SKYFLOW_COLUMN_MAP.putAll(COLUMN_MAPPINGS);

        Dataset<Row> tokenizedData = createSampleTokenizedDataset();
        // Initial response: 1 success, 1 retryable error
        DetokenizeResponseObject resp0 = mock(DetokenizeResponseObject.class);
        when(resp0.getToken()).thenReturn("token0");
        when(resp0.getValue()).thenReturn("John");

        ErrorRecord error1 = mock(ErrorRecord.class);
        when(error1.getIndex()).thenReturn(1);
        when(error1.getCode()).thenReturn(503); // Retryable

        DetokenizeSummary summaryInitial = mock(DetokenizeSummary.class);
        when(summaryInitial.getTotalFailed()).thenReturn(1);

        when(detokenizeResponseMock.getSuccess()).thenReturn(Collections.singletonList(resp0));
        when(detokenizeResponseMock.getErrors()).thenReturn(Collections.singletonList(error1));
        when(detokenizeResponseMock.getSummary()).thenReturn(summaryInitial);

        // Retry response: success on retry
        DetokenizeResponseObject retryResp = mock(DetokenizeResponseObject.class);
        when(retryResp.getToken()).thenReturn("token2");
        when(retryResp.getValue()).thenReturn("Alice");

        DetokenizeSummary summaryRetry = mock(DetokenizeSummary.class);
        when(summaryRetry.getTotalFailed()).thenReturn(0);

        DetokenizeResponse retryResponse = mock(DetokenizeResponse.class);
        when(retryResponse.getSuccess()).thenReturn(Collections.singletonList(retryResp));
        when(retryResponse.getErrors()).thenReturn(Collections.emptyList());
        when(retryResponse.getSummary()).thenReturn(summaryRetry);

        when(vaultMock.bulkDetokenize(any()))
                .thenReturn(detokenizeResponseMock) // Initial
                .thenReturn(retryResponse); // Retry

        Dataset<Row> result = vaultHelper.detokenize(tableHelperMock, tokenizedData, null);

        assertNotNull(result);
        List<Row> rows = result.collectAsList();

        // Verify retry fixed the error
        assertEquals("John", rows.get(0).getAs("first_nm"));
        assertEquals("Alice", rows.get(1).getAs("first_nm"));
        assertEquals("200", rows.get(1).getAs("skyflow_status_code"));

        // Verify retry occurred
        verify(vaultMock, times(2)).bulkDetokenize(any());
    }

}
