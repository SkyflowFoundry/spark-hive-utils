import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skyflow.spark.Helper;
import com.skyflow.spark.ColumnMapping;
import com.skyflow.spark.Constants;

import com.skyflow.vault.data.ErrorRecord;
import com.skyflow.vault.data.InsertResponse;
import com.skyflow.vault.data.InsertRequest;
import com.skyflow.vault.data.InsertRecord;
import com.skyflow.vault.data.DetokenizeRequest;
import com.skyflow.vault.data.DetokenizeResponse;
import com.skyflow.vault.data.DetokenizeResponseObject;
import com.skyflow.vault.data.TokenGroupRedactions;
import com.skyflow.vault.data.Success;
import com.skyflow.vault.data.Token;
import com.skyflow.errors.SkyflowException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class HelperTest {

    private StructType sampleSchema;

    private SparkSession spark;

    // Simulate COLUMN_MAPPINGS for this test
    private static final Map<String, ColumnMapping> COLUMN_MAPPINGS = new HashMap<>();

    @BeforeEach
    void setup() {
        spark = SparkSession.builder()
                .appName("HelperGetBatchesTest")
                .master("local[*]")
                .getOrCreate();

        COLUMN_MAPPINGS.clear();
        COLUMN_MAPPINGS.put("name", new ColumnMapping("name", "name", "deterministic_name", "name_redaction"));
        COLUMN_MAPPINGS.put("phone", new ColumnMapping("phone", "phone", ""));
        COLUMN_MAPPINGS.put("email", new ColumnMapping("email", "email", "deterministic_email"));

        sampleSchema = new StructType(new StructField[] {
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("phone", DataTypes.StringType, true, Metadata.empty()),
                new StructField("email", DataTypes.StringType, true, Metadata.empty())
        });
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private StructType createSchema(String... fields) {
        List<StructField> structFields = new ArrayList<>();
        for (String f : fields) {
            structFields.add(new StructField(f, DataTypes.StringType, true, Metadata.empty()));
        }
        return new StructType(structFields.toArray(new StructField[0]));
    }

    private Row createRowWithSchema(StructType schema, Object... values) {
        return new GenericRowWithSchema(values, schema);
    }

    // region Column mapping configuration tests

    @Test
    void configure_column_mappings_uses_only_provided_entries() throws SkyflowException {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{" +
                        "\"name\": {\"tableName\": \"customer_table\", \"columnName\": \"customer_column\", \"tokenGroupName\": \"customer_group\", \"redaction\": \"default_redaction\"}"
                        +
                        "}");

        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);

        assertEquals(1, columnMappingMap.size());
        ColumnMapping mapping = columnMappingMap.get("name");
        assertNotNull(mapping);
        assertEquals("customer_table", mapping.getTableName());
        assertEquals("customer_column", mapping.getColumnName());
        assertEquals("customer_group", mapping.getTokenGroupName());
        assertEquals("default_redaction", mapping.getRedaction());
    }

    @Test
    void configure_column_mappings_honours_properties() throws SkyflowException {

        // Apply a custom mapping to ensure map changes
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"override_table\", \"columnName\": \"override_column\"}}");
        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);
        assertEquals(1, columnMappingMap.size());
        ColumnMapping mapping = columnMappingMap.get("name");
        assertNotNull(mapping);
        assertEquals("override_table", mapping.getTableName());
        assertEquals("override_column", mapping.getColumnName());
    }

    @Test
    void configure_column_mappings_skips_columns_not_in_schema() throws SkyflowException {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{" +
                        "\"unknown_column\": {\"tableName\": \"custom_table\", \"columnName\": \"custom_column\", \"tokenGroupName\" : \"custom_group\"},"
                        +
                        "\"name\": {\"tableName\": \"first_table\", \"columnName\": \"first_name\", \"tokenGroupName\": \"first_group\"}"
                        +
                        "}");

        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);

        assertFalse(columnMappingMap.containsKey("unknown_column"));

        ColumnMapping firstNameMapping = columnMappingMap.get("name");
        assertEquals("first_table", firstNameMapping.getTableName());
        assertEquals("first_name", firstNameMapping.getColumnName());
        assertEquals("first_group", firstNameMapping.getTokenGroupName());
        assertEquals(1, columnMappingMap.size());
    }

    @Test
    void configure_column_mappings_defaults_missing_token_group_and_redaction() throws SkyflowException {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"name_table\", \"columnName\": \"name_column\", \"unique\": \"false\"}}");

        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);
        ColumnMapping mapping = columnMappingMap.get("name");
        assertNotNull(mapping);
        assertEquals("name_table", mapping.getTableName());
        assertEquals("name_column", mapping.getColumnName());
        assertNull(mapping.getTokenGroupName());
        assertNull(mapping.getRedaction());
    }

    @Test
    void configure_column_mappings_defaults_blank_token_group_and_redaction() throws SkyflowException {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"name_table\", \"columnName\": \"name_column\", \"tokenGroupName\": \"  \", \"redaction\": \"  \"}}");

        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);
        ColumnMapping mapping = columnMappingMap.get("name");
        assertNotNull(mapping);
        assertEquals("name_table", mapping.getTableName());
        assertEquals("name_column", mapping.getColumnName());
        assertNull(mapping.getTokenGroupName());
        assertNull(mapping.getRedaction());
    }

    @Test
    void configure_column_mappings_throws_for_empty_object_mapping() {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING, "{\"name\": {}}");

        assertThrows(SkyflowException.class,
                () -> Helper.configureColumnMappings(sampleSchema, properties));
    }

    @Test
    void configure_column_mappings_skips_null_entries() throws SkyflowException {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING, "{\"first_nm\": null}");

        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);

        assertTrue(columnMappingMap.isEmpty());
    }

    @Test
    void configure_column_mappings_missing_table_or_column_throws_exception() {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"customer_table\"}}");

        assertThrows(SkyflowException.class,
                () -> Helper.configureColumnMappings(sampleSchema, properties));
    }

    @Test
    void configure_column_mappings_throws_when_property_blank() {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING, "");

        assertThrows(SkyflowException.class,
                () -> Helper.configureColumnMappings(sampleSchema, properties));
    }

    @Test
    void configure_column_mappings_throws_when_properties_null() {
        assertThrows(SkyflowException.class,
                () -> Helper.configureColumnMappings(sampleSchema, null));
    }

    @Test
    void configure_column_mappings_throws_when_json_invalid() {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING, "{invalid-json");

        assertThrows(SkyflowException.class,
                () -> Helper.configureColumnMappings(sampleSchema, properties));
    }

    @Test
    void configure_column_mappings_defaults_unique_true_when_not_specified() throws SkyflowException {
        Properties properties = new Properties();
        properties.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"customer_table\", \"columnName\": \"customer_name\"}}");

        Map<String, ColumnMapping> columnMappingMap = Helper.configureColumnMappings(sampleSchema, properties);
        ColumnMapping mapping = columnMappingMap.get("name");
        assertNotNull(mapping);
        assertNotNull(mapping.getIsUnique());
        assertTrue(mapping.getIsUnique());
    }

    @Test
    void construct_insert_request_honours_unique_flag() throws SkyflowException {
        StructType schema = createSchema("name");
        Row row = createRowWithSchema(schema, "Alice");

        Properties defaultUniqueProps = new Properties();
        defaultUniqueProps.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"customer_table\", \"columnName\": \"customer_name\"}}");
        Map<String, ColumnMapping> defaultMappings = Helper.configureColumnMappings(schema, defaultUniqueProps);
        InsertRequest defaultRequest = Helper.constructInsertRequest(defaultMappings, Collections.singletonList(row));
        InsertRecord defaultRecord = defaultRequest.getRecords().get(0);
        assertNotNull(defaultRecord.getUpsert(), "Default unique should populate upsert list");
        assertEquals(Collections.singletonList("customer_name"), defaultRecord.getUpsert());

        Properties nonUniqueProps = new Properties();
        nonUniqueProps.setProperty(Constants.COLUMN_MAPPING,
                "{\"name\": {\"tableName\": \"customer_table\", \"columnName\": \"customer_name\", \"unique\": \"false\"}}");
        Map<String, ColumnMapping> nonUniqueMappings = Helper.configureColumnMappings(schema, nonUniqueProps);
        InsertRequest nonUniqueRequest = Helper.constructInsertRequest(nonUniqueMappings, Collections.singletonList(row));
        InsertRecord nonUniqueRecord = nonUniqueRequest.getRecords().get(0);
        assertNull(nonUniqueRecord.getUpsert(), "Non-unique columns should not populate upsert list");
    }

    // endregion Column mapping configuration tests

    // region concatWithUnderscore tests

    @Test
    void concat_with_underscore_handles_standard_values() {
        assertEquals("hello_123", Helper.concatWithUnderscore("hello", 123));
    }

    @Test
    void concat_with_underscore_handles_null_first_value() {
        assertEquals("_world", Helper.concatWithUnderscore(null, "world"));
    }

    @Test
    void concat_with_underscore_handles_null_second_value() {
        assertEquals("hello_", Helper.concatWithUnderscore("hello", null));
    }

    @Test
    void concat_with_underscore_handles_both_null_values() {
        assertEquals("_", Helper.concatWithUnderscore(null, null));
    }

    // endregion concatWithUnderscore tests

    // region Dataset batching tests

    @Test
    void get_batches_exact_multiple_batches() {
        // Create dataset with 6 rows
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty())
        });
        List<Integer> dataList = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Row> rows = dataList.stream()
                .map(data -> createRowWithSchema(schema, data))
                .collect(Collectors.toList());
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        int batchSize = 2;

        Iterable<List<Row>> batches = Helper.getBatches(dataset, batchSize);

        Iterator<List<Row>> iterator = batches.iterator();

        List<List<Integer>> batchValues = new ArrayList<>();

        while (iterator.hasNext()) {
            List<Row> batch = iterator.next();
            List<Integer> ids = new ArrayList<>();
            for (Row r : batch) {
                ids.add(r.getInt(0));
            }
            batchValues.add(ids);
        }

        // Expect 3 batches of size 2 each
        assertEquals(3, batchValues.size());
        assertEquals(Arrays.asList(1, 2), batchValues.get(0));
        assertEquals(Arrays.asList(3, 4), batchValues.get(1));
        assertEquals(Arrays.asList(5, 6), batchValues.get(2));
    }

    @Test
    void get_batches_handles_zero_batch_size() {
        StructType schema = createSchema("name", "phone");
        Dataset<Row> data = spark.createDataFrame(
                Arrays.asList(RowFactory.create("Alice", "111"), RowFactory.create("Bob", "222")),
                schema);

        Map<String, ColumnMapping> schemaMappings = Collections.emptyMap();
        int batchSize = Helper.calculateRowBatchSize(schemaMappings, 1000);
        assertEquals(0, batchSize);

        assertThrows(IllegalArgumentException.class, () -> Helper.getBatches(data, batchSize).iterator().hasNext());
    }

    @Test
    void get_batches_non_exact_multiple_batches() {
        // Create dataset with 5 rows
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty())
        });
        List<Row> rows = Arrays.asList(
                RowFactory.create(1),
                RowFactory.create(2),
                RowFactory.create(3),
                RowFactory.create(4),
                RowFactory.create(5));
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        int batchSize = 2;

        Iterable<List<Row>> batches = Helper.getBatches(dataset, batchSize);

        Iterator<List<Row>> iterator = batches.iterator();

        List<List<Integer>> batchValues = new ArrayList<>();

        while (iterator.hasNext()) {
            List<Row> batch = iterator.next();
            List<Integer> ids = new ArrayList<>();
            for (Row r : batch) {
                ids.add(r.getInt(0));
            }
            batchValues.add(ids);
        }

        // Expect 3 batches: two batches of size 2, one batch of size 1
        assertEquals(3, batchValues.size());
        assertEquals(Arrays.asList(1, 2), batchValues.get(0));
        assertEquals(Arrays.asList(3, 4), batchValues.get(1));
        assertEquals(Collections.singletonList(5), batchValues.get(2));
    }

    @Test
    void get_batches_batch_size_larger_than_dataset() {
        // Create dataset with 3 rows
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty())
        });
        List<Row> rows = Arrays.asList(
                RowFactory.create(1),
                RowFactory.create(2),
                RowFactory.create(3));
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        int batchSize = 10;

        Iterable<List<Row>> batches = Helper.getBatches(dataset, batchSize);

        Iterator<List<Row>> iterator = batches.iterator();

        assertTrue(iterator.hasNext());
        List<Row> batch = iterator.next();

        assertEquals(3, batch.size());
        assertFalse(iterator.hasNext());
    }

    @Test
    void get_batches_empty_dataset() {
        // Create empty dataset
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty())
        });
        List<Row> rows = Collections.emptyList();
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        int batchSize = 3;

        Iterable<List<Row>> batches = Helper.getBatches(dataset, batchSize);

        Iterator<List<Row>> iterator = batches.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    void get_batches_batch_size_one() {
        // Create dataset with 3 rows
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty())
        });
        List<Row> rows = Arrays.asList(
                RowFactory.create(10),
                RowFactory.create(20),
                RowFactory.create(30));
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        int batchSize = 1;

        Iterable<List<Row>> batches = Helper.getBatches(dataset, batchSize);

        Iterator<List<Row>> iterator = batches.iterator();

        List<Integer> collectedIds = new ArrayList<>();
        int countBatches = 0;
        while (iterator.hasNext()) {
            List<Row> batch = iterator.next();
            countBatches++;
            assertEquals(1, batch.size());
            collectedIds.add(batch.get(0).getInt(0));
        }

        assertEquals(3, countBatches);
        assertEquals(Arrays.asList(10, 20, 30), collectedIds);
    }

    // endregion Dataset batching tests

    // region Insert request helper tests

    @Test
    void get_insert_success_map_uses_value_and_table_name_as_key() {
        ArrayList<InsertRecord> records = new ArrayList<>();
        records.add(InsertRecord.builder()
                .data(new HashMap<String, Object>() {
                    {
                        put("name", "shared");
                    }
                })
                .table("table_one")
                .build());
        records.add(InsertRecord.builder()
                .data(new HashMap<String, Object>() {
                    {
                        put("name", "shared");
                    }
                })
                .table("table_two")
                .build());

        Success successOne = mock(Success.class);
        when(successOne.getIndex()).thenReturn(0);
        Success successTwo = mock(Success.class);
        when(successTwo.getIndex()).thenReturn(1);

        InsertResponse insertResponse = mock(InsertResponse.class);
        when(insertResponse.getSuccess()).thenReturn(Arrays.asList(successOne, successTwo));

        Map<Object, Success> successMap = Helper.getInsertSuccessMap(insertResponse, records);

        assertEquals(2, successMap.size());
        assertTrue(successMap.containsKey(Helper.concatWithUnderscore("table_one", "shared")));
        assertTrue(successMap.containsKey(Helper.concatWithUnderscore("table_two", "shared")));
    }

    @Test
    void get_insert_error_map_uses_value_and_table_name_as_key() {
        ArrayList<InsertRecord> records = new ArrayList<>();
        records.add(InsertRecord.builder()
                .data(new HashMap<String, Object>() {
                    {
                        put("name", "shared");
                    }
                })
                .table("table_one")
                .build());
        records.add(InsertRecord.builder()
                .data(new HashMap<String, Object>() {
                    {
                        put("name", "shared");
                    }
                })
                .table("table_two")
                .build());

        ErrorRecord errorOne = mock(ErrorRecord.class);
        when(errorOne.getIndex()).thenReturn(0);
        ErrorRecord errorTwo = mock(ErrorRecord.class);
        when(errorTwo.getIndex()).thenReturn(1);

        InsertResponse insertResponse = mock(InsertResponse.class);
        when(insertResponse.getErrors()).thenReturn(Arrays.asList(errorOne, errorTwo));

        Map<Object, ErrorRecord> errorsMap = Helper.getInsertErrorsMap(insertResponse, records);

        assertEquals(2, errorsMap.size());
        assertTrue(errorsMap.containsKey(Helper.concatWithUnderscore("table_one", "shared")));
        assertTrue(errorsMap.containsKey(Helper.concatWithUnderscore("table_two", "shared")));
    }

    @Test
    void construct_insert_request_with_multiple_rows_and_columns() {
        StructType schema = createSchema("name", "phone", "email");
        List<Map<String, String>> dataList = Arrays.asList(
                new HashMap<String, String>() {
                    {
                        put("name", "Alice");
                        put("phone", "1111");
                        put("email", "alice@example.com");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("name", "Bob");
                        put("phone", "2222");
                        put("email", "bob@example.com");
                    }
                });
        List<Row> batch = dataList.stream()
                .map(data -> {
                    Object[] values = Arrays.stream(schema.fieldNames())
                            .map(data::get)
                            .toArray();
                    return createRowWithSchema(schema, values);
                })
                .collect(Collectors.toList());

        InsertRequest request = Helper.constructInsertRequest(COLUMN_MAPPINGS, batch);

        // Expect 6 records total (3 columns * 2 rows)
        assertEquals(6, request.getRecords().size());

        Set<String> expectedVaultColumns = new HashSet<>(Arrays.asList("name", "phone", "email"));
        Set<Object> collectedValues = new HashSet<>();
        for (InsertRecord record : request.getRecords()) {
            assertEquals(1, record.getData().size());
            String key = record.getData().keySet().iterator().next();
            assertTrue(expectedVaultColumns.contains(key));
            Object val = record.getData().values().iterator().next();
            assertNotNull(val);
            collectedValues.add(val);
        }

        Set<Object> inputValues = new HashSet<>(
                Arrays.asList("Alice", "1111", "alice@example.com", "Bob", "2222", "bob@example.com"));
        assertTrue(collectedValues.containsAll(inputValues));
    }

    @Test
    void construct_insert_request_with_duplicate_values_across_rows() {
        StructType schema = createSchema("name", "phone");
        List<Map<String, String>> dataList = Arrays.asList(
                new HashMap<String, String>() {
                    {
                        put("name", "Alice");
                        put("phone", "1111");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("name", "Alice");
                        put("phone", "2222");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("name", "Bob");
                        put("phone", "1111");
                    }
                });

        COLUMN_MAPPINGS.remove("email");

        List<Row> batch = dataList.stream()
                .map(data -> {
                    Object[] values = Arrays.stream(schema.fieldNames())
                            .map(data::get)
                            .toArray();
                    return createRowWithSchema(schema, values);
                })
                .collect(Collectors.toList());

        InsertRequest request = Helper.constructInsertRequest(COLUMN_MAPPINGS, batch);

        // Deduplicated unique values:
        // name_column: Alice, Bob (2)
        // phone_column: 1111, 2222 (2)
        assertEquals(4, request.getRecords().size());

        Map<String, Set<Object>> vaultToValues = new HashMap<>();
        for (InsertRecord record : request.getRecords()) {
            String key = record.getData().keySet().iterator().next();
            Object val = record.getData().values().iterator().next();
            vaultToValues.putIfAbsent(key, new HashSet<>());
            vaultToValues.get(key).add(val);
        }

        assertEquals(new HashSet<>(Arrays.asList("Alice", "Bob")), vaultToValues.get("name"));
        assertEquals(new HashSet<>(Arrays.asList("1111", "2222")), vaultToValues.get("phone"));
    }

    @Test
    void construct_insert_request_with_empty_batch() {
        List<Row> batch = Collections.emptyList();
        InsertRequest request = Helper.constructInsertRequest(COLUMN_MAPPINGS, batch);
        assertTrue(request.getRecords().isEmpty());
    }

    @Test
    void construct_insert_request_with_partial_schema() {
        StructType schema = createSchema("name", "email");
        List<Map<String, String>> dataList = Arrays.asList(
                new HashMap<String, String>() {
                    {
                        put("name", "Alice");
                        put("email", "alice@example.com");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("name", "Bob");
                        put("email", "bob@example.com");
                    }
                });

        COLUMN_MAPPINGS.remove("phone");

        List<Row> batch = dataList.stream()
                .map(data -> {
                    Object[] values = Arrays.stream(schema.fieldNames())
                            .map(data::get)
                            .toArray();
                    return createRowWithSchema(schema, values);
                })
                .collect(Collectors.toList());

        InsertRequest request = Helper.constructInsertRequest(COLUMN_MAPPINGS, batch);

        for (InsertRecord record : request.getRecords()) {
            String key = record.getData().keySet().iterator().next();
            assertTrue("name".equals(key) || "email".equals(key));
        }
    }

    // endregion Insert request helper tests

    // region Token helper tests

    @Test
    void get_token_returns_token_when_no_target_group() {
        Success success = mock(Success.class);
        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("tokenValue1");
        when(token1.getTokenGroupName()).thenReturn(null);

        Map<String, List<Token>> tokenMap = new HashMap<>();
        tokenMap.put("columnA", Collections.singletonList(token1));
        when(success.getTokens()).thenReturn(tokenMap);

        ColumnMapping skyflowColumnMapping = new ColumnMapping("tableA", "columnA");

        String token = Helper.getToken(success, skyflowColumnMapping);
        assertEquals("tokenValue1", token);
    }

    @Test
    void get_token_returns_token_matching_target_group() {
        Success success = mock(Success.class);

        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("tokenValue1");
        when(token1.getTokenGroupName()).thenReturn("groupA");

        Token token2 = mock(Token.class);
        when(token2.getToken()).thenReturn("tokenValue2");
        when(token2.getTokenGroupName()).thenReturn("groupB");

        Map<String, List<Token>> tokenMap = new HashMap<>();
        tokenMap.put("columnA", Arrays.asList(token1, token2));
        when(success.getTokens()).thenReturn(tokenMap);

        ColumnMapping skyflowColumnMapping = new ColumnMapping("tableA", "columnA", "groupA");

        String token = Helper.getToken(success, skyflowColumnMapping);
        assertEquals("tokenValue1", token);
    }

    @Test
    void get_token_returns_null_when_no_tokens_for_column() {
        Success success = mock(Success.class);
        when(success.getTokens()).thenReturn(Collections.emptyMap());

        ColumnMapping skyflowColumnMapping = new ColumnMapping("columnA", null);

        String token = Helper.getToken(success, skyflowColumnMapping);
        assertNull(token);
    }

    @Test
    void get_token_returns_null_when_token_list_empty() {
        Success success = mock(Success.class);
        Map<String, List<Token>> tokenMap = new HashMap<>();
        tokenMap.put("columnA", Collections.emptyList());
        when(success.getTokens()).thenReturn(tokenMap);

        ColumnMapping skyflowColumnMapping = new ColumnMapping("columnA", null);

        String token = Helper.getToken(success, skyflowColumnMapping);
        assertNull(token);
    }

    @Test
    public void get_token_returns_null_when_no_token_matches_target_group() {
        Success success = mock(Success.class);

        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("tokenValue1");
        when(token1.getTokenGroupName()).thenReturn("groupA");

        Map<String, List<Token>> tokenMap = new HashMap<>();
        tokenMap.put("columnA", Collections.singletonList(token1));
        when(success.getTokens()).thenReturn(tokenMap);

        ColumnMapping skyflowColumnMapping = new ColumnMapping("columnA", "groupB"); // Target group doesn't match
        // token1's group

        String token = Helper.getToken(success, skyflowColumnMapping);
        assertNull(token);
    }

    // endregion Token helper tests

    // endregion Token helper tests

    // region Insert retry helper tests

    @Test
    public void construct_insert_retry_request_empty_errors_map() {
        ArrayList<InsertRecord> allRecords = new ArrayList<>();
        Map<Object, ErrorRecord> errorsMap = new HashMap<>();

        InsertRequest result = Helper.constructInsertRetryRequest(allRecords, errorsMap);

        assertTrue(result.getRecords().isEmpty());
    }

    @Test
    public void construct_insert_retry_request_only_non_retryable_errors() {
        List<InsertRecord> allRecords = Collections.singletonList(
                InsertRecord.builder().data(new HashMap<String, Object>() {
                    {
                        put("name", "Alice");
                    }
                }).build());

        Map<Object, ErrorRecord> errorsMap = new HashMap<>();
        ErrorRecord errorRecord = mock(ErrorRecord.class);
        when(errorRecord.getCode()).thenReturn(400);
        errorsMap.put("Alice", errorRecord);

        InsertRequest result = Helper.constructInsertRetryRequest(allRecords, errorsMap);

        assertTrue(result.getRecords().isEmpty());
    }

    @Test
    public void construct_insert_retry_request_only_retryable_errors() {
        ArrayList<InsertRecord> allRecords = new ArrayList<>();
        allRecords.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Alice");
            }
        }).build());
        allRecords.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Bob");
            }
        }).build());

        Map<Object, ErrorRecord> errorsMap = new HashMap<>();
        ErrorRecord errorRecord = mock(ErrorRecord.class);
        when(errorRecord.getCode()).thenReturn(429);
        errorsMap.put("Alice", errorRecord);
        errorsMap.put("Bob", errorRecord);

        InsertRequest result = Helper.constructInsertRetryRequest(allRecords, errorsMap);

        assertEquals(2, result.getRecords().size());
    }

    @Test
    public void construct_insert_retry_request_mixed_retryable_and_non_retryable_errors() {
        ArrayList<InsertRecord> records;
        records = new ArrayList<>();
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Alice");
            }
        }).table("name").build());
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Bob");
            }
        }).build());

        Map<Object, ErrorRecord> errorsMap = new HashMap<>();
        ErrorRecord errorRecord1 = mock(ErrorRecord.class);
        when(errorRecord1.getCode()).thenReturn(429);
        errorsMap.put("Alice", errorRecord1);

        ErrorRecord errorRecord2 = mock(ErrorRecord.class);
        when(errorRecord2.getCode()).thenReturn(400);
        errorsMap.put("Bob", errorRecord2);

        InsertRequest result = Helper.constructInsertRetryRequest(records, errorsMap);

        assertEquals(1, result.getRecords().size());
        assertEquals("Alice", result.getRecords().get(0).getData().get("name"));
    }

    @Test
    public void construct_insert_retry_request_duplicate_retryable_indexes() {
        List<InsertRecord> allRecords = Arrays.asList(
                InsertRecord.builder().data(new HashMap<String, Object>() {
                    {
                        put("name", "Alice");
                    }
                }).build(),
                InsertRecord.builder().data(new HashMap<String, Object>() {
                    {
                        put("name", "Bob");
                    }
                }).build());

        Map<Object, ErrorRecord> errorsMap = new HashMap<>();
        ErrorRecord errorRecord1 = mock(ErrorRecord.class);
        when(errorRecord1.getCode()).thenReturn(429);
        when(errorRecord1.getIndex()).thenReturn(0);
        ErrorRecord errorRecord2 = mock(ErrorRecord.class);
        when(errorRecord2.getCode()).thenReturn(429);
        when(errorRecord2.getIndex()).thenReturn(1);
        errorsMap.put("Alice", errorRecord1);
        errorsMap.put("Bob", errorRecord2);

        InsertRequest result = Helper.constructInsertRetryRequest(allRecords, errorsMap);

        assertEquals(2, result.getRecords().size());
    }

    // endregion Insert retry helper tests

    // region Token replacement tests

    @Test
    public void replace_data_with_tokens_success() {
        StructType schema = createSchema("phone");
        Row row = createRowWithSchema(schema, 123);
        Success success = mock(Success.class);
        Map<Object, Success> successMap = Collections.singletonMap(Helper.concatWithUnderscore("phone", 123), success);
        Map<String, List<Token>> tokenMap = new HashMap<>();
        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("token123");
        when(token1.getTokenGroupName()).thenReturn("deterministic_phone");
        tokenMap.put("phone", Collections.singletonList(token1));
        when(success.getTokens()).thenReturn(tokenMap);
        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                new HashMap<>());
        assertEquals("token123", out.get(0).getString(0));
        assertEquals("200", out.get(0).getString(1));
        assertNull(out.get(0).get(2));
    }

    @Test
    public void replace_data_with_tokens_sets_error_when_token_group_mismatch() {
        StructType schema = createSchema("name");
        Row row = createRowWithSchema(schema, "Alice");
        ColumnMapping mapping = new ColumnMapping("name_table", "name_column", "expected_group");
        Map<String, ColumnMapping> mappings = new HashMap<>();
        mappings.put("name", mapping);

        Success success = mock(Success.class);
        Token token = mock(Token.class);
        when(token.getToken()).thenReturn("tokenValue");
        when(token.getTokenGroupName()).thenReturn("different_group");
        Map<String, List<Token>> tokenMap = new HashMap<>();
        tokenMap.put("name_column", Collections.singletonList(token));
        when(success.getTokens()).thenReturn(tokenMap);

        Map<Object, Success> successMap = Collections
                .singletonMap(Helper.concatWithUnderscore("name_table", "Alice"), success);

        List<Row> out = Helper.replaceDataWithTokens(mappings, Collections.singletonList(row), successMap,
                Collections.emptyMap());

        Row result = out.get(0);
        assertEquals("Alice", result.getString(0));
        assertEquals(Constants.STATUS_ERROR, result.getString(1));
        assertEquals(Constants.INSERT_FAILED, result.get(2));
    }

    @Test
    public void replace_data_with_tokens_sets_error_when_value_absent_in_maps() {
        StructType schema = createSchema("name");
        Row row = createRowWithSchema(schema, "Bob");
        ColumnMapping mapping = new ColumnMapping("name_table", "name_column");
        Map<String, ColumnMapping> mappings = new HashMap<>();
        mappings.put("name", mapping);

        List<Row> out = Helper.replaceDataWithTokens(mappings, Collections.singletonList(row), Collections.emptyMap(),
                Collections.emptyMap());

        Row result = out.get(0);
        assertEquals("Bob", result.getString(0));
        assertEquals(Constants.STATUS_ERROR, result.getString(1));
        assertEquals(Constants.INSERT_FAILED, result.get(2));
    }

    @Test
    public void replace_data_with_tokens_success_token_not_populated() {
        StructType schema = createSchema("phone");
        Row row = createRowWithSchema(schema, 123);
        Success success = mock(Success.class);
        Map<Object, Success> successMap = Collections.singletonMap(Helper.concatWithUnderscore("phone", 123), success);
        Map<String, List<Token>> tokenMap = new HashMap<>();
        tokenMap.put("phone", Collections.emptyList());
        when(success.getTokens()).thenReturn(tokenMap);
        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                new HashMap<>());
        assertEquals(123, out.get(0).getInt(0));
        assertEquals("500", out.get(0).getString(1));
        assertEquals(Constants.INSERT_FAILED, out.get(0).get(2));
    }

    @Test
    public void replace_data_with_tokens_success_no_token_group() {
        StructType schema = createSchema("phone");
        Row row = createRowWithSchema(schema, 123);
        Success success = mock(Success.class);
        Map<Object, Success> successMap = Collections.singletonMap(Helper.concatWithUnderscore("phone", 123), success);
        Map<String, List<Token>> tokenMap = new HashMap<>();
        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("token123");
        when(token1.getTokenGroupName()).thenReturn("deterministic_phone");
        tokenMap.put("phone", Collections.singletonList(token1));
        when(success.getTokens()).thenReturn(tokenMap);
        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                new HashMap<>());
        assertEquals("token123", out.get(0).getString(0));
        assertEquals("200", out.get(0).getString(1));
        assertNull(out.get(0).get(2));
    }

    @Test
    public void replace_data_with_tokens_but_null_token() {
        StructType schema = createSchema("phone");
        Row row = createRowWithSchema(schema, 123);
        Success success = mock(Success.class);
        Map<Object, Success> successMap = Collections.singletonMap(123, success);

        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                new HashMap<>());
        assertEquals("500", out.get(0).getString(1)); // error row
    }

    @Test
    public void replace_data_with_tokens_with_error() {
        StructType schema = createSchema("phone");
        Row row = createRowWithSchema(schema, 123);
        ErrorRecord errorRecord = mock(ErrorRecord.class);
        when(errorRecord.getError()).thenReturn("Bad Request");
        Map<Object, ErrorRecord> errorsMap = Collections.singletonMap("phone_123", errorRecord);
        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), new HashMap<>(),
                errorsMap);
        assertEquals("500", out.get(0).getString(1));
        assertEquals("Bad Request", out.get(0).getString(2));
    }

    @Test
    public void replace_data_with_tokens_not_in_any_map() {
        StructType schema = createSchema("phone");
        Row row = createRowWithSchema(schema, 123);

        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), new HashMap<>(),
                new HashMap<>());
        assertEquals("500", out.get(0).getString(1));
    }

    @Test
    public void replace_data_tokens_with_no_tokens() {
        StructType schema = createSchema("first_nm");
        Row row = createRowWithSchema(schema, "Alice");

        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), new HashMap<>(),
                new HashMap<>());
        assertEquals("Alice", out.get(0).getString(0));
        assertEquals("200", out.get(0).getString(1));
    }

    @Test
    public void replace_data_with_tokens_success_and_failure() {
        StructType schema = createSchema("phone", "name");
        Row row = createRowWithSchema(schema, "123", "Alice");
        Success success = mock(Success.class);
        Map<Object, Success> successMap = Collections.singletonMap("123", success);
        Map<String, List<Token>> tokenMap = new HashMap<>();
        Token token1 = mock(Token.class);
        when(token1.getToken()).thenReturn("token123");
        tokenMap.put("phone", Collections.singletonList(token1));
        when(success.getTokens()).thenReturn(tokenMap);
        List<Row> out = Helper.replaceDataWithTokens(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                new HashMap<>());
        assertEquals("123", out.get(0).getString(0));
        assertEquals("Alice", out.get(0).getString(1));
        assertEquals("500", out.get(0).getString(2));
    }

    // endregion Token replacement tests

    // region Insert retry merge tests

    @Test
    public void insert_merge_retry_results_only_successes() {
        ArrayList<InsertRecord> records;
        InsertResponse retryResponse;
        Map<Object, Success> successMap;
        Map<Object, ErrorRecord> errorsMap;
        records = new ArrayList<>();
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Alice");
            }
        }).build());

        retryResponse = mock(InsertResponse.class);
        successMap = new HashMap<>();
        errorsMap = new HashMap<>();
        successMap.put(Helper.concatWithUnderscore("name", "Alice"), mock(Success.class));

        Helper.mergeInsertRetryResults(records, retryResponse, successMap, errorsMap);

        assertEquals(1, successMap.size());
        assertTrue(successMap.containsKey(Helper.concatWithUnderscore("name", "Alice")));
        assertTrue(errorsMap.isEmpty());
    }

    @Test
    public void insert_merge_retry_results_only_errors() {
        ArrayList<InsertRecord> records;
        InsertResponse retryResponse;
        Map<Object, Success> successMap;
        Map<Object, ErrorRecord> errorsMap;
        records = new ArrayList<>();

        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Alice");
            }
        }).table("name").build());
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Bob");
            }
        }).table("name").build());

        retryResponse = mock(InsertResponse.class);
        successMap = new HashMap<>();
        errorsMap = new HashMap<>();
        ErrorRecord errorRecord1 = mock(ErrorRecord.class);
        when(errorRecord1.getCode()).thenReturn(429);
        when(errorRecord1.getIndex()).thenReturn(0);
        ErrorRecord errorRecord2 = mock(ErrorRecord.class);
        when(errorRecord2.getCode()).thenReturn(429);
        when(errorRecord2.getIndex()).thenReturn(1);
        errorsMap.put(Helper.concatWithUnderscore("name", "Alice"), errorRecord1);
        errorsMap.put(Helper.concatWithUnderscore("name", "Bob"), errorRecord2);

        when(retryResponse.getSuccess()).thenReturn(Collections.emptyList());
        when(retryResponse.getErrors()).thenReturn(Arrays.asList(errorRecord1, errorRecord2));
        Helper.mergeInsertRetryResults(records, retryResponse, successMap, errorsMap);

        assertEquals(2, errorsMap.size());
        assertTrue(errorsMap.containsKey(Helper.concatWithUnderscore("name", "Bob")));
        assertTrue(successMap.isEmpty());
    }

    @Test
    public void insert_merge_retry_results_successes_and_errors() {
        ArrayList<InsertRecord> records;
        InsertResponse retryResponse;
        Map<Object, Success> successMap;
        Map<Object, ErrorRecord> errorsMap;
        records = new ArrayList<>();

        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Alice");
            }
        }).table("name").build());
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Bob");
            }
        }).table("name").build());

        retryResponse = mock(InsertResponse.class);
        successMap = new HashMap<>();
        errorsMap = new HashMap<>();

        Success success = mock(Success.class);
        when(success.getIndex()).thenReturn(0);

        ErrorRecord error = mock(ErrorRecord.class);
        when(error.getIndex()).thenReturn(1);

        when(retryResponse.getSuccess()).thenReturn(Collections.singletonList(success));
        when(retryResponse.getErrors()).thenReturn(Collections.singletonList(error));

        Helper.mergeInsertRetryResults(records, retryResponse, successMap, errorsMap);

        assertEquals(1, successMap.size());
        assertEquals(1, errorsMap.size());
        assertTrue(successMap.containsKey(Helper.concatWithUnderscore("name", "Alice")));
        assertTrue(errorsMap.containsKey(Helper.concatWithUnderscore("name", "Bob")));
    }

    @Test

    public void insert_merge_retry_results_empty_retry_response() {
        ArrayList<InsertRecord> records;
        InsertResponse retryResponse;
        Map<Object, Success> successMap;
        Map<Object, ErrorRecord> errorsMap;
        records = new ArrayList<>();
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Alice");
            }
        }).build());
        records.add(InsertRecord.builder().data(new HashMap<String, Object>() {
            {
                put("name", "Bob");
            }
        }).build());

        retryResponse = mock(InsertResponse.class);
        successMap = new HashMap<>();
        errorsMap = new HashMap<>();

        Helper.mergeInsertRetryResults(records, retryResponse, successMap, errorsMap);

        assertTrue(successMap.isEmpty());
        assertTrue(errorsMap.isEmpty());
    }

    // endregion Insert retry merge tests

    // region Detokenize helper tests

    @Test
    public void construct_detokenize_request_single_mapping() {

        COLUMN_MAPPINGS.remove("phone");
        COLUMN_MAPPINGS.remove("email");
        StructType schema = createSchema("name");
        Row row = createRowWithSchema(schema, "token123");

        DetokenizeRequest req = Helper.constructDetokenizeRequest(COLUMN_MAPPINGS, Collections.singletonList(row));

        assertEquals(1, req.getTokens().size());
        assertTrue(req.getTokens().contains("token123"));

        assertEquals(1, req.getTokenGroupRedactions().size());
        TokenGroupRedactions red = req.getTokenGroupRedactions().get(0);
        assertEquals("deterministic_name", red.getTokenGroupName());
        assertEquals("name_redaction", red.getRedaction());
    }

    @Test
    public void construct_detokenize_request_multiple_mappings_and_dedup() {
        COLUMN_MAPPINGS.remove("phone");
        StructType schema = createSchema("name", "email");
        Row row1 = createRowWithSchema(schema, "tokenA", "tokenB");
        Row row2 = createRowWithSchema(schema, "tokenA", "tokenC"); // tokenA appears twice

        DetokenizeRequest req = Helper.constructDetokenizeRequest(COLUMN_MAPPINGS, Arrays.asList(row1, row2));

        assertEquals(3, req.getTokens().size()); // tokenA, tokenB, tokenC
        assertTrue(req.getTokens().containsAll(Arrays.asList("tokenA", "tokenB", "tokenC")));

        assertEquals(1, req.getTokenGroupRedactions().size());
    }

    @Test
    public void construct_detokenize_request_schema_has_no_mappings() {
        StructType schema = createSchema("unmapped");
        Row row = createRowWithSchema(schema, "value");
        COLUMN_MAPPINGS.clear();
        DetokenizeRequest req = Helper.constructDetokenizeRequest(COLUMN_MAPPINGS, Collections.singletonList(row));

        assertTrue(req.getTokens().isEmpty());
        assertTrue(req.getTokenGroupRedactions().isEmpty());
    }

    @Test
    public void construct_detokenize_request_skip_mapping_without_token_group_or_redaction() {
        COLUMN_MAPPINGS.remove("email");
        COLUMN_MAPPINGS.remove("phone");
        COLUMN_MAPPINGS.put("name", new ColumnMapping("name", "name", ""));
        StructType schema = createSchema("name");
        Row row = createRowWithSchema(schema, "tokenZ");

        DetokenizeRequest req = Helper.constructDetokenizeRequest(COLUMN_MAPPINGS, Collections.singletonList(row));

        assertEquals(1, req.getTokens().size());
        assertTrue(req.getTokens().contains("tokenZ"));

        assertTrue(req.getTokenGroupRedactions().isEmpty()); // skipped
    }

    @Test
    public void construct_detokenize_request_mixed_valid_and_invalid_mappings() {
        StructType schema = createSchema("name", "email");
        // no tokenGroupName
        COLUMN_MAPPINGS.remove("phone");
        List<Row> batch = Collections.singletonList(
                createRowWithSchema(schema, "token1", "token2"));

        DetokenizeRequest request = Helper.constructDetokenizeRequest(COLUMN_MAPPINGS, batch);

        // Both values should appear
        assertTrue(request.getTokens().contains("token1"));
        assertTrue(request.getTokens().contains("token2"));

        // Only valid mapping should produce a redaction
        assertEquals(1, request.getTokenGroupRedactions().size());
        assertEquals("deterministic_name", request.getTokenGroupRedactions().get(0).getTokenGroupName());
    }

    @Test
    void get_detokenize_success_map() {
        DetokenizeResponseObject obj1 = mock(DetokenizeResponseObject.class);
        when(obj1.getToken()).thenReturn("token1");

        DetokenizeResponse detokenizeResponse = mock(DetokenizeResponse.class);
        when(detokenizeResponse.getSuccess()).thenReturn(Collections.singletonList(obj1));

        Map<String, DetokenizeResponseObject> successMap = Helper.getDetokenizeSuccessMap(detokenizeResponse);

        assertEquals(1, successMap.size());
        assertTrue(successMap.containsKey("token1"));
    }

    @Test
    void get_detokenize_errors_map() {
        ErrorRecord error = mock(ErrorRecord.class);
        when(error.getIndex()).thenReturn(0);

        DetokenizeResponse detokenizeResponse = mock(DetokenizeResponse.class);
        when(detokenizeResponse.getErrors()).thenReturn(Collections.singletonList(error));

        List<String> tokens = Collections.singletonList("token1");

        Map<String, ErrorRecord> errorMap = Helper.geDetokenizeErrorsMap(detokenizeResponse, tokens);

        assertEquals(1, errorMap.size());
        assertTrue(errorMap.containsKey("token1"));
    }

    @Test
    void replace_tokens_with_data_successful_replacement() {
        StructType schema = createSchema("name", "phone", "other");

        Row row = createRowWithSchema(schema, "token1", "token2", "static");

        DetokenizeResponseObject respName = mock(DetokenizeResponseObject.class);
        when(respName.getValue()).thenReturn("Alice");
        DetokenizeResponseObject respPhone = mock(DetokenizeResponseObject.class);
        when(respPhone.getValue()).thenReturn("1111");

        Map<String, DetokenizeResponseObject> successMap = new HashMap<>();
        successMap.put("token1", respName);
        successMap.put("token2", respPhone);

        Map<String, ErrorRecord> errorsMap = new HashMap<>();

        List<Row> outputRows = Helper.replaceTokensWithData(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                errorsMap);

        assertEquals(1, outputRows.size());
        Row outputRow = outputRows.get(0);

        assertEquals("Alice", outputRow.getString(0));
        assertEquals("1111", outputRow.getString(1));
        assertEquals("static", outputRow.getString(2));
        assertEquals(Constants.STATUS_OK, outputRow.getString(3));
        assertNull(outputRow.get(4));
    }

    @Test
    void replace_tokens_with_data_failure_due_to_error() {
        StructType schema = createSchema("name");

        Row row = createRowWithSchema(schema, "token1");

        Map<String, DetokenizeResponseObject> successMap = new HashMap<>();
        Map<String, ErrorRecord> errorsMap = new HashMap<>();
        ErrorRecord errorRecord = mock(ErrorRecord.class);
        when(errorRecord.getError()).thenReturn("Token not found");
        errorsMap.put("token1", errorRecord);

        List<Row> outputRows = Helper.replaceTokensWithData(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                errorsMap);

        assertEquals(1, outputRows.size());
        Row outputRow = outputRows.get(0);

        assertEquals("token1", outputRow.getString(0));
        assertEquals(Constants.STATUS_ERROR, outputRow.getString(1));
        assertEquals("Token not found", outputRow.getString(2));
    }

    @Test
    void replace_tokens_with_data_success_map_key_not_string() {
        StructType schema = new StructType(new StructField[] {
                new StructField("name", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("phone", DataTypes.StringType, true, Metadata.empty())
        });

        // Prepare batch rows with tokens
        Row row = createRowWithSchema(schema, 100, "token2");

        // successMap with non-string keys (Integer keys)
        DetokenizeResponseObject resp = mock(DetokenizeResponseObject.class);
        when(resp.getValue()).thenReturn("replacedValue");

        Map<Object, DetokenizeResponseObject> successMapWrongKey = new HashMap<>();
        successMapWrongKey.put(100, resp); // Integer key, not String

        // Cast to Map<String, DetokenizeResponseObject> unsafely for test
        @SuppressWarnings("unchecked")
        Map<String, DetokenizeResponseObject> successMap = (Map<String, DetokenizeResponseObject>) (Map<?, ?>) successMapWrongKey;

        Map<Object, ErrorRecord> errorRecordMapKey = new HashMap<>();
        // No errors
        @SuppressWarnings("unchecked")
        Map<String, ErrorRecord> errorsMap = (Map<String, ErrorRecord>) (Map<?, ?>) errorRecordMapKey;

        List<Row> result = Helper.replaceTokensWithData(COLUMN_MAPPINGS, Collections.singletonList(row), successMap,
                errorsMap);

        assertEquals(1, result.size());
        Row row1 = result.get(0);

        // Because successMap keys are not strings, tokens won't be replaced
        assertEquals(100, row1.get(0));
        assertEquals("token2", row1.get(1));

        // Row should be successful since no errors found
        int size = row1.size();
        assertEquals("500", row1.get(size - 2));
        assertNotNull(row1.get(size - 1));
    }

    // endregion Detokenize helper tests
    // region Backoff utility tests

    @Test
    void sleep_with_exponential_backoff_does_not_throw() {
        // Just test that method runs without exception and does not exceed max sleep
        // time
        int[] retriesToTest = { 0, 1, 2, 3, 4, 5, 6 };

        for (int retry : retriesToTest) {
            long start = System.currentTimeMillis();

            // Run in separate thread to avoid blocking main test thread too long
            Thread t = new Thread(() -> Helper.sleepWithExponentialBackoff(retry));
            t.start();
            try {
                t.join(2000); // timeout to prevent hanging test
            } catch (InterruptedException e) {
                Assertions.fail("Test thread interrupted");
            }

            long end = System.currentTimeMillis();
            long elapsed = end - start;

            // The max sleep time is Constants.MAX_DELAY_MILLI_SECONDS, so elapsed should
            // not exceed that by a large margin
            assertTrue(elapsed <= Constants.MAX_DELAY_MILLI_SECONDS + 200,
                    "Sleep time exceeded max delay with margin for retry " + retry + ": " + elapsed + "ms");
        }
    }

    @Test
    void sleep_with_exponential_backoff_interrupted_status_restored() throws InterruptedException {
        Thread testThread = new Thread(() -> {
            // We interrupt this thread while it sleeps, then test if interrupted status is
            // restored
            Helper.sleepWithExponentialBackoff(3);
            assertTrue(Thread.currentThread().isInterrupted(),
                    "Thread interrupted status should be restored after InterruptedException");
        });

        testThread.start();

        // Wait briefly and interrupt
        Thread.sleep(50);
        testThread.interrupt();

        testThread.join(1000);
        // If assertion inside thread fails, test will fail
    }

    // endregion Backoff utility tests
    // region Row batch size calculation tests

    @Test
    public void calculate_row_batch_size_returns_zero_when_no_tokenizable_columns() {
        COLUMN_MAPPINGS.clear();
        int result = Helper.calculateRowBatchSize(COLUMN_MAPPINGS, 1000);
        assertEquals(0, result);
    }

    @Test
    public void calculate_row_batch_size_returns_target_batch_size_when_one_tokenizable_column() {
        COLUMN_MAPPINGS.remove("name");
        COLUMN_MAPPINGS.remove("phone");
        int result = Helper.calculateRowBatchSize(COLUMN_MAPPINGS, 1000);
        assertEquals(1000, result);
    }

    @Test
    public void calculate_row_batch_size_divides_target_by_number_of_tokenizable_columns() {
        COLUMN_MAPPINGS.remove("email");
        int result = Helper.calculateRowBatchSize(COLUMN_MAPPINGS, 1000);
        assertEquals(500, result);
    }

    @Test
    public void calculate_row_batch_size_rounds_up_when_not_evenly_divisible() {
        int result = Helper.calculateRowBatchSize(COLUMN_MAPPINGS, 1000);
        // 1000/3 = 333.33 -> ceil = 334
        assertEquals(334, result);
    }

    @Test
    public void calculate_row_batch_size_ignores_non_tokenizable_columns() {
        COLUMN_MAPPINGS.remove("email");
        int result = Helper.calculateRowBatchSize(COLUMN_MAPPINGS, 1000);
        // only first_nm and last_nm are tokenizable
        assertEquals(500, result);
    }

    // endregion Row batch size calculation tests
}
