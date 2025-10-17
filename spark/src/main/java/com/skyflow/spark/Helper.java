package com.skyflow.spark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.skyflow.errors.SkyflowException;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.Properties;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.logging.Logger;

import static com.skyflow.spark.Constants.*;

public class Helper {

    // Constants for wrapper batch sizes
    private static final Logger logger = LoggerUtil.getLogger(Helper.class);

    static com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    private static final TypeReference<Map<String, Map<String, String>>> COLUMN_MAPPING_TYPE = new TypeReference<Map<String, Map<String, String>>>() {
    };

    public static Map<String, ColumnMapping> configureColumnMappings(StructType schema, Properties properties,
            String datasetTableName) throws SkyflowException {
        if (properties == null) {
            return constructDefaultSchemaMappings(schema, datasetTableName);
        }

        String columnMappingConfig = properties.getProperty(COLUMN_MAPPING);
        if (columnMappingConfig == null || columnMappingConfig.trim().isEmpty()) {
            return constructDefaultSchemaMappings(schema, datasetTableName);
        }

        try {
            Map<String, Map<String, String>> propertyColumnMappings = mapper.readValue(
                    columnMappingConfig, COLUMN_MAPPING_TYPE);
            Map<String, ColumnMapping> columnMappingsMap = new LinkedHashMap<>();
            for (String datasetColumn : schema.fieldNames()) {
                Map<String, String> mappingDetails = propertyColumnMappings.get(datasetColumn);
                if (mappingDetails == null) {
                    // Mapping is not found, considering column as non-tokenizable
                    continue;
                }
                String tableName = mappingDetails.get(TABLE_NAME);
                String columnName = mappingDetails.get(COLUMN_NAME);

                if (isBlank(tableName) || isBlank(columnName)) {
                    throw new SkyflowException(
                            "Table name and column name are required for column '" + datasetColumn + "'.");
                }

                // Have to specify token group name and redaction if a column is subscribed to
                // multiple token groups
                String tokenGroupName = mappingDetails.get(TOKEN_GROUP_NAME);
                if (isBlank(tokenGroupName)) {
                    tokenGroupName = null;
                }
                String redaction = mappingDetails.get(REDACTION);
                if (isBlank(redaction)) {
                    redaction = null;
                }
                columnMappingsMap.put(datasetColumn, buildMapping(tableName, columnName, tokenGroupName, redaction));
            }
            return columnMappingsMap;
        } catch (Exception e) {
            throw new SkyflowException("Failed to parse column mappings from properties.", e);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static String concatWithUnderscore(String value1, Object value2) {
        if (value1 == null)
            value1 = "";
        if (value2 == null)
            value2 = "";
        return value1 + "_" + value2;
    }

    private static ColumnMapping buildMapping(String tableName, String columnName, String tokenGroupName,
            String redaction) {
        if (redaction != null) {
            return new ColumnMapping(tableName, columnName, tokenGroupName, redaction);
        }
        if (tokenGroupName != null) {
            return new ColumnMapping(tableName, columnName, tokenGroupName);
        }
        return new ColumnMapping(tableName, columnName);
    }

    public static Map<String, ColumnMapping> constructDefaultSchemaMappings(StructType schema,
            String datasetTableName) {
        Map<String, ColumnMapping> schemaMappings = new HashMap<>();
        for (String datasetColumn : schema.fieldNames()) {
            String lookupColumn = datasetColumn;
            // need table name check in case if gender_cd, as it is present in two tables
            if (datasetColumn.equals("gender_cd")) {
                lookupColumn = datasetTableName.toLowerCase() + "." + datasetColumn;
            }
            if (!SKYFLOW_COLUMN_MAP.containsKey(lookupColumn)) {
                // moving ahead, if mapping is not found considers column as non-tokenizable
                continue;
            }
            String vaultFieldName = SKYFLOW_COLUMN_MAP.get(lookupColumn).toString();
            schemaMappings.put(datasetColumn, new ColumnMapping(vaultFieldName, vaultFieldName, vaultFieldName));
        }
        return schemaMappings;
    }

    // Splits a Dataset<Row> into batches of a specified size
    public static Iterable<List<Row>> getBatches(Dataset<Row> dataset, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than 0");
        }
        return () -> new Iterator<List<Row>>() {
            private final Iterator<Row> rowIterator = dataset.toLocalIterator();

            @Override
            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            @Override
            public List<Row> next() {
                List<Row> batch = new ArrayList<>(batchSize);
                int count = 0;
                while (rowIterator.hasNext() && count < batchSize) {
                    batch.add(rowIterator.next());
                    count++;
                }
                return batch;
            }
        };
    }

    /**
     * Tokenize util methods
     */

    // Constructs an InsertRequest object from a batch of rows and column mappings
    public static InsertRequest constructInsertRequest(Map<String, ColumnMapping> schemaMappings, List<Row> batch) {
        ArrayList<InsertRecord> records = new ArrayList<>();

        // Track seen values per table + vault column
        Map<String, Map<String, Set<Object>>> valuesDedupMap = new HashMap<>();
        for (Row row : batch) {
            List<InsertRecord> rowRecords = constructInsertRecordsForRow(row, valuesDedupMap, schemaMappings);
            records.addAll(rowRecords);
        }
        return InsertRequest.builder()
                .records(records)
                .build();
    }

    private static List<InsertRecord> constructInsertRecordsForRow(Row row,
            Map<String, Map<String, Set<Object>>> seenValues, Map<String, ColumnMapping> schemaMappings) {
        List<InsertRecord> records = new ArrayList<>();
        for (Map.Entry<String, ColumnMapping> entry : schemaMappings.entrySet()) {

            String datasetColumn = entry.getKey();
            ColumnMapping skyflowColumnMapping = entry.getValue();
            String tableName = skyflowColumnMapping.getTableName();
            String vaultColumn = skyflowColumnMapping.getColumnName();

            Object value = row.getAs(datasetColumn);

            // Initialize per table + vault column
            seenValues.computeIfAbsent(tableName, k -> new HashMap<>());
            seenValues.get(tableName).computeIfAbsent(vaultColumn, k -> new HashSet<>());

            // Skip only if this value was already added for this table + vault column
            if (seenValues.get(tableName).get(vaultColumn).contains(value))
                continue;

            seenValues.get(tableName).get(vaultColumn).add(value);

            // Build record
            HashMap<String, Object> record = new HashMap<>();
            record.put(vaultColumn, row.getAs(datasetColumn));
            records.add(InsertRecord.builder().data(record).table(skyflowColumnMapping.getTableName())
                    .upsert(Collections.singletonList(skyflowColumnMapping.getColumnName())).build());
        }
        return records;
    }

    // Converts InsertResponse success records into a map for quick lookup
    public static Map<Object, Success> getInsertSuccessMap(InsertResponse insertResponse,
            ArrayList<InsertRecord> records) {
        Map<Object, Success> successMap = new HashMap<>();
        for (Success success : insertResponse.getSuccess()) {
            InsertRecord record = records.get(success.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTable(), value);
            successMap.put(key, success);
        }
        return successMap;
    }

    // Converts InsertResponse error records into a map for quick lookup
    public static Map<Object, ErrorRecord> getInsertErrorsMap(InsertResponse insertResponse,
            ArrayList<InsertRecord> records) {
        Map<Object, ErrorRecord> errorsMap = new HashMap<>();
        for (ErrorRecord errorRecord : insertResponse.getErrors()) {
            InsertRecord record = records.get(errorRecord.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTable(), value);
            errorsMap.put(key, errorRecord);
        }
        return errorsMap;
    }

    // Replaces data in rows with tokens based on success and error maps
    public static List<Row> replaceDataWithTokens(Map<String, ColumnMapping> schemaMappings, List<Row> batch,
            Map<Object, Success> successMap, Map<Object, ErrorRecord> errorsMap) {
        List<Row> outputRows = new ArrayList<>();

        for (Row row : batch) {
            List<Object> rowData = new ArrayList<>();
            boolean hasFailure = false;
            for (String field : row.schema().fieldNames()) {
                ColumnMapping skyflowColumnMapping = schemaMappings.get(field);
                Object value = row.getAs(field);
                if (skyflowColumnMapping != null) {
                    // key would be value + tableName
                    String key = concatWithUnderscore(skyflowColumnMapping.getTableName(), value);
                    if (successMap.containsKey(key)) {
                        String token = getToken(successMap.get(key), skyflowColumnMapping);
                        if (token != null) {
                            rowData.add(token);
                        } else {
                            // Token is not populated, treating it as failure, failing row
                            hasFailure = true;
                            break;
                        }
                    } else if (errorsMap.containsKey(key)) {
                        // If tokenization failed for this value, failing row
                        hasFailure = true;
                        break;
                    } else {
                        // Token not present in either map — treat as failure, failing row
                        hasFailure = true;
                        break;
                    }
                } else {
                    // Not a tokenizable value, no column mapping found, copying as is
                    rowData.add(value);
                }
            }
            if (hasFailure) {
                rowData = populateErrorRow(row);
            } else {
                rowData.add(Constants.STATUS_OK);
                rowData.add(null);
            }
            outputRows.add(RowFactory.create(rowData.toArray()));
        }
        return outputRows;
    }

    // Gets tokens for a given success object and mapping
    public static String getToken(Success success, ColumnMapping skyflowColumnMapping) {
        List<Token> tokenObj = success.getTokens().get(skyflowColumnMapping.getColumnName());
        // failing if there are no tokens
        if (tokenObj != null && !tokenObj.isEmpty()) {
            String targetGroup = skyflowColumnMapping.getTokenGroupName();
            // considering token group only if it is passed
            if (targetGroup != null && !targetGroup.isEmpty()) {
                Optional<Token> token = tokenObj.stream()
                        .filter(t -> targetGroup.equals(t.getTokenGroupName()))
                        .findFirst();
                return token.map(Token::getToken).orElse(null);
            } else {
                // gets the first token from the list of tokens, if token group is not passed
                return tokenObj.get(0).getToken();
            }
        } else {
            return null;
        }
    }

    // Build a row containing original values plus error status and message.
    private static List<Object> populateErrorRow(Row in) {
        List<Object> rowData = copyRowData(in);
        rowData.add(Constants.STATUS_ERROR);
        rowData.add(Constants.INSERT_FAILED);
        return rowData;
    }

    // Copies all values from the given Row into a mutable List<Object>.
    private static List<Object> copyRowData(Row in) {
        List<Object> rowData = new ArrayList<>();
        for (int j = 0; j < in.size(); j++) {
            rowData.add(in.get(j));
        }
        return rowData;
    }

    // Builds a retry request for failed records with retryable error codes
    public static InsertRequest constructInsertRetryRequest(
            List<InsertRecord> allRecords,
            Map<Object, ErrorRecord> errorsMap) {
        ArrayList<InsertRecord> retryRecords = new ArrayList<>();
        for (ErrorRecord errorRecord : errorsMap.values()) {
            if (Constants.RETRYABLE_ERROR_CODES.contains(errorRecord.getCode())) {
                InsertRecord originalRecord = allRecords.get(errorRecord.getIndex());
                retryRecords.add(originalRecord);
            }
        }
        return InsertRequest.builder()
                .records(retryRecords)
                .build();
    }

    // Merges retry results into the original success and error maps for insert
    public static void mergeInsertRetryResults(
            List<InsertRecord> records,
            InsertResponse retryResponse,
            Map<Object, Success> successMap,
            Map<Object, ErrorRecord> errorsMap) {
        ArrayList<InsertRecord> retryRecords = new ArrayList<>(records);
        Map<Object, Success> retrySuccessMap = getInsertSuccessMap(
                retryResponse, retryRecords);
        Map<Object, ErrorRecord> retryErrorsMap = getInsertErrorsMap(
                retryResponse, retryRecords);
        for (Success success : retrySuccessMap.values()) {
            InsertRecord record = retryRecords.get(success.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTable(), value);
            successMap.put(key, success);
            errorsMap.remove(key);
        }
        for (ErrorRecord errorRecord : retryErrorsMap.values()) {
            InsertRecord record = retryRecords.get(errorRecord.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTable(), value);
            errorsMap.put(key, errorRecord);
        }
        logger.fine(LOG_PREFIX + "Merged " + retrySuccessMap.size() + " success entries and " + retryErrorsMap.size()
                + " error entries.");
    }

    /**
     * Detokenize util methods
     */

    // Constructs a set of tokens for detokenization from a batch of rows and column
    // mappings
    public static DetokenizeRequest constructDetokenizeRequest(Map<String, ColumnMapping> schemaMappings,
            List<Row> batch) {
        Set<String> tokens = new HashSet<>();
        for (Row row : batch) {
            for (Map.Entry<String, ColumnMapping> mapping : schemaMappings.entrySet()) {
                tokens.add(row.getAs(mapping.getKey()));
            }
        }
        List<TokenGroupRedactions> tokenGroupRedactions = new ArrayList<>();
        for (Map.Entry<String, ColumnMapping> mapping : schemaMappings.entrySet()) {
            String tokenGroupName = mapping.getValue().getTokenGroupName();
            String redaction = mapping.getValue().getRedaction();
            if (tokenGroupName != null && redaction != null) {
                tokenGroupRedactions.add(
                        TokenGroupRedactions.builder().redaction(redaction).tokenGroupName(tokenGroupName).build());
            }
        }
        List<String> tokensList = new ArrayList<>(tokens);
        return DetokenizeRequest.builder().tokens(tokensList).tokenGroupRedactions(tokenGroupRedactions).build();
    }

    // Converts DetokenizeResponse success records into a map for quick lookup
    public static Map<String, DetokenizeResponseObject> getDetokenizeSuccessMap(DetokenizeResponse detokenizeResponse) {
        Map<String, DetokenizeResponseObject> successMap = new HashMap<>();
        for (DetokenizeResponseObject detokenizeResponseObject : detokenizeResponse.getSuccess()) {
            successMap.put(detokenizeResponseObject.getToken(), detokenizeResponseObject);
        }
        return successMap;
    }

    // Converts DetokenizeResponse error records into a map for quick lookup
    public static Map<String, ErrorRecord> geDetokenizeErrorsMap(DetokenizeResponse detokenizeResponse,
            List<String> tokens) {
        Map<String, ErrorRecord> errorsMap = new HashMap<>();
        for (ErrorRecord errorRecord : detokenizeResponse.getErrors()) {
            errorsMap.put(tokens.get(errorRecord.getIndex()), errorRecord);
        }
        return errorsMap;
    }

    // Replaces tokens in rows with actual data based on success and error maps
    public static List<Row> replaceTokensWithData(
            Map<String, ColumnMapping> schemaMappings,
            List<Row> batch,
            Map<String, DetokenizeResponseObject> successMap,
            Map<String, ErrorRecord> errorsMap) {

        List<Row> outputRows = new ArrayList<>();

        for (Row row : batch) {
            List<Object> rowData = new ArrayList<>();
            boolean hasFailure = false;

            for (String field : row.schema().fieldNames()) {
                ColumnMapping skyflowColumnMapping = schemaMappings.get(field);
                Object cell = row.getAs(field);

                if (skyflowColumnMapping != null) {
                    if (cell instanceof String) {
                        if (successMap.containsKey(cell)) {
                            // This field is expected to be detokenized
                            rowData.add(successMap.get(cell).getValue());
                        } else if (errorsMap.containsKey(cell)) {
                            // If detokenization failed for this token
                            rowData.add(cell); // keep original token
                            hasFailure = true;
                        } else {
                            // Token not present in either map — treat as failure
                            rowData.add(cell);
                            hasFailure = true;
                        }
                    } else {
                        // mapping is present but the cell is not a string, token should always be a
                        // string
                        hasFailure = true;
                        rowData.add(cell);
                    }
                } else {
                    // Not a token, no column mapping found, copying as is
                    rowData.add(cell);
                }
            }
            if (hasFailure) {
                rowData.add(Constants.STATUS_ERROR);
                rowData.add(Constants.DETOKENIZE_FAILED);
            } else {
                rowData.add(Constants.STATUS_OK);
                rowData.add(null);
            }

            outputRows.add(RowFactory.create(rowData.toArray()));
        }
        return outputRows;
    }

    // Merges retry results into the original success and error maps for
    // detokenization operations
    public static void mergeDetokenizeRetryResults(DetokenizeResponse detokenizeResponse, List<String> tokens,
            Map<String, DetokenizeResponseObject> successMap, Map<String, ErrorRecord> errorsMap) {
        Map<String, DetokenizeResponseObject> retrySuccessMap = getDetokenizeSuccessMap(detokenizeResponse);
        Map<String, ErrorRecord> retryErrorsMap = geDetokenizeErrorsMap(detokenizeResponse, tokens);
        for (DetokenizeResponseObject detokenizeResponseObject : retrySuccessMap.values()) {
            String token = detokenizeResponseObject.getToken();
            successMap.put(token, detokenizeResponseObject);
            errorsMap.remove(token);
        }
        for (ErrorRecord errorRecord : retryErrorsMap.values()) {
            errorsMap.put(tokens.get(errorRecord.getIndex()), errorRecord);
        }
        logger.fine(LOG_PREFIX + "Merged " + retrySuccessMap.size() + " success entries and " + retryErrorsMap.size()
                + " error entries.");
    }

    // implements exponential backoff with jitter
    public static void sleepWithExponentialBackoff(int currentRetry) {
        try {
            long delay = (long) (Constants.BASE_MILLI_SECONDS * Math.pow(2, currentRetry));
            long jitter = (long) (Math.random() * delay);
            long sleepTime = Math.min(delay + jitter, Constants.MAX_DELAY_MILLI_SECONDS);
            logger.fine(LOG_PREFIX + "Retry " + (currentRetry + 1) + " after " + sleepTime + " ms");
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // Restore interrupted status
        }
    }

    // Calculates no.of rows to be batched for a given batch size
    public static int calculateRowBatchSize(Map<String, ColumnMapping> schemaMappings,
            int targetRecordBatchSize) {
        int tokenizableColumnCount = schemaMappings.size();
        if (tokenizableColumnCount == 0) {
            return 0; // nothing to tokenize
        }
        // rows needed = targetRecords / tokenizableColumns
        return (int) Math.ceil((double) targetRecordBatchSize / tokenizableColumnCount);
    }
}
