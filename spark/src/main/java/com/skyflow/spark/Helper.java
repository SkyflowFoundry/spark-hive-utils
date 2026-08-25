package com.skyflow.spark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.skyflow.errors.SkyflowException;
import com.skyflow.vault.data.ErrorRecord;
import com.skyflow.vault.data.BulkInsertRequest;
import com.skyflow.vault.data.BulkInsertRequestRecord;
import com.skyflow.vault.data.BulkInsertResponse;
import com.skyflow.vault.data.BulkInsertResponseRecord;
import com.skyflow.vault.data.InsertRequestRecord;
import com.skyflow.vault.data.InsertResponseRecord;
import com.skyflow.vault.data.BulkDetokenizeRequest;
import com.skyflow.vault.data.BulkDetokenizeResponse;
import com.skyflow.vault.data.BulkDetokenizeResponseRecord;
import com.skyflow.vault.data.TokenGroupRedactions;
import com.skyflow.vault.data.UpsertOptions;
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

    public static Map<String, ColumnMapping> configureColumnMappings(StructType schema, Properties properties) throws SkyflowException {
        if (properties == null) {
            throw new SkyflowException(
                    "Invalid properties, properties passed are either null or empty");
        }

        String columnMappingConfig = properties.getProperty(COLUMN_MAPPING);
        if (columnMappingConfig == null || columnMappingConfig.trim().isEmpty()) {
            throw new SkyflowException(
                    "Column mapping config is empty or null");
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
                boolean isUnique = true;
                String isColumnUnique = mappingDetails.get(UNIQUE);
                if (!isBlank(isColumnUnique)) {
                    isUnique = Boolean.parseBoolean(isColumnUnique);
                }
                columnMappingsMap.put(datasetColumn, buildMapping(tableName, columnName, tokenGroupName, redaction, isUnique));
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
            String redaction, Boolean isUnique) {
        if (redaction != null) {
            return new ColumnMapping(tableName, columnName, tokenGroupName, redaction, isUnique);
        }
        if (tokenGroupName != null) {
            return new ColumnMapping(tableName, columnName, tokenGroupName, isUnique);
        }
        return new ColumnMapping(tableName, columnName, isUnique);
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

    // Constructs a BulkInsertRequest object from a batch of rows and column mappings
    public static BulkInsertRequest constructInsertRequest(Map<String, ColumnMapping> schemaMappings, List<Row> batch) {
        ArrayList<InsertRequestRecord> records = new ArrayList<>();

        // Track seen values per table + vault column
        Map<String, Map<String, Set<Object>>> valuesDedupMap = new HashMap<>();
        for (Row row : batch) {
            List<InsertRequestRecord> rowRecords = constructInsertRecordsForRow(row, valuesDedupMap, schemaMappings);
            records.addAll(rowRecords);
        }
        return BulkInsertRequest.builder()
                .records(records)
                .build();
    }

    private static List<InsertRequestRecord> constructInsertRecordsForRow(Row row,
            Map<String, Map<String, Set<Object>>> seenValues, Map<String, ColumnMapping> schemaMappings) {
        List<InsertRequestRecord> records = new ArrayList<>();
        for (Map.Entry<String, ColumnMapping> entry : schemaMappings.entrySet()) {

            String datasetColumn = entry.getKey();
            ColumnMapping skyflowColumnMapping = entry.getValue();
            String tableName = skyflowColumnMapping.getTableName();
            String vaultColumn = skyflowColumnMapping.getColumnName();

            Object value = row.getAs(datasetColumn);

            if (value == null) {
                continue;
            }

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
            if(skyflowColumnMapping.getIsUnique() != null && skyflowColumnMapping.getIsUnique() == true) {
                records.add(BulkInsertRequestRecord.builder().data(record).tableName(skyflowColumnMapping.getTableName())
                        .upsert(UpsertOptions.builder()
                                .uniqueColumns(Collections.singletonList(skyflowColumnMapping.getColumnName()))
                                .build())
                        .build());
            } else {
                records.add(BulkInsertRequestRecord.builder().data(record).tableName(skyflowColumnMapping.getTableName()).build());
            }
        }
        return records;
    }

    // Converts BulkInsertResponse records into a success map for quick lookup
    public static Map<Object, BulkInsertResponseRecord> getInsertSuccessMap(BulkInsertResponse insertResponse,
            List<InsertRequestRecord> records) {
        Map<Object, BulkInsertResponseRecord> successMap = new HashMap<>();
        for (BulkInsertResponseRecord responseRecord : insertResponse.getRecords()) {
            if (responseRecord.getError() != null) {
                continue;
            }
            InsertRequestRecord record = records.get(responseRecord.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTableName(), value);
            successMap.put(key, responseRecord);
        }
        return successMap;
    }

    // Converts BulkInsertResponse records into an error map for quick lookup
    public static Map<Object, ErrorRecord> getInsertErrorsMap(BulkInsertResponse insertResponse,
            List<InsertRequestRecord> records) {
        Map<Object, ErrorRecord> errorsMap = new HashMap<>();
        for (BulkInsertResponseRecord responseRecord : insertResponse.getRecords()) {
            if (responseRecord.getError() == null) {
                continue;
            }
            InsertRequestRecord record = records.get(responseRecord.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTableName(), value);
            errorsMap.put(key, new ErrorRecord(responseRecord.getIndex(), responseRecord.getError(), responseRecord.getHttpCode()));
        }
        return errorsMap;
    }

    // Replaces data in rows with tokens based on success and error maps
    public static List<Row> replaceDataWithTokens(Map<String, ColumnMapping> schemaMappings, List<Row> batch,
            Map<Object, BulkInsertResponseRecord> successMap, Map<Object, ErrorRecord> errorsMap) {
        List<Row> outputRows = new ArrayList<>();

        for (Row row : batch) {
            List<Object> rowData = new ArrayList<>();
            boolean hasFailure = false;
            ErrorRecord errorRecord = new ErrorRecord(0, INSERT_FAILED, 500);
            for (String field : row.schema().fieldNames()) {
                ColumnMapping skyflowColumnMapping = schemaMappings.get(field);
                Object value = row.getAs(field);
                if (skyflowColumnMapping != null && value != null) {
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
                        errorRecord = errorsMap.get(key);
                        break;
                    } else {
                        // Token not present in either map — treat as failure, failing row
                        hasFailure = true;
                        break;
                    }
                } else {
                    // Not a tokenizable value, no column mapping found or null value copying as is
                    rowData.add(value);
                }
            }
            if (hasFailure) {
                rowData = populateErrorRow(row, errorRecord);
            } else {
                rowData.add(Constants.STATUS_OK);
                rowData.add(null);
            }
            outputRows.add(RowFactory.create(rowData.toArray()));
        }
        return outputRows;
    }

    // Gets the token for a given successful insert record and mapping
    public static String getToken(InsertResponseRecord successRecord, ColumnMapping skyflowColumnMapping) {
        if (successRecord.getTokens() == null) {
            return null;
        }
        List<Token> tokenObj = successRecord.getTokens().get(skyflowColumnMapping.getColumnName());
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
    private static List<Object> populateErrorRow(Row in, ErrorRecord error) {
        List<Object> rowData = copyRowData(in);
        rowData.add(String.valueOf(error.getCode()));
        rowData.add(error.getError());
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
    public static BulkInsertRequest constructInsertRetryRequest(
            List<InsertRequestRecord> allRecords,
            Map<Object, ErrorRecord> errorsMap) {
        ArrayList<InsertRequestRecord> retryRecords = new ArrayList<>();
        for (ErrorRecord errorRecord : errorsMap.values()) {
            if (Constants.RETRYABLE_ERROR_CODES.contains(errorRecord.getCode())) {
                InsertRequestRecord originalRecord = allRecords.get(errorRecord.getIndex());
                retryRecords.add(originalRecord);
            }
        }
        return BulkInsertRequest.builder()
                .records(retryRecords)
                .build();
    }

    // Merges retry results into the original success and error maps for insert
    public static void mergeInsertRetryResults(
            List<InsertRequestRecord> records,
            BulkInsertResponse retryResponse,
            Map<Object, BulkInsertResponseRecord> successMap,
            Map<Object, ErrorRecord> errorsMap) {
        ArrayList<InsertRequestRecord> retryRecords = new ArrayList<>(records);
        Map<Object, BulkInsertResponseRecord> retrySuccessMap = getInsertSuccessMap(
                retryResponse, retryRecords);
        Map<Object, ErrorRecord> retryErrorsMap = getInsertErrorsMap(
                retryResponse, retryRecords);
        for (BulkInsertResponseRecord success : retrySuccessMap.values()) {
            InsertRequestRecord record = retryRecords.get(success.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTableName(), value);
            successMap.put(key, success);
            errorsMap.remove(key);
        }
        for (ErrorRecord errorRecord : retryErrorsMap.values()) {
            InsertRequestRecord record = retryRecords.get(errorRecord.getIndex());
            // Get the only value from the record
            Object value = record.getData().values().iterator().next();
            // key also includes table name, as we are deduping per table
            String key = concatWithUnderscore(record.getTableName(), value);
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
    public static BulkDetokenizeRequest constructDetokenizeRequest(Map<String, ColumnMapping> schemaMappings,
            List<Row> batch) {
        Set<String> tokens = new HashSet<>();
        for (Row row : batch) {
            for (Map.Entry<String, ColumnMapping> mapping : schemaMappings.entrySet()) {
                if (row.getAs(mapping.getKey()) == null) {
                    continue;
                }
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
        return BulkDetokenizeRequest.builder().tokens(tokensList).tokenGroupRedactions(tokenGroupRedactions).build();
    }

    // Converts BulkDetokenizeResponse records into a success map for quick lookup
    public static Map<String, BulkDetokenizeResponseRecord> getDetokenizeSuccessMap(BulkDetokenizeResponse detokenizeResponse) {
        Map<String, BulkDetokenizeResponseRecord> successMap = new HashMap<>();
        for (BulkDetokenizeResponseRecord record : detokenizeResponse.getRecords()) {
            if (record.getError() == null) {
                successMap.put(record.getToken(), record);
            }
        }
        return successMap;
    }

    // Converts BulkDetokenizeResponse records into an error map for quick lookup
    public static Map<String, ErrorRecord> getDetokenizeErrorsMap(BulkDetokenizeResponse detokenizeResponse,
            List<String> tokens) {
        Map<String, ErrorRecord> errorsMap = new HashMap<>();
        for (BulkDetokenizeResponseRecord record : detokenizeResponse.getRecords()) {
            if (record.getError() != null) {
                errorsMap.put(tokens.get(record.getIndex()), new ErrorRecord(record.getIndex(), record.getError(), record.getHttpCode()));
            }
        }
        return errorsMap;
    }

    // Replaces tokens in rows with actual data based on success and error maps
    public static List<Row> replaceTokensWithData(
            Map<String, ColumnMapping> schemaMappings,
            List<Row> batch,
            Map<String, BulkDetokenizeResponseRecord> successMap,
            Map<String, ErrorRecord> errorsMap) {

        List<Row> outputRows = new ArrayList<>();

        for (Row row : batch) {
            List<Object> rowData = new ArrayList<>();
            boolean hasFailure = false;
            ErrorRecord errorRecord = new ErrorRecord(0, DETOKENIZE_FAILED, 500);
            for (String field : row.schema().fieldNames()) {
                ColumnMapping skyflowColumnMapping = schemaMappings.get(field);
                Object cell = row.getAs(field);

                if (skyflowColumnMapping != null && cell != null) {
                    if (cell instanceof String) {
                        if (successMap.containsKey(cell)) {
                            // This field is expected to be detokenized
                            rowData.add(successMap.get(cell).getValue());
                        } else if (errorsMap.containsKey(cell)) {
                            // If detokenization failed for this token
                            rowData.add(cell); // keep original token
                            errorRecord = errorsMap.get(cell);
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
                    // Not a token, no column mapping found or null value copying as is
                    rowData.add(cell);
                }
            }
            if (hasFailure) {
                rowData.add(String.valueOf(errorRecord.getCode()));
                rowData.add(errorRecord.getError());
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
    public static void mergeDetokenizeRetryResults(BulkDetokenizeResponse detokenizeResponse, List<String> tokens,
            Map<String, BulkDetokenizeResponseRecord> successMap, Map<String, ErrorRecord> errorsMap) {
        Map<String, BulkDetokenizeResponseRecord> retrySuccessMap = getDetokenizeSuccessMap(detokenizeResponse);
        Map<String, ErrorRecord> retryErrorsMap = getDetokenizeErrorsMap(detokenizeResponse, tokens);
        for (BulkDetokenizeResponseRecord record : retrySuccessMap.values()) {
            String token = record.getToken();
            successMap.put(token, record);
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
