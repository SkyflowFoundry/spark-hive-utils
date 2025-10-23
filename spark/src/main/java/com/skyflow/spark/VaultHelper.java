package com.skyflow.spark;

import com.skyflow.Skyflow;
import com.skyflow.config.VaultConfig;
import com.skyflow.config.Credentials;
import com.skyflow.errors.SkyflowException;
import com.skyflow.vault.data.ErrorRecord;
import com.skyflow.vault.data.InsertResponse;
import com.skyflow.vault.data.InsertRequest;
import com.skyflow.vault.data.DetokenizeRequest;
import com.skyflow.vault.data.DetokenizeResponse;
import com.skyflow.vault.data.DetokenizeResponseObject;
import com.skyflow.vault.data.Success;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.skyflow.spark.Constants.*;
import static org.apache.spark.sql.functions.lit;

public class VaultHelper {

    public Skyflow skyflowClient; // Skyflow client instance
    private Integer insertBatchSize = INSERT_BATCH_SIZE;
    private Integer detokenizeBatchSize = DETOKENIZE_BATCH_SIZE;
    private Integer retryCount = 1;

    private final SparkSession sparkSession;// Spark session for data processing
    private static final Logger logger = LoggerUtil.getLogger(VaultHelper.class);

    public Skyflow.SkyflowClientBuilder getSkyflowBuilder() {
        return Skyflow.builder();
    }

    // Constructor initializes the helper and Spark session
    public VaultHelper(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    // Constructor initializes Spark session, sets insert and detokenize batch sizes
    public VaultHelper(SparkSession sparkSession, Integer insertBatchSize, Integer detokenizeBatchSize) {
        this.sparkSession = sparkSession;
        this.insertBatchSize = insertBatchSize;
        this.detokenizeBatchSize = detokenizeBatchSize;
    }

    // Constructor initializes Spark session, sets insert and detokenize batch sizes
    // and retry count
    public VaultHelper(SparkSession sparkSession, Integer insertBatchSize, Integer detokenizeBatchSize,
            Integer retryCount) {
        this.sparkSession = sparkSession;
        this.insertBatchSize = insertBatchSize;
        this.detokenizeBatchSize = detokenizeBatchSize;
        this.retryCount = retryCount;
    }

    // Initializes the Skyflow client with vault and cluster details
    public void initializeSkyflowClient(TableHelper tableHelper) throws SkyflowException {
        try {
            Credentials credentials = new Credentials();
            credentials.setCredentialsString(tableHelper.getVaultCredentials());
            VaultConfig vaultConfig = new VaultConfig();
            vaultConfig.setVaultId(tableHelper.getVaultId());
            vaultConfig.setClusterId(tableHelper.getClusterId());
            vaultConfig.setEnv(tableHelper.getEnv());
            vaultConfig.setCredentials(credentials);
            this.skyflowClient = getSkyflowBuilder()
                    .setLogLevel(tableHelper.getJvmLogLevel())
                    .addVaultConfig(vaultConfig)
                    .build();
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(tableHelper.getLogLevel());
            consoleHandler.setFormatter(new Formatter() {
                @Override
                public synchronized String format(LogRecord record) {
                    return record.getLevel() + ": " + record.getMessage() + System.lineSeparator();
                }
            });
            logger.addHandler(consoleHandler);
            logger.setLevel(tableHelper.getLogLevel());
            logger.info(LOG_PREFIX + "Skyflow client initialized successfully");
        } catch (Exception e) {
            logger.severe(LOG_PREFIX + "Failed to initialize Skyflow client: " + e.getMessage());
            throw new SkyflowException("Failed to initialize Skyflow client: " + e.getMessage(), e);
        }
    }

    // Tokenizes data by sending it to Skyflow vault
    public Dataset<Row> tokenize(TableHelper tableHelper, Dataset<Row> dataToIngest, Properties properties)
            throws SkyflowException {
        try {
            logger.info(LOG_PREFIX + "Starting tokenization");
            // Initialize Skyflow client
            initializeSkyflowClient(tableHelper);

            // Define schema with additional columns for status and error
            StructType schema = dataToIngest.schema();
            StructType newSchema = schema
                    .add(SKYFLOW_STATUS_CODE, DataTypes.StringType, true)
                    .add(ERROR, DataTypes.StringType, true);

            List<Row> outputRows = new ArrayList<>();
            Map<String, ColumnMapping> schemaMappings = Helper.configureColumnMappings(schema, properties);
            if (schemaMappings.isEmpty()) {
                logger.warning(
                        LOG_PREFIX + "No tokenizable columns found for provided configuration. Skipping tokenization.");
                return dataToIngest
                        .withColumn(SKYFLOW_STATUS_CODE, lit(STATUS_OK))
                        .withColumn(ERROR, lit(null));
            }
            logger.info(LOG_PREFIX + "No.of tokenizable columns: " + schemaMappings.size());
            insertBatchSize = Helper.calculateRowBatchSize(schemaMappings, insertBatchSize);
            logger.info(LOG_PREFIX + "Using batch size: " + insertBatchSize + " Retry count: " + retryCount);

            int batchNumber = 1;
            // Process data in batches
            for (List<Row> batch : Helper.getBatches(dataToIngest, insertBatchSize)) {

                // Construct and send insert request
                InsertRequest insertRequest = Helper.constructInsertRequest(schemaMappings, batch);
                logger.info(LOG_PREFIX + "Processing batch #" + batchNumber + ", No.of records: "
                        + insertRequest.getRecords().size());
                InsertResponse insertResponse = skyflowClient.vault().bulkInsert(insertRequest);

                // Process success and error responses
                Map<Object, Success> successMap = Helper.getInsertSuccessMap(insertResponse,
                        insertRequest.getRecords());
                Map<Object, ErrorRecord> errorsMap = Helper.getInsertErrorsMap(insertResponse,
                        insertRequest.getRecords());
                logger.fine(LOG_PREFIX + "Success count: " + successMap.size() + " Error count: " + errorsMap.size());

                // Retry failed records if necessary
                if (insertResponse.getSummary().getTotalFailed() > 0) {
                    retryFailedRecords(insertRequest, successMap, errorsMap);
                }

                // Replace data with tokens and add to output rows
                List<Row> batchOutputRows = Helper.replaceDataWithTokens(schemaMappings, batch, successMap, errorsMap);
                outputRows.addAll(batchOutputRows);
                batchNumber++;
            }

            // Create a DataFrame with the new schema
            Dataset<Row> df = sparkSession.createDataFrame(outputRows, newSchema);
            logger.info(LOG_PREFIX + PROCESSED_ALL_BATCHES + " Tokenization completed");
            return df;
        } catch (Exception e) {
            throw new SkyflowException(e);
        }
    }

    // Detokenizes data by retrieving original values from Skyflow vault
    public Dataset<Row> detokenize(TableHelper tableHelper, Dataset<Row> tokenizedData, Properties properties)
            throws SkyflowException {
        try {
            logger.info(LOG_PREFIX + "Starting detokenization");

            // Initialize Skyflow client
            initializeSkyflowClient(tableHelper);

            // Define schema with additional columns for status and error
            StructType schema = tokenizedData.schema();

            StructType newSchema = schema
                    .add(SKYFLOW_STATUS_CODE, DataTypes.StringType, true)
                    .add(ERROR, DataTypes.StringType, true);

            List<Row> outputRows = new ArrayList<>();
            Map<String, ColumnMapping> schemaMappings = Helper.configureColumnMappings(schema, properties);
            if (schemaMappings.isEmpty()) {
                logger.warning(LOG_PREFIX
                        + "No detokenizable columns found for provided configuration. Skipping detokenization.");
                return tokenizedData
                        .withColumn(SKYFLOW_STATUS_CODE, lit(STATUS_OK))
                        .withColumn(ERROR, lit(null));
            }
            logger.info(LOG_PREFIX + "No.of detokenizable columns: " + schemaMappings.size());
            detokenizeBatchSize = Helper.calculateRowBatchSize(schemaMappings, detokenizeBatchSize);
            logger.info(LOG_PREFIX + "Using batch size: " + detokenizeBatchSize + " Retry count: " + retryCount);

            int batchNumber = 1;

            // Process data in batches
            for (List<Row> batch : Helper.getBatches(tokenizedData, detokenizeBatchSize)) {
                // Construct and send detokenize request
                DetokenizeRequest detokenizeRequest = Helper.constructDetokenizeRequest(schemaMappings, batch);
                logger.info(LOG_PREFIX + "Processing batch #" + batchNumber + ", No.of records: "
                        + detokenizeRequest.getTokens().size());
                DetokenizeResponse detokenizeResponse = skyflowClient.vault().bulkDetokenize(detokenizeRequest);

                // Process success and error responses
                Map<String, DetokenizeResponseObject> successMap = Helper.getDetokenizeSuccessMap(detokenizeResponse);
                Map<String, ErrorRecord> errorsMap = Helper.geDetokenizeErrorsMap(detokenizeResponse,
                        detokenizeRequest.getTokens());
                logger.fine(LOG_PREFIX + "Success count: " + successMap.size() + " Error count: " + errorsMap.size());
                // Retry failed tokens if necessary
                if (detokenizeResponse.getSummary().getTotalFailed() > 0) {
                    retryFailedTokens(detokenizeRequest, successMap, errorsMap);
                }
                // Replace tokens with data and add to output rows
                List<Row> batchOutputRows = Helper.replaceTokensWithData(schemaMappings, batch, successMap, errorsMap);
                outputRows.addAll(batchOutputRows);
                batchNumber++;
            }
            // Create a DataFrame with the new schema
            Dataset<Row> df = sparkSession.createDataFrame(outputRows, newSchema);
            logger.info(LOG_PREFIX + PROCESSED_ALL_BATCHES + " Detokenization completed");
            return df;
        } catch (Exception e) {
            throw new SkyflowException(e);
        }
    }

    // Helper method to retry failed records with exponential backoff and jitter
    private void retryFailedRecords(InsertRequest request,
            Map<Object, Success> successMap,
            Map<Object, ErrorRecord> errorsMap) throws SkyflowException {
        int currentRetry = 0;
        while (!errorsMap.isEmpty() && currentRetry < retryCount) {
            InsertRequest retryRequest = Helper.constructInsertRetryRequest(request.getRecords(), errorsMap);
            if (retryRequest.getRecords().isEmpty()) {
                logger.fine(LOG_PREFIX + NO_RETRIES_NEEDED_PROCEEDING);
                break;
            } else {
                logger.fine(
                        LOG_PREFIX + "Retrying " + retryRequest.getRecords().size() + " failed records. Attempt: "
                                + (currentRetry + 1));
                InsertResponse retryResponse = skyflowClient.vault().bulkInsert(retryRequest);
                Helper.sleepWithExponentialBackoff(currentRetry);
                Helper.mergeInsertRetryResults(retryRequest.getRecords(), retryResponse, successMap, errorsMap);
                logger.fine(LOG_PREFIX + "After retry, Success count: " + successMap.size() + " Error count: "
                        + errorsMap.size());
                currentRetry++;
            }
        }
    }

    // Helper method to retry failed tokens with exponential backoff and jitter
    private void retryFailedTokens(DetokenizeRequest detokenizeRequest,
            Map<String, DetokenizeResponseObject> successMap, Map<String, ErrorRecord> errorsMap)
            throws SkyflowException {
        int currentRetry = 0;
        while (!errorsMap.isEmpty() && currentRetry < retryCount) {
            List<String> retryableTokens = errorsMap.entrySet().stream()
                    .filter(entry -> RETRYABLE_ERROR_CODES.contains(entry.getValue().getCode()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            if (retryableTokens.isEmpty()) {
                logger.fine(LOG_PREFIX + NO_RETRIES_NEEDED_PROCEEDING);
                break;
            } else {
                logger.fine(
                        LOG_PREFIX + "Retrying " + retryableTokens.size() + " failed tokens. Attempt: "
                                + (currentRetry + 1));
                Helper.sleepWithExponentialBackoff(currentRetry);
                DetokenizeResponse retryResponse = skyflowClient.vault()
                        .bulkDetokenize(DetokenizeRequest.builder().tokens(retryableTokens)
                                .tokenGroupRedactions(detokenizeRequest.getTokenGroupRedactions()).build());
                Helper.mergeDetokenizeRetryResults(retryResponse, retryableTokens, successMap, errorsMap);
                logger.fine(LOG_PREFIX + "After retry, Success count: " + successMap.size() + " Error count: "
                        + errorsMap.size());
                currentRetry++;
            }
        }
    }
}
