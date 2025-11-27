Spark Integration
====================

This module packages reusable helpers that integrate Spark batch workloads with Skyflow’s bulk tokenization and detokenization APIs. It is intended for Spark 3.4+ jobs that need to protect or retrieve PII while operating on large datasets.

Overview
--------

* `com.skyflow.spark.VaultHelper` orchestrates Skyflow bulk insert (tokenization) and bulk detokenize calls with automatic batching, retry handling, and status tracking.
* `com.skyflow.spark.Helper` builds Skyflow requests from Spark `Dataset<Row>` batches, merges API responses, and performs value deduplication per table/column.
* `com.skyflow.spark.TableHelper` is a configuration holder for vault credentials, cluster ID, environment, and logging levels.
* `com.skyflow.spark.Constants` centralizes default column mappings for datasets, retryable status codes, and default batch sizes.

Prerequisites
-------------

* Java 8 (the module targets source/target 1.8).
* Maven 3.8+.
* Access to a Spark 3.4.x cluster with the Skyflow Java SDK (3.0.0-beta.6) compatible dependencies.
* Skyflow vault credentials stored as a JSON string or resolvable via your secret manager.

Building The Jar
----------------

```bash
cd spark
mvn clean package
```

The build produces `target/skyflow-spark-<version>.jar` (with dependencies shaded, excluding Spark/Scala) plus a dependency-reduced POM that can be used in downstream jobs.

Using `VaultHelper`
-------------------

```java
SparkSession spark = SparkSession.builder()
    .appName("tokenize-customer-profiles")
    .master("yarn")
    .getOrCreate();

Dataset<Row> data = spark.table("raw.customer_profiles");

TableHelper tableHelper = new TableHelper(
    "vault-url",
    "<vault-id>",
    "<credentials-json>",
    "customers",
    "<cluster-id>",
    Env.SANDBOX,
    Level.INFO,
    LogLevel.INFO
);

Properties props = new Properties();
props.put("columnMapping", "{ \"first_nm\": { \"tableName\": \"customers\", \"columnName\": \"first_name\" } }");

VaultHelper helper = new VaultHelper(spark, 1000, 3);
Dataset<Row> tokenized = helper.tokenize(tableHelper, data, props);

tokenized
    .filter(col(Constants.SKYFLOW_STATUS_CODE).equalTo(Constants.STATUS_ERROR))
    .show(false);
```

Key behaviours:

* Batch size is automatically adjusted (`Helper.calculateRowBatchSize`) based on tokenizable column count.
* Duplicate values per (table, column) are deduplicated within a batch to minimize API calls.
* Retry logic (`retryFailedRecords` / `retryFailedTokens`) replays only records with HTTP 429/5xx codes using exponential backoff and jitter.
* Output datasets include `skyflow_status_code` and `error` columns so downstream pipelines can act on failures.

Column Mappings
---------------

Mappings can be supplied in a `Properties` object under the key `columnMapping`. If omitted, the helper throws an exception. Column mapping is a required property.

Mapping format:

```json
{
  "raw_column": {
    "tableName": "vault_table",
    "columnName": "vault_column",
    "tokenGroupName": "optional_group",
    "redaction": "optional_redaction"
  }
}
```
