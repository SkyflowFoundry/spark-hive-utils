Sample Hive Standalone Tokenizer Application
=====================

This module is a **reference application** that shows how to read rows from a Hive table, apply Skyflow tokenization via the shared Spark helper, and write the transformed data back to Hive.

What It Demonstrates
--------------------

- Creating a Hive-enabled `SparkSession`.
- Loading configuration from environment variables (with optional `.env` overrides) using `TokenizeHiveConfig`.
- Invoking `VaultHelper.tokenize` to replace sensitive fields with Skyflow tokens.
- Writing the resulting `Dataset<Row>` back to Hive, dropping diagnostic columns.

Key Classes
-----------

- `TokenizeHiveTable`: the minimal Spark `main` class that wires everything together.
- `TokenizeHiveConfig`: reads and validates configuration inputs for the sample.
- `src/test/java`: JUnit tests that capture the expected behaviour of the config loader.

Configuration Inputs
--------------------

The sample expects environment variables that describe the Hive tables and Skyflow credentials.

- `TOKENIZER_SOURCE_TABLE`, `TOKENIZER_DEST_TABLE`: source and destination Hive tables.
- `TOKENIZER_FS_URI`, `TOKENIZER_METASTORE_URI`: connection details for Hive and HDFS.
- `TOKENIZER_SPARK_APP_NAME`, `TOKENIZER_SPARK_MASTER`: basic Spark settings for your sandbox.
- `SKYFLOW_COLUMN_MAPPING`: JSON describing which columns should be tokenized.
- `SKYFLOW_VAULT_ID`, `SKYFLOW_CREDENTIAL_JSON`, `SKYFLOW_TOKEN_TABLE`, `SKYFLOW_CLUSTER_ID`: vault-specific parameters consumed by `VaultHelper`.
- Optional partition filters (`TOKENIZER_PARTITION_SPEC` or column/value pair) to limit the dataset while testing.
