# Spark and Hive Integrations

Skyflow helpers in this repository cover batch tokenization and detokenization workflows across Spark and Hive. The modules ship reusable libraries and jobs rather than runnable deployments, so teams can wire Skyflow into their own pipelines.

## Repository Modules

- `spark/` – Spark helper library for bulk tokenization and detokenization.
- `hive/tokenization/` – Spark entry point that tokenizes Hive tables using the helper library.
- `hive/detokenization/` – Hive UDF that resolves Skyflow tokens inside SQL.

## Prerequisites

- Java 8 for `spark/` and `hive/detokenization/`, Java 11 for `hive/tokenization/`.
- Maven 3.8+.
- Spark 3.4+ and Hive 4.x (or compatible) runtimes.
- Skyflow vault credentials with access to the target tables/token groups.

---

## Spark Helper Library (`spark/`)

### Capabilities

- Wraps the Skyflow Java SDK so Spark jobs can batch tokenization/detokenization on `Dataset<Row>` inputs.
- Automatically sizes row batches from the desired record batch size and number of tokenizable columns.
- De-duplicates values per Skyflow table/column to minimize API calls.
- Retries only HTTP 429/5xx responses with exponential backoff and jitter.
- Emits per-row status columns (`skyflow_status_code`, `error`) so downstream jobs can monitor failures.

### Building

```sh
cd spark
mvn clean package
```

The shaded JAR (`target/skyflow-spark-<version>.jar`) contains the helper library and its API dependencies.

### Usage

1. Create a `TableHelper` with vault ID, credentials JSON, token table, cluster ID, and log levels.
2. Prepare `Properties` that include a `columnMapping` JSON payload (required).
3. Instantiate `VaultHelper`, providing optional overrides for insert/detokenize batch sizes and retry count.
4. Call `tokenize(...)` or `detokenize(...)` with the dataset and mapping properties. The helper initializes the Skyflow SDK on demand.
5. Inspect the resulting dataset for the appended status/error columns before writing to your sink.

### Column Mapping Contract

Column mappings must be supplied through the `columnMapping` property; the helper no longer ships built-in defaults. Each dataset field that should be tokenized requires an entry:

```json
{
  "email": {
    "tableName": "customers",
    "columnName": "email",
    "tokenGroupName": "deterministic_customers",
    "redaction": "DEFAULT",
    "unique": "true"
  }
}
```

- `tableName` and `columnName` are mandatory.
- `tokenGroupName` and `redaction` are optional when multiple token groups or redaction strategies are in play.
- Set `unique` to `"true"` to force upsert semantics on the Skyflow table for that field; omit or use `"false"` for insert-only behavior.

### Batchng and Retry

`VaultHelper` constructors accept `(insertBatchSize, detokenizeBatchSize, retryCount)`. Batch sizing is recalculated internally so that the number of Skyflow records per request does not exceed your target. Retries stop after `retryCount` attempts or when only non-retryable errors remain.

---

## Hive Tokenization using Spark (`hive/tokenization/`)

`TokenizeHiveTable` wraps the Spark helper so Hive datasets can be tokenized with consistent batching and retry logic.

- Configuration is sourced from environment variables merged with an optional `.env` file. Runtime variables take precedence.
- Required keys mirror the Spark helper: source/destination tables, Hive metastore and filesystem URIs, Spark app/master details, and the JSON `SKYFLOW_COLUMN_MAPPING`.
- Optional keys control partition pruning (`TOKENIZER_PARTITION_SPEC` or `TOKENIZER_PARTITION_COLUMN`/`TOKENIZER_PARTITION_VALUE`) and logging (`SKYFLOW_SPARK_LOG_LEVEL`, `SKYFLOW_SDK_LOG_LEVEL`).
- The job constructs its own `TableHelper` and `VaultHelper` using the provided batch-size and retry settings, runs tokenization, and drops the helper status columns before writing to the destination table.

---

## Hive Detokenization UDF (`hive/detokenization/`)

The `detokenize(token)` UDF resolves Skyflow tokens during Hive query execution.

- Session configuration must supply either `skyflow.config` (inline JSON) **or** `skyflow.config.file` (path to JSON). The helper validates that atleast one source is provided. If both `skyflow.config` and `skyflow.config.file` are provided, `skyflow.config` is prioritised.
- Configuration JSON expects `vaultId`, `clusterId`, optional `env`, and either `credentials` (inline JSON string) or `filePath` pointing to credentials on disk.
- The helper initializes the Skyflow SDK once per session and caches the client for subsequent UDF calls.

---