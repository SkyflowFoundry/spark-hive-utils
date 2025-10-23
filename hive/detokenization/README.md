Hive Detokenize UDF
===============================

This module provides a Hive 4.x compatible user-defined function that converts Skyflow tokens back into their original values inside SQL queries. It also includes utilities to validate session configuration and initialize the Skyflow Java SDK.

Components
----------

* `com.skyflow.hive.DetokenizeUDF` – Implements `GenericUDF` and exposes `detokenize(token)` to Hive.
* `com.skyflow.hive.utils.Helper` – Reads Skyflow configuration from Hive session state and initializes the SDK.
* `com.skyflow.hive.utils.Validations` – Validates JSON payloads and file-backed configuration.
* `com.skyflow.hive.logs` / `com.skyflow.hive.errors` – Structured logging and error messages for diagnostics.

Prerequisites
-------------

* Java 8.
* Maven 3.8+.
* Hive 4.0.0 (matching the dependency in `pom.xml`).
* Hadoop libraries aligned with Hive (`3.3.6` in this project).
* Skyflow access credentials that the Hive session can read.

Building
--------

```bash
cd hive/detokenization
mvn clean package
```

The build generates a shaded jar: `target/skyflow-hive-1.0.0.jar`.

Deploying
---------

1. Copy the jar to a location accessible by Hive (HDFS, S3, or local path).
2. In your Hive session:

```sql
ADD JAR hdfs:///libs/skyflow-hive-1.0.0.jar;
CREATE TEMPORARY FUNCTION detokenize AS 'com.skyflow.hive.DetokenizeUDF';
```

Configuring Skyflow Access
--------------------------

Before invoking the UDF, configure one of the following session variables:

```sql
-- Inline JSON configuration
SET skyflow.config={
  "vaultId": "<vault-id>",
  "clusterId": "<cluster-id>",
  "env": "SANDBOX",
  "credentials": "{\"clientID\":\"...\",\"privateKey\":\"...\"}"
};

-- Or point to a JSON file
SET skyflow.config.file=/path/to/skyflow-config.json;
```

Constraints:

* Either `skyflow.config` **or** `skyflow.config.file` must be provided (not both).
* `vaultId` and `clusterId` are mandatory.
* Provide **exactly one** of `credentials` (inline JSON string) or `filePath` (inside the JSON file).
* `env` is optional; when absent, the Skyflow SDK defaults to `PROD`.

Using The UDF
-------------

```sql
SELECT
  customer_id,
  detokenize(card_token) AS card_number
FROM analytics.tokenized_cards
WHERE country = 'US';
```

Null tokens return `NULL`. Errors during Skyflow calls raise a `HiveException` with the underlying message.
