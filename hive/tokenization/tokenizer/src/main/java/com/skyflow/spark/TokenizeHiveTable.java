package com.skyflow.spark;

import com.skyflow.errors.SkyflowException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Column;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class TokenizeHiveTable {

    public static void main(String[] args) throws SkyflowException {
        TokenizeHiveConfig config = TokenizeHiveConfig.load();
        SparkSession spark = SparkSession.builder()
                .appName(config.getSparkAppName())
                .master(config.getSparkMaster())
                .enableHiveSupport()
                .config("spark.hadoop.fs.defaultFS", config.getFsUri())
                .config("hive.metastore.uris", config.getMetastoreUri())
                .config("spark.hadoop.dfs.client.use.datanode.hostname",
                        Boolean.toString(config.useDatanodeHostname()))
                .getOrCreate();

        Dataset<Row> srcDf = applyPartitionFilter(spark, spark.table(config.getSourceTable()), config);
        srcDf.show();
        Dataset<Row> tokDf = sparkTokenize(spark, srcDf, config);

        if (!tableExists(spark, config.getDestinationTable())) {
            tokDf.write().mode(SaveMode.Overwrite).format("hive").saveAsTable(config.getDestinationTable());
        } else {
            tokDf.write().mode(SaveMode.Append).format("hive").saveAsTable(config.getDestinationTable());
        }
        spark.stop();
    }

    static Dataset<Row> applyPartitionFilter(SparkSession spark, Dataset<Row> dataset, TokenizeHiveConfig config) {
        boolean hasPartitionFilter = config.getPartitionSpec().isPresent()
                || (config.getPartitionColumn().isPresent() && config.getPartitionValue().isPresent());
        if (!hasPartitionFilter) {
            return dataset;
        }
        if (!tableHasPartitions(spark, config.getSourceTable())) {
            return dataset;
        }
        if (config.getPartitionSpec().isPresent()) {
            return dataset.where(config.getPartitionSpec().get());
        }
        return dataset.where(
                col(config.getPartitionColumn().orElseThrow())
                        .equalTo(config.getPartitionValue().orElseThrow())
        );
    }

    private static Dataset<Row> sparkTokenize(
        SparkSession spark, Dataset<Row> df, TokenizeHiveConfig config) throws SkyflowException {
        TableHelper tableHelper = new TableHelper(
                config.getVaultId(),
                config.getCredentialJson(),
                config.getTokenTable(),
                config.getClusterId(),
                config.getEnv(),
                config.getJavaLogLevel(),
                config.getSkyflowLogLevel()
        );

        VaultHelper helper = new VaultHelper(
                spark,
                config.getInsertBatchSize(),
                config.getDetokenizeBatchSize(),
                config.getRetryCount()
        );
        df.show(false);
        Dataset<Row> tokenDF = helper.tokenize(tableHelper, df, config.buildTokenizationProperties());
        tokenDF = tokenDF.drop("error", "skyflow_status_code");
        tokenDF.show(false);
        return tokenDF;
    }

    private static boolean tableExists(SparkSession spark, String tableName) {
        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            return spark.catalog().tableExists(parts[0], parts[1]);
        }
        return spark.catalog().tableExists(tableName);
    }

    private static boolean tableHasPartitions(SparkSession spark, String tableName) {
        try {
            Dataset<Column> columns;
            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.", 2);
                columns = spark.catalog().listColumns(parts[0], parts[1]);
            } else {
                columns = spark.catalog().listColumns(tableName);
            }
            List<Column> columnList = columns.collectAsList();
            for (Column column : columnList) {
                if (column.isPartition()) {
                    return true;
                }
            }
            return false;
        } catch (AnalysisException e) {
            throw new IllegalArgumentException("Unable to inspect partitions for table " + tableName, e);
        }
    }
}
