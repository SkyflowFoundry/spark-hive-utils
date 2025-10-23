package com.skyflow.spark;

/**
 * Holds the Skyflow table/column pairing for a dataset field plus optional token group,
 * redaction strategy, and uniqueness flag used during upserts.
 */
public class ColumnMapping {
    private String tableName;
    private String columnName;
    private String tokenGroupName;
    private String redaction;
    private Boolean isUnique;

    public ColumnMapping(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public ColumnMapping(String tableName, String columnName, Boolean isUnique) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.isUnique = isUnique;
    }

    public ColumnMapping(String tableName, String columnName, String tokenGroupName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tokenGroupName = tokenGroupName;
    }

    public ColumnMapping(String tableName, String columnName, String tokenGroupName, Boolean isUnique) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tokenGroupName = tokenGroupName;
        this.isUnique = isUnique;
    }

    public ColumnMapping(String tableName, String columnName, String tokenGroupName, String redaction, Boolean isUnique) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tokenGroupName = tokenGroupName;
        this.redaction = redaction;
        this.isUnique = isUnique;
    }

    public ColumnMapping(String tableName, String columnName, String tokenGroupName, String redaction) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tokenGroupName = tokenGroupName;
        this.redaction = redaction;
    }

    @Override
    public String toString() {
        return tableName + "." + columnName + " (" + tokenGroupName + ")";
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTokenGroupName(String tokenGroupName) {
        this.tokenGroupName = tokenGroupName;
    }

    public void setRedaction(String redaction) {
        this.redaction = redaction;
    }

    public void setIsUnique(Boolean unique) {
        isUnique = unique;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTokenGroupName() {
        return tokenGroupName;
    }

    public String getRedaction() {
        return redaction;
    }

    public Boolean getIsUnique() {
        return isUnique;
    }
}
