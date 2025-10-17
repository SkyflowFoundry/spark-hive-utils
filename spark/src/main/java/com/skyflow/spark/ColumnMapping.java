package com.skyflow.spark;

public class ColumnMapping {
    private String tableName;
    private String columnName;
    private String tokenGroupName;
    private String redaction;

    public ColumnMapping(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public ColumnMapping(String tableName, String columnName, String tokenGroupName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tokenGroupName = tokenGroupName;
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
}