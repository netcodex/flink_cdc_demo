package com.lizard.flink.cdc;

import java.io.Serializable;

/**
 * @author X
 * @version 1.0
 * @since 2023-05-20 22:19
 **/
public class LtsJobChangeInfo {
    private String schema;
    private String table;

    private String operation;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }
}
