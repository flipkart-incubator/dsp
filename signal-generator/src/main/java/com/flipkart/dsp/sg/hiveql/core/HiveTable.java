package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.query.SelectQuery;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.LinkedList;
import java.util.List;

/**
 */

@Builder
@AllArgsConstructor
public class HiveTable implements Table {
    private Long refreshId;
    private final String db;
    private String delimiter;
    private final String name;
    private SelectQuery subQuery;
    private final LinkedList<Column> columns;

    public Long getRefreshId() {
        return refreshId;
    }

    public void setRefreshId(Long refreshId) {
        this.refreshId = refreshId;
    }

    @Override
    public String getDbName() {
        return db;
    }

    @Override
    public String getTableName() {
        return name;
    }

    @Override
    public List<? extends Column> getColumns() {
        return columns;
    }

    @Override
    public boolean containsColumn(String columnName) {
        return columns.stream().anyMatch(column -> column.getColumnName().equals(columnName));
    }

    @Override
    public boolean containsColumn(Column column) {
        return columns.stream().anyMatch(column1 -> column1.equals(column));
    }

    @Override
    public Column getColumn(String columnName) {
        for (Column column : columns) {
            if (column.getColumnName().equals(columnName)) return column;
        }
        return null;
    }

    @Override
    public SelectQuery getSubQuery() {
        return subQuery;
    }

    @Override
    public void setSubQuery(SelectQuery subQuery) {
        this.subQuery = subQuery;
    }

    @Override
    public boolean hasSubQuery() {
        return subQuery != null;
    }

    @Override
    public String getDelimiter() {
        return (this.delimiter == null) ? "," : this.delimiter;
    }
}
