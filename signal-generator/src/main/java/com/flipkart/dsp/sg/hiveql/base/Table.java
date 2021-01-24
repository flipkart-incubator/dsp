package com.flipkart.dsp.sg.hiveql.base;

import com.flipkart.dsp.sg.hiveql.query.SelectQuery;

import java.util.List;

/**
 */
public interface Table {
    public String getDbName();
    public String getTableName();
    public SelectQuery getSubQuery();
    public void setSubQuery(SelectQuery subQuery);
    public boolean hasSubQuery();
    public List<? extends Column> getColumns();
    public boolean containsColumn(String columnName);
    public boolean containsColumn(Column column);
    public Column getColumn(String columnName);
    public String getDelimiter();
}
