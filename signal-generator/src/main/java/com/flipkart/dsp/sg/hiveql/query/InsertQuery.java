package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.sg.exceptions.InvalidQueryException;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Query;
import com.flipkart.dsp.sg.hiveql.base.Table;
import lombok.AllArgsConstructor;

import java.util.LinkedHashSet;

import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;
import static java.util.stream.Collectors.toList;

/**
 */

@AllArgsConstructor
public class InsertQuery implements Query {
    public static final String PARTITION = "PARTITION";
    public static final String INSERT_OVERWRITE_TABLE = "INSERT OVERWRITE TABLE";
    private final Table table;
    private LinkedHashSet<String> partitions;
    private final SelectQuery selectQuery;


    @Override
    public QueryType getQueryType() {
        return QueryType.INSERT_QUERY;
    }

    @Override
    public String constructQuery() throws InvalidQueryException {
        validateQuery();
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append(INSERT_OVERWRITE_TABLE);
        stringBuffer.append(SINGLE_SPACE);
        stringBuffer.append(table.getDbName());
        stringBuffer.append(DOT);
        stringBuffer.append(table.getTableName());
        if (partitions != null && !partitions.isEmpty()) {
            stringBuffer.append(SINGLE_SPACE);
            stringBuffer.append(PARTITION);
            stringBuffer.append(OPEN_BRACKET);
            for (String partitionedColumn : partitions) {
                stringBuffer.append(partitionedColumn);
                stringBuffer.append(COMMA);
            }
            stringBuffer.deleteCharAt(stringBuffer.lastIndexOf(COMMA));
            stringBuffer.append(CLOSE_BRACKET);
        }
        stringBuffer.append(System.lineSeparator());
        stringBuffer.append(selectQuery.constructQuery());
        return stringBuffer.toString();
    }

    private void validateQuery() throws InvalidQueryException {
        validatePartitionColumnsExist();
        validateSelectQuery();
    }

    private void validatePartitionColumnsExist() throws InvalidQueryException {
        boolean valid = partitions.stream().noneMatch(partition -> table.getColumns().stream().filter(column ->
                column.getColumnName().equalsIgnoreCase(partition)).anyMatch(column -> !column.isPartitionColumn()));
        if (!valid) throw new InvalidQueryException("partition information not matching for insertQuery");
    }

    private void validateSelectQuery() throws InvalidQueryException {
        boolean valid = false;
        if (selectQuery.getSelectColumns().size() != 0)
            valid = selectQuery.getSelectColumns().stream()
                    .allMatch(selectColumn -> table.getColumns().stream().map(Column::getColumnName).collect(toList()).contains(selectColumn.getName()));

        if (!valid) throw new InvalidQueryException("selectQuery columns not matching with insertQuery");
    }
}
