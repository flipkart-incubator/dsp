package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Query;
import com.flipkart.dsp.sg.hiveql.base.Table;
import lombok.RequiredArgsConstructor;

import java.util.function.Predicate;

import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;

/**
 */

@RequiredArgsConstructor
public class CreateQuery implements Query {

    private final Table table;

    @Override
    public QueryType getQueryType() {
        return QueryType.CREATE_QUERY;
    }

    @Override
    public String constructQuery() {
        return constructQueryIfNotExist(false);
    }

    private String constructQueryIfNotExist(boolean notExist) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("CREATE TABLE");
        if (notExist) stringBuffer.append(" IF NOT EXISTS");
        stringBuffer.append(SINGLE_SPACE);
        stringBuffer.append(table.getDbName());
        stringBuffer.append(DOT);
        stringBuffer.append(table.getTableName());
        stringBuffer.append(OPEN_BRACKET);
        stringBuffer.append(System.lineSeparator());

        //List down all the column names with data types and every column in a new line.
        table.getColumns().stream().filter(column -> !column.isPartitionColumn()).forEach(column -> {
            stringBuffer.append("`");
            stringBuffer.append(column.getColumnName());
            stringBuffer.append("`");
            stringBuffer.append(SINGLE_SPACE);
            stringBuffer.append(column.getColumnDataType().name());
            stringBuffer.append(COMMA);
            stringBuffer.append(System.lineSeparator());
        });
        stringBuffer.deleteCharAt(stringBuffer.lastIndexOf(COMMA));
        stringBuffer.append(CLOSE_BRACKET);
        stringBuffer.append(System.lineSeparator());


        //Check if there is any column with partition, if yes then make it part of the create query
        if (table.getColumns().stream().anyMatch(Column::isPartitionColumn)) {
            stringBuffer.append("PARTITIONED BY");
            stringBuffer.append(OPEN_BRACKET);
            table.getColumns().stream().filter((Predicate<Column>) Column::isPartitionColumn).forEach(column -> {
                stringBuffer.append(column.getColumnName());
                stringBuffer.append(SINGLE_SPACE);
                stringBuffer.append(column.getColumnDataType().name());
                stringBuffer.append(COMMA);
            });
            stringBuffer.deleteCharAt(stringBuffer.lastIndexOf(COMMA));
            stringBuffer.append(CLOSE_BRACKET);
            stringBuffer.append(System.lineSeparator());
        }
        stringBuffer.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + table.getDelimiter() + "' NULL DEFINED AS ''");
        return stringBuffer.toString();
    }

}
