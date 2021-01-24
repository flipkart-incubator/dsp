package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.sg.hiveql.base.Query;
import com.flipkart.dsp.sg.hiveql.base.Table;
import lombok.RequiredArgsConstructor;

import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;

/**
 */
@RequiredArgsConstructor
public class DropQuery implements Query {
    private final Table table;

    @Override
    public QueryType getQueryType() {
        return QueryType.DROP_QUERY;
    }

    @Override
    public String constructQuery() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(DROP);
        stringBuilder.append(SINGLE_SPACE);
        stringBuilder.append(TABLE);
        stringBuilder.append(SINGLE_SPACE);
        stringBuilder.append(table.getDbName());
        stringBuilder.append(DOT);
        stringBuilder.append(table.getTableName());
        return stringBuilder.toString();
    }

}
