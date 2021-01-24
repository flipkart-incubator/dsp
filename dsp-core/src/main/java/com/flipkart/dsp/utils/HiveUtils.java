package com.flipkart.dsp.utils;

import com.flipkart.dsp.actors.DataTableActor;
import com.flipkart.dsp.models.sg.DataTable;

import static com.flipkart.dsp.utils.Constants.dot;
import static java.lang.String.format;

public class HiveUtils {

    public static String getFullyQualifiedTableName(String tableName, DataTableActor dataTableActor) {
        if (tableName.contains(dot))
            return tableName;

        DataTable dataTable = dataTableActor.getTable(tableName);
        return format("%s.%s", dataTable.getDataSource().getId(), tableName);
    }

    public static String getTableNameWithOutDBPrefix(String tableName) {
        if (!tableName.contains(dot))
            return tableName;

        return tableName.substring(tableName.lastIndexOf(".") + 1);
    }
}
