package com.flipkart.dsp.sg.hiveql.query;

import com.flipkart.dsp.models.sg.AggregationType;
import com.flipkart.dsp.sg.exceptions.InvalidQueryException;
import com.flipkart.dsp.sg.hiveql.base.*;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.ConstraintType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.flipkart.dsp.sg.hiveql.core.ColumnDataType.STRING;
import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;
import static com.flipkart.dsp.utils.Constants.comma;
import static com.flipkart.dsp.utils.Constants.dot;

/**
 */
@Data
@Slf4j
@AllArgsConstructor
public class SelectQuery implements Query {

    public static final String BETWEEN = "BETWEEN";
    public static final String UNIX_TIMESTAMP = "UNIX_TIMESTAMP";
    public static final String ASTERIK = "*";

    private final Set<Join> joins;
    private final Set<Constraint> constraints;
    private final Set<String> groupByColumns;
    private final LinkedHashSet<Table> tables;
    private final LinkedHashSet<SelectColumn> selectColumns;
    private final QueryBehaviorType queryBehaviorType;

    @Override
    public QueryType getQueryType() {
        return QueryType.SELECT_QUERY;
    }

    @Override
    public String constructQuery() throws InvalidQueryException {
        validateQuery();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT");
        stringBuilder.append(checkForDistinct(queryBehaviorType, groupByColumns) ? " DISTINCT" : SINGLE_SPACE);
        stringBuilder.append(SINGLE_SPACE);
        stringBuilder.append(System.lineSeparator());
        for (SelectColumn selectColumn : selectColumns) {
            String columnName = selectColumn.getName();
            Table table = getTableContainsColumn(columnName);
            Column column = getTableColumn(columnName, table);
            stringBuilder.append(processCount(selectColumn));
            stringBuilder.append(processGranularityColumn(table, column, selectColumn));
            stringBuilder.append(processColumn(table, column, selectColumn));
        }

        stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMMA));
        stringBuilder.append(SINGLE_SPACE);
        stringBuilder.append(FROM);
        stringBuilder.append(SINGLE_SPACE);
        stringBuilder.append(processCartesianJoin());
        stringBuilder.append(processNoJoin());
        stringBuilder.append(processCrossJoin());
        stringBuilder.append(processOuterJoin());
        stringBuilder.append(processGroupBy());
        return stringBuilder.toString();
    }

    private boolean checkForDistinct(QueryBehaviorType queryBehaviorType, Set<String> groupByColumns) {
        return !(queryBehaviorType.equals(QueryBehaviorType.CARTESIAN_MULTIPLICATION)
                || (queryBehaviorType.equals(QueryBehaviorType.LEFT_OUTER_JOIN))
                || (queryBehaviorType.equals(QueryBehaviorType.CROSS_JOIN)))
                && (groupByColumns == null || groupByColumns.isEmpty());
    }

    private Table getTableContainsColumn(String columnName) {
        for (Table table : tables) {
            if (table.containsColumn(columnName))
                return table;
        }
        log.error("Table not found for column : {} in select query : {}", columnName, this.toString());
        return null;
    }

    private Column getTableColumn(String columnName, Table table) {
        return columnName.equals(ASTERIK) ? null : table.getColumn(columnName);
    }

    private String processCount(SelectColumn selectColumn) {
        return (selectColumn.getName().equals(ASTERIK) && selectColumn.getHiveAggregationType().name().equals(AggregationType.COUNT.name()))
                ? selectColumn.getHiveAggregationType().name() + OPEN_BRACKET + ASTERIK + CLOSE_BRACKET : "";
    }

    private String processGranularityColumn(Table table, Column column, SelectColumn selectColumn) {
        return selectColumn.getIsGranularity() ? table.getTableName() + DOT + "`" + column.getColumnName() + "`" + SINGLE_SPACE + AS +
                SINGLE_SPACE + "`" + selectColumn.getAlias() + "`" : "";
    }

    private String processColumn(Table table, Column column, SelectColumn selectColumn) {
        if (selectColumn.getIsGranularity() || (selectColumn.getName().equals(ASTERIK)
                && selectColumn.getHiveAggregationType().name().equals(AggregationType.COUNT.name())))
            return ",";

        return aggregationTypePrefix(selectColumn) + COALESCE + OPEN_BRACKET + table.getTableName() + dot + "`"
                + selectColumn.getName() + "`" + comma + processDefaultValue(column) + CLOSE_BRACKET
                + aggregationTypePostfix(selectColumn) + SINGLE_SPACE + AS + SINGLE_SPACE + "`" + selectColumn.getAlias()
                + "`" + COMMA + System.lineSeparator();
    }

    private String aggregationTypePrefix(SelectColumn selectColumn) {
        return selectColumn.getHiveAggregationType().name().equals("NA") ? "" : selectColumn.getHiveAggregationType().name() + OPEN_BRACKET;
    }

    private String processDefaultValue(Column column) {
        if (Objects.nonNull(column) && Objects.nonNull(column.getDefaultValue()) && StringUtils.isNotBlank(column.getDefaultValue().toString())) {
            if (column.getColumnDataType() == STRING && !column.getDefaultValue().toString().contains("'")) {
                return "'" + column.getDefaultValue().toString() + "'";
            } else {
                return column.getDefaultValue().toString();
            }
        } else {
            return ColumnDataType.defaultValue(column.getColumnDataType());
        }
    }

    private String aggregationTypePostfix(SelectColumn selectColumn) {
        return selectColumn.getHiveAggregationType().name().equals("NA") ? "" : CLOSE_BRACKET;
    }

    private String processNoJoin() {
        if (queryBehaviorType == QueryBehaviorType.NO_JOIN) {
            if (tables.size() != 1) {
                throw new IllegalArgumentException("NO_JOIN Can't have more than one tables");
            }
            return constructCartesianJoin();
        }
        return "";
    }

    private String processCrossJoin() {
        StringBuilder stringBuilder = new StringBuilder();
        if (queryBehaviorType == QueryBehaviorType.CROSS_JOIN) {
            Table prevTable = null;
            for (Table table : tables) {
                if (prevTable != null) {
                    stringBuilder.append(SINGLE_SPACE);
                    stringBuilder.append(JOIN);
                    stringBuilder.append(SINGLE_SPACE);
                }
                stringBuilder.append(table.getDbName());
                stringBuilder.append(DOT);
                stringBuilder.append(table.getTableName());
                if (prevTable != null) {
                    addConstraints(stringBuilder, prevTable, false);
                }
                prevTable = table;
            }
            addConstraints(stringBuilder, prevTable, true);
        }
        return stringBuilder.toString();
    }

    private String processSubQuery(Table table) throws InvalidQueryException {
        if (table.hasSubQuery()) {
            return OPEN_BRACKET + table.getSubQuery().constructQuery() + CLOSE_BRACKET + SINGLE_SPACE + table.getTableName();
        } else {
            return table.getDbName() + dot + table.getTableName();
        }
    }

    private String processJoinColumns(Join join) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<Column, Column> joinPair : join.getJoinColumns().entrySet()) {
            stringBuilder.append(join.getLeftTable().getTableName());
            stringBuilder.append(DOT);
            stringBuilder.append(joinPair.getKey().getColumnName());
            stringBuilder.append(SINGLE_SPACE);
            stringBuilder.append(EQUAL);
            stringBuilder.append(SINGLE_SPACE);
            stringBuilder.append(join.getRightTable().getTableName());
            stringBuilder.append(DOT);
            stringBuilder.append(joinPair.getValue().getColumnName());
            stringBuilder.append(SINGLE_SPACE);
            stringBuilder.append(AND);
            stringBuilder.append(SINGLE_SPACE);
            stringBuilder.append(System.lineSeparator());
        }
        return stringBuilder.toString();
    }

    private String processOuterJoin() throws InvalidQueryException {
        Set<Table> alreadyJoinedTable = new HashSet<>();
        StringBuilder stringBuilder = new StringBuilder();
        if (queryBehaviorType.name().equals(QueryBehaviorType.JOIN.name()) || queryBehaviorType.name().equals(QueryBehaviorType.LEFT_OUTER_JOIN.name())) {
            for (Join join : joins) {
                if (!alreadyJoinedTable.contains(join.getLeftTable())) {
                    stringBuilder.append(processSubQuery(join.getLeftTable()));
                    alreadyJoinedTable.add(join.getLeftTable());
                }
                stringBuilder.append(SINGLE_SPACE);
                stringBuilder.append(System.lineSeparator());
                stringBuilder.append(String.valueOf(queryBehaviorType).equalsIgnoreCase("LEFT_OUTER_JOIN") ? LEFT_OUTER_JOIN : String.valueOf(queryBehaviorType));
                stringBuilder.append(SINGLE_SPACE);
                stringBuilder.append(System.lineSeparator());
                stringBuilder.append(processSubQuery(join.getRightTable()));
                stringBuilder.append(SINGLE_SPACE);
                stringBuilder.append(System.lineSeparator());
                alreadyJoinedTable.add(join.getRightTable());
                stringBuilder.append(ON);
                stringBuilder.append(SINGLE_SPACE);
                stringBuilder.append(processJoinColumns(join));
                stringBuilder.delete(stringBuilder.lastIndexOf(AND), stringBuilder.lastIndexOf(AND) + AND.length());
                stringBuilder.append(processJoinClause(join));
            }
        }
        return stringBuilder.toString();
    }

    private String processJoinClause(Join join) {
        StringBuilder stringBuilder = new StringBuilder();
        if (join.getJoinClause() != null) {
            for (Constraint joinClause : join.getJoinClause()) {
                stringBuilder.append(AND);
                stringBuilder.append(SINGLE_SPACE);
                ConstraintType constraintType = joinClause.getConstraintType();
                Table table;
                if (join.getLeftTable().containsColumn(joinClause.getConstraintColumn()))
                    table = join.getLeftTable();
                else table = join.getRightTable();
                String constraintString = SINGLE_SPACE + constraintType.getPrefix() + SINGLE_SPACE + constraintType.addTableName(table.getTableName())
                        + joinClause.getConstraintColumn().getColumnName() + SINGLE_SPACE
                        + constraintType.getPostfix() + SINGLE_SPACE
                        + constraintType.addConstraintValue(joinClause) + SINGLE_SPACE;
                stringBuilder.append(constraintString);
            }
        }
        return stringBuilder.toString();
    }

    private String processGroupBy() {
        StringBuilder stringBuilder = new StringBuilder();
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            stringBuilder.append(GROUP_BY);
            for (String groupByColumn : groupByColumns) {
                stringBuilder.append(SINGLE_SPACE);
                stringBuilder.append(getTableContainsColumn(groupByColumn).getTableName());
                stringBuilder.append(DOT);
                stringBuilder.append(groupByColumn);
                stringBuilder.append(COMMA);
            }
            stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMMA));
        }
        return stringBuilder.toString();
    }

    private String processCartesianJoin() {
        return queryBehaviorType.name().equals(QueryBehaviorType.CARTESIAN_MULTIPLICATION.name()) ? constructCartesianJoin() : "";
    }


    private String constructCartesianJoin() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Table table : tables) {
            stringBuilder.append(table.getDbName());
            stringBuilder.append(DOT);
            stringBuilder.append(table.getTableName());
            stringBuilder.append(COMMA);
            stringBuilder.append(SINGLE_SPACE);
        }
        stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMMA));
        if (constraints != null && !constraints.isEmpty()) {
            stringBuilder.append(WHERE);
            stringBuilder.append(SINGLE_SPACE);
            for (Constraint constraint : constraints) {
                String table = constraint.getTableName();
                stringBuilder.append(ConstraintType.getConstraints(table, constraint));
                stringBuilder.append(SINGLE_SPACE);
                stringBuilder.append(AND);
                stringBuilder.append(SINGLE_SPACE);
            }
            stringBuilder.delete(stringBuilder.lastIndexOf(AND), stringBuilder.lastIndexOf(AND) + AND.length());
        }
        return stringBuilder.toString();
    }

    private void addConstraints(StringBuilder stringBuilder, Table prevTable, boolean isLastTable) {
        if (constraints != null && !constraints.isEmpty()) {
            stringBuilder.append(SINGLE_SPACE);
            if (isLastTable) {
                int index = stringBuilder.lastIndexOf(ON);
                if (index != -1) {
                    if (!ON.equals(stringBuilder.substring(index).trim())) {
                        stringBuilder.append(AND);
                        stringBuilder.append(SINGLE_SPACE);
                    }
                }
            } else {
                stringBuilder.append(ON);
                stringBuilder.append(SINGLE_SPACE);
            }
            for (Constraint constraint : constraints) {
                HashMap<String, Integer> consTrainPickedMap = new HashMap<>();
                String table = constraint.getTableName();
                String key = constraint.getTableName() + "." + constraint.getConstraintColumn().getColumnName();
                if (constraint.getTableName().equalsIgnoreCase(prevTable.getTableName()) && (consTrainPickedMap.get(key) == null)) {
                    consTrainPickedMap.put(key, 1);
                    stringBuilder.append(ConstraintType.getConstraints(table, constraint));
                    stringBuilder.append(SINGLE_SPACE);
                    stringBuilder.append(AND);
                    stringBuilder.append(SINGLE_SPACE);
                }
            }
            if (stringBuilder.lastIndexOf(AND) != -1) {
                stringBuilder.delete(stringBuilder.lastIndexOf(AND), stringBuilder.lastIndexOf(AND) + AND.length() + 1);
            }
        }
    }

    private void validateQuery() {
        //1) all distinct tables in joins should match with tables
        //2) all columns should be present in at least one of the given table
        //3) In case of join at least one granularity column should be present in all the tables mentioned.
        //4) If query type is join and joins should not be empty and vice versa.
        //5) In join query type atleast one granularity column has to be there
    }

}
