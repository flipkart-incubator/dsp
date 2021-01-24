package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.entities.sg.dto.*;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ConstraintType;
import com.google.common.collect.Sets;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;


/**
 */

public class GeneratorUtils {
    public static final String DATA_FRAME_TABLE_NAME = "%s__%d_%d";
    public static final String FACT_TABLE_NAME_IG = "%s__%s__%s___%d";
    public static final String GRANULARITY_TABLE_NAME = "%s__granularity_table___%d";


    public static String getDataFrameTableName(Long dataFrameAuditRunId, String dataFrameName, Long dataframeId) {
        return String.format(DATA_FRAME_TABLE_NAME, dataFrameName, dataframeId, dataFrameAuditRunId);
    }

    public static String getGranularityTableName(Long dataFrameAuditRunId, String dateFrameId) {
        return String.format(GRANULARITY_TABLE_NAME, dateFrameId, dataFrameAuditRunId);
    }

    public static String getFactTableName(Long dataFrameAuditRunId, String tableName, String firstSignal, String dataFrameId) {
        return String.format(FACT_TABLE_NAME_IG, firstSignal, tableName, dataFrameId, dataFrameAuditRunId);
    }

    public static LinkedHashSet<Column> getPartitionKeys(Table table) {
        LinkedHashSet<Column> columns = Sets.newLinkedHashSet();
        for (Column column : table.getColumns()) {
            if (column.isPartitionColumn())
                columns.add(column);
        }
        return columns;
    }

    public static Triplet<Object, Pair<Object, Object>, Set<Object>> convertAbstractPredicateClauseToTriplet(AbstractPredicateClause abstractPredicateClause) {
        ConstraintType constraintType = ConstraintType.from(abstractPredicateClause.getPredicateType());
        Object constraintValue = null;
        Pair<Object, Object> pairValues = null;
        Set<Object> rangeValues = null;
        switch (constraintType) {
            case NOT_IN:
            case IN:
                rangeValues = ((MultiValuePredicateClause) abstractPredicateClause).getValues();
                break;
            case EQUAL:
                constraintValue = ((UniValuePredicateClause) abstractPredicateClause).getValue();
                break;
            case GREATER_THAN:
                constraintValue = ((UniValuePredicateClause) abstractPredicateClause).getValue();
                break;
            case LESS_THAN:
                constraintValue = ((UniValuePredicateClause) abstractPredicateClause).getValue();
                break;
            case RANGE:
            case TIME_YYYYMMDD_RANGE:
            case TIME_YYYYWW_RANGE:
            case INCREMENTAL_WEEK_RANGE:
            case INCREMENTAL_DATE_RANGE:
                BiValuePredicateClause biValuePredicateClause = ((BiValuePredicateClause) abstractPredicateClause);
                pairValues = Pair.with(biValuePredicateClause.getValue1(), biValuePredicateClause.getValue2());
                break;
        }
        return Triplet.with(constraintValue, pairValues, rangeValues);
    }

    public static DataFrameKey convertAbstractPredicateClauseToDataFrameKey(AbstractPredicateClause abstractPredicateClause) {
        PredicateType predicateType = abstractPredicateClause.getPredicateType();
        switch (predicateType) {
            case EQUAL:
                Set<String> values = Sets.newHashSet(((UniValuePredicateClause) abstractPredicateClause).getValue().toString());
                return new DataFrameMultiKey(DataFrameColumnType.IN, values);
            case IN:
                Set<String> stringSet = ((MultiValuePredicateClause) abstractPredicateClause).getValues().stream().map(Object::toString).collect(Collectors.toSet());
                return new DataFrameMultiKey(DataFrameColumnType.IN, stringSet);
            case RANGE:
            case TIME_YYYYWW_RANGE:
            case INCREMENTAL_WEEK_RANGE:
            case TIME_YYYYMMDD_RANGE:
            case INCREMENTAL_DATE_RANGE:
                BiValuePredicateClause clause = (BiValuePredicateClause) abstractPredicateClause;
                return new DataFrameBinaryKey(DataFrameColumnType.RANGE, clause.getValue1().toString(), clause.getValue2().toString());
            case GREATER_THAN:
            case LESS_THAN:
                return new DataFrameUnaryKey(DataFrameColumnType.EQUAL, ((UniValuePredicateClause) abstractPredicateClause).getValue().toString());
            default:
                throw new RuntimeException(String.format("Failed to convert AbstractPredicateClause of type %s to DataFrameKey.",
                        abstractPredicateClause.getPredicateType()));
        }
    }
}
