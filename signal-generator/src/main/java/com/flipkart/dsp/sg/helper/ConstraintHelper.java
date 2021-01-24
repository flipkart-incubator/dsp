package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.ConstraintType;
import com.flipkart.dsp.sg.hiveql.core.HiveColumn;
import com.flipkart.dsp.sg.hiveql.core.HiveConstraint;
import com.flipkart.dsp.utils.Constants;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.Map;
import java.util.Set;

import static com.flipkart.dsp.sg.utils.GeneratorUtils.convertAbstractPredicateClauseToTriplet;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;

public class ConstraintHelper {

    public Set<Constraint> buildConstraintSet(Map<String/*SignalBaseEntity*/, Pair<String, SignalDefinition>> signalDefinitionMap,
                                              Set<DataFrameScope> scopeSet, Map<Table, Long> tableToRefreshId,
                                              DataTable dataTable, DataFrame dataFrame) {
        Set<Constraint> constraintSet = Sets.newHashSet();
        Map<String, Constraint> signalBaseEntityConstraint = Maps.newHashMap();
        for (DataFrameScope dataframeScope : scopeSet) {
            Signal signal = dataframeScope.getSignal();
            if (signalDefinitionMap.containsKey(signal.getSignalBaseEntity())) {
                ConstraintType constraintType = ConstraintType.from(dataframeScope.getAbstractPredicateClause().getPredicateType());
                Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = convertAbstractPredicateClauseToTriplet(dataframeScope.getAbstractPredicateClause());

                String columnName = signal.getSignalBaseEntity();
                ColumnDataType columnDataType = ColumnDataType.from(signal.getSignalDataType());
                Column constraintColumn = HiveColumn.builder().name(columnName).columnDataType(columnDataType).build();

                String dataTableName;
                if (!isNull(dataTable))
                    dataTableName = dataTable.getId();
                else
                    dataTableName = getDataTableName(dataframeScope, dataFrame);

                Constraint constraint = HiveConstraint.builder().constraintColumn(constraintColumn).constraintType(constraintType).
                        inValues(triplet.getValue2()).constraintValue(triplet.getValue0()).rangeValues(triplet.getValue1()).tableName(dataTableName).build();
                signalBaseEntityConstraint.put(dataframeScope.getSignal().getSignalBaseEntity(), constraint);
            }
        }

        for (Map.Entry<String /*SignalBaseEntity*/, Pair<String, SignalDefinition>> signalBaseEntityToSignalDefinition : signalDefinitionMap.entrySet()) {
            if (signalBaseEntityToSignalDefinition.getValue().getValue1().getSignalValueType().equals(SignalValueType.CONDITIONAL))
                for (SignalDefinitionScope signalDefinitionScope : signalBaseEntityToSignalDefinition.getValue().getValue1().getSignalDefinitionScopeSet()) {
                    ConstraintType constraintType = ConstraintType.from(signalDefinitionScope.getPredicateClause().getPredicateType());
                    Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = convertAbstractPredicateClauseToTriplet(signalDefinitionScope.getPredicateClause());
                    Column constraintColumn = HiveColumn.builder().name(signalDefinitionScope.getPredicateEntity()).build();
                    String dataTableName = signalBaseEntityToSignalDefinition.getValue().getValue0();
                    Constraint constraint = HiveConstraint.builder().constraintColumn(constraintColumn).constraintType(constraintType).
                            inValues(triplet.getValue2()).constraintValue(triplet.getValue0()).rangeValues(triplet.getValue1()).
                            tableName(dataTableName).build();
                    signalBaseEntityConstraint.put(signalDefinitionScope.getPredicateEntity(), constraint);
                }
        }

        tableToRefreshId.forEach((table, refreshId) -> {
            Column column = HiveColumn.builder().name(Constants.REFRESH_ID).columnDataType(ColumnDataType.BIGINT).build();
            Constraint constraint = HiveConstraint.builder().constraintColumn(column).constraintType(ConstraintType.EQUAL)
                    .constraintValue(refreshId).tableName(table.getTableName()).build();
            signalBaseEntityConstraint.put(table.getDbName() + "." + table.getTableName(), constraint);
        });

        constraintSet.addAll(signalBaseEntityConstraint.values());
        return constraintSet;
    }

    private String getDataTableName(DataFrameScope dataframeScope, DataFrame dataFrame) {
        Set<DataTable> dataTables = dataFrame.getSignalGroup().getSignalMetas().stream().filter(sm -> sm.getSignal().
                equals(dataframeScope.getSignal())).map(SignalGroup.SignalMeta::getDataTable).collect(toSet());

        if (dataTables.size() != 1)
            throw new IllegalStateException("Data table not found for given parameters. Dataframe ID : " + dataFrame.getId());

        return Iterables.get(dataTables, 0).getId();
    }

}
