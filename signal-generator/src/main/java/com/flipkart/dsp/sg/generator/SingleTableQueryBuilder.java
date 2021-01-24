package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.exceptions.InvalidQueryException;
import com.flipkart.dsp.sg.helper.ConstraintHelper;
import com.flipkart.dsp.sg.helper.PartitionHelper;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.HiveColumn;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import com.flipkart.dsp.sg.hiveql.query.*;
import com.flipkart.dsp.sg.override.OverrideManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.dsp.models.sg.AggregationType.NA;
import static com.flipkart.dsp.sg.utils.GeneratorUtils.getDataFrameTableName;
import static java.lang.String.format;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
class SingleTableQueryBuilder {

    private final HiveConfig hiveConfig;
    private final PartitionHelper partitionHelper;
    private final OverrideManager overrideManager;
    private final ConstraintHelper constraintHelper;
    private final DataFrameBuilder dataFrameBuilder;

    Pair<Table, List<String>> buildQuery(Long runId, DataFrame dataFrame, DataFrameGenerateRequest request,
                                         Set<DataFrameScope> dataFrameScope) throws DataFrameGeneratorException {
        String databaseName = hiveConfig.getSgDatabase();
        DataTable oldDataTable = getOldDataTable(dataFrame, request);
        DataTable dataTable = getDataTable(dataFrame, request);

        verifySignals(dataFrame);
        LinkedHashSet<Signal> partitionedSignals = partitionHelper.getPartitionForDataframe(dataFrame);
        LinkedHashSet<Signal> nonPartitionedSignals = getNonPartitionedSignals(dataFrame, partitionedSignals);
        LinkedList<Column> columns = getColumns(partitionedSignals, nonPartitionedSignals);

        HiveTable upstreamTable = HiveTable.builder().db(dataTable.getDataSource().getConfiguration().getDatabase())
                .name(dataTable.getId()).columns(columns).refreshId(null).build();
        if (SGType.NO_QUERY.equals(dataFrame.getSgType()))
            return processNoQuery(oldDataTable, dataFrame, upstreamTable, request);

        Set<Constraint> constraints = getConstraints(request, dataFrame, dataFrameScope);
        String dataFrameTableName = getDataFrameTableName(runId, dataFrame.getName(), dataFrame.getId());
        Table dataFrameTable = HiveTable.builder().db(databaseName).name(dataFrameTableName).columns(columns).build();

        String createQuery = new CreateQuery(dataFrameTable).constructQuery();
        SelectQuery selectQuery = getSelectQuery(upstreamTable, constraints, columns);
        String insertQuery = getInsertQuery(dataFrame, dataFrameTable, selectQuery, partitionedSignals);
        resetDataFrame(dataFrame, oldDataTable);
        return Pair.with(dataFrameTable, Lists.newArrayList(createQuery, insertQuery));
    }

    private DataTable getOldDataTable(DataFrame dataFrame, DataFrameGenerateRequest request) {
        if (request.getDataFrameOverrideMap().containsKey(dataFrame.getName()))
            return dataFrame.getSignalGroup().getSignalMetas().get(0).getDataTable();
        return null;
    }

    private DataTable getDataTable(DataFrame dataFrame, DataFrameGenerateRequest request) {
        DataTable dataTable = dataFrame.getSignalGroup().getSignalMetas().get(0).getDataTable();
        if (request.getDataFrameOverrideMap().containsKey(dataFrame.getName())) {
            return getDataTable(dataFrame, dataTable, request.getDataFrameOverrideMap());
        }
        return dataTable;
    }

    private DataTable getDataTable(DataFrame dataFrame, DataTable oldDataTable, Map<String, DataframeOverride> dataFrameOverrideMap) {
        String dataFrameName = dataFrame.getName();
        DataframeOverride dataframeOverride = dataFrameOverrideMap.get(dataFrameName);
        DataTable dataTable = overrideManager.getDataTableForOverride(dataFrame, dataframeOverride, oldDataTable);
        for (SignalGroup.SignalMeta signalMeta : dataFrame.getSignalGroup().getSignalMetas()) {
            signalMeta.setDataTable(dataTable);
        }
        return dataTable;
    }

    private void verifySignals(DataFrame dataFrame) throws HiveGeneratorException {
        int noOfSignalsInSignalGroup = dataFrame.getSignalGroup().getSignalMetas().size();
        int noOfVisibleSignals = dataFrame.getDataFrameConfig().getVisibleSignals().size();

        if (noOfVisibleSignals > noOfSignalsInSignalGroup) {
            throw new HiveGeneratorException("Number of signal in visible signal can't be greater then no of signals in signal group " +
                    " visible signals " + dataFrame.getDataFrameConfig().getVisibleSignals().size() +
                    " signals in signal group " + dataFrame.getSignalGroup().getSignalMetas());
        }
    }

    private LinkedHashSet<Signal> getNonPartitionedSignals(DataFrame dataFrame, LinkedHashSet<Signal> partitionedSignals) {
        return dataFrame.getSignalGroup().getSignalMetas().stream()
                .filter(signalMeta -> !partitionedSignals.contains(signalMeta.getSignal())).collect(Collectors.toList())
                .stream().map(SignalGroup.SignalMeta::getSignal).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private LinkedList<Column> getColumns(LinkedHashSet<Signal> partitionedSignals, LinkedHashSet<Signal> nonPartitionedSignals) {
        LinkedList<Column> columnList = Lists.newLinkedList();
        nonPartitionedSignals.forEach(s -> columnList.add(getColumnFromSignal(s, false)));
        partitionedSignals.forEach(s -> columnList.add(getColumnFromSignal(s, true)));
        return columnList;
    }

    private Column getColumnFromSignal(Signal signal, boolean isPartition) {
        return HiveColumn.builder().name(signal.getSignalBaseEntity())
                .columnDataType(ColumnDataType.from(signal.getSignalDataType()))
                .defaultValue(signal.getSignalDefinition().getDefaultValue()).isPartition(isPartition).build();
    }

    private Pair<Table, List<String>> processNoQuery(DataTable dataTableOld, DataFrame dataFrame, HiveTable upstreamTable,
                                                     DataFrameGenerateRequest request) {
        Long refreshId = getRefreshId(request.getTables(), upstreamTable);
        upstreamTable.setRefreshId(refreshId);
        resetDataFrame(dataFrame, dataTableOld);
        return new Pair<>(upstreamTable, new ArrayList<>());
    }

    private Long getRefreshId(Map<String, Long> tableRefreshIDMap, Table upstreamTable) {
        String upStreamTableKey = format("%s.%s", upstreamTable.getDbName(), upstreamTable.getTableName());
        if (MapUtils.isNotEmpty(tableRefreshIDMap))
            return tableRefreshIDMap.get(upStreamTableKey);
        else
            log.error("Dashboard can't be created for input table " + upStreamTableKey + " as refreshId is not present");
        return null;
    }

    private Set<Constraint> getConstraints(DataFrameGenerateRequest request, DataFrame dataFrame, Set<DataFrameScope> scopeSet) throws DataFrameGeneratorException {
        LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignalMap = dataFrameBuilder.getDataTableToSignalMap(dataFrame, false);
        Map<String/*SignalBaseEntity*/, Pair<String/*DataTable*/, SignalDefinition>> signalDefinitionMap = dataFrameBuilder.getSignalDefinitionMap(tableToSignalMap);
        DataTable dataTable = dataFrame.getSignalGroup().getSignalMetas().get(0).getDataTable();
        LinkedHashSet<Table> upstreamDataTables = dataFrameBuilder.getUpstreamDataTables(tableToSignalMap);
        Map<Table, Long> tableToRefreshId = dataFrameBuilder.getTableToRefreshId(request, upstreamDataTables);
        return constraintHelper.buildConstraintSet(signalDefinitionMap, scopeSet, tableToRefreshId, dataTable, dataFrame);
    }

    private SelectQuery getSelectQuery(HiveTable upstreamTable, Set<Constraint> constraints, LinkedList<Column> columns) {
        LinkedHashSet<SelectColumn> selectColumns = new LinkedHashSet<>();
        columns.forEach(c -> selectColumns.add(new SelectColumn(c.getColumnName(), false, NA, c.getColumnName())));
        return new SelectQuery(Sets.newHashSet(), constraints, Sets.newHashSet(), new LinkedHashSet<>(Collections.singletonList(upstreamTable)),
                selectColumns, QueryBehaviorType.NO_JOIN);
    }

    private String getInsertQuery(DataFrame dataFrame, Table dataFrameTable, SelectQuery selectQuery,
                                  LinkedHashSet<Signal> partitionedSignals) throws HiveGeneratorException {
        LinkedHashSet<String> partitionedNames = partitionedSignals.stream().map(Signal::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        try {
            return new InsertQuery(dataFrameTable, partitionedNames, selectQuery).constructQuery();
        } catch (InvalidQueryException e) {
            throw new HiveGeneratorException(format("Failed to create insert query for the dataFrame table : %s",
                    dataFrame.getTableName()), e);
        }
    }

    private void resetDataFrame(DataFrame dataFrame, DataTable dataTableOld) {
        if (!Objects.isNull(dataTableOld)) {
            for (SignalGroup.SignalMeta signalMeta : dataFrame.getSignalGroup().getSignalMetas()) {
                signalMeta.setDataTable(dataTableOld);
            }
        }
    }

}
