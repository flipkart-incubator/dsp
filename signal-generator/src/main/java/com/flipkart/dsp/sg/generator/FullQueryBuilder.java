package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.exceptions.InvalidQueryException;
import com.flipkart.dsp.sg.helper.ConstraintHelper;
import com.flipkart.dsp.sg.helper.PartitionHelper;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Join;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.HiveColumn;
import com.flipkart.dsp.sg.hiveql.core.HiveJoin;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import com.flipkart.dsp.sg.hiveql.query.*;
import com.flipkart.dsp.utils.Constants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.thrift.TException;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.flipkart.dsp.sg.utils.GeneratorUtils.*;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
class FullQueryBuilder {
    private final HiveConfig hiveConfig;
    private final PartitionHelper partitionHelper;
    private final MetaStoreClient metaStoreClient;
    private final DataFrameBuilder dataFrameBuilder;
    private final ConstraintHelper constraintHelper;


    Pair<Table, List<String>> buildQuery(Long runId, DataFrame dataFrame, DataFrameGenerateRequest request,
                                         Set<DataFrameScope> dataFrameScopes) throws DataFrameGeneratorException {
        Table granularityTable = createGranularityTable(runId, dataFrame, dataFrameScopes, request);
        Set<Table> intermediateTables = createIntermediateFactTables(runId, dataFrame, dataFrameScopes, request);
        Set<String> seenColumnIds = Sets.newHashSet();
        String databaseName = hiveConfig.getSgDatabase();
        LinkedHashSet<Column> partitionColumns = Sets.newLinkedHashSet();
        LinkedHashSet<String> partitionIds = Sets.newLinkedHashSet();
        LinkedList<Column> dataFrameColumns = new LinkedList<>();

        LinkedHashSet<Signal> partitions = partitionHelper.getPartitionForDataframe(dataFrame);
        createPartitionColumns(granularityTable, partitions, seenColumnIds, partitionIds, partitionColumns);

        List<Set<Table>> masterTableSet = Lists.newArrayList(Sets.newHashSet(granularityTable), intermediateTables);
        SelectQuery selectQuery = getSelectQuery(granularityTable, dataFrame, dataFrameColumns, intermediateTables, seenColumnIds, masterTableSet);

        String dataFrameTableName = getDataFrameTableName(runId, dataFrame.getName(), dataFrame.getId());
        Table table = HiveTable.builder().db(databaseName).name(dataFrameTableName).columns(dataFrameColumns).build();
        String createQuery = new CreateQuery(table).constructQuery();
        String insertQuery = null;
        try {
            insertQuery = new InsertQuery(table, partitionIds, selectQuery).constructQuery();
        } catch (InvalidQueryException e) {
            String errorMessage = format("Failed to create insert query for the dataframe table : %s", dataFrame.getTableName());
            throw new HiveGeneratorException(errorMessage, e);
        }
        return Pair.with(table, Lists.newArrayList(createQuery, insertQuery));
    }

    private Set<Table> createIntermediateFactTables(Long runId, DataFrame dataframe, Set<DataFrameScope> scopeSet, DataFrameGenerateRequest request) {
        String databaseName = hiveConfig.getSgDatabase();
        Map<DataTable, LinkedHashSet<Signal>> dataTableSetMap = dataFrameBuilder.getDataTableToSignalMap(dataframe, false);
        Map<DataTable, LinkedHashSet<Signal>> primaryDataTableSetMap = dataFrameBuilder.getDataTableToSignalMap(dataframe, true);
        List<Signal> primarySignals = primaryDataTableSetMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        Map<Pair<DataTable, Set<String/*SignalBaseEntity*/>>, Set<Signal>> dataTableAndGroupByMapToSignals
                = getDataTableAndGroupByMapToSignals(dataTableSetMap);
        LinkedHashSet<Column> lowestCommonDescendants = getLowestCommonDescendantColumns(dataframe);
        // Al signals which are one to one will in one group, all signals with conditional as signal type and same group by will have their groups and all signals with
        return dataTableAndGroupByMapToSignals.entrySet().stream().map(e -> {
            try {
                final DataTable dataTable = e.getKey().getValue0();
                final Set<String> signalBaseEntitySet = e.getKey().getValue1();
                final Set<Signal> signalSet = e.getValue();
                return createIntermediateFactTable(dataTable, signalBaseEntitySet, signalSet,
                        lowestCommonDescendants, runId, databaseName, request, scopeSet, dataframe, primarySignals);
            } catch (HiveGeneratorException e1) {
                throw new RuntimeException("Exception while generating intermediate fact tables", e1);
            }
        }).collect(Collectors.toSet());
    }

    private Table createGranularityTable(Long runId, DataFrame dataframe, Set<DataFrameScope> scopeSet,
                                         DataFrameGenerateRequest request) throws DataFrameGeneratorException {
        String granularityTableName = getGranularityTableName(runId, dataframe.getName());

        LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToPrimarySignals = dataFrameBuilder.getDataTableToSignalMap(dataframe, true);
        Table granularityTable = getGranularityTable(granularityTableName, tableToPrimarySignals);
        LinkedHashSet<Table> tables = dataFrameBuilder.getUpstreamDataTables(tableToPrimarySignals);
        Set<Signal> signals = new HashSet<>();
        tableToPrimarySignals.values().forEach(signals::addAll);

        QueryBehaviorType queryBehaviorType = getQueryBehaviorType(tables);
        Map<Table, Long> tableToRefreshId = dataFrameBuilder.getTableToRefreshId(request, tables);
        LinkedHashSet<SelectColumn> selectColumns = getSelectColumns(signals);
        Map<String, Pair<String, SignalDefinition>> signalDefinitions = dataFrameBuilder.getSignalDefinitionMap(tableToPrimarySignals);
        Set<Constraint> constraints = constraintHelper.buildConstraintSet(signalDefinitions, scopeSet, tableToRefreshId, null, dataframe);
        SelectQuery selectQuery = new SelectQuery(Sets.newHashSet(), constraints, Sets.newHashSet(), tables, selectColumns, queryBehaviorType);
        granularityTable.setSubQuery(selectQuery);
        return granularityTable;
    }

    private LinkedHashSet<SelectColumn> getSelectColumns(Set<Signal> signals) {
        return signals.stream().map(s -> new SelectColumn(s.getSignalBaseEntity(), true, AggregationType.NA, s.getName()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Table getGranularityTable(String granularityTableName, LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToPrimarySignals) {
        String databaseName = hiveConfig.getSgDatabase();
        LinkedList<Column> columns = dataFrameBuilder.constructColumns(tableToPrimarySignals, new ArrayList<>());
        return HiveTable.builder().db(databaseName).name(granularityTableName).columns(columns).build();
    }

    private QueryBehaviorType getQueryBehaviorType(LinkedHashSet<Table> tableSet) {
        if (tableSet.size() == 1)
            return QueryBehaviorType.NO_JOIN;
        else if (tableSet.size() == 2)
            return QueryBehaviorType.CROSS_JOIN;
        else
            return QueryBehaviorType.CARTESIAN_MULTIPLICATION;
    }

    private LinkedHashSet<SelectColumn> getSelectColumns(Set<Signal> signals, LinkedHashSet<Column> commonPrimaryColumns,
                                                         Map<String, Signal> primarySignalToBaseEntityMap) {
        LinkedHashSet<SelectColumn> selectColumns = Sets.newLinkedHashSet();
        for (Column column : commonPrimaryColumns) {
            Signal signal = primarySignalToBaseEntityMap.get(column.getColumnName());
            selectColumns.add(new SelectColumn(signal.getSignalBaseEntity(), true, AggregationType.NA, signal.getName()));
        }

        for (Signal signal : signals) {
            String signalBaseEntity = signal.getSignalBaseEntity();
            AggregationType aggregationType = signal.getSignalDefinition().getAggregationType();
            aggregationType = aggregationType == null ? AggregationType.NA : aggregationType;
            selectColumns.add(new SelectColumn(signalBaseEntity, false, aggregationType, signal.getName()));
        }
        return selectColumns;
    }

    private LinkedHashSet<Table> getTableLinkedHashSet(Table table) {
        LinkedHashSet<Table> tableLinkedHashSet = Sets.newLinkedHashSet();
        tableLinkedHashSet.add(table);
        return tableLinkedHashSet;
    }

    private LinkedList<Column> getUpStreamTableColumns(Set<Signal> signals, LinkedHashSet<Column> commonPrimaryColumns,
                                                       Map<String, Signal> primarySignalToBaseEntityMap) {
        LinkedList<Column> upstreamTableColumns = Lists.newLinkedList();
        for (Column column : commonPrimaryColumns) {
            Signal signal = primarySignalToBaseEntityMap.get(column.getColumnName());
            String baseEntityId = signal.getSignalBaseEntity();
            Object defaultValue = signal.getSignalDefinition().getDefaultValue();
            upstreamTableColumns.add(HiveColumn.builder().name(baseEntityId).defaultValue(defaultValue).build());
        }

        for (Signal signal : signals) {
            Object defaultValue = signal.getSignalDefinition().getDefaultValue();
            String signalBaseEntity = signal.getSignalBaseEntity();
            upstreamTableColumns.add(HiveColumn.builder().name(signalBaseEntity).defaultValue(defaultValue).build());
        }
        return upstreamTableColumns;
    }

    private SelectQuery getSelectQuery(DataTable dataTable, DataFrame dataFrame, DataFrameGenerateRequest request, Set<Signal> signals,
                                       LinkedHashSet<Column> commonPrimaryColumns, Set<String> groupBySignalBaseEntitySet,
                                       Map<String, Signal> primarySignalToBaseEntityMap, Set<DataFrameScope> scopeSet,
                                       Map<String /*SignalBaseEntity*/, Pair<String, SignalDefinition>> baseEntityToSignalDefinition) throws HiveGeneratorException {

        Map<Table, Long> tableToRefreshId = Maps.newHashMap();
        LinkedHashSet<SelectColumn> selectColumns = getSelectColumns(signals, commonPrimaryColumns, primarySignalToBaseEntityMap);
        LinkedList<Column> upstreamTableColumns = getUpStreamTableColumns(signals, commonPrimaryColumns, primarySignalToBaseEntityMap);
        Table table = getTable(request, tableToRefreshId, upstreamTableColumns, dataTable);
        LinkedHashSet<Table> tableLinkedHashSet = getTableLinkedHashSet(table);
        Set<Constraint> constraintSet = constraintHelper.buildConstraintSet(baseEntityToSignalDefinition, scopeSet, tableToRefreshId, dataTable, dataFrame);
        Set<String> groupByColumns = Objects.isNull(groupBySignalBaseEntitySet) ? new HashSet<>() : groupBySignalBaseEntitySet;
        return new SelectQuery(new HashSet<>(), constraintSet, groupByColumns, tableLinkedHashSet, selectColumns, QueryBehaviorType.CARTESIAN_MULTIPLICATION);
    }


    private Table createIntermediateFactTable(DataTable dataTable, Set<String> groupBySignalBaseEntitySet, Set<Signal> signals,
                                              LinkedHashSet<Column> lowestPrimaryDescendantColumn, Long runId, String databaseName,
                                              DataFrameGenerateRequest request, Set<DataFrameScope> scopeSet, DataFrame dataframe, List<Signal> primarySignals) throws HiveGeneratorException {

        //Appending name of the first signal, so that name of the table is different everytime incase where multiple signals are dervied from the same table name.
        String factTableName = getFactTableName(runId, dataTable.getId(), signals.iterator().next().getName(), dataframe.getName());
        Map<String, Signal> primarySignalToBaseEntityMap = primarySignals.stream().collect(toMap(Signal::getName, Function.identity()));
        LinkedHashSet<Column> commonPrimaryColumns = getCommonPrimaryColumns(lowestPrimaryDescendantColumn, dataTable,
                groupBySignalBaseEntitySet, primarySignalToBaseEntityMap);

        LinkedList<Column> columnSet = new LinkedList<>(commonPrimaryColumns);
        Map<String /*SignalBaseEntity*/, Pair<String, SignalDefinition>> baseEntityToSignalDefinition = Maps.newHashMap();
        for (Signal signal : signals) {
            final Object defaultValue = signal.getSignalDefinition().getDefaultValue();
            columnSet.add(HiveColumn.builder().name(signal.getName()).isPartition(false)
                    .columnDataType(ColumnDataType.from(signal.getSignalDataType())).defaultValue(defaultValue).build());
            final String signalBaseEntity = signal.getSignalBaseEntity();
            baseEntityToSignalDefinition.put(signalBaseEntity, Pair.with(dataTable.getId(), signal.getSignalDefinition()));
        }

        //Create interim fact table and execute it
        SelectQuery selectQuery = getSelectQuery(dataTable, dataframe, request, signals, commonPrimaryColumns,
                groupBySignalBaseEntitySet, primarySignalToBaseEntityMap, scopeSet, baseEntityToSignalDefinition);
        return HiveTable.builder().db(databaseName).name(factTableName).columns(columnSet).subQuery(selectQuery).build();
    }


    private LinkedHashSet<Column> getLowestCommonDescendantColumns(DataFrame dataframe) {
        return dataframe.getSignalGroup().getSignalMetas().stream().filter(SignalGroup.SignalMeta::isPrimary)
                .map(this::getHiveColumn).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private HiveColumn getHiveColumn(SignalGroup.SignalMeta signalMeta) {
        Signal signal = signalMeta.getSignal();
        return HiveColumn.builder().name(signal.getName()).columnDataType(ColumnDataType.from(signal.getSignalDataType())).build();
    }

    private Table getTable(DataFrameGenerateRequest request, Map<Table, Long> tableToRefreshId, LinkedList<Column> columnSet,
                           DataTable upstreamDataTable) {
        Table upstreamTable;
        String upstreamTableDatabase = upstreamDataTable.getDataSource().getConfiguration().getDatabase();
        if (MapUtils.isNotEmpty(request.getTables())) {
            upstreamTable = HiveTable.builder().name(upstreamDataTable.getId()).db(upstreamTableDatabase).columns(columnSet).build();
            String upStreamTableKey = format("%s.%s", upstreamTable.getDbName(), upstreamTable.getTableName());
            tableToRefreshId.put(upstreamTable, request.getTables().get(upStreamTableKey));
        } else {
            throw new IllegalArgumentException("Cannot create Table with empty tables list");
        }
        return upstreamTable;
    }

    private Map<Pair<DataTable, Set<String/*SignalBaseEntity*/>>, Set<Signal>> getDataTableAndGroupByMapToSignals(Map<DataTable, LinkedHashSet<Signal>> dataTableSetMap) {
        Map<Pair<DataTable, Set<String/*SignalBaseEntity*/>>, Set<Signal>> result = new HashMap<>();
        dataTableSetMap.forEach((k, v) -> {
            for (Signal signal : v) {
                Pair<DataTable, Set<String/*SignalBaseEntity*/>> dataTableSetPair = new Pair<>(k, signal.getSignalDefinition().getGroupBy());
                Set<Signal> signalSet;
                if (result.containsKey(dataTableSetPair)) {
                    result.get(dataTableSetPair).add(signal);
                } else {
                    signalSet = new HashSet<>();
                    signalSet.add(signal);
                    result.put(dataTableSetPair, signalSet);
                }
            }
        });
        return result;
    }

    private LinkedHashSet<Column> getCommonPrimaryColumns(LinkedHashSet<Column> primaryColumns, DataTable dataTable,
                                                          Set<String> groupBy, Map<String, Signal> primaryToBaseEntityMap) throws HiveGeneratorException {
        Map<String, Column> primaryColumnIdToColumnMap = primaryColumns.stream().collect(toMap(column ->
                primaryToBaseEntityMap.get(column.getColumnName()).getSignalBaseEntity(), column -> column));

        Set<String> signalBaseEntitySet = getSignalBaseEntitySet(dataTable, groupBy);
        LinkedHashSet<Column> commonPrimaryColumns = signalBaseEntitySet.stream().filter(primaryColumnIdToColumnMap::containsKey).map(primaryColumnIdToColumnMap::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (commonPrimaryColumns.size() == 0)
            throw new HiveGeneratorException(format("Fact table %s.%s doesn't have any of the granularity columns",
                    dataTable.getDataSource().getConfiguration().getDatabase(), dataTable.getId()));
        return commonPrimaryColumns;
    }

    private Set<String> getSignalBaseEntitySet(DataTable dataTable, Set<String> groupBy) throws HiveGeneratorException {
        String fullTableName = dataTable.getDataSource().getId() + Constants.dot + dataTable.getId();
        try {
            return Objects.isNull(groupBy) ? Sets.newHashSet(metaStoreClient.getColumnNames(fullTableName)) : groupBy;
        } catch (TException | TableNotFoundException e) {
            throw new HiveGeneratorException("Failed to get column names for table: " + fullTableName, e);
        }
    }

    private void createPartitionColumns(Table granularityTable, LinkedHashSet<Signal> partitions, Set<String> seenColumnIds,
                                        LinkedHashSet<String> partitionIds, LinkedHashSet<Column> partitionColumns) throws HiveGeneratorException {
        for (Signal partitionSignal : partitions) {
            Column partitionColumn = HiveColumn.builder().name(partitionSignal.getName()).isPartition(true).
                    columnDataType(ColumnDataType.from(partitionSignal.getSignalDataType())).build();
            seenColumnIds.add(partitionColumn.getColumnName());
            partitionColumns.add(partitionColumn);
            partitionIds.add(partitionSignal.getName());
        }
    }

    private SelectQuery getSelectQuery(Table granularityTable, DataFrame dataFrame, LinkedList<Column> dataFrameColumns,
                                       Set<Table> intermediateTables, Set<String> seenColumnIds, List<Set<Table>> masterTableSet) {
        Set<Join> joinSet = Sets.newLinkedHashSet();
        LinkedHashSet<SelectColumn> selectColumns = Sets.newLinkedHashSet();
        LinkedHashSet<Table> allTables = new LinkedHashSet<>(intermediateTables);
        allTables.add(granularityTable);

        LinkedHashSet<Pair<Column, SelectColumn>> columns = getColumns(granularityTable, dataFrame, joinSet, seenColumnIds, masterTableSet);
        columns.forEach(pair -> {
            dataFrameColumns.add(pair.getValue0());
            selectColumns.add(pair.getValue1());
        });
        return new SelectQuery(joinSet, Sets.newHashSet(), Sets.newHashSet(), allTables, selectColumns, QueryBehaviorType.LEFT_OUTER_JOIN);
    }

    private LinkedHashSet<Pair<Column, SelectColumn>> getColumns(Table granularityTable, DataFrame dataFrame, Set<Join> joinSet,
                                                                 Set<String> seenColumnIds, List<Set<Table>> masterTableSet) {
        Set<String> primaryColumnIds = granularityTable.getColumns().stream().map(Column::getColumnName).collect(Collectors.toSet());
        Map<String, String> signalToBaseEntityMap = dataFrame.getSignalGroup().getSignalMetas().stream()
                .collect(toMap(p -> p.getSignal().getName(), q -> q.getSignal().getSignalBaseEntity()));
        LinkedHashSet<Pair<Column, SelectColumn>> columns = Sets.newLinkedHashSet();
        Table lastTable = null;

        for (Set<Table> tableSet : masterTableSet) {
            List<Pair<Column, SelectColumn>> columnList = Lists.newArrayList();
            for (Table table : tableSet) {
                for (Column column : table.getColumns()) {
                    if (!seenColumnIds.contains(column.getColumnName())) {
                        boolean coalesce = false;
                        if (primaryColumnIds.contains(column.getColumnName()))
                            coalesce = true;
                        columnList.add(Pair.with(column, new SelectColumn(column.getColumnName(), coalesce, AggregationType.NA, column.getColumnName())));
                        seenColumnIds.add(column.getColumnName());
                    }
                }
                if (lastTable != null) {
                    Join join = HiveJoin.builder().leftTable(granularityTable).rightTable(table).joinColumns(getJoinColumns(granularityTable.getColumns(), table.getColumns(), signalToBaseEntityMap)).build();
                    joinSet.add(join);
                }
                lastTable = table;
            }
            columnList.sort(Comparator.comparing(o -> o.getValue0().getColumnName()));
            columns.addAll(columnList);
        }
        return columns;
    }

    private Map<Column, Column> getJoinColumns(List<? extends Column> columns1, List<? extends Column> columns2, final Map<String, String> signalToBaseEntityMap) {
        Map<String, Column> columnIdToColumn = Maps.newHashMap();
        Map<Column, Column> intersectingColumns = Maps.newHashMap();
        for (Column column : columns1) {
            String columnName = getColumnName(column, signalToBaseEntityMap);

            if (columnName != null) {
                columnIdToColumn.put(columnName, column);
            }
        }
        for (Column column : columns2) {
            String columnName = getColumnName(column, signalToBaseEntityMap);

            if (columnName != null && columnIdToColumn.containsKey(columnName)) {
                intersectingColumns.put(columnIdToColumn.get(columnName), column);
            }
        }
        return intersectingColumns;
    }

    private String getColumnName(Column column, final Map<String, String> signalToBaseEntityMap) {
        String columnName = column.getColumnName();

        String baseEntity = signalToBaseEntityMap.get(columnName);
        if (baseEntity != null) {
            return baseEntity;
        }
        return columnName;
    }

}
