package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.core.ColumnDataType;
import com.flipkart.dsp.sg.hiveql.core.HiveColumn;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toCollection;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
class DataFrameBuilder {
    LinkedHashMap<DataTable, LinkedHashSet<Signal>> getDataTableToSignalMap(DataFrame dataframe, Boolean isPrimary) {
        return dataframe.getSignalGroup().getSignalMetas().stream().filter(sm -> sm.isPrimary() == isPrimary)
                .collect(Collectors.groupingBy(SignalGroup.SignalMeta::getDataTable, LinkedHashMap::new, mapping(SignalGroup.SignalMeta::getSignal, toCollection(LinkedHashSet::new))));
    }

    Map<String/*SignalBaseEntity*/, Pair<String/*DataTable*/, SignalDefinition>> getSignalDefinitionMap(LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignalMap) {
        Map<String, Pair<String, SignalDefinition>> signalDefinitionMap = Maps.newHashMap();

        for (Map.Entry<DataTable, LinkedHashSet<Signal>> dataTableLinkedHashSetEntry : tableToSignalMap.entrySet()) {
            for (Signal signal : dataTableLinkedHashSetEntry.getValue()) {
                signalDefinitionMap.put(signal.getSignalBaseEntity(),
                        Pair.with(dataTableLinkedHashSetEntry.getKey().getId(),
                                signal.getSignalDefinition()));
            }
        }
        return signalDefinitionMap;
    }

    LinkedList<Column> constructColumns(LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignalMap, List<String> partitions) {
        LinkedList<Column> columnLinkedList = Lists.newLinkedList();
        for (Map.Entry<DataTable, LinkedHashSet<Signal>> signalBooleanPair : tableToSignalMap.entrySet()) {
            for (Signal signal : signalBooleanPair.getValue()) {
                boolean isPartitioned = partitions.contains(signal.getName());
                columnLinkedList.add(HiveColumn.builder().name(signal.getName())
                        .columnDataType(ColumnDataType.from(signal.getSignalDataType()))
                        .isPartition(isPartitioned)
                        .build());
            }
        }
        return columnLinkedList;
    }

    LinkedHashSet<Table> getUpstreamDataTables(LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignalMap) {
        LinkedHashSet<Table> upStreamTables = new LinkedHashSet<>();
        for (Map.Entry<DataTable, LinkedHashSet<Signal>> dataTableLinkedHashSetEntry : tableToSignalMap.entrySet()) {
            LinkedList<Column> columnSet = Lists.newLinkedList();
            for (Signal signal : dataTableLinkedHashSetEntry.getValue()) {
                String upstreamColumnName = signal.getSignalBaseEntity();
                columnSet.add(HiveColumn.builder().name(upstreamColumnName).isPartition(false)
                        .columnDataType(ColumnDataType.from(signal.getSignalDataType())).build());
            }
            DataTable upstreamDataTable = dataTableLinkedHashSetEntry.getKey();
            Table upstreamTable = getTable(upstreamDataTable, columnSet);
            upStreamTables.add(upstreamTable);
        }
        return upStreamTables;
    }

    private Table getTable(DataTable upstreamDataTable, LinkedList<Column> columnSet) {
        String upstreamTableDatabase = upstreamDataTable.getDataSource().getConfiguration().getDatabase();
        return HiveTable.builder().name(upstreamDataTable.getId()).db(upstreamTableDatabase).columns(columnSet).build();
    }

    Map<Table, Long> getTableToRefreshId(DataFrameGenerateRequest request, LinkedHashSet<Table> upstreamDataTables) throws DataFrameGeneratorException {
        Map<Table, Long> tableToRefreshId = new HashMap<>();
        if (MapUtils.isNotEmpty(request.getTables())) {
            for (Table upstreamTable : upstreamDataTables) {
                String upStreamTableKey = format("%s.%s", upstreamTable.getDbName(), upstreamTable.getTableName());
                tableToRefreshId.put(upstreamTable, request.getTables().get(upStreamTableKey));
            }
        } else {
            throw new DataFrameGeneratorException("Table To RefreshId Map shouldn't be empty");
        }
        return tableToRefreshId;
    }

}
