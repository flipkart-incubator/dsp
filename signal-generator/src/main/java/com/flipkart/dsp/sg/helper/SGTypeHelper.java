package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.override.OverrideManager;
import com.flipkart.dsp.utils.Constants;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.isEmpty;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SGTypeHelper {

    private final PartitionHelper partitionHelper;
    private final OverrideManager overrideManager;
    private final MetaStoreClient metaStoreClient;

    private static final String ACCEPTED_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";


    public SGType calculateSGType(DataFrame dataframe, DataFrameGenerateRequest request, Set<DataFrameScope> finalDataFrameScope) throws TException, TableNotFoundException {
        final String dataframeName = dataframe.getName();
        boolean signalsFromSameTable = dataframe.getSignalGroup().getSignalMetas().stream()
                .map(SignalGroup.SignalMeta::getDataTable).distinct().count() <= 1;

        boolean hasConditionalSignal = dataframe.getSignalGroup().getSignalMetas().stream()
                .anyMatch(s -> s.getSignal().getSignalDefinition().getSignalValueType() == SignalValueType.CONDITIONAL);

        log.info("SGType Calculator: signalsFromSameTable: {}, hasConditionalSignal: {}", signalsFromSameTable, hasConditionalSignal);
        if (!signalsFromSameTable || hasConditionalSignal) return SGType.FULL_QUERY;

        List<String> baseEntities = dataframe.getSignalGroup().getSignalMetas().stream()
                .map(s -> s.getSignal().getSignalBaseEntity()).collect(Collectors.toList());
        baseEntities.add(Constants.REFRESH_ID);

        DataTable dataTable = dataframe.getSignalGroup().getSignalMetas().get(0).getDataTable();
        final Map<String, DataframeOverride> dataframeOverrideMap = request.getDataFrameOverrideMap();

        if (dataframeOverrideMap.containsKey(dataframeName)) {
            final DataframeOverride dataframeOverride = dataframeOverrideMap.get(dataframeName);

            dataTable = overrideManager.getDataTableForOverride(dataframe, dataframeOverride, dataTable);
            for (SignalGroup.SignalMeta signalMeta : dataframe.getSignalGroup().getSignalMetas()) {
                signalMeta.setDataTable(dataTable);
            }
        }

        final String database = dataTable.getDataSource().getId();
        if (request.getDataFrameOverrideMap().containsKey(dataframeName)) {
            return SGType.SINGLE_TABLE_QUERY;
        }

        String fullTableName = database + Constants.dot + dataTable.getId();
        List<String> columns = metaStoreClient.getColumnNames(fullTableName);

        LinkedHashSet<String> userGivenPartitions = Sets.newLinkedHashSet();
        userGivenPartitions.add(Constants.REFRESH_ID); //DSP won't have refresh_id partition in its signal as that is default
        userGivenPartitions.addAll(dataframe.getPartitions());

        log.debug("fullTableName for existing Partitions: " + fullTableName);
        Set<String> requiredPartitions = userGivenPartitions.stream().filter(c -> columns.contains(c)).collect(Collectors.toSet());
        Set<String> existingPartitions = metaStoreClient.getPartitionedColumnNames(fullTableName);
        String storageFormat = metaStoreClient.getHiveTableStorageFormat(fullTableName);
        log.debug("requiredPartitions: " + requiredPartitions);
        log.debug("existingPartitions: " + existingPartitions);
        boolean doesPartitionMatch = partitionHelper.doesPartitionMatches(requiredPartitions, existingPartitions);
        boolean emptyDataFrameScope = isEmpty(finalDataFrameScope);
        boolean signalsToColumns = columns.containsAll(baseEntities);
        boolean columnsToSignals = baseEntities.containsAll(columns);
        final boolean storageFormatMatch = storageFormat.contains(ACCEPTED_INPUT_FORMAT);
        log.info("SGType Calculator: doesPartitionMatch: {}, emptyDataFrameScope: {}, signalsToColumns: {}, columnsToSignals: {}, storageFormat: {}",
                doesPartitionMatch, emptyDataFrameScope, signalsToColumns, columnsToSignals, storageFormatMatch);

        if (signalsToColumns && columnsToSignals && doesPartitionMatch && emptyDataFrameScope && storageFormatMatch
                && verifySignalSequence(dataframe.getDataFrameConfig().getVisibleSignals(), columns, userGivenPartitions,
                request.getInputDataFrameType(), dataframeName)) {
            return SGType.NO_QUERY;
        } else {
            return SGType.SINGLE_TABLE_QUERY;
        }
    }

    // Verify if the table sequence and the sequence given in visible signal is same given Datatype is set to DATAFRAME
    private boolean verifySignalSequence(LinkedHashSet<Signal> visibleSignals, List<String> columns,
                                         LinkedHashSet<String> DFSupportedPartitionColumnNames,
                                         Map<String, DataType> inputDataframeType, String dataframeName) {
        boolean isNoQuery = true;
        if (inputDataframeType.containsKey(dataframeName) && inputDataframeType.get(dataframeName) == DataType.DATAFRAME) {
            List<String> unPartitionedColumns = columns.stream().filter(s -> !DFSupportedPartitionColumnNames.contains(s)).collect(Collectors.toList());
            LinkedHashSet<Signal> unpartitionedVisibleSignals = visibleSignals.stream().filter(s ->
                    !DFSupportedPartitionColumnNames.contains(s.getSignalBaseEntity())).collect(Collectors.toCollection(LinkedHashSet::new));

            int i = 0;
            for (Signal signal : unpartitionedVisibleSignals) {
                if (!signal.getSignalBaseEntity().equals(unPartitionedColumns.get(i))) {
                    isNoQuery = false;
                }
                i++;
            }
            if (!isNoQuery) {
                log.info("Expected column sequence for NO_QUERY " + unPartitionedColumns.toString() + " Column sequence which is available "
                        + unpartitionedVisibleSignals.stream().map(Signal::getSignalBaseEntity).collect(Collectors.toList()).toString() +
                        " .Because of mismatch of column sequence query is converted to SINGLE_QUERY");
            }
        }
        return isNoQuery;
    }
}
