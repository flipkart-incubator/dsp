package com.flipkart.dsp.helper;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.sg.dto.DataFrameMultiKey;
import com.flipkart.dsp.entities.sg.dto.SGJobOutputPayload;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.HiveOutputLocation;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.DataframeUtils;
import com.google.common.collect.Iterables;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowOutputTableHelper {
    private final HiveClient hiveClient;
    private final MetaStoreClient metaStoreClient;

    /**
     * Update the hive partitions. Supported operations are add partition or drop partition
     */
    @Timed
    @Metered
    private void updatePartitions(SGJobOutputPayload payload, String tableName,
                                  WorkflowDetails workflowDetails, String refreshId,
                                  String operation, List<String> partitions) throws HiveClientException {
        Workflow workflow = workflowDetails.getWorkflow();
        String workflowName = workflow.getName();
        SGUseCasePayload refPayload = DataframeUtils.getReferenceDFForParallelism(workflowName, partitions, new ArrayList<>(payload.getSgUseCasePayloadSet()));
        Map<List<DataFrameKey>, Set<String>> dataframes = refPayload.getDataframes();
        Set<LinkedHashMap<String, String>> partitionMapSet = dataframes.keySet().stream().map(
                dataFrameKeys -> getPartitionsMap(refreshId, dataFrameKeys, partitions)).collect(toSet());
        hiveClient.updatePartitions(tableName, partitionMapSet, operation);
    }

    private LinkedHashMap<String, String> getPartitionsMap(String refresh_id, List<DataFrameKey> dataFrameKeys, List<String> partitions) {
        LinkedHashMap<String, String> partitionsMap = new LinkedHashMap<>();
        partitionsMap.put(Constants.REFRESH_ID, refresh_id);

        dataFrameKeys.stream().filter(dataFrameKey -> partitions.contains(dataFrameKey.getName())).forEach(
                dataFrameKey -> {
                    String location = Iterables.get(((DataFrameMultiKey) dataFrameKey).getValues(), 0);
                    partitionsMap.put(dataFrameKey.getName(), location);

                }
        );
        return partitionsMap;
    }



    private List<String> getHiveOutputTables(Set<ScriptVariable> outputVariables) {
        return outputVariables.stream().filter(output -> (output.getDataType().equals(DataType.DATAFRAME))
                && output.getOutputLocationDetailsList() != null && !output.getOutputLocationDetailsList().isEmpty())
                .flatMap(scriptVariable -> scriptVariable.getOutputLocationDetailsList().stream())
                .filter(outputLocation -> outputLocation instanceof HiveOutputLocation)
                .map(outputLocation -> ((HiveOutputLocation) outputLocation).getDatabase() + dot + ((HiveOutputLocation) outputLocation).getTable())
                .collect(toList());
    }

}
