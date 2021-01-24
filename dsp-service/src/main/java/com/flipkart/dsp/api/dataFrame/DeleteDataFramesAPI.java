package com.flipkart.dsp.api.dataFrame;

import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.SignalGroup;
import com.flipkart.dsp.exceptions.ConfigurableSGException;
import com.flipkart.dsp.dao.DataFrameDAO;
import com.flipkart.dsp.validation.DataFrameValidator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DeleteDataFramesAPI {
    private final SignalActor signalActor;
    private final WorkFlowActor workFlowActor;
    private final DataTableActor dataTableActor;
    private final DataFrameActor dataFrameActor;
    private final SignalGroupActor signalGroupActor;
    private final DataFrameValidator dataFrameValidator;
    private final SignalGroupToSignalActor signalGroupToSignalActor;

    public String deleteDataFrames(List<Long> dataFrameIds) {
        List<DataFrame> dataFramesToBeDeleted = new ArrayList<>();
        List<DataFrame> sgDataFrames = dataFrameValidator.validateDataFrames(dataFrameIds);
        sgDataFrames.forEach(sgDataFrame -> {
            if (workFlowActor.getWorkFlowCount(sgDataFrame.getId()) == 0) dataFramesToBeDeleted.add(sgDataFrame);
            else throw new ConfigurableSGException("DataFrame Id " + sgDataFrame.getId() + " is Linked to a workflow. Can't delete it");
        });
        performDeletion(dataFramesToBeDeleted);
        return "Following dataFrames are deleted " + dataFrameIds.toString();
    }

    private void performDeletion(List<DataFrame> dataFramesToBeDeleted) {
        List<String> signalGroupToBeDeleted = identifySignalGroupToBeDeleted(dataFramesToBeDeleted);
        Map<String, Integer> probableSignalDeletionCount = new HashMap<>();
        Map<String, Integer> probableDataTableDeletionCount = new HashMap<>();
        getProbableDeletionCount(signalGroupToBeDeleted, probableSignalDeletionCount, probableDataTableDeletionCount);
        List<String> signalToBeDeleted = getSignalNamesForDeletion(probableSignalDeletionCount);
        List<String> actualDataTableDeletionCount = DataTableToDelete(probableDataTableDeletionCount);
        purgeEntries(dataFramesToBeDeleted, signalGroupToBeDeleted, signalToBeDeleted, actualDataTableDeletionCount);
    }

    private List<String> identifySignalGroupToBeDeleted(List<DataFrame> dataFramesToBeDeleted) {
        Map<String, List<Long>> signalGroupToDataFrame = new HashMap<>();

        dataFramesToBeDeleted.forEach(sgDataFrame -> {
            String signalGroupName = sgDataFrame.getSignalGroup().getId();
            if (signalGroupToDataFrame.containsKey(signalGroupName))
                signalGroupToDataFrame.get(signalGroupName).add(sgDataFrame.getId());
            else signalGroupToDataFrame.put(signalGroupName, Collections.singletonList(sgDataFrame.getId()));
        });

        return signalGroupToDataFrame.entrySet().stream().filter(entry -> dataFrameActor
                .getUsedDataFrameCount(entry.getKey()) == entry.getValue().size()).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    private void purgeEntries(List<DataFrame> dataFramesToBeDeleted, List<String> signalGroupToBeDeleted,
                              List<String> signalsToBeDeleted, List<String> actualDataTableDeletionCount) {
        signalGroupToSignalActor.deleteSignalGroup(signalGroupToBeDeleted);
        dataTableActor.deleteDataTable(actualDataTableDeletionCount);
        signalActor.deleteSignals(signalsToBeDeleted);
        dataFrameActor.deleteDataFrames(dataFramesToBeDeleted);
        signalGroupActor.deleteSignalGroups(signalGroupToBeDeleted);
    }

    private List<String> DataTableToDelete(Map<String, Integer> datatableTotalCount) {
        List<String> dataTableToBeDeleted = new ArrayList<>();
        for(Map.Entry<String, Integer> entry : datatableTotalCount.entrySet()) {
            Long count = signalGroupToSignalActor.getDataTableCount(entry.getKey());
            if(count.intValue() == entry.getValue()) {
                dataTableToBeDeleted.add(entry.getKey());
            }
        }
        return dataTableToBeDeleted;
    }

    private List<String> getSignalNamesForDeletion(Map<String, Integer> signalTotalCount) {
        List<String> signalToBeDeleted = new ArrayList<>();
        for(Map.Entry<String, Integer> entry : signalTotalCount.entrySet()) {
            Long count = signalGroupToSignalActor.getSignalCount(entry.getKey());
            if(count.intValue() == entry.getValue()) {
                signalToBeDeleted.add(entry.getKey());
            }
        }
        return signalToBeDeleted;
    }

    private void getProbableDeletionCount(List<String> signalGroupToBeDeleted,
                                          Map<String, Integer> probableSignalDeletionCount,
                                          Map<String, Integer> probableDataTableDeletionCount) {

        for(String signalGroupName : signalGroupToBeDeleted) {
            SignalGroup signalGroup = signalGroupActor.getSignalGroup(signalGroupName);
            for(SignalGroup.SignalMeta signalMeta : signalGroup.getSignalMetas()) {
                String signalId = signalMeta.getSignal().getName();
                String datableId = signalMeta.getDataTable().getId();
                if(probableSignalDeletionCount.containsKey(signalId)) {
                    int no =  probableSignalDeletionCount.get(signalId);
                    probableSignalDeletionCount.put(signalId, no+1);
                } else {
                    probableSignalDeletionCount.put(signalId, 1);
                }

                if(probableDataTableDeletionCount.containsKey(datableId)) {
                    int no =  probableDataTableDeletionCount.get(datableId);
                    probableDataTableDeletionCount.put(datableId, no+1);
                } else {
                    probableDataTableDeletionCount.put(datableId, 1);
                }
            }
        }
    }


}
