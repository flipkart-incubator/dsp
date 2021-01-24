package com.flipkart.dsp.utils;

import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
public class DataframeUtils {

    /**
     * @return Reference dataframe Audit for parallelising workflow
     * <p>
     * UseCase can have multiple data frames. All data frames need not be partitioned at the same level, as they might not have required columns.
     * We need a reference dataframe for parallelising workflow. Reference dataframe is one which has all the partitions of the UseCase
     */
    public static SGUseCasePayload getReferenceDFForParallelism(String workflowName, List<String> partitions,
                                                                List<SGUseCasePayload> payloads) {
        Optional<SGUseCasePayload> referencePayloadOptional = payloads.stream()
                        .filter(payload -> doesDFContainAllPartitions(partitions, payload)).findFirst();

        if (!referencePayloadOptional.isPresent()) {
            log.error(
                    "None of the data frames partitions matching with use case partition!! Workflow Name : {}",
                    workflowName);
            throw new RuntimeException(
                    "None of the data frames partitions matching with use case partition!!");
        }
        return referencePayloadOptional.get();
    }

    private static boolean doesDFContainAllPartitions(List<String> partitions, SGUseCasePayload payload) {
        return  partitions.size() == payload.getColumnMetaData().keySet().size() &&
                payload.getColumnMetaData().keySet().containsAll(partitions);
    }
}
