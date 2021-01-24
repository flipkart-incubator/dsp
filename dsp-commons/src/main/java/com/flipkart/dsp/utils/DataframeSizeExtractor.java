package com.flipkart.dsp.utils;

import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.sg.dto.DataFrameColumnType;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Triplet;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataframeSizeExtractor {

    private final HdfsUtils hdfsUtils;

    public List<Triplet<ConfigPayload, Long, Long>> calculateDFSize(List<ConfigPayload> payloadList) {
        if (payloadList == null || payloadList.isEmpty()) {
            throw new IllegalArgumentException("Payload list cannot be empty or null");
        }

        List<Triplet<ConfigPayload, Long, Long>> payLoadTripletList = new ArrayList<>();
        for (ConfigPayload configPayload : payloadList) {
            Long trainingDataframeSize = getTotalDFSize(configPayload.getCsvLocation());
            Long executionDataframeSize = getTotalDFSize(configPayload.getFutureCSVLocation());
            payLoadTripletList.add(Triplet.with(configPayload, trainingDataframeSize, executionDataframeSize));
        }
        return payLoadTripletList;
    }

    private Long getTotalDFSize(Map<String, String> csvLocations) {
        if (Objects.isNull(csvLocations) || csvLocations.isEmpty())
            return 0L;
        return csvLocations.values().stream().map(this::getFolderSize).reduce(Long::sum).get();
    }

    private Long getFolderSize(String path) {
        try {
            return hdfsUtils.getFolderSize(path);
        } catch (IOException e) {
            e.printStackTrace();
            log.info("Failed to get csv size for path: " + path);
            return 0L;
        }
    }

    public long getDataframeSize(SGUseCasePayload payload) {
        try {
            Map<List<DataFrameKey>, Set<String>> dataframes = payload.getDataframes();
            LinkedHashMap<String, DataFrameColumnType> columnMetaData = payload.getColumnMetaData();
            if (!dataframes.isEmpty()) {
                Map.Entry<List<DataFrameKey>, Set<String>> dataframe = dataframes.entrySet().iterator().next();
                String dataframePath = dataframe.getValue().iterator().next();
                String dataframeRootPath = null;
                try {
                    String[] split = dataframePath.split("/");
                    int lastElementIndex = split.length - columnMetaData.size();
                    if (!hdfsUtils.isDirectory(dataframePath)) {
                        lastElementIndex -= 1;
                    }
                    String lastElement = split[lastElementIndex];
                    dataframeRootPath = dataframePath.split(lastElement)[0];
                } catch (Exception e) {
                    log.warn("Failed to figure out dataframe root path for:" + dataframePath, e);
                }

                if (dataframeRootPath == null || dataframeRootPath.isEmpty()) {
                    return 0;
                } else {
                    return hdfsUtils.getFolderSize(dataframeRootPath);
                }
            } else {
                return 0;
            }
        } catch (IOException e) {
            log.warn("Failed to update Dataframe Size", e);
        }
        return 0;
    }

}
