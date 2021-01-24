package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.entities.sg.dto.DataFrameColumnType;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.sg.dto.DataFrameMultiKey;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataFrameScope;
import com.flipkart.dsp.models.sg.PredicateType;
import com.flipkart.dsp.models.sg.SGType;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants;
import com.flipkart.dsp.sg.utils.HivePathUtils;
import com.flipkart.dsp.sg.utils.StrictHashMap;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.HdfsUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileStatus;

import java.util.*;

import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;
import static com.flipkart.dsp.sg.utils.GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey;
import static com.flipkart.dsp.sg.utils.GeneratorUtils.getPartitionKeys;
import static java.lang.String.format;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SGUseCasePayloadBuilder {
    private final HdfsUtils hdfsUtils;
    private final HivePathUtils hivePathUtils;

    public SGUseCasePayload build(DataFrameGenerateRequest dataframeGenerateRequest, Table dataFrameTable, Set<DataFrameScope> dataframeScopeSet,
                                  DataFrame dataFrame) throws DataFrameGeneratorException {
        Long requestId = dataframeGenerateRequest.getRequestId();
        Map<String, Long> tables = dataframeGenerateRequest.getTables();
        Map<String, DataType> inputDataframeType = dataframeGenerateRequest.getInputDataFrameType();
        LinkedHashMap<String, DataFrameColumnType> columnMetaData = Maps.newLinkedHashMap();
        try {
            dataFrameTable.getColumns().forEach(column -> {
                if (column.isPartitionColumn()) {
                    columnMetaData.put(column.getColumnName(), DataFrameColumnType.IN);
                }
            });

            for (DataFrameScope dataframeScope : dataframeScopeSet) {
                if (!columnMetaData.containsKey(dataframeScope.getSignal().getName())) {
                    PredicateType predicateType = dataframeScope.getAbstractPredicateClause().getPredicateType();
                    try {
                        columnMetaData.put(dataframeScope.getSignal().getName(), DataFrameColumnType.from(predicateType));
                    } catch (Exception e) {
                        throw new HiveGeneratorException(format("Failed to convert PredicateDataType : %s to DataFrameColumnType", predicateType), e);
                    }
                }
            }

            String path = hivePathUtils.getHDFSPathFromHiveTable(requestId, dataFrameTable);

            if (SGType.NO_QUERY.equals(dataFrame.getSgType())) {
                path = hivePathUtils.getPartitionPath(path, dataFrameTable, tables);
            }
            LinkedHashSet<Column> partitionColumns = getPartitionKeys(dataFrameTable);
            StrictHashMap<List<DataFrameKey>, Set<String>> dataframes = new StrictHashMap<>();

            for (Column partitionColumn : partitionColumns)
                path += SLASH + partitionColumn.getColumnName() + EQUAL + STAR;
            path += SLASH + STAR;

            log.info("Partition Path " + path);
            for (FileStatus fileStatus : hdfsUtils.getFilesUnderDirectory(new org.apache.hadoop.fs.Path(path))) {
                boolean ignore = false;
                LinkedHashMap<String, DataFrameKey> key = Maps.newLinkedHashMap();
                String pathString = fileStatus.getPath().toUri().getPath();

                if (pathString.contains(Constants.hidden)) continue;
                StringTokenizer tokenizedBySlash = new StringTokenizer(pathString, SLASH);
                while (tokenizedBySlash.hasMoreElements()) {
                    String keyValueString = tokenizedBySlash.nextToken();
                    String[] keyValueArray = keyValueString.split(EQUAL);
                    if (keyValueArray.length > 1 && !Constants.REFRESH_ID.equalsIgnoreCase(keyValueArray[0])) {
                        if (HiveQueryConstants.DEFAULT_HIVE_PARTITION.equals(keyValueArray[1])) {
                            ignore = true;
                            continue;
                        }
                        key.put(keyValueArray[0], new DataFrameMultiKey(DataFrameColumnType.IN, Sets.newHashSet(keyValueArray[1])));
                    }
                }

                if (ignore) {
                    continue;
                }
                for (DataFrameScope dataframeScope : dataframeScopeSet) {
                    if (!key.containsKey(dataframeScope.getSignal().getName())) {
                        key.put(dataframeScope.getSignal().getName(), convertAbstractPredicateClauseToDataFrameKey(dataframeScope.getAbstractPredicateClause()));
                    }
                }

                if (inputDataframeType.containsKey(dataFrame.getName()) &&
                        DataType.DATAFRAME_PATH.toString().equalsIgnoreCase(inputDataframeType.get(dataFrame.getName()).toString())) {
                    Set<String> value = !dataframes.containsKey(new ArrayList<>(key.values())) ? Sets.newHashSet() : dataframes.get(Sets.newLinkedHashSet(key.values()));
                    value.add(fileStatus.getPath().toString());
                    dataframes.put(new ArrayList<>(key.values()), value);
                } else {
                    if (!dataframes.containsKey(new ArrayList<>(key.values()))) {
                        dataframes.put(new ArrayList<>(key.values()), Sets.newHashSet(fileStatus.getPath().getParent().toString()));
                    }
                }
            }

            if (MapUtils.isEmpty(dataframes))
                throw new DataFrameGeneratorException("No real dataframe generated for Dataframe : " + dataFrame.getName() + " because there is no data in it." );

            return new SGUseCasePayload(requestId, dataFrame.getName(), columnMetaData, dataframes);
        } catch (HDFSUtilsException e) {
            throw new DataFrameGeneratorException("No real dataframe generated for Dataframe : " + dataFrame.getName(), e);
        } catch (StrictHashMap.StrictHashMapException e) {
            if (e.getMessage().contains("already present") || e.getMessage().contains("is not present")) {
                throw new DataFrameGeneratorException("hdfs paths for dataframe " + dataFrame.getName() + " have multiple files!. " +
                        "Multiple Files Not supported for DATAFRAME_PATH, please migrate to DATAFRAME");
            } else
                throw e;
        }

    }
}
