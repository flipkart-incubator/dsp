package com.flipkart.hadoopcluster2.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@JsonDeserialize(builder = HivePersistedResultCoordinate.HivePersistedResultCoordinateBuilder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@ToString
public class HivePersistedResultCoordinate extends PersistedResultCoordinate {
    /**
     * Output hive table to which the result is persisted
     */
    private final String outputTable;
    /**
     * The directory the output table is poiting to
     */
    private final String outputDirLocation;
    /**
     * Number of files exposed by hive/HDFS in the directory as exposed by hive metastore
     */
    private final long numFiles;
    /**
     * Number of rows in the table as exposed by hive metastore
     */
    private final long numRows;
    /**
     * Total size of the data as exposed by hive metastore
     */
    private final long totalSize;
    /**
     * Uncompressed raw data size as exposed by hive metastore
     */
    private final long rawDataSize;

    @Builder
    private HivePersistedResultCoordinate(final QueryHandle queryHandle, final QueryResultMeta queryResultMeta,
                                          final String sourceName, final List<ResultRow> sampleData,
                                          final String outputTable, final String outputDirLocation,
                                          final long numFiles, final long numRows, final long totalSize, final long rawDataSize) {
        super(queryHandle, sourceName, sampleData, queryResultMeta);
        this.outputTable = outputTable;
        this.outputDirLocation = outputDirLocation;
        this.numFiles = numFiles;
        this.numRows = numRows;
        this.totalSize = totalSize;
        this.rawDataSize = rawDataSize;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class HivePersistedResultCoordinateBuilder {
    }
}
