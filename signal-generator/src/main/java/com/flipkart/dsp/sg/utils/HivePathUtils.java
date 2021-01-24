package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.utils.Constants;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;

import java.util.*;

import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.EQUAL;
import static java.lang.String.format;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HivePathUtils {
    private final MetaStoreClient metadataClient;

    public String getHDFSPathFromHiveTable(Long requestId, Table dataFrameTable) throws HiveGeneratorException {
        String dbName = dataFrameTable.getDbName();
        String tableName = dataFrameTable.getTableName();
        String path;
        try {
            path = metadataClient.getTableLocation(dbName + "." + tableName);
        } catch (TException | TableNotFoundException e) {
            String message = format("Could not build path for UseCase Payload. Request ID : %s DB Name : %s Table Name : %s", requestId, dbName, tableName);
            throw new HiveGeneratorException(message, e);
        }
        return path;
    }

    public String getPartitionPath(String path, Table dataFrameTable, Map<String, Long> tables) {

        String completeTableName = dataFrameTable.getDbName() + Constants.dot + dataFrameTable.getTableName();
        Optional<String> tableName = tables.keySet().stream().filter(table -> table.equalsIgnoreCase(completeTableName)).findFirst();
        Long refreshId;
        if (tableName.isPresent()) {
            refreshId = tables.get(tableName.get());
        } else {
            throw new NoSuchElementException("No refresh-id found for table : " + dataFrameTable.getDbName() + "." + dataFrameTable + " in input data tables. tables: " + tables);
        }
        return path + EQUAL + refreshId;
    }
}
