package com.flipkart.dsp.utils;

import com.flipkart.dsp.exceptions.MetaStoreException;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;

import java.util.*;

import static java.lang.String.format;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HivePathUtils {
    private final MetaStoreClient metadataClient;

    public String getHDFSPathFromHiveTable(Long requestId, String dbName, String tableName) throws MetaStoreException {
        String path;
        try {
            path = metadataClient.getTableLocation(dbName + "." + tableName);
        } catch (TException | TableNotFoundException e) {
            String message = format("Could not build path for UseCase Payload. RequestEntity ID : %s DB Name : %s Table Name : %s", requestId, dbName, tableName);
            throw new MetaStoreException(message, e);
        }
        return path;
    }

    public String getPartitionPath(String path, String dbName, String tableName, Map<String, Long> tables) {

        String completeTableName = dbName + Constants.dot + tableName;
        Optional<String> tableName1 = tables.keySet()
                .stream().filter(table -> table.equalsIgnoreCase(completeTableName)).findFirst();
        Long refreshId;
        if (tableName1.isPresent()) {
            refreshId = tables.get(tableName1.get());
        } else {
            throw new NoSuchElementException("No refresh-id found for table : " + dbName + "." + tableName + " in input data tables. tables: " + tables);
        }
        return path + "=" + refreshId;
    }
}
