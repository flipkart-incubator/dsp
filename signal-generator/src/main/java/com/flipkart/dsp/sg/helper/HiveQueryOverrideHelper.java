package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.exceptions.EncryptionException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.overrides.HiveQueryDataframeOverride;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.utils.*;
import com.flipkart.hadoopcluster2.models.DataTypeMapper;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.dsp.utils.Constants.*;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HiveQueryOverrideHelper {
    private final HdfsUtils hdfsUtils;
    private final HiveClient hiveClient;
    private final MetaStoreClient metaStoreClient;
    private final MiscConfig miscConfig;
    private final DataFrameOverrideHelper dataFrameOverrideHelper;

    public String getOverrideHash(HiveQueryDataframeOverride hiveQueryDataframeOverride) throws EncryptionException {
        String saltKey = miscConfig.getSaltKey();
        String queryHash = Encryption.encrypt(hiveQueryDataframeOverride.getQuery(), saltKey);
        String columnMappingHash = Encryption.encrypt(JsonUtils.DEFAULT.toJson(hiveQueryDataframeOverride.getColumnMapping()), saltKey);
        return Encryption.encrypt(queryHash + underscore + columnMappingHash, saltKey);
    }

    public String getCreateColumnQueryForHiveQuery(HiveQueryDataframeOverride hiveQueryDataframeOverride) {
        return hiveQueryDataframeOverride.getColumnMapping().entrySet().stream()
                .map(e -> String.format("%s %s", e.getKey(), DataTypeMapper.HiveMap.get(e.getValue())))
                .collect(Collectors.joining(","));
    }

    public void resolveQuery(HiveQueryDataframeOverride hiveQueryDataframeOverride) throws DataframeOverrideException {
        String resolvedQuery = hiveQueryDataframeOverride.getQuery();
        Map<String, Long> refreshIdMap = getLatestRefreshIds(hiveQueryDataframeOverride.getTableMapping());
        Map<String, String> tableMap = hiveQueryDataframeOverride.getTableMapping();
        // replace table versions first, else pattern will break
        for (String key : refreshIdMap.keySet())
            resolvedQuery = resolvedQuery.replaceAll("\\$" + key + "#version", refreshIdMap.get(key).toString());
        // replace table
        for (String key : tableMap.keySet())
            resolvedQuery = resolvedQuery.replaceAll("\\$" + key, tableMap.get(key));
        log.info("resolved query: " + resolvedQuery);
        hiveQueryDataframeOverride.setQuery(resolvedQuery);
    }

    public Map<String, Long> getLatestRefreshIds(Map<String, String> tableMapping) throws DataframeOverrideException {
        Map<String, Long> refreshIdMap = new HashMap<>();
        for (String key : tableMapping.keySet()) {
            try {
                LinkedHashMap<String, String> columnsMap = metaStoreClient.getColumnsWithType(tableMapping.get(key));
                if (!columnsMap.containsKey(Constants.REFRESH_ID)) continue;
                hiveClient.setQueue(PRODUCTION_HIVE_QUEUE);
                Long latestRefreshId = hiveClient.getLatestRefreshId(tableMapping.get(key));
                refreshIdMap.put(key, latestRefreshId);
            } catch (TableNotFoundException | TException e) {
                throw new DataframeOverrideException(e.getMessage());
            } catch (HiveClientException e) {
                throw new DataframeOverrideException("Failed to fetch latest refreshId for " + tableMapping.get(key));
            }
        }
        return refreshIdMap;
    }

    public Map<String, Long> executeQuery(String database, String tableName, String hiveQueue, String dataFrameName,
                                          HiveQueryDataframeOverride hiveQueryDataframeOverride)
            throws DataframeOverrideException, ValidationException, IOException {
        Long refreshId = new Date().getTime(); // generate refresh_id
        executeUserQuery(database, tableName, refreshId, hiveQueue, hiveQueryDataframeOverride.getQuery());
        Map<String, Long> tableInformation = new HashMap<>();
        tableInformation.put(HIVE_QUERY_DATABASE + dot + tableName, refreshId);
        return tableInformation;
    }

    private void executeUserQuery(String database, String tableName, Long refreshId,
                                  String hiveQueue, String query) throws DataframeOverrideException {
        try {
            String fullTableName = database + dot + tableName;
            String insertCommand = String.format("INSERT OVERWRITE TABLE %s partition(refresh_id=%s) %s", fullTableName, refreshId, query);
            hiveClient.setQueue(hiveQueue);
            hiveClient.executeQuery(insertCommand);
        } catch (HiveClientException e) {
            String errorMessage = "Exception while executing hive query.\nquery: " + query + "\n" + "errorMessage: " + e.getMessage();
            throw new DataframeOverrideException(errorMessage);
        }
    }

}

