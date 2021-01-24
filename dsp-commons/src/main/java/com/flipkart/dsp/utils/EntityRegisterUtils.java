package com.flipkart.dsp.utils;

import com.flipkart.dsp.entities.misc.EntityIngestedNotificationResponse;
import com.google.common.base.Splitter;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 */
@Slf4j
@Singleton
public class EntityRegisterUtils {
    private static final Splitter SPLITTER = Splitter.on(".").trimResults().omitEmptyStrings();

    public static EntityIngestedNotificationResponse getEntityIngestedNotificationDto(long refreshId, List<String> tables){
        EntityIngestedNotificationResponse entityIngestedNotificationDTO = new EntityIngestedNotificationResponse();
        Map<String, EntityIngestedNotificationResponse.IngestionAttributes> entities = com.google.common.collect.Maps.newHashMap();
        for(String hiveTableName : tables) {
            entities.put(getEntityName(hiveTableName), new EntityIngestedNotificationResponse.IngestionAttributes(String.valueOf(refreshId), "None"));/* "None" is assigned to comments*/
        }
        if(entities.isEmpty()) {
            return null;
        }
        entityIngestedNotificationDTO.setEntities(entities);
        log.info("Ingestion entity partition in DCP : {}", JsonUtils.DEFAULT.toJson(entityIngestedNotificationDTO));
        return entityIngestedNotificationDTO;
    }

    private static String getEntityName(String hiveTableName) {
        List<String> tokens = SPLITTER.splitToList(hiveTableName);
        return tokens.size() > 1 ? tokens.get(1) : tokens.get(0);
    }

}
