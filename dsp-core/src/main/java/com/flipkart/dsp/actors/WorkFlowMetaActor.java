package com.flipkart.dsp.actors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.db.entities.WorkflowMetaEntity;
import com.flipkart.dsp.entities.workflow.WorkflowMeta;
import com.flipkart.dsp.utils.JsonUtils;

import java.util.List;
import java.util.Objects;

/**
 * +
 */
public class WorkFlowMetaActor implements SGActor<WorkflowMetaEntity, WorkflowMeta> {
    @Override
    public WorkflowMetaEntity unWrap(WorkflowMeta dto) {
        if (Objects.nonNull(dto)) {
            String callBacks = Objects.isNull(dto.getCallbackEntities()) ? null : JsonUtils.DEFAULT.toJson(dto.getCallbackEntities());
            return WorkflowMetaEntity.builder().callbackEntities(callBacks)
                    .callbackUrl(dto.getCallbackUrl()).hiveQueue(dto.getHiveQueue())
                    .killTimeForNotification(216000000L).mesosQueue(dto.getMesosQueue())
                    .warningTimeForNotification(10800000L).build();
        }
        return null;
    }

    @Override
    public WorkflowMeta wrap(WorkflowMetaEntity entity) {
        if (Objects.nonNull(entity)) {
            List<String> callBacks = Objects.isNull(entity.getCallbackEntities()) ? null :
                    JsonUtils.DEFAULT.fromJson(entity.getCallbackEntities(), new TypeReference<List<String>>() {});
            return WorkflowMeta.builder().id(entity.getId()).callbackEntities(callBacks)
                    .callbackUrl(entity.getCallbackUrl()).hiveQueue(entity.getHiveQueue())
                    .killTimeForNotification(entity.getKillTimeForNotification()).mesosQueue(entity.getMesosQueue())
                    .warningTimeForNotification(entity.getWarningTimeForNotification()).build();
        }
        return null;
    }

}
