package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.EventAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.dsp.models.sg.PredicateType.*;

public class EventAuditsDAO extends AbstractDAO<EventAuditEntity> {

    @Inject
    public EventAuditsDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<EventAuditEntity> getEvents(Long id, Long requestId, EventType eventType, List<EventLevel> eventLevels) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(id).predicateType(GREATER_THAN).build());
        sqlParams.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("eventLevel").paramValues(eventLevels).predicateType(IN).build());
        sqlParams.add(SqlParam.builder().paramName("eventType").paramValues(eventType).predicateType(EQUAL).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("id", "asc");
        return createQuery(EventAuditEntity.class, sqlParams, orderByMap).list();
    }

    public List<EventAuditEntity> getEvents(long requestId, EventLevel eventLevel, EventType eventType) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("eventLevel").paramValues(eventLevel).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("eventType").paramValues(eventType).predicateType(EQUAL).build());
        return createQuery(EventAuditEntity.class, sqlParams).list();
    }

}
