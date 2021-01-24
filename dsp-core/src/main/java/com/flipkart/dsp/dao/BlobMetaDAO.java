package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.BlobMetaEntity;
import com.flipkart.dsp.entities.enums.BlobStatus;
import com.flipkart.dsp.entities.enums.BlobType;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

public class BlobMetaDAO extends AbstractDAO<BlobMetaEntity> {

    @Inject
    public BlobMetaDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public BlobMetaEntity getBlobsByRequestIdAndType(String requestId, BlobType type) {
        List<SqlParam> sqlParamList = new ArrayList<>();
        sqlParamList.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParamList.add(SqlParam.builder().paramName("type").paramValues(type).predicateType(EQUAL).build());
        return createQuery(BlobMetaEntity.class, sqlParamList).uniqueResult();
    }

    public BlobMetaEntity getCompletedBlobsByRequestIdAndType(String requestId, BlobType type) {
        List<SqlParam> sqlParamList = new ArrayList<>();
        sqlParamList.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParamList.add(SqlParam.builder().paramName("type").paramValues(type).predicateType(EQUAL).build());
        sqlParamList.add(SqlParam.builder().paramName("status").paramValues(BlobStatus.COMPLETED).predicateType(EQUAL).build());
        return createQuery(BlobMetaEntity.class, sqlParamList).uniqueResult();
    }

    public BlobMetaEntity updateBlobStatus(Long id, BlobStatus status) {
        BlobMetaEntity blobMetaEntity = get(id);
        if (blobMetaEntity != null) {
            blobMetaEntity.setStatus(status);
            return persist(blobMetaEntity);
        }
        return null;
    }
}
