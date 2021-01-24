package com.flipkart.dsp.actors;

import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.dao.BlobMetaDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.BlobMetaEntity;
import com.flipkart.dsp.dto.*;
import com.flipkart.dsp.entities.enums.BlobStatus;
import com.flipkart.dsp.entities.enums.BlobType;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.utils.HdfsUtils;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.HDFS_CLUSTER_PREFIX;
import static com.flipkart.dsp.utils.Constants.slash;


@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class BlobMetaActor {

    @Inject
    MiscConfig miscConfig;

    private final BlobMetaDAO blobMetaDAO;
    private final HdfsUtils hdfsUtils;
    private final TransactionLender transactionLender;

    public BlobMetaEntity unWrap(BlobRequest request, String location) {
        if (Objects.nonNull(request)) {
            return BlobMetaEntity.builder()
                    .requestId(request.getRequestId())
                    .status(request.getStatus())
                    .type(request.getType())
                    .location(location)
                    .build();
        }
        return null;
    }

    public BlobResponse wrap(BlobMetaEntity entity) {
        if (Objects.nonNull(entity)) {
            return BlobResponse.builder()
                    .requestId(entity.getRequestId())
                    .status(entity.getStatus())
                    .type(entity.getType())
                    .location(entity.getLocation())
                    .build();
        }
        return null;
    }

    public BlobResponse persist(BlobRequest blobRequest) throws HDFSUtilsException {

        AtomicReference<BlobMetaEntity> blobMetaEntityAtomicReference = new AtomicReference<>(null);
        String blobLocation = HDFS_CLUSTER_PREFIX + miscConfig.getBlobBasePath() + slash + blobRequest.getType() + slash + blobRequest.getRequestId() + slash;
        BlobResponse blobResponse = getBlobsByRequestIdAndType(blobRequest.getRequestId(), blobRequest.getType());
        if (blobResponse != null) {
            if (blobResponse.getStatus().equals(BlobStatus.COMPLETED)) {
                return blobResponse;
            } else if (blobResponse.getStatus().equals(BlobStatus.STARTED)) {
                for (int i = 0; i < 10; i++) {
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                    blobResponse = getBlobsByRequestIdAndType(blobRequest.getRequestId(), blobRequest.getType());
                    if (blobResponse.getStatus().equals(BlobStatus.COMPLETED)) {
                        return blobResponse;
                    }
                }
            }
        }
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                blobMetaEntityAtomicReference.set(blobMetaDAO.persist(unWrap(blobRequest, blobLocation)));
            }
        }, "Error While persisting DataFrame Audit. Entity: " + JsonUtils.DEFAULT.toJson(blobRequest));

        try {
            hdfsUtils.createHDFSDirIfNotExists(new Path(blobLocation));
        } catch (IOException e) {
            throw new HDFSUtilsException("Exception received while trying to create the directory at location : " + blobLocation, e);
        }

        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                blobMetaEntityAtomicReference.set(blobMetaDAO.updateBlobStatus(blobMetaEntityAtomicReference.get().getId(), BlobStatus.COMPLETED));
            }
        }, "Error While persisting DataFrame Audit. Entity: " + JsonUtils.DEFAULT.toJson(blobRequest));

        return wrap(blobMetaEntityAtomicReference.get());
    }

    public BlobResponse getBlobsByRequestIdAndType(String requestId, BlobType type) {
        AtomicReference<BlobMetaEntity> blobMetaEntityListAtomicReference = new AtomicReference<>(null);

        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                blobMetaEntityListAtomicReference.set(blobMetaDAO.getBlobsByRequestIdAndType(requestId, type));
            }
        }, "Error while getting blob details for requestId: " + requestId + " and type: " + type);
        return wrap(blobMetaEntityListAtomicReference.get());
    }

    public BlobResponse getCompletedBlobsByRequestIdAndType(String requestId, BlobType type) {
        AtomicReference<BlobMetaEntity> blobMetaEntityListAtomicReference = new AtomicReference<>(null);

        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                blobMetaEntityListAtomicReference.set(blobMetaDAO.getCompletedBlobsByRequestIdAndType(requestId, type));
            }
        }, "Error while getting blob details for requestId: " + requestId + " and type: " + type);
        return wrap(blobMetaEntityListAtomicReference.get());
    }

    public BlobVariableResponse getAllBlobVariables(String blobLocation) throws HDFSUtilsException {
        BlobVariableResponse blobVariableResponse = new BlobVariableResponse();
        blobVariableResponse.setBlobs(hdfsUtils.getAllFileRelativePath(blobLocation));
        return blobVariableResponse;
    }
}
