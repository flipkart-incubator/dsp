package com.flipkart.dsp.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.dao.ExternalCredentialsDao;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExternalCredentialsEntity;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.externalentities.ExternalEntity;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.underscore;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExternalCredentialsActor implements SGActor<ExternalCredentialsEntity, ExternalCredentials> {
    private final TransactionLender transactionLender;
    private final ExternalCredentialsDao externalCredentialsDao;

    @Override
    public ExternalCredentialsEntity unWrap(ExternalCredentials dto) {
        if (Objects.nonNull(dto))
            return ExternalCredentialsEntity.builder().clientAlias(dto.getClientAlias()).details(dto.getDetails()).build();
        return null;
    }

    @Override
    public ExternalCredentials wrap(ExternalCredentialsEntity entity) {
        if (Objects.nonNull(entity))
            return ExternalCredentials.builder().clientAlias(entity.getClientAlias()).details(entity.getDetails()).build();
        return null;
    }

    public ExternalCredentials getCredentials(String clientAlias) throws DSPCoreException {
        final AtomicReference<List<ExternalCredentialsEntity>> listAtomicReference = new AtomicReference<>(null);

        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(externalCredentialsDao.getCredentialsByClientAlias(clientAlias));
            }
        }, "Error While Getting credentials for client_alias: " + clientAlias);

        if (listAtomicReference.get().size() == 0)
            throw new DSPCoreException(String.format("No externalCredentials found for clientAlias: %s", clientAlias));
        return wrap(listAtomicReference.get().get(0));
    }

    public ExternalCredentials createCredentials(String entity, ExternalEntity externalEntity) {
        AtomicReference<ExternalCredentialsEntity> externalCredentialsAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                while (true) {
                    String clientAlias = entity.toLowerCase() + underscore + (100000 + new Random().nextInt(900000));
                    List<ExternalCredentialsEntity> existedCredentials = externalCredentialsDao.getCredentialsByClientAlias(clientAlias);
                    if (existedCredentials.size() == 0) {
                        ExternalCredentials externalCredentials = ExternalCredentials.builder().clientAlias(clientAlias)
                                .details(JsonUtils.DEFAULT.toJson(externalEntity)).build();
                        externalCredentialsAtomicReference.set(externalCredentialsDao.persist(unWrap(externalCredentials)));
                        break;
                    }
                }
            }
        });
        log.debug("Credentials Created for external Entity: {}, clientAlias {}", entity, externalCredentialsAtomicReference.get().getClientAlias());
        return wrap(externalCredentialsAtomicReference.get());
    }
}
