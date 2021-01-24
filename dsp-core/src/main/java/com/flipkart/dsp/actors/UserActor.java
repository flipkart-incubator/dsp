package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.UserDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.RequestEntity;
import com.flipkart.dsp.db.entities.UserEntity;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class UserActor {
    private final UserDAO userDAO;
    private final TransactionLender transactionLender;

    public UserEntity getUserByName(String userName) {
        AtomicReference<UserEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                UserEntity userEntity = userDAO.getUserByName(userName);
                if (userEntity == null) {
                    userEntity = new UserEntity();
                    userEntity.setUserId(userName);
                    userEntity = userDAO.persist(userEntity);
                }
                atomicReference.set(userEntity);
            }
        });
        return atomicReference.get();
    }
}
