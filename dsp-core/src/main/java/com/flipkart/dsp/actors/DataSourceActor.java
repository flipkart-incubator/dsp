package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.DataSourceDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.DataSourceEntity;
import com.flipkart.dsp.models.sg.DataSource;
import com.flipkart.dsp.models.sg.DataSourceConfiguration;
import com.flipkart.dsp.models.sg.DataSourceConfigurationType;
import com.google.inject.Inject;
import javolution.io.Struct;
import lombok.RequiredArgsConstructor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.dot;

/**
 */

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataSourceActor implements SGActor<DataSourceEntity, DataSource> {

    private final DataSourceDAO dataSourceDAO;
    private final TransactionLender transactionLender;

    @Override
    public DataSourceEntity unWrap(DataSource dto) {
        if (Objects.nonNull(dto))
            return new DataSourceEntity(dto.getId(), dto.getConfiguration());
        return null;
    }

    @Override
    public DataSource wrap(DataSourceEntity entity) {
        if (Objects.nonNull(entity))
            return new DataSource(entity.getId(), entity.getDataSourceConfiguration());
        return null;
    }

    public DataSourceEntity getDataSourceEntity(String dataSourceId) {
        AtomicReference<DataSourceEntity> dataSourceAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataSourceAtomicReference.set(dataSourceDAO.get(dataSourceId));
            }
        });
        return dataSourceAtomicReference.get();
    }

    public void persistIfNotExist(String dataSourceId) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                DataSourceEntity dataSource = dataSourceDAO.getDataSource(dataSourceId);
                if (dataSource == null) {
                    DataSourceConfiguration dataSourceConfiguration = new DataSourceConfiguration(DataSourceConfigurationType.HIVE, "0.0.0.0", dataSourceId);
                    DataSourceEntity newDataSource = new DataSourceEntity(dataSourceId, dataSourceConfiguration);
                    dataSourceDAO.persist(newDataSource);
                    dataSourceDAO.flushSession();
                    dataSourceDAO.clearSession();
                }
            }
        });
    }

    public DataSource getDataSource(String dataSourceId) {
        AtomicReference<DataSourceEntity> dataSourceAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                DataSourceEntity dataSource = dataSourceDAO.getDataSource(dataSourceId);
                dataSourceAtomicReference.set(dataSource);
            }
        }, "Error while getting DataSource for id: " + dataSourceId + dot);

        return wrap(dataSourceAtomicReference.get());
    }

    public boolean DoesDSExistsInDB(String dataSourceId) {
        DataSource dataSource = getDataSource(dataSourceId);
        return dataSource != null;
    }

}
