package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.DataTableDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.DataTableEntity;
import com.flipkart.dsp.models.sg.DataTable;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.flipkart.dsp.utils.Constants.dot;

/**
 */

@Singleton
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataTableActor implements SGActor<DataTableEntity, DataTable> {

    private final DataTableDAO dataTableDAO;
    private final DataSourceActor dataSourceActor;
    private final TransactionLender transactionLender;

    @Override
    public DataTableEntity unWrap(DataTable dto) {
        if (Objects.nonNull(dto))
            return new DataTableEntity(dto.getId(), dto.getDescription(), dataSourceActor.unWrap(dto.getDataSource()));
        return null;
    }

    @Override
    public DataTable wrap(DataTableEntity entity) {
        if (Objects.nonNull(entity))
            return new DataTable(entity.getId(), entity.getDescription(), dataSourceActor.wrap(entity.getDataSource()));
        return null;
    }

    public DataTableEntity persistIfNotExist(DataTableEntity dataTableEntity) {
        DataTableEntity dataTableEntityOld = dataTableDAO.getTable(dataTableEntity.getId());
        return Objects.isNull(dataTableEntityOld) ? dataTableDAO.persist(dataTableEntity) : dataTableEntityOld;
    }

    public DataTable getTable(String tableId) {
        if (StringUtils.contains(tableId, dot)) {
            tableId = StringUtils.split(tableId, dot)[1];
        }
        String finalTableId = tableId;
        AtomicReference<DataTableEntity> tableAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                tableAtomicReference.set(dataTableDAO.getTable(finalTableId));
            }
        }, "Error while getting DataTable for id: " + tableId + dot);
        return wrap(tableAtomicReference.get());
    }

    public void deleteDataTable(List<String> dataTableNames) {
        if (dataTableNames.size() > 0) dataTableDAO.deleteDataTable(dataTableNames);
    }

}
