package com.flipkart.dsp.dto.ConfigurableSG;


import com.flipkart.dsp.db.entities.DataTableEntity;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.SignalEntity;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class SGEntity {
    private Map<DataTableEntity, List<Pair<SignalEntity,Boolean>>> dataTableSignalListMap;
    private DataFrameEntity dataFrame;
}
