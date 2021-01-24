package com.flipkart.dsp.sg.generator;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.DataFrameScope;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;

import java.util.List;
import java.util.Set;

/**
 */

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
class QueryGenerator {
    private final FullQueryBuilder fullQueryBuilder;
    private final SingleTableQueryBuilder singleTableQueryBuilder;

    @Timed
    Pair<Table, List<String>> generateQuery(Long runId, DataFrame dataframe, DataFrameGenerateRequest request,
                                            Set<DataFrameScope> finalDataFrameScope) throws DataFrameGeneratorException {
        log.info("Generating dataframe Id: {} Name: {} of type: {} in thread {} ", dataframe.getId(), dataframe.getName(), dataframe.getSgType(), Thread.currentThread().getId());

        Pair<Table, List<String>> dataFrameTableQueryPair = null;
        switch (dataframe.getSgType()) {
            case NO_QUERY:
            case SINGLE_TABLE_QUERY:
                dataFrameTableQueryPair = singleTableQueryBuilder.buildQuery(runId, dataframe, request, finalDataFrameScope);
                break;
            case FULL_QUERY:
                dataFrameTableQueryPair = fullQueryBuilder.buildQuery(runId, dataframe, request, finalDataFrameScope);
                break;
        }

        log.info("Done generating following queries for dataFrame: {} output with table {},{}: \n {}", dataframe.getId(),
                dataFrameTableQueryPair.getValue0().getDbName(), dataFrameTableQueryPair.getValue0().getTableName(), dataFrameTableQueryPair.getValue1());
        return dataFrameTableQueryPair;
    }
}
