package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.utils.GeneratorUtils;
import com.flipkart.dsp.utils.YearWeek;
import org.apache.commons.lang.math.NumberUtils;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

public class ScopeHelper {
    public Set<DataFrameScope> getFinalDataFrameScope(DataFrame dataFrame, DataFrameGenerateRequest dataframeGenerateRequest) {
        Set<DataFrameScope> dataFrameScopeOriginal = dataFrame.getDataFrameConfig().getDataFrameScopeSet();
        Set<DataFrameScope> dataFrameScopeFromRequest = dataframeGenerateRequest.getScopes();
        Set<DataFrameScope> result = new HashSet<>();
        Set<DataFrameScope> absoluteOriginal = getAbsoluteScope(dataFrameScopeOriginal);

        if (!isEmpty(dataFrameScopeFromRequest))
            result = dataFrameScopeFromRequest;

        Set<Signal> requestSignals = result.stream().map(DataFrameScope::getSignal).collect(toSet());

        Set<DataFrameScope> nonConflictingOriginalSignals = absoluteOriginal.stream().filter(o -> !requestSignals.contains(o))
                .collect(toSet());

        result.addAll(nonConflictingOriginalSignals);

        return result;
    }

    private Set<DataFrameScope> getAbsoluteScope(Set<DataFrameScope> dataFrameScope) {
        Set<DataFrameScope> result = new HashSet<>();
        if (dataFrameScope == null) return result;
        for (DataFrameScope scope : dataFrameScope) {
            PredicateType predicateType = scope.getAbstractPredicateClause().getPredicateType();
            if (predicateType == PredicateType.TIME_YYYYMMDD_RANGE || predicateType == PredicateType.INCREMENTAL_DATE_RANGE) {
                Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = GeneratorUtils.convertAbstractPredicateClauseToTriplet(scope.getAbstractPredicateClause());
                LocalDateTime currentTime = LocalDateTime.now();
                LocalDate currentDate = currentTime.toLocalDate();
                String start = (String) triplet.getValue1().getValue0();
                String end = (String) triplet.getValue1().getValue1();
                LocalDate startDate;
                LocalDate endDate;
                AbstractPredicateClause finalClause = null;
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                if (NumberUtils.isNumber(end)) {
                    endDate = currentDate.plusDays(Long.parseLong(end));//end=70
                } else {
                    endDate = LocalDate.parse(end);//end=2017-01-09
                }
                if (scope.getAbstractPredicateClause().getPredicateType() == PredicateType.TIME_YYYYMMDD_RANGE) {
                    if (NumberUtils.isNumber(start)) {
                        startDate = currentDate.plusDays(Long.parseLong(start));//start=70
                    } else {
                        startDate = LocalDate.parse(start);//start=2017-01-09
                    }
                    finalClause = new BiValuePredicateClause(scope.getAbstractPredicateClause().getPredicateType(), startDate.format(formatter), endDate.format(formatter));

                } else if (scope.getAbstractPredicateClause().getPredicateType() == PredicateType.INCREMENTAL_DATE_RANGE) {
                    finalClause = new BiValuePredicateClause(scope.getAbstractPredicateClause().getPredicateType(), start, endDate.format(formatter));

                }
                DataFrameScope finalScope = new DataFrameScope(scope.getSignal(), finalClause);
                result.add(finalScope);
            } else if (predicateType == PredicateType.TIME_YYYYWW_RANGE || predicateType == PredicateType.INCREMENTAL_WEEK_RANGE) {
                Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = GeneratorUtils.convertAbstractPredicateClauseToTriplet(scope.getAbstractPredicateClause());
                String startWeek = (String) triplet.getValue1().getValue0();
                String endWeek = (String) triplet.getValue1().getValue1();
                String fromYearWeek;
                if (predicateType == PredicateType.TIME_YYYYWW_RANGE) {
                    Integer startWeekRelative = Integer.parseInt(startWeek);
                    fromYearWeek = YearWeek.getWeekyear(startWeekRelative).toString();
                } else {
                    fromYearWeek = startWeek;
                }
                Integer endWeekRelative = Integer.parseInt(endWeek);
                String toYearWeek = YearWeek.getWeekyear(endWeekRelative).toString();

                AbstractPredicateClause finalClause = new BiValuePredicateClause(scope.getAbstractPredicateClause().getPredicateType(), fromYearWeek, toYearWeek);
                DataFrameScope finalScope = new DataFrameScope(scope.getSignal(), finalClause);
                result.add(finalScope);
            } else {
                result.add(scope);
            }
        }
        return result;
    }
}
