package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.utils.GeneratorUtils;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ScopeHelper.class, GeneratorUtils.class})
public class ScopeHelperTest {
    @Mock private Signal signal;
    @Mock private DataFrame dataFrame;
    @Mock private DataFrameScope dataFrameScope;
    @Mock private DataFrameConfig dataFrameConfig;
    @Mock private AbstractPredicateClause abstractPredicateClause;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private ScopeHelper scopeHelper;
    private Set<DataFrameScope> dataFrameScopes = new HashSet<>();


    @Before
    public void setUp() {
        PowerMockito.mockStatic(GeneratorUtils.class);
        MockitoAnnotations.initMocks(this);
        this.scopeHelper = spy(new ScopeHelper());

        dataFrameScopes.add(dataFrameScope);

        when(dataFrame.getDataFrameConfig()).thenReturn(dataFrameConfig);
        when(dataFrameConfig.getDataFrameScopeSet()).thenReturn(dataFrameScopes);
        when(dataFrameGenerateRequest.getScopes()).thenReturn(dataFrameScopes);
        when(dataFrameScope.getAbstractPredicateClause()).thenReturn(abstractPredicateClause);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.TIME_YYYYMMDD_RANGE);
    }

    @Test
    public void testGetFinalDataFrameScopeCase1() {
        Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>("0","0"), new HashSet<>());
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause)).thenReturn(triplet);

        Set<DataFrameScope> expected = scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        assertNotNull(expected);
        assertEquals(expected.size(), 2);
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getDataFrameScopeSet();
        verify(dataFrameGenerateRequest).getScopes();
        verify(dataFrameScope, times(4)).getAbstractPredicateClause();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
    }

    @Test
    public void testGetFinalDataFrameScopeCase2() {
        Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>(LocalDate.now().toString(),
                LocalDate.now().toString()), new HashSet<>());
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause)).thenReturn(triplet);

        Set<DataFrameScope> expected = scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        assertNotNull(expected);
        assertEquals(expected.size(), 2);
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getDataFrameScopeSet();
        verify(dataFrameGenerateRequest).getScopes();
        verify(dataFrameScope, times(4)).getAbstractPredicateClause();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
    }

    @Test
    public void testGetFinalDataFrameScopeCase3() {
        Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>(LocalDate.now().toString(),
                LocalDate.now().toString()), new HashSet<>());
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.INCREMENTAL_DATE_RANGE);
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause)).thenReturn(triplet);

        Set<DataFrameScope> expected = scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        assertNotNull(expected);
        assertEquals(expected.size(), 2);
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getDataFrameScopeSet();
        verify(dataFrameGenerateRequest).getScopes();
        verify(dataFrameScope, times(5)).getAbstractPredicateClause();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
    }

    @Test
    public void testGetFinalDataFrameScopeCase4() {
        Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>("1", "4"), new HashSet<>());

        when(dataFrameScope.getSignal()).thenReturn(signal);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.TIME_YYYYWW_RANGE);
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause)).thenReturn(triplet);

        Set<DataFrameScope> expected = scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        assertNotNull(expected);
        assertEquals(expected.size(), 2);
        verify(dataFrameScope, times(3)).getAbstractPredicateClause();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
        verify(dataFrameScope, times(2)).getSignal();
    }

    @Test
    public void testGetFinalDataFrameScopeCase5() {
        Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>("1", "4"), new HashSet<>());

        when(dataFrameScope.getSignal()).thenReturn(signal);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.INCREMENTAL_WEEK_RANGE);
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause)).thenReturn(triplet);

        Set<DataFrameScope> expected = scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        assertNotNull(expected);
        assertEquals(expected.size(), 2);
        verify(dataFrameScope, times(3)).getAbstractPredicateClause();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
        verify(dataFrameScope, times(2)).getSignal();
    }

    @Test
    public void testGetFinalDataFrameScopeCase6() {
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.EQUAL);
        Set<DataFrameScope> expected = scopeHelper.getFinalDataFrameScope(dataFrame, dataFrameGenerateRequest);
        assertNotNull(expected);
        assertEquals(expected.size(), 1);
        verify(dataFrameScope, times(1)).getAbstractPredicateClause();
    }
}
