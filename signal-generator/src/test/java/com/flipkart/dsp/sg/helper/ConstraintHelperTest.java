package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Table;
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

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConstraintHelper.class, GeneratorUtils.class})
public class ConstraintHelperTest {

    @Mock private Table table;
    @Mock private Signal signal;
    @Mock private DataTable dataTable;
    @Mock private DataFrame dataFrame;
    @Mock private SignalGroup signalGroup;
    @Mock private DataFrameScope dataFrameScope;
    @Mock private SignalGroup.SignalMeta signalMeta;
    @Mock private SignalDefinition signalDefinition;
    @Mock private SignalDefinitionScope signalDefinitionScope;
    @Mock private AbstractPredicateClause abstractPredicateClause;

    private Long dataFrameId = 1L;
    private String baseEntity = "baseEntity";
    private ConstraintHelper constraintHelper;
    private Set<DataFrameScope> scopeSet = new HashSet<>();
    private Map<Table, Long> tableToRefreshId = new HashMap<>();
    private List<SignalGroup.SignalMeta> signalMetas = new ArrayList<>();
    private Set<SignalDefinitionScope> signalDefinitionScopes = new HashSet<>();
    private Map<String, Pair<String, SignalDefinition>> signalDefinitionMap = new HashMap<>();
    private Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>(0,0), new HashSet<>());

    @Before
    public void setUp() {
        PowerMockito.mockStatic(GeneratorUtils.class);
        MockitoAnnotations.initMocks(this);
        this.constraintHelper =  spy(new ConstraintHelper());

        Pair<String, SignalDefinition> signalDefinitionPair = Pair.with("value", signalDefinition);
        scopeSet.add(dataFrameScope);
        signalMetas.add(signalMeta);
        tableToRefreshId.put(table, 1L);
        signalDefinitionScopes.add(signalDefinitionScope);
        signalDefinitionMap.put(baseEntity, signalDefinitionPair);

        when(signal.getSignalBaseEntity()).thenReturn(baseEntity);
        when(signal.getSignalDataType()).thenReturn(SignalDataType.TEXT);
        when(signalMeta.getSignal()).thenReturn(signal);
        when(signalMeta.getDataTable()).thenReturn(dataTable);
        when(signalGroup.getSignalMetas()).thenReturn(signalMetas);
        when(signalDefinition.getSignalValueType()).thenReturn(SignalValueType.CONDITIONAL);
        when(signalDefinition.getSignalDefinitionScopeSet()).thenReturn(signalDefinitionScopes);
        when(signalDefinitionScope.getPredicateClause()).thenReturn(abstractPredicateClause);
        when(signalDefinitionScope.getPredicateEntity()).thenReturn("predictiveEntity");

        when(table.getDbName()).thenReturn("db_name");
        when(dataTable.getId()).thenReturn("dataTableId");
        when(table.getTableName()).thenReturn("table_name");

        when(dataFrameScope.getSignal()).thenReturn(signal);
        when(dataFrame.getSignalGroup()).thenReturn(signalGroup);
        when(dataFrameScope.getAbstractPredicateClause()).thenReturn(abstractPredicateClause);

        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause)).thenReturn(triplet);
    }

    @Test
    public void testBuildConstraintSetCase1() {
        Set<Constraint> expected = constraintHelper.buildConstraintSet(signalDefinitionMap, scopeSet, tableToRefreshId, dataTable, dataFrame);
        assertNotNull(expected);
        assertEquals(expected.size(), 3);
        verify(dataFrameScope, times(2)).getSignal();
        verify(signal, times(3)).getSignalBaseEntity();
        verify(dataFrameScope, times(2)).getAbstractPredicateClause();
        verify(abstractPredicateClause, times(2)).getPredicateType();
        PowerMockito.verifyStatic(GeneratorUtils.class, times(2));
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
        verify(signal).getSignalDataType();
        verify(dataTable).getId();
        verify(signalDefinition).getSignalValueType();
        verify(signalDefinition).getSignalDefinitionScopeSet();
        verify(signalDefinitionScope,  times(2)).getPredicateClause();
        verify(signalDefinitionScope, times(2)).getPredicateEntity();
        verify(table, times(2)).getTableName();
        verify(table, times(2)).getTableName();

    }

    @Test
    public void testBuildConstraintSetCase2() {
        Set<Constraint> expected = constraintHelper.buildConstraintSet(signalDefinitionMap, scopeSet, tableToRefreshId, null, dataFrame);
        assertNotNull(expected);
        assertEquals(expected.size(), 3);
        assertEquals(expected.size(), 3);
        verify(dataFrameScope, times(3)).getSignal();
        verify(signal, times(3)).getSignalBaseEntity();
        verify(dataFrameScope, times(2)).getAbstractPredicateClause();
        verify(abstractPredicateClause, times(2)).getPredicateType();
        PowerMockito.verifyStatic(GeneratorUtils.class, times(2));
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
        verify(signal).getSignalDataType();
        verify(dataTable).getId();
        verify(signalDefinition).getSignalValueType();
        verify(signalDefinition).getSignalDefinitionScopeSet();
        verify(signalDefinitionScope,  times(2)).getPredicateClause();
        verify(signalDefinitionScope, times(2)).getPredicateEntity();
        verify(table, times(2)).getTableName();
        verify(table, times(2)).getTableName();
        verify(dataFrame).getSignalGroup();
        verify(signalGroup).getSignalMetas();
        verify(signalMeta).getSignal();
        verify(signalMeta).getDataTable();
    }

    @Test
    public void testBuildConstraintSetFailure() {
        when(signalMeta.getSignal()).thenReturn(Signal.builder().build());
        when(dataFrame.getId()).thenReturn(dataFrameId);

        boolean isException = false;
        try {
            constraintHelper.buildConstraintSet(signalDefinitionMap, scopeSet, tableToRefreshId, null, dataFrame);
        } catch (IllegalStateException e) {
            isException = true;
            assertEquals(e.getMessage(), "Data table not found for given parameters. Dataframe ID : " + dataFrameId);
        }

        assertTrue(isException);
        verify(dataFrameScope, times(2)).getSignal();
        verify(signal, times(2)).getSignalBaseEntity();
        verify(dataFrameScope, times(2)).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToTriplet(abstractPredicateClause);
        verify(signal).getSignalDataType();
        verify(dataFrame).getSignalGroup();
        verify(signalGroup).getSignalMetas();
        verify(signalMeta).getSignal();
        verify(dataFrame).getId();
    }
}
