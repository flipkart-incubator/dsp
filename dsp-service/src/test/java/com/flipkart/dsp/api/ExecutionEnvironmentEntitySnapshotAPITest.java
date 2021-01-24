//package com.flipkart.dsp.api;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.flipkart.dsp.dao.ExecutionEnvironmentDAO;
//import com.flipkart.dsp.dao.ExecutionEnvironmentSnapshotDAO;
//import com.flipkart.dsp.dao.core.TransactionLender;
//import com.flipkart.dsp.dao.core.WorkUnit;
//import com.flipkart.dsp.db.entities.ExecutionEnvironmentEntity;
//import com.flipkart.dsp.db.entities.ExecutionEnvironmentSnapshotEntity;
//import com.flipkart.dsp.entities.misc.ImageDetail;
//import com.flipkart.dsp.models.ImageLanguageEnum;
//import com.flipkart.dsp.utils.JsonUtils;
//import org.hibernate.Session;
//import org.hibernate.SessionFactory;
//import org.hibernate.Transaction;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import static io.dropwizard.testing.FixtureHelpers.fixture;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.*;
//import static org.powermock.api.mockito.PowerMockito.spy;
//
///**
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({ExecutionEnvironmentSnapshotEntity.class, WorkUnit.class})
//public class ExecutionEnvironmentSnapshotAPITest {
//
//    @Mock private Session session;
//    @Mock private WorkUnit workUnit;
//    @Mock private Transaction transaction;
//    private Map<String, String> librarySet;
//    @Mock private SessionFactory sessionFactory;
//    private ExecutionEnvironmentEntity executionEnvironmentEntity;
//    @Mock private ExecutionEnvironmentDAO executionEnvironmentDAO;
//    private ExecutionEnvironmentSnapshotEntity executionEnvironmentSnapshot;
//    private ExecutionEnvironmentSnapshotAPI executionEnvironmentSnapshotAPI;
//    @Mock private ExecutionEnvironmentSnapshotDAO executionEnvironmentSnapshotDAO;
//    private List<ExecutionEnvironmentEntity> executionEnvironments = new ArrayList<>();
//    private List<ExecutionEnvironmentSnapshotEntity> executionEnvironmentSnapshotEntities = new ArrayList<>();
//
//    @Before
//    public void setUp() throws Exception {
//        MockitoAnnotations.initMocks(this);
//        TransactionLender transactionLender = new TransactionLender(sessionFactory);
//        this.executionEnvironmentSnapshotAPI = spy(new ExecutionEnvironmentSnapshotAPI(transactionLender, executionEnvironmentDAO, executionEnvironmentSnapshotDAO));
//
//        PowerMockito.whenNew(WorkUnit.class).withNoArguments().thenReturn(workUnit);
//        when(sessionFactory.getCurrentSession()).thenReturn(session);
//        when(session.getTransaction()).thenReturn(transaction);
//        when(transaction.isActive()).thenReturn(true);
//
//        String testLanguage = "PYTHON3";
//        librarySet = JsonUtils.DEFAULT.mapper.readValue(fixture("fixtures/library_set.json"),new TypeReference<Map<String, String>>(){});
//        executionEnvironmentSnapshot = new ExecutionEnvironmentSnapshotEntity();
//        executionEnvironmentSnapshot.setLibrarySet(JsonUtils.DEFAULT.mapper.writeValueAsString(librarySet));
//        executionEnvironmentSnapshot.setVersion(1);
//        executionEnvironmentSnapshotEntities.add(executionEnvironmentSnapshot);
//
//        executionEnvironmentEntity = new ExecutionEnvironmentEntity();
//        executionEnvironmentEntity.setImageLanguage(ImageLanguageEnum.valueOf(testLanguage));
//        executionEnvironmentEntity.setExecutionEnvironmentEntity("PYTHON3");
//        executionEnvironmentEntity.setExecutionEnvironmentSnapshotEntities(executionEnvironmentSnapshotEntities);
//        executionEnvironments.add(executionEnvironmentEntity);
//    }
//
//    @Test
//    public void testGetImageDetailsSuccessCase1() throws Exception {
//        when(executionEnvironmentDAO.getAllEnvironments()).thenReturn(executionEnvironments);
//
//        List<ImageDetail> actual =  executionEnvironmentSnapshotAPI.getImageDetails();
//        assertEquals(actual.size(), 1);
//        assertEquals(actual.get(0).getImageLanguage(), executionEnvironmentEntity.getImageLanguage());
//        assertEquals(actual.get(0).getImageName(), executionEnvironmentEntity.getExecutionEnvironmentEntity());
//        assertEquals(actual.get(0).getLibrarySet().size(), librarySet.size());
//
//        verify(sessionFactory, times(2)).getCurrentSession();
//        verify(session, times(1)).getTransaction();
//        verify(transaction, times(1)).isActive();
//        verify(executionEnvironmentDAO, times(1)).getAllEnvironments();
//    }
//
//    @Test
//    public void testGetImageDetailsSuccessCase2() {
//        executionEnvironmentEntity.setExecutionEnvironmentSnapshotEntities(new ArrayList<>());
//        when(executionEnvironmentDAO.getAllEnvironments()).thenReturn(executionEnvironments);
//
//        List<ImageDetail> actual =  executionEnvironmentSnapshotAPI.getImageDetails();
//        assertEquals(actual.size(), 1);
//        assertEquals(actual.get(0).getImageLanguage(), executionEnvironmentEntity.getImageLanguage());
//        assertEquals(actual.get(0).getImageName(), executionEnvironmentEntity.getExecutionEnvironmentEntity());
//        assertEquals(actual.get(0).getLibrarySet().size(), 0);
//
//        verify(sessionFactory, times(2)).getCurrentSession();
//        verify(session, times(1)).getTransaction();
//        verify(transaction, times(1)).isActive();
//        verify(executionEnvironmentDAO, times(1)).getAllEnvironments();
//    }
//
//    @Test
//    public void testGetImageDetailsFailure() {
//        boolean isException = false;
//        String librarySet = "InvalidLibrarySetString";
//        executionEnvironmentSnapshot.setLibrarySet(librarySet);
//        when(executionEnvironmentDAO.getAllEnvironments()).thenReturn(executionEnvironments);
//
//        try {
//            executionEnvironmentSnapshotAPI.getImageDetails();
//        } catch (Exception e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Transaction failed with following exception");
//        }
//        assertTrue(isException);
//
//        verify(sessionFactory, times(2)).getCurrentSession();
//        verify(session, times(2)).getTransaction();
//        verify(transaction, times(1)).isActive();
//        verify(executionEnvironmentDAO, times(1)).getAllEnvironments();
//    }
//
//    @Test
//    public void testSaveExecutionEnvironmentSnapshot() throws Exception {
//        com.flipkart.dsp.entities.execution_environment.ExecutionEnvironmentSnapshotEntity executionEnvironmentSnapshot = com.flipkart.dsp.entities.execution_environment.ExecutionEnvironmentSnapshotEntity
//                .builder()
//            .version(1).executionEnvironmentId(1).latestImageDigest("latestImageDigest")
//            .librarySet(JsonUtils.DEFAULT.mapper.writeValueAsString(librarySet)).build();
//
//        when(executionEnvironmentSnapshotDAO.save(any())).thenReturn(1l);
//        Long actual = executionEnvironmentSnapshotAPI.saveExecutionEnvironmentSnapshot(executionEnvironmentSnapshot);
//        assertEquals((long) actual, 1L);
//
//        verify(sessionFactory, times(2)).getCurrentSession();
//        verify(session, times(1)).getTransaction();
//        verify(transaction, times(1)).isActive();
//    }
//}
