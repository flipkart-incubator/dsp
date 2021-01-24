//package com.flipkart.dsp.api;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.flipkart.dsp.dao.ExternalCredentialsDao;
//import com.flipkart.dsp.dao.core.TransactionLender;
//import com.flipkart.dsp.dao.core.WorkUnit;
//import com.flipkart.dsp.db.entities.ExternalCredentialsEntity;
//import com.flipkart.dsp.models.externalentities.ExternalEntity;
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
//
//import static org.junit.Assert.assertNotNull;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.when;
//
///**
// * +
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({ExternalCredentialsAPI.class, WorkUnit.class})
//public class ExternalCredentialsAPITest {
//    @Mock private Session session;
//    @Mock private WorkUnit workUnit;
//    @Mock private Transaction transaction;
//    @Mock private ObjectMapper objectMapper;
//    @Mock private ExternalEntity externalEntity;
//    @Mock private SessionFactory sessionFactory;
//    @Mock private ExternalCredentialsEntity externalCredentials;
//    @Mock private ExternalCredentialsDao externalCredentialsDao;
//
//    private ExternalCredentialsAPI externalCredentialsAPI;
//
//    @Before
//    public void setUp() throws Exception {
//        MockitoAnnotations.initMocks(this);
//        TransactionLender transactionLender = new TransactionLender(sessionFactory);
//        this.externalCredentialsAPI = spy(new ExternalCredentialsAPI(objectMapper, transactionLender, externalCredentialsDao));
//
//        when(transaction.isActive()).thenReturn(true);
//        when(session.getTransaction()).thenReturn(transaction);
//        when(sessionFactory.getCurrentSession()).thenReturn(session);
//        when(externalCredentials.getClientAlias()).thenReturn("client_alias");
//        PowerMockito.whenNew(WorkUnit.class).withNoArguments().thenReturn(workUnit);
//    }
//
//    @Test
//    public void testCreateCredentials() {
//        when(externalCredentialsDao.getCredentialsByClientAlias(anyString())).thenReturn(new ArrayList<>());
//        when(externalCredentialsDao.save(anyString(), any())).thenReturn(externalCredentials);
//        com.flipkart.dsp.models.ExternalCredentialsEntity actaul =  externalCredentialsAPI.createCredentials("FTP", externalEntity);
//        assertNotNull(actaul);
//    }
//}
