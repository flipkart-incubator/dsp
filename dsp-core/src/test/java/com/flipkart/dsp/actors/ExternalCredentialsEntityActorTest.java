package com.flipkart.dsp.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.dao.ExternalCredentialsDao;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExternalCredentialsEntity;
import com.flipkart.dsp.exceptions.DSPCoreException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * +
 */
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExternalCredentialsActor.class, WorkUnit.class})
public class ExternalCredentialsEntityActorTest {

    @Mock private Session session;
    @Mock private WorkUnit workUnit;
    @Mock private Transaction transaction;
    @Mock private SessionFactory sessionFactory;
    @Mock private ObjectMapper objectMapper;
    @Mock private ExternalCredentialsEntity externalCredentialsEntity;
    @Mock private ExternalCredentialsDao externalCredentialsDao;

    private ExternalCredentialsActor externalCredentialsActor;
    private List<ExternalCredentialsEntity> externalCredentialsEntityList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        TransactionLender transactionLender = new TransactionLender(sessionFactory);
        this.externalCredentialsActor = spy(new ExternalCredentialsActor(transactionLender, externalCredentialsDao));

        externalCredentialsEntityList.add(externalCredentialsEntity);

        when(transaction.isActive()).thenReturn(true);
        when(session.getTransaction()).thenReturn(transaction);
        when(sessionFactory.getCurrentSession()).thenReturn(session);
        PowerMockito.whenNew(WorkUnit.class).withNoArguments().thenReturn(workUnit);
    }

    @Test
    public void testGetCredentialsSuccess() throws Exception {
        String clientAlias = "client_alias";
        when(externalCredentialsDao.getCredentialsByClientAlias(clientAlias)).thenReturn(externalCredentialsEntityList);

        com.flipkart.dsp.models.ExternalCredentials actual = externalCredentialsActor.getCredentials("client_alias");
        assertNotNull(actual);
        verify(externalCredentialsDao, times(1)).getCredentialsByClientAlias(clientAlias);
    }

    @Test
    public void testGetCredentialsFailure() throws Exception {
        boolean isException = false;
        String clientAlias = "client_alias";
        when(externalCredentialsDao.getCredentialsByClientAlias(clientAlias)).thenReturn(new ArrayList<>());

        try {
            com.flipkart.dsp.models.ExternalCredentials actual = externalCredentialsActor.getCredentials("client_alias");
        } catch (DSPCoreException e) {
            isException = true;
            assertEquals(e.getMessage(), String.format("No externalCredentials found for clientAlias: %s", clientAlias));
        }
        assertTrue(isException);
        verify(externalCredentialsDao, times(1)).getCredentialsByClientAlias(clientAlias);
    }
}
