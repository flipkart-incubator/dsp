package com.flipkart.dsp.validation;

import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.client.GithubClient;
import com.flipkart.dsp.dao.ExternalCredentialsDao;
import com.flipkart.dsp.dao.RequestDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExternalCredentialsEntity;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.overrides.FTPDataframeOverride;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.utils.HdfsUtils;
import com.flipkart.dsp.utils.HivePathUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class ScriptValidationTest {
    @Mock
    private GithubClient githubClient;
    @Mock
    private DataFrameAuditActor dataFrameAuditActor;
    @Mock
    private ExternalCredentialsDao externalCredentialsDao;
    @Mock
    private SessionFactory sessionFactory;
    @Mock
    private ExternalCredentialsEntity externalCredentialsEntity;
    @Mock
    private Session session;
    @Mock
    private WorkUnit workUnit;
    @Mock
    private Transaction transaction;

    private ScriptValidator scriptValidator;
    private ExternalCredentialsActor externalCredentialsActor;
    private List<ExternalCredentialsEntity> externalCredentialsEntityList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        TransactionLender transactionLender = new TransactionLender(sessionFactory);
        this.externalCredentialsActor = spy(new ExternalCredentialsActor(transactionLender, externalCredentialsDao));
        this.scriptValidator = spy(new ScriptValidator(githubClient, externalCredentialsActor));

        externalCredentialsEntityList.add(externalCredentialsEntity);

        when(transaction.isActive()).thenReturn(true);
        when(session.getTransaction()).thenReturn(transaction);
        when(sessionFactory.getCurrentSession()).thenReturn(session);
        PowerMockito.whenNew(WorkUnit.class).withNoArguments().thenReturn(workUnit);
    }

    @Test
    public void testValidateCephCredentials() {
        String clientAlias = "client_alias";
        when(externalCredentialsDao.getCredentialsByClientAlias(clientAlias)).thenReturn(externalCredentialsEntityList);

        List<OutputLocation> outputLocationList = new ArrayList<>();
        CephOutputLocation cephOutputLocation = new CephOutputLocation();
        cephOutputLocation.setClientAlias(clientAlias);
        outputLocationList.add(cephOutputLocation);


        assertDoesNotThrow(() -> scriptValidator.validateCephAlias(outputLocationList));
    }

    @Test
    public void testValidateFtpCredentialsException() {
        String clientAlias = "client_alias";
        when(externalCredentialsDao.getCredentialsByClientAlias(clientAlias)).thenReturn(new ArrayList<>());

        List<OutputLocation> outputLocationList = new ArrayList<>();
        CephOutputLocation cephOutputLocation = new CephOutputLocation();
        cephOutputLocation.setClientAlias(clientAlias);
        outputLocationList.add(cephOutputLocation);

        assertThrows(ValidationException.class,() -> scriptValidator.validateCephAlias(outputLocationList));
    }
}
