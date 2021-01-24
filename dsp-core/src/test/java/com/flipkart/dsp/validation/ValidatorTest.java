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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class ValidatorTest {

    @Mock
    private WorkFlowActor workFlowActor;
    @Mock
    private RequestActor requestActor;
    @Mock
    private HdfsUtils hdfsUtils;
    @Mock
    private HivePathUtils hivePathUtils;
    @Mock
    private DataTableActor dataTableActor;
    @Mock
    private MetaStoreClient metaStoreClient;
    @Mock
    private HiveClient hiveClient;
    @Mock
    private RequestDAO requestDAO;
    @Mock
    private ScriptActor scriptActor;
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

    @Mock
    private GithubClient githubClient;

    private Validator validator;
    private ExternalCredentialsActor externalCredentialsActor;
    private List<ExternalCredentialsEntity> externalCredentialsEntityList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        TransactionLender transactionLender = new TransactionLender(sessionFactory);
        this.externalCredentialsActor = spy(new ExternalCredentialsActor(transactionLender, externalCredentialsDao));
        this.validator = spy(new Validator(workFlowActor, requestActor, hdfsUtils, hivePathUtils, dataTableActor, metaStoreClient, hiveClient, requestDAO, scriptActor,
                githubClient, dataFrameAuditActor, externalCredentialsActor));

        externalCredentialsEntityList.add(externalCredentialsEntity);

        when(transaction.isActive()).thenReturn(true);
        when(session.getTransaction()).thenReturn(transaction);
        when(sessionFactory.getCurrentSession()).thenReturn(session);
        PowerMockito.whenNew(WorkUnit.class).withNoArguments().thenReturn(workUnit);
    }

    @Test
    public void testValidateFtpCredentials() {
        String clientAlias = "client_alias";
        when(externalCredentialsDao.getCredentialsByClientAlias(clientAlias)).thenReturn(externalCredentialsEntityList);

        FTPDataframeOverride ftpDataframeOverride = new FTPDataframeOverride("", clientAlias, null, null);

        Map<String, DataframeOverride> ftpDataframeOverrideMap = new HashMap<>();
        ftpDataframeOverrideMap.put("key", ftpDataframeOverride);

        RequestOverride requestOverride = new RequestOverride(null, null);
        requestOverride.setDataframeOverrideMap(ftpDataframeOverrideMap);

        assertDoesNotThrow(() -> validator.validateFtpCredentials(requestOverride));
    }

    @Test
    public void testValidateFtpCredentialsException() {
        String clientAlias = "client_alias";
        when(externalCredentialsDao.getCredentialsByClientAlias(clientAlias)).thenReturn(new ArrayList<>());

        FTPDataframeOverride ftpDataframeOverride = new FTPDataframeOverride("", clientAlias, null, null);

        Map<String, DataframeOverride> ftpDataframeOverrideMap = new HashMap<>();
        ftpDataframeOverrideMap.put("key", ftpDataframeOverride);

        RequestOverride requestOverride = new RequestOverride(null, null);
        requestOverride.setDataframeOverrideMap(ftpDataframeOverrideMap);

        assertThrows(ValidationException.class, () -> validator.validateFtpCredentials(requestOverride));
    }

}
