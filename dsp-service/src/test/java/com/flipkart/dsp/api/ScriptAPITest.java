//package com.flipkart.dsp.api;
//
//import com.flipkart.dsp.cache.ScriptCache;
//import com.flipkart.dsp.client.GithubClient;
//import com.flipkart.dsp.config.GithubConfig;
//import com.flipkart.dsp.dao.ExecutionEnvironmentDAO;
//import com.flipkart.dsp.dao.ScriptDAO;
//import com.flipkart.dsp.entities.scriptEntity.ScriptMeta;
//import org.junit.After;
//import org.junit.Before;
//
////import static com.flipkart.dsp.api.mock.MockEntities.*;
//import static org.mockito.Mockito.mock;
//
///**
// */
//public class ScriptAPITest extends AbstractAPITest{
//
//    public static final String GIT_FOLDER = "/production/FSN_FORECASTING_V0";
//    protected ScriptAPI scriptAPI;
//    private long id;
//
//    @Before
//    public void setup() {
//        super.setup();
//        GithubClient githubClient = mockGithubUtils();
//        scriptAPI = new ScriptAPI(new ScriptDAO(sessionFactory), new ScriptCache(new GithubConfig()),new ExecutionEnvironmentDAO(sessionFactory), transactionLender, githubClient);
//    }
//
//    private GithubClient mockGithubUtils() {
//        GithubClient githubClient = mock(GithubClient.class);
//        return githubClient;
//    }
//
//    @After
//    public void tearDown() {
//        super.tearDown();
//    }
//
//    private void testRegisterScript() throws Exception {
//        id = scriptAPI.registerScript(GIT_REPO_NAME, GIT_FOLDER, GIT_FILE_PATH_1, GIT_COMMIT_ID, "PYTHON",
//                SCRIPT_INPUT_VARIABLES_1, SCRIPT_OUTPUT_VARIABLES_1, "").getId();
//        assert (id == 1);
//    }
//
//    private void testUpdateScript() throws Exception {
//        scriptAPI.updateScript(1L,  SCRIPT_INPUT_VARIABLES_2, SCRIPT_OUTPUT_VARIABLES_2, null);
//    }
//
//    private void testGetScript() throws Exception {
//        ScriptMeta scriptMeta = scriptAPI.getScriptEntity(id);
//        assert (scriptMeta != null);
//        assert (scriptMeta.getExecEnv().equals("PYTHON"));
//        assert (scriptMeta.getInputVariables().size() == 4);
//    }
//
//    private void testGetScriptByGitDetails() throws Exception {
//        ScriptMeta scriptMeta = scriptAPI.getLatestScriptByGitDetails(GIT_REPO_NAME,GIT_FOLDER, GIT_FILE_PATH_1, GIT_COMMIT_ID);
//        assert (scriptMeta != null);
//        assert (scriptMeta.getExecEnv().equals("PYTHON"));
//        assert (scriptMeta.getInputVariables().size() == 4);
//    }
//
//
////    @Test
////    public void testScriptAPI() throws Exception {
////        testRegisterScript();
////        testUpdateScript();
////        testGetScript();
////        testGetScriptByGitDetails();
////        testGetScriptContent();
////    }
//}
