package com.flipkart.dsp.mesos;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.actors.ExecutionEnvironmentSnapShotActor;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.mesos.framework.DSPMesosFramework;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ImageDetailsMesosExecutionDriver.class, DSPMesosFramework.class})
public class ImageDetailsMesosExecutionDriverTest {

    @Mock private MiscConfig miscConfig;
    @Mock private DSPServiceConfig dspServiceConfig;
    @Mock private DSPMesosFramework dspMesosFramework;
    @Mock private DSPServiceConfig.MesosConfig mesosConfig;
    @Mock private DSPServiceConfig.ImageSnapShotConfig imageSnapShotConfig;
    @Mock private ExecutionEnvironmentSnapShotActor executionEnvironmentSnapshotActor;

    private ImageDetailsMesosExecutionDriver imageDetailsMesosExecutionDriver;
    private List<ExecutionEnvironmentSummary> executionEnvironments = new ArrayList<>();
    private List<ExecutionEnvironmentSnapshot> executionEnvironmentSnapshots = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.imageDetailsMesosExecutionDriver = spy(new ImageDetailsMesosExecutionDriver(dspServiceConfig, executionEnvironmentSnapshotActor));

        String testLanguage = "PYTHON3";
        Map<String, String> librarySet = JsonUtils.DEFAULT.mapper.readValue(fixture("fixtures/library_set.json"),new TypeReference<Map<String, String>>(){});
        ExecutionEnvironmentSnapshot executionEnvironmentSnapshot = new ExecutionEnvironmentSnapshot();
        executionEnvironmentSnapshot.setLibrarySet(JsonUtils.DEFAULT.mapper.writeValueAsString(librarySet));
        executionEnvironmentSnapshot.setVersion(1);
        executionEnvironmentSnapshots.add(executionEnvironmentSnapshot);

        ExecutionEnvironmentSummary executionEnvironmentSummary = new ExecutionEnvironmentSummary();
//        executionEnvironmentSummary.setImageLanguage(ImageLanguageEnum.valueOf(testLanguage));
//        executionEnvironmentSummary.setExecutionEnvironmentEnum("PYTHON3");
//        executionEnvironmentSummary.setExecutionEnvironmentSnapshotList(executionEnvironmentSnapshots);
//        executionEnvironmentSummary.setStartUpScriptPath("/usr/share/ipp-dsp-workflowEntity-executor/__VERSION__/bin/prod/startup-py2.sh");
        executionEnvironments.add(executionEnvironmentSummary);

        Map<String, String> mesosContainerOption = Maps.newHashMap();
        Map<String, String> mesosMountInfo = Maps.newHashMap();
        mesosMountInfo.put("key1", "value1");

        when(imageSnapShotConfig.getCpus()).thenReturn(1d);
        when(imageSnapShotConfig.getRetires()).thenReturn(3);
        when(imageSnapShotConfig.getMemory()).thenReturn(2048d);
        when(mesosConfig.getMountInfo()).thenReturn(mesosMountInfo);
        when(miscConfig.getExecutorJarVersion()).thenReturn("0.0.1");
        when(dspServiceConfig.getMesosConfig()).thenReturn(mesosConfig);
        when(mesosConfig.getContainerOptions()).thenReturn(mesosContainerOption);
        when(mesosConfig.getZookeeperAddress()).thenReturn("mesosZookeeperAddress");
        when(dspServiceConfig.getImageSnapShotConfig()).thenReturn(imageSnapShotConfig);
    }

    @Test
    public void testExecuteCase1() {
        imageDetailsMesosExecutionDriver.execute(new ArrayList<>());
        verify(imageSnapShotConfig, times(0)).getCpus();
    }

    @Test
    public void testExecuteCase2() throws Exception {
        whenNew(DSPMesosFramework.class).withAnyArguments().thenReturn(dspMesosFramework);
        when(dspMesosFramework.run(anyList(), eq(false))).thenReturn(new ArrayList<>());

        imageDetailsMesosExecutionDriver.execute(executionEnvironments);
        verify(imageSnapShotConfig, times(1)).getCpus();
        verify(imageSnapShotConfig, times(1)).getRetires();
        verify(imageSnapShotConfig, times(1)).getMemory();
        verify(mesosConfig, times(1)).getMountInfo();
        verify(dspServiceConfig, times(3)).getMesosConfig();
        verify(dspServiceConfig, times(3)).getImageSnapShotConfig();
        verify(mesosConfig, times(1)).getContainerOptions();
        verify(mesosConfig, times(1)).getZookeeperAddress();
        verify(dspMesosFramework, times(1)).run(anyList(), eq(false));
    }
}
