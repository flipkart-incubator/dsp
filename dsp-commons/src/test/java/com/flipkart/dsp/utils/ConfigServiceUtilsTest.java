package com.flipkart.dsp.utils;

import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.exceptions.ConfigServiceException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * +
 */
public class ConfigServiceUtilsTest {

    private final String configClassName = "mesosConfig";
    private final Map<String, String> mountInfo = new HashMap<>();
    private final Map<String, Object> configMap = new HashMap<>();

    @Before
    public void setUp() {
        mountInfo.put("mount1", "95gb");
        configMap.put("mesosConfig-maxAgentCpu", 10);
    }

    @Test
    public void testGetConfigCase1Success() throws Exception {
        configMap.put("mesosConfig-mountInfo", JsonUtils.DEFAULT.toJson(mountInfo));

        DSPServiceConfig.MesosConfig expected = ConfigServiceUtils.getConfig(configMap, configClassName, DSPServiceConfig.MesosConfig.class);
        assertNotNull(expected);
        assertEquals(expected.getMaxAgentCpu().longValue(), 10L);
        assertEquals(expected.getMountInfo().get("mount1"), "95gb");
        assertNull(expected.getMaxAgentMemory());
    }

    @Test
    public void testConfigCaseFailure() {
        boolean isException = false;
        configMap.put("mesosConfig-mountInfo", mountInfo);

        try {
            ConfigServiceUtils.getConfig(configMap, configClassName, DSPServiceConfig.MesosConfig.class);
        } catch (ConfigServiceException e) {
            isException = true;
            String error = String.format("Error while desrialising config class object %s from Config Service", configClassName);
            assertEquals(e.getMessage(), error);
        }
        assertTrue(isException);
    }
}
