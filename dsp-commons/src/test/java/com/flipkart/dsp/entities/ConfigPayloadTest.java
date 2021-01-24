package com.flipkart.dsp.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import org.junit.Test;

import java.io.IOException;

import static com.flipkart.dsp.TestUtils.fixture;
import static org.junit.Assert.assertNotNull;

public class ConfigPayloadTest {

  @Test
  public void test() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    ConfigPayload
        payload =
        objectMapper.readValue(fixture("fixtures/test_config_payload.json"), ConfigPayload.class);
    assertNotNull(payload);
  }
}
