package com.flipkart.dsp.entities.sg.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import static com.flipkart.dsp.TestUtils.fixture;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public class DataFrameKeyTest {

  @Test
  public void testToString() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    SGUseCasePayload payload = objectMapper.readValue(fixture("fixtures/test_dataframe_keys.json"), SGUseCasePayload.class);

    List<DataFrameKey> dataFrameKeys = Iterables.get(payload.getDataframes().keySet(), 0);

    String dataFrameKeyString = dataFrameKeys.stream().map(DataFrameKey::toString).collect(joining(", "));
    String
        expected =
        "DataFrameMultiKey(super=DataFrameKey(columnType=IN, name=null), values=[BGM%23AutoAccessorys]), DataFrameBinaryKey(super=DataFrameKey(columnType=RANGE, name=null), firstValue=2014-01-06, secondValue=2018-02-10)";
    assertEquals(expected, dataFrameKeyString);
  }
}
