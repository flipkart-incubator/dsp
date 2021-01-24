package com.flipkart.dsp.client.misc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.client.DSPServiceClient;
import com.flipkart.dsp.client.misc.CreateExecutionEnvironmentSnapshotRequest;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.utils.JsonUtils;
import com.ning.http.client.RequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
public class CreateExecutionEnvironmentSnapshotRequestTest {

    @Mock
    DSPServiceClient serviceClient;
    private CreateExecutionEnvironmentSnapshotRequest createExecutionEnvironmentSnapshotRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);;
        Map<String, String> librarySet = JsonUtils.DEFAULT.mapper.readValue(fixture("fixtures/library_set.json"),new TypeReference<Map<String, String>>(){});
        ExecutionEnvironmentSnapshot executionEnvironmentSnapshot = new ExecutionEnvironmentSnapshot();
        executionEnvironmentSnapshot.setLibrarySet(JsonUtils.DEFAULT.mapper.writeValueAsString(librarySet));
        executionEnvironmentSnapshot.setVersion(1);
        createExecutionEnvironmentSnapshotRequest = spy(new CreateExecutionEnvironmentSnapshotRequest(serviceClient, executionEnvironmentSnapshot));
    }

    @Test
    public void testGetMethod() {
        assertEquals(createExecutionEnvironmentSnapshotRequest.getMethod(), "POST");
    }

    @Test
    public void testGetPath() {
        assertEquals(createExecutionEnvironmentSnapshotRequest.getPath(),"/v1/execution-environment-snapshots/create");
    }

    @Test
    public void testGetReturnType() {
        assertEquals(createExecutionEnvironmentSnapshotRequest.getReturnType(), JsonUtils.DEFAULT.mapper.getTypeFactory().constructType(Void.class));
    }

    @Test
    public void testBuildRequest() {
        RequestBuilder requestBuilder = new RequestBuilder();
        RequestBuilder actual = createExecutionEnvironmentSnapshotRequest.buildRequest(requestBuilder);
        assertNotNull(actual);
    }
}
