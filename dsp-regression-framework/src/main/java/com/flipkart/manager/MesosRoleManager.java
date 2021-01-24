package com.flipkart.manager;

import com.flipkart.dsp.client.HttpRequestClient;
import com.flipkart.dsp.config.DSPClientConfig;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dto.MesosRoleQuotaUpdatePayload;
import com.flipkart.exception.TestBedException;
import com.flipkart.utils.DSPConstants;
import com.flipkart.utils.HttpURLConnectionUtil;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class MesosRoleManager {
    private final DSPClientConfig dspClientConfig;
    private final HttpRequestClient httpRequestClient;
    private final HttpURLConnectionUtil httpURLConnectionUtil;

    public void updateMesosRoleQuota() throws TestBedException {
        String targetUrl = String.format(DSPConstants.MESOS_ROLE_QUOTA_UPDATE_URL_FORMAT, dspClientConfig.getHost());
        MesosRoleQuotaUpdatePayload mesosRoleQuotaUpdatePayload = MesosRoleQuotaUpdatePayload.builder().force(true)
                .role(Constants.EXPERIMENTATION_MESOS_QUEUE).resourceInformation(getResourceInformation()).build();
        try {
            HttpURLConnection httpURLConnection = httpURLConnectionUtil.getHttpURLConnection(targetUrl, "application/json", "application/json");
            httpRequestClient.postRequest(httpURLConnection, mesosRoleQuotaUpdatePayload);
        } catch (IOException e) {
            String errorMessage = String.format("Error while updating quote for mesos role %s. Error: %s", Constants.EXPERIMENTATION_MESOS_QUEUE, e.getMessage());
            throw new TestBedException(errorMessage);
        }
    }

    private List<MesosRoleQuotaUpdatePayload.ResourceInformation> getResourceInformation() {
        List<MesosRoleQuotaUpdatePayload.ResourceInformation> resourceInformationList = new ArrayList<>();
        resourceInformationList.add(MesosRoleQuotaUpdatePayload.ResourceInformation.builder()
                .name("cpus").type("SCALAR").scalar(getScalar(8L)).build());
        resourceInformationList.add(MesosRoleQuotaUpdatePayload.ResourceInformation.builder()
                .name("mem").type("SCALAR").scalar(getScalar(40000L)).build());
        return resourceInformationList;
    }

    private MesosRoleQuotaUpdatePayload.Scalar getScalar(Long value) {
        return MesosRoleQuotaUpdatePayload.Scalar.builder().value(value).build();
    }

}
