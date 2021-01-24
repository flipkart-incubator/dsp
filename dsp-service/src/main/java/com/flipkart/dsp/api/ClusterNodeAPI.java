package com.flipkart.dsp.api;

import com.flipkart.dsp.dto.ClusterNodesResponse;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ClusterNodeAPI {

    public ClusterNodesResponse getClusterNodes(String clusterName) throws IOException {
        File f = new File("/etc/hosts");
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            StringBuilder hostNames = new StringBuilder();
            br.lines().forEach(line -> {
                if (line.contains(clusterName))
                hostNames.append(line).append("\n");
            });

            return new ClusterNodesResponse(hostNames.toString());
        }
    }
}
