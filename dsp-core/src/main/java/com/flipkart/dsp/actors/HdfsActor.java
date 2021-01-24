package com.flipkart.dsp.actors;

import com.flipkart.dsp.dto.NameNodeResponse;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.utils.HdfsUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import static com.flipkart.dsp.utils.Constants.*;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HdfsActor {

    private final HdfsUtils hdfsUtils;

    public NameNodeResponse getActiveNameNode(String clusterName) throws HDFSUtilsException {

        String clusterAddress = HDFS_CLUSTER_PREFIX + clusterName + "/user/hadoop/";
        try {
            InetSocketAddress inetSocketAddress = hdfsUtils.getActiveNameNode(clusterAddress);
            NameNodeResponse nameNodeResponse = new NameNodeResponse();
            //inetSocketAddress.getAddress return address as "prod-hadoop-hadoopcluster2-nn-0002/0.0.0.0"
            nameNodeResponse.setActiveNN(inetSocketAddress.getAddress().toString().split("/")[1]);
            nameNodeResponse.setCluster(clusterName);
            return nameNodeResponse;
        } catch (IOException e) {
            throw new HDFSUtilsException("Exception in getting active namenode for cluster: " + clusterName, e);
        }
    }
}
