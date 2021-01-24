package com.flipkart.dsp.utils;

import com.flipkart.dsp.config.AzkabanConfig;

/**
 */

public class AzkabanLinkCreator {

    public static StringBuilder createAzkabanLink(AzkabanConfig azkabanConfig,
                                                  Long azkabanExecId) {
        String elbEndPoint = azkabanConfig.getElbEndPoint();
        Integer port = azkabanConfig.getPort();
        StringBuilder azkabanLink = new StringBuilder();
        azkabanLink.append(Constants.http);
        azkabanLink.append(elbEndPoint);
        azkabanLink.append(Constants.colon);
        azkabanLink.append(port);
        azkabanLink.append(Constants.slash);
        azkabanLink.append(Constants.executor);
        azkabanLink.append(Constants.questionMark);
        azkabanLink.append(Constants.execid);
        azkabanLink.append(Constants.equal);
        azkabanLink.append(azkabanExecId);
        return azkabanLink;
    }
}
