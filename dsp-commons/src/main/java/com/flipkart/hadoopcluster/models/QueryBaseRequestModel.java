package com.flipkart.hadoopcluster2.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryBaseRequestModel {

    private String query;

    private String sourceName;

    public QueryBaseRequestModel(String query, String sourceName) {
        this.query = query;
        this.sourceName = sourceName;
    }
}
