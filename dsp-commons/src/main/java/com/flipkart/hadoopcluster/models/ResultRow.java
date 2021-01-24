package com.flipkart.hadoopcluster2.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ResultRow implements Serializable {
    /**
     * List representation of a row.
     */
    public List<Object> row = new ArrayList<>();

    public ResultRow(List<Object> row) {
        this.row = row;
    }

    public ResultRow() {
    }
}
