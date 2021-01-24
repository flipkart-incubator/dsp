package com.flipkart.dsp.models.sg;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 */

@Data
@AllArgsConstructor
public class SignalGroup {
    private String id;
    private String description;
    private List<SignalMeta> signalMetas;

    @Data
    @AllArgsConstructor
    public static class SignalMeta {
        private Signal signal;
        private boolean primary;
        private DataTable dataTable;
    }
}
