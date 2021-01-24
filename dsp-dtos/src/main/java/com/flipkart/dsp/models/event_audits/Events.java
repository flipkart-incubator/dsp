package com.flipkart.dsp.models.event_audits;

import com.fasterxml.jackson.annotation.*;
import com.flipkart.dsp.models.event_audits.event_type.FlowStartInfoEvent;
import com.flipkart.dsp.models.event_audits.event_type.FlowTerminationSignal;
import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.OutputIngestionErrorEvent;
import com.flipkart.dsp.models.event_audits.event_type.output_ingestion.ceph_ingestion_node.*;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.*;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.hive_query.*;
import com.flipkart.dsp.models.event_audits.event_type.terminal_node.*;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.*;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({

        @JsonSubTypes.Type(value = FlowTerminationSignal.class, name = "FlowTerminationSignal"),
        @JsonSubTypes.Type(value = FlowStartInfoEvent.class, name = "FlowStartInfoEvent"),

        //Subtype for SG node
        @JsonSubTypes.Type(value = SGStartInfoEvent.class, name = "SGStartInfoEvent"),
        @JsonSubTypes.Type(value = SGOverrideEndDebugEvent.class, name = "SGOverrideEndDebugEvent"),
        @JsonSubTypes.Type(value = SGOverrideStartDebugEvent.class, name = "SGOverrideStartDebugEvent"),

        @JsonSubTypes.Type(value = RunIdOverrideErrorEvent.class, name = "RunIdOverrideErrorEvent"),
        @JsonSubTypes.Type(value = RunIdOverrideReusedDebugEvent.class, name = "RunIdOverrideReusedDebugEvent"),
        @JsonSubTypes.Type(value = RunIdOverrideStartDebugEvent.class, name = "RunIdOverrideStartDebugEvent"),

        @JsonSubTypes.Type(value = PartitionOverrideEndDebugEvent.class, name = "PartitionOverrideEndDebugEvent"),
        @JsonSubTypes.Type(value = PartitionOverrideStartDebugEvent.class, name = "PartitionOverrideStartDebugEvent"),

        @JsonSubTypes.Type(value = CSVOverrideEndDebugEvent.class, name = "CSVOverrideEndDebugEvent"),
        @JsonSubTypes.Type(value = CSVOverrideStartDebugEvent.class, name = "CSVOverrideStartDebugEvent"),
        @JsonSubTypes.Type(value = CSVOverrideErrorEvent.class, name = "CSVOverrideErrorEvent"),

        @JsonSubTypes.Type(value = DefaultDataFrameOverrideForceRunDebugEvent.class, name = "DefaultDataFrameOverrideForceRunDebugEvent"),
        @JsonSubTypes.Type(value = DefaultDataFrameOverrideReusedDebugEvent.class, name = "DefaultDataFrameOverrideReusedDebugEvent"),
        @JsonSubTypes.Type(value = DefaultDataFrameOverrideStartDebugEvent.class, name = "DefaultDataFrameOverrideStartDebugEvent"),

        @JsonSubTypes.Type(value = HiveTableOverrideManagerStartDebugEvent.class, name = "HiveTableOverrideManagerStartDebugEvent"),
        @JsonSubTypes.Type(value = HiveTableOverrideManagerEndDebugEvent.class, name = "HiveTableOverrideManagerEndDebugEvent"),

        @JsonSubTypes.Type(value = HiveQueryOverrideManagerErrorEvent.class, name = "HiveQueryOverrideManagerErrorEvent"),
        @JsonSubTypes.Type(value = HiveQueryOverrideManagerEndDebugEvent.class, name = "HiveQueryOverrideManagerEndDebugEvent"),
        @JsonSubTypes.Type(value = HiveQueryOverrideManagerStartDebugEvent.class, name = "HiveQueryOverrideManagerStartDebugEvent"),
        @JsonSubTypes.Type(value = HiveQueryOverrideManagerReusedDebugEvent.class, name = "HiveQueryOverrideManagerReusedDebugEvent"),

        @JsonSubTypes.Type(value = AllDataFrameCompletionDebugEvent.class, name = "AllDataFrameCompletionDebugEvent"),
        @JsonSubTypes.Type(value = AllDataFrameCompletionInfoEvent.class, name = "AllDataFrameCompletionInfoEvent"),
        @JsonSubTypes.Type(value = DataFrameCompletionInfoEvent.class, name = "DataFrameCompletionInfoEvent"),
        @JsonSubTypes.Type(value = DataFrameGenerationErrorEvent.class, name = "DataFrameGenerationErrorEvent"),

        @JsonSubTypes.Type(value = DataFrameGenerationStartInfoEvent.class, name = "DataFrameGenerationStartInfoEvent"),
        @JsonSubTypes.Type(value = DataFrameQueryExecutionErrorEvent.class, name = "DataFrameQueryExecutionErrorEvent"),
        @JsonSubTypes.Type(value = DataFrameQueryGenerationErrorEvent.class, name = "DataFrameQueryGenerationErrorEvent"),
        @JsonSubTypes.Type(value = SGEndInfoEvent.class, name = "SGEndInfoEvent"),
        @JsonSubTypes.Type(value = SGErrorEvent.class, name = "SGErrorEvent"),

        //Subtype for WF node
        @JsonSubTypes.Type(value = WFStartInfoEvent.class, name = "WFStartInfoEvent"),
        @JsonSubTypes.Type(value = WFErrorEvent.class, name = "WFErrorEvent"),
        @JsonSubTypes.Type(value = WFEndInfoEvent.class, name = "WFEndInfoEvent"),
        @JsonSubTypes.Type(value = WFContainerCompletedInfoEvent.class, name = "WFContainerCompletedInfoEvent"),
        @JsonSubTypes.Type(value = WFContainerFailedEvent.class, name = "WFContainerFailedEvent"),
        @JsonSubTypes.Type(value = WFContainerStartedDebugEvent.class, name = "WFContainerStartedDebugEvent"),
        @JsonSubTypes.Type(value = WFContainerStartedInfoEvent.class, name = "WFContainerStartedInfoEvent"),
        @JsonSubTypes.Type(value = WFSubmittedDebugEvent.class, name = "WFSubmittedDebugEvent"),

        //Subtype for Terminal node
        @JsonSubTypes.Type(value = TerminalNodeInfoEvent.class, name = "TerminalNodeInfoEvent"),
        @JsonSubTypes.Type(value = TerminalNodeErrorEvent.class, name = "TerminalNodeErrorEvent"),
        @JsonSubTypes.Type(value = TerminalNodeDebugEvent.class, name = "TerminalNodeDebugEvent"),

        //CephIngestion
        @JsonSubTypes.Type(value = CephIngestionStartInfoEvent.class, name = "CephIngestionStartInfoEvent"),
        @JsonSubTypes.Type(value = CephIngestionEndInfoEvent.class, name = "CephIngestionEndInfoEvent"),
        @JsonSubTypes.Type(value = CephIngestionErrorEvent.class, name = "CephIngestionErrorEvent"),

        @JsonSubTypes.Type(value = OutputIngestionErrorEvent.class, name = "OutputIngestionErrorEvent"),
})
public abstract class Events {
    public abstract String prettyFormat();
}
