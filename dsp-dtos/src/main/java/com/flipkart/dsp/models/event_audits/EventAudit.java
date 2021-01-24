package com.flipkart.dsp.models.event_audits;

import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventAudit {
    private Long id;
    private long requestId;
    private long workflowId;
    private EventLevel eventLevel;
    private EventType eventType;
    private Events payload;
}
