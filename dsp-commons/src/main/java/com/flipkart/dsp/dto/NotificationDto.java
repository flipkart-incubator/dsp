package com.flipkart.dsp.dto;

import com.flipkart.dsp.models.RequestStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationDto {
    private Long killTime;
    private long requestId;
    private long workflowId;
    private Long warningTime;
    private long azkabanExecId;
    private Boolean isNotified;
    private long internalRequestId;
    private RequestStatus requestStatus;
}
