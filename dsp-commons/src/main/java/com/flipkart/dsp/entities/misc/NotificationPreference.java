package com.flipkart.dsp.entities.misc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.dsp.models.misc.EmailNotifications;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown=true)
public class NotificationPreference  implements Serializable {
    private Long id;
    private Long workflowId;
    private EmailNotifications emailNotificationPreferences;
}
