package com.flipkart.dsp.notifier;

import com.flipkart.dsp.notifier.enums.NotificationType;
import com.flipkart.dsp.notifier.interfaces.Notification;
import lombok.*;

@AllArgsConstructor
@Builder
@NoArgsConstructor
@Data
public class EmailNotification implements Notification {

    private String from;
    private String[] to;
    private String subject;
    private String body;
    private boolean isBodyTypeHtml = false;

    @Override
    public NotificationType getNotificationType() {
        return NotificationType.EMAIL;
    }
}
