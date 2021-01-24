package com.flipkart.dsp.notifier;

import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.notifier.interfaces.Notification;
import com.flipkart.dsp.notifier.interfaces.Notifier;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EmailNotifier implements Notifier {

    private final TextBodyTypeEmailNotificationClient textBodyTypeEmailNotificationClient;
    private final HtmlBodyTypeEmailNotificationClient htmlBodyTypeEmailNotificationClient;

    @Inject
    public EmailNotifier(TextBodyTypeEmailNotificationClient textBodyTypeEmailNotificationClient, HtmlBodyTypeEmailNotificationClient htmlBodyTypeEmailNotificationClient) {
        this.textBodyTypeEmailNotificationClient = textBodyTypeEmailNotificationClient;
        this.htmlBodyTypeEmailNotificationClient = htmlBodyTypeEmailNotificationClient;
    }

    @Override
    public boolean notify(Notification notification) throws EmailException {
        EmailNotification emailNotification = (EmailNotification) notification;
        Map<String, String> arguments = new HashMap<>();
        arguments.put(Constants.NOTIFICATION_EMAIL_HOST, Constants.LOCALHOST);
        if(((EmailNotification) notification).isBodyTypeHtml()) {
             return htmlBodyTypeEmailNotificationClient.sendNotification(emailNotification, arguments);
        } else {
            return textBodyTypeEmailNotificationClient.sendNotification(emailNotification, arguments);
        }
    }
}
