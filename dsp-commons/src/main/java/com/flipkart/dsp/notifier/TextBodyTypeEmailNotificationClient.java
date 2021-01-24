package com.flipkart.dsp.notifier;

import com.flipkart.dsp.notifier.interfaces.Notification;
import com.flipkart.dsp.notifier.interfaces.NotificationClient;
import com.flipkart.dsp.utils.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

import java.util.Map;

@Slf4j
public class TextBodyTypeEmailNotificationClient implements NotificationClient {


    @Override
    public Boolean sendNotification(Notification notification, Map<String, String> extraArguments) throws EmailException {
        EmailNotification emailNotification = (EmailNotification) notification;

        if (null == emailNotification.getSubject()) {
            throw new EmailException("\"Subject\" of email cannot be empty.");
        }

        Email email = new SimpleEmail();
        email.setFrom(emailNotification.getFrom());
        email.setSubject(emailNotification.getSubject());
        email.setMsg(emailNotification.getBody());
        addRecipients(emailNotification.getTo(), email);
        if(extraArguments != null) {
            if(extraArguments.containsKey(Constants.NOTIFICATION_EMAIL_HOST)) {
                email.setHostName(extraArguments.get(Constants.NOTIFICATION_EMAIL_HOST));
            }
        }
        email.send();
        return true;
    }

    private void addRecipients(String[] recipients, Email email) throws EmailException {
        for(String recipient : recipients) {
            email.addTo(recipient);
        }
    }
}
