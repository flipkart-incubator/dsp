package com.flipkart.dsp.notifier;

import com.flipkart.dsp.notifier.interfaces.Notification;
import com.flipkart.dsp.notifier.interfaces.NotificationClient;
import com.flipkart.dsp.utils.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import java.util.Map;

@Slf4j
public class HtmlBodyTypeEmailNotificationClient implements NotificationClient {

    @Override
    public Boolean sendNotification(Notification notification, Map<String, String> extraArguments) throws EmailException {
        EmailNotification emailNotification = (EmailNotification) notification;

        if (null == emailNotification.getSubject()) {
            throw new EmailException("\"Subject\" of email cannot be empty.");
        }
        HtmlEmail htmlEmail = new HtmlEmail();
        htmlEmail.setFrom(emailNotification.getFrom());
        htmlEmail.setSubject(emailNotification.getSubject());
        htmlEmail.setHtmlMsg(emailNotification.getBody());
        addRecipients(emailNotification.getTo(), htmlEmail);
        if(extraArguments != null) {
            if(extraArguments.containsKey(Constants.NOTIFICATION_EMAIL_HOST)) {
                htmlEmail.setHostName(extraArguments.get(Constants.NOTIFICATION_EMAIL_HOST));
            }
        }
        htmlEmail.send();
        return true;
    }

    private void addRecipients(String[] recipients, Email email) throws EmailException {
        for(String recipient : recipients) {
            email.addTo(recipient);
        }
    }
}
