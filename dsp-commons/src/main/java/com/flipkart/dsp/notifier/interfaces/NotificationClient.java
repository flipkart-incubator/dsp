package com.flipkart.dsp.notifier.interfaces;

import java.util.Map;

public interface NotificationClient {

     Boolean sendNotification(Notification notification, Map<String,String> extraArguments) throws Exception;
}
