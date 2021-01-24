package com.flipkart.manager;

import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.notifier.EmailNotification;
import com.flipkart.dsp.notifier.EmailNotifier;
import com.flipkart.dto.TestExecutionDetails;
import com.flipkart.enums.TestExecutionStatus;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static j2html.TagCreator.*;
import static j2html.TagCreator.td;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ReportManager {
    private final MiscConfig miscConfig;
    private final EmailNotifier emailNotifier;

    public void generateReports(List<TestExecutionDetails> testExeStatusesList,
                                String recipientEmailID) {

        log.info("Started sending email ");
        EmailNotification emailNotification = new EmailNotification();

        emailNotification.setFrom(miscConfig.getDefaultNotificationEmailId());
        emailNotification.setSubject("Regression Test Results as on " + new Date());
        emailNotification.setBodyTypeHtml(true);
        String[] toArray = new String[1];
        toArray[0] = recipientEmailID;
        emailNotification.setTo(toArray);

        StringBuffer body = new StringBuffer("Test Execution Status \n");
        generateEmailBody(testExeStatusesList, body);
        try {
            emailNotification.setBody(body.toString());
            emailNotifier.notify(emailNotification);
        } catch (EmailException e) {
            throw new IllegalStateException("Exception occurred while notifying through email", e);
        }
        log.info("Email send successfully");
    }

    public void generateInternalFailureMail(String recipientEmailID, Exception e) {
        log.info("Started sending email ");
        EmailNotification emailNotification = new EmailNotification();

        emailNotification.setFrom("dsp-oncall@flipkart.com");
        emailNotification.setSubject("Regression Run Failure Alert " + new Date());
        emailNotification.setBodyTypeHtml(true);
        String[] toArray = new String[1];
        toArray[0] = recipientEmailID;
        emailNotification.setTo(toArray);

        StringBuilder body = new StringBuilder("Failure Reason \n");
        body.append(e.getMessage() + "\n" + "Stack Trace Details : " + "\n");

        body.append(Arrays.toString(e.getStackTrace()));
        try {
            emailNotification.setBody(body.toString());
            emailNotifier.notify(emailNotification);
        } catch (EmailException emailException) {
            throw new IllegalStateException("Exception occurred while notifying through email", emailException);
        }
    }

    private void generateEmailBody(List<TestExecutionDetails> testExeStatusesList, StringBuffer body) {
        String output = html(
                head(
                        title("Title"),
                        style("table, th, td {\n" +
                                "  border: 1px solid black;\n" +
                                "  border-collapse: collapse;\n" +
                                "}")
                ),
                body(
                        table(
                                tr(th("TEST SCENARIO"), th("RESULT"), th("DESCRIPTION"), th("REASON")),
                                each(testExeStatusesList, tt ->
                                        tr(td(tt.getTestScenarioName()), iffElse(tt.getTestExecutionStatus() == TestExecutionStatus.PASSED, td("PASSED").withStyle("background-color:green") , td("FAILED").withStyle("background-color:red")),
                                                td(tt.getTestDescription()), td(tt.getFailureReason())))
                        )
                )
        ).render();

        body.append(output);
    }
}
