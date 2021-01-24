package com.flipkart.dsp.service;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import ch.qos.logback.core.util.FileSize;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.util.Size;

import javax.validation.constraints.NotNull;

/**
 * Date: 08/09/15
 * Time: 4:04 PM.
 */

@JsonTypeName("file-size-rolled")
public class SizeBasedRollingFileAppenderFactory extends FileAppenderFactory {
    public static final String DEFAULT_MAX_FILE_SIZE_STR = "500MB" ;

    @NotNull
    @JsonProperty
    String maxFileSize = DEFAULT_MAX_FILE_SIZE_STR;

    public SizeBasedRollingFileAppenderFactory() {
    }

    @JsonProperty
    public Size getMaxFileSize() {
        return Size.parse(maxFileSize);
    }

    @JsonProperty
    public void setMaxFileSize(String maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    /**
     * override FileAppenderFactory's buildAppender
     * @param context
     * @return
     */
    @Override
    protected FileAppender<ILoggingEvent> buildAppender(LoggerContext context) {
        final RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setFile(getCurrentLogFilename());

        final FixedWindowRollingPolicy rollingPolicy = new FixedWindowRollingPolicy();
        rollingPolicy.setContext(context);
        rollingPolicy.setFileNamePattern(getArchivedLogFilenamePattern());
        rollingPolicy.setMinIndex(0);
        rollingPolicy.setMaxIndex(getArchivedFileCount() - 1);
        rollingPolicy.setParent(appender);
        rollingPolicy.start();

        appender.setRollingPolicy(rollingPolicy);

        final SizeBasedTriggeringPolicy<ILoggingEvent>
            triggeringPolicy =
            new SizeBasedTriggeringPolicy<>();
        triggeringPolicy.setMaxFileSize(new FileSize(getMaxFileSize().toBytes()));
        triggeringPolicy.setContext(context);
        appender.setTriggeringPolicy(triggeringPolicy);

        return appender;
    }
}
