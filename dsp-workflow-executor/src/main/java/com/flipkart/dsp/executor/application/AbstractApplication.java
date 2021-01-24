package com.flipkart.dsp.executor.application;

import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public abstract class AbstractApplication {
    public abstract String getName();

    public abstract void execute(String[] args) throws ApplicationException;

    ConfigPayload deserializeConfigPayload(String configPayloadString) throws ApplicationException {
        try {
            return JsonUtils.DEFAULT.mapper.readValue(configPayloadString, ConfigPayload.class);
        } catch (IOException e) {
            log.error("Error while deserialising ConfigPayload. ConfigPayload : {}", configPayloadString, e);
            throw new ApplicationException(getName(), e);
        }
    }
}
