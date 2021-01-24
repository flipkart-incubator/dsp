package com.flipkart.dsp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ObjectPayload implements Payload {
    private Object content;
}
